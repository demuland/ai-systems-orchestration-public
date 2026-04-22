from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import uuid, json
from database import init_db, get_db
from models import (
    MigrationCreate, MigrationResponse, MappingApproval,
    ExecutionStart, MigrationStatus
)
from core.mapper import MappingEngine
from core.field_provisioner import FieldProvisioner
from core.executor import MigrationExecutor
from core.schema_discovery import SchemaDiscovery
from connectors.hubspot import HubSpotConnector
from connectors.pipedrive import PipedriveConnector
from connectors.zoho import ZohoConnector
from connectors.monday import MondayConnector
from connectors.activecampaign import ActiveCampaignConnector
from connectors.freshsales import FreshsalesConnector
from connectors.copper import CopperConnector

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield

app = FastAPI(title="CRM Migration Workbench", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_connector(platform: str, credentials: dict):
    if platform == "hubspot":
        return HubSpotConnector(credentials)
    elif platform == "pipedrive":
        return PipedriveConnector(credentials)
    elif platform == "zoho":
        return ZohoConnector(credentials)
    elif platform == "monday":
        return MondayConnector(credentials)
    elif platform == "activecampaign":
        return ActiveCampaignConnector(credentials)
    elif platform == "freshsales":
        return FreshsalesConnector(credentials)
    elif platform == "copper":
        return CopperConnector(credentials)
    raise HTTPException(400, f"Unsupported platform: {platform}")


@app.post("/api/migrations", response_model=MigrationResponse)
async def create_migration(data: MigrationCreate):
    db = get_db()
    migration_id = str(uuid.uuid4())
    db.execute(
        """INSERT INTO migrations (id, client_name, source_platform, dest_platform,
           source_credentials, dest_credentials, status)
           VALUES (?, ?, ?, ?, ?, ?, 'pending')""",
        (migration_id, data.client_name, data.source_platform, data.dest_platform,
         json.dumps(data.source_credentials), json.dumps(data.dest_credentials))
    )
    db.commit()
    return {"id": migration_id, "status": "pending", **data.dict()}


@app.get("/api/migrations")
async def list_migrations():
    db = get_db()
    rows = db.execute(
        "SELECT id, client_name, source_platform, dest_platform, status, total_records, migrated_records, created_at FROM migrations ORDER BY created_at DESC"
    ).fetchall()
    return [dict(r) for r in rows]


@app.get("/api/migrations/{migration_id}")
async def get_migration(migration_id: str):
    db = get_db()
    row = db.execute("SELECT * FROM migrations WHERE id = ?", (migration_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Migration not found")
    return dict(row)


@app.post("/api/migrations/{migration_id}/discover")
async def discover_schemas(migration_id: str):
    db = get_db()
    row = db.execute("SELECT * FROM migrations WHERE id = ?", (migration_id,)).fetchone()
    if not row:
        raise HTTPException(404)
    m = dict(row)
    src_creds = json.loads(m["source_credentials"])
    dst_creds = json.loads(m["dest_credentials"])
    src_conn = get_connector(m["source_platform"], src_creds)
    dst_conn = get_connector(m["dest_platform"], dst_creds)
    discovery = SchemaDiscovery(src_conn, dst_conn)
    schemas = await discovery.discover_all()

    db.execute("UPDATE migrations SET status = 'discovered' WHERE id = ?", (migration_id,))
    db.commit()
    return schemas


@app.post("/api/migrations/{migration_id}/map")
async def propose_mappings(migration_id: str):
    db = get_db()
    row = db.execute("SELECT * FROM migrations WHERE id = ?", (migration_id,)).fetchone()
    if not row:
        raise HTTPException(404)
    m = dict(row)
    src_creds = json.loads(m["source_credentials"])
    dst_creds = json.loads(m["dest_credentials"])
    src_conn = get_connector(m["source_platform"], src_creds)
    dst_conn = get_connector(m["dest_platform"], dst_creds)
    discovery = SchemaDiscovery(src_conn, dst_conn)
    schemas = await discovery.discover_all()
    engine = MappingEngine()
    all_mappings = []
    for obj_type, schema_pair in schemas.items():
        mappings = await engine.propose_mappings(
            m["source_platform"], m["dest_platform"],
            obj_type, schema_pair["source"], schema_pair["dest"]
        )
        for mapping in mappings:
            mapping_id = str(uuid.uuid4())
            db.execute(
                """INSERT OR REPLACE INTO field_mappings
                   (id, migration_id, object_type, dest_object_type, source_field, source_type,
                    dest_field, dest_type, confidence, llm_reasoning, operator_action)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')""",
                (mapping_id, migration_id, obj_type, schema_pair.get("dest_object_type"),
                 mapping["source_field"], mapping.get("source_type", "string"),
                 mapping.get("dest_field"), mapping.get("dest_type", "string"),
                 mapping.get("confidence", 0.0), mapping.get("reasoning", ""))
            )
            mapping["id"] = mapping_id
            mapping["object_type"] = obj_type
            mapping["dest_object_type"] = schema_pair.get("dest_object_type")
            all_mappings.append(mapping)
    db.execute("UPDATE migrations SET status = 'mapping' WHERE id = ?", (migration_id,))
    db.commit()
    return {"mappings": all_mappings}


@app.get("/api/migrations/{migration_id}/mappings")
async def get_mappings(migration_id: str):
    db = get_db()
    rows = db.execute(
        "SELECT * FROM field_mappings WHERE migration_id = ? ORDER BY object_type, confidence DESC",
        (migration_id,)
    ).fetchall()
    return [dict(r) for r in rows]


@app.put("/api/migrations/{migration_id}/mappings/{mapping_id}")
async def update_mapping(migration_id: str, mapping_id: str, approval: MappingApproval):
    db = get_db()
    db.execute(
        """UPDATE field_mappings SET operator_action = ?, operator_override = ?
           WHERE id = ? AND migration_id = ?""",
        (approval.action, approval.override_dest_field, mapping_id, migration_id)
    )
    db.commit()
    return {"ok": True}


@app.post("/api/migrations/{migration_id}/approve-all")
async def approve_all_high_confidence(migration_id: str):
    db = get_db()
    db.execute(
        """UPDATE field_mappings SET operator_action = 'accepted'
           WHERE migration_id = ? AND confidence >= 0.85 AND operator_action = 'pending'""",
        (migration_id,)
    )
    db.commit()
    count = db.execute(
        "SELECT COUNT(*) as c FROM field_mappings WHERE migration_id = ? AND operator_action = 'accepted'",
        (migration_id,)
    ).fetchone()["c"]
    return {"auto_accepted": count}


@app.post("/api/migrations/{migration_id}/ready")
async def mark_ready(migration_id: str):
    db = get_db()
    pending = db.execute(
        "SELECT COUNT(*) as c FROM field_mappings WHERE migration_id = ? AND operator_action = 'pending'",
        (migration_id,)
    ).fetchone()["c"]
    if pending > 0:
        raise HTTPException(400, f"{pending} mappings still need review")
    db.execute("UPDATE migrations SET status = 'ready' WHERE id = ?", (migration_id,))
    db.commit()
    return {"ok": True}



@app.post("/api/migrations/{migration_id}/cancel")
async def cancel_migration(migration_id: str):
    """Request cancellation of a running migration. Executor checks this flag between batches."""
    db = get_db()
    row = db.execute("SELECT status FROM migrations WHERE id = ?", (migration_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Migration not found")
    if row["status"] not in ("running",):
        raise HTTPException(400, f"Cannot cancel a migration in '{row['status']}' state")
    db.execute(
        "UPDATE migrations SET cancel_requested = true WHERE id = ?",
        (migration_id,)
    )
    db.commit()
    return {"ok": True, "message": "Cancellation requested — migration will stop between batches"}


@app.put("/api/migrations/{migration_id}/notes")
async def update_notes(migration_id: str, data: dict):
    """Save internal operator notes on a migration."""
    db = get_db()
    note = data.get("notes", "")
    db.execute(
        "UPDATE migrations SET internal_notes = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (note, migration_id)
    )
    db.commit()
    return {"ok": True}


@app.get("/api/migrations/{migration_id}/preflight")
async def preflight_check(migration_id: str):
    """
    Run preflight checks before execution:
    - mapping summary (accepted, skipped, unmapped)
    - connection test for both platforms
    - list of fields that need manual creation
    Returns structured go/no-go summary.
    """
    db = get_db()
    row = db.execute("SELECT * FROM migrations WHERE id = ?", (migration_id,)).fetchone()
    if not row:
        raise HTTPException(404)
    m = dict(row)
    src_creds = json.loads(m["source_credentials"])
    dst_creds = json.loads(m["dest_credentials"])

    # Connection tests
    src_conn = get_connector(m["source_platform"], src_creds)
    dst_conn = get_connector(m["dest_platform"], dst_creds)
    src_ok = await src_conn.test_connection()
    dst_ok = await dst_conn.test_connection()

    # Mapping summary
    mappings = db.execute(
        """SELECT operator_action,
                  COALESCE(operator_override, dest_field) AS effective_dest_field,
                  object_type
           FROM field_mappings
           WHERE migration_id = ?""",
        (migration_id,)
    ).fetchall()
    total = len(mappings)
    accepted = sum(1 for r in mappings if r["operator_action"] in ("accepted", "corrected"))
    skipped = sum(1 for r in mappings if r["operator_action"] == "skipped")
    pending = sum(1 for r in mappings if r["operator_action"] == "pending")
    unmapped = sum(1 for r in mappings if r["operator_action"] in ("accepted", "corrected") and not r["effective_dest_field"])

    # Fields that would need provisioning
    provisioner = FieldProvisioner(m["dest_platform"], dst_creds, db)
    try:
        provision_preview = await provisioner.provision(migration_id, preview_only=True)
        fields_to_create = len(provision_preview.get("created", []))
        fields_manual = len(provision_preview.get("manual", []))
        fields_failed = len(provision_preview.get("failed", []))
    except Exception:
        fields_to_create = fields_manual = fields_failed = 0
        provision_preview = {}

    go = src_ok and dst_ok and pending == 0
    issues = []
    if not src_ok:
        issues.append(f"Cannot connect to source ({m['source_platform']})")
    if not dst_ok:
        issues.append(f"Cannot connect to destination ({m['dest_platform']})")
    if pending > 0:
        issues.append(f"{pending} mappings still need review")

    return {
        "go": go,
        "issues": issues,
        "source_platform": m["source_platform"],
        "dest_platform": m["dest_platform"],
        "source_connected": src_ok,
        "dest_connected": dst_ok,
        "mappings": {
            "total": total,
            "accepted": accepted,
            "skipped": skipped,
            "pending": pending,
            "unmapped_fields": unmapped,
        },
        "fields": {
            "will_create": fields_to_create,
            "require_manual": fields_manual,
            "failed": fields_failed,
        },
    }


@app.get("/api/migrations/{migration_id}/errors/csv")
async def download_error_csv(migration_id: str):
    """Export all unresolved errors as a downloadable CSV for client handoff."""
    import csv, io
    from fastapi.responses import StreamingResponse

    db = get_db()
    row = db.execute("SELECT client_name FROM migrations WHERE id = ?", (migration_id,)).fetchone()
    if not row:
        raise HTTPException(404)

    errors = db.execute(
        """SELECT object_type, source_id, error_type, error_message, retry_count, resolved, resolved_note, created_at
           FROM migration_errors WHERE migration_id = ? ORDER BY object_type, created_at""",
        (migration_id,)
    ).fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Object Type", "Source ID", "Error Type", "Error Message", "Retries", "Resolved", "Resolution Note", "Timestamp"])
    for e in errors:
        writer.writerow([
            e["object_type"], e["source_id"], e["error_type"],
            e["error_message"], e["retry_count"],
            "Yes" if e["resolved"] else "No",
            e["resolved_note"] or "",
            e["created_at"],
        ])

    output.seek(0)
    filename = f"errors_{row['client_name'].replace(' ', '_')}_{migration_id[:8]}.csv"
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )



@app.post("/api/migrations/{migration_id}/provision-fields")
async def provision_fields(migration_id: str):
    """Preview and create missing custom fields in the destination before execution.
    Safe to call multiple times — skips fields that already exist."""
    db = get_db()
    row = db.execute("SELECT * FROM migrations WHERE id = ?", (migration_id,)).fetchone()
    if not row:
        raise HTTPException(404)
    m = dict(row)
    dst_creds = json.loads(m["dest_credentials"])
    provisioner = FieldProvisioner(m["dest_platform"], dst_creds, db)
    result = await provisioner.provision(migration_id)
    return result


@app.post("/api/migrations/{migration_id}/execute")
async def execute_migration(migration_id: str):
    db = get_db()
    row = db.execute("SELECT * FROM migrations WHERE id = ?", (migration_id,)).fetchone()
    if not row:
        raise HTTPException(404)
    m = dict(row)
    if m["status"] not in ("ready",):
        raise HTTPException(400, "Migration must be in ready state before executing")
    src_creds = json.loads(m["source_credentials"])
    dst_creds = json.loads(m["dest_credentials"])
    src_conn = get_connector(m["source_platform"], src_creds)
    dst_conn = get_connector(m["dest_platform"], dst_creds)

    # Provision any missing custom fields in the destination before running
    # This creates fields that had no match during mapping so no data is lost
    provisioner = FieldProvisioner(m["dest_platform"], dst_creds, db)
    provision_result = await provisioner.provision(migration_id)

    # If any fields need manual creation (Freshsales), warn but don't block
    if provision_result.get("manual"):
        manual_fields = [f["field"] for f in provision_result["manual"]]
        print(f"[Provisioner] {len(manual_fields)} fields require manual creation in Freshsales: {manual_fields}")

    if provision_result.get("failed"):
        failed_fields = [f["field"] for f in provision_result["failed"]]
        print(f"[Provisioner] Warning: {len(failed_fields)} fields failed to create: {failed_fields}")

    created_count = len(provision_result.get("created", []))
    if created_count:
        print(f"[Provisioner] Created {created_count} custom fields in {m['dest_platform']}")

    mappings = db.execute(
        "SELECT * FROM field_mappings WHERE migration_id = ? AND operator_action != 'skipped'",
        (migration_id,)
    ).fetchall()
    approved_mappings = [dict(r) for r in mappings]

    # Freeze a snapshot of the mappings used for this run
    snapshot = [{
        "object_type": r["object_type"],
        "source_field": r["source_field"],
        "dest_field": r["dest_field"],
        "operator_action": r["operator_action"],
        "confidence": r["confidence"],
    } for r in mappings]
    db.execute(
        "UPDATE migrations SET mapping_snapshot = ?, cancel_requested = false WHERE id = ?",
        (json.dumps(snapshot), migration_id)
    )
    db.commit()

    executor = MigrationExecutor(src_conn, dst_conn, approved_mappings, migration_id, db,
                                  source_platform=m["source_platform"], dest_platform=m["dest_platform"])
    db.execute("UPDATE migrations SET status = 'running' WHERE id = ?", (migration_id,))
    db.commit()
    import asyncio
    asyncio.create_task(executor.run())
    return {
        "ok": True,
        "message": "Migration started",
        "fields_created": created_count,
        "fields_manual": len(provision_result.get("manual", [])),
        "fields_failed": len(provision_result.get("failed", [])),
        "provision_detail": provision_result
    }


@app.get("/api/migrations/{migration_id}/progress")
async def get_progress(migration_id: str):
    db = get_db()
    row = db.execute(
        "SELECT status, total_records, migrated_records, failed_records, reused_records, cancel_requested FROM migrations WHERE id = ?",
        (migration_id,)
    ).fetchone()
    if not row:
        raise HTTPException(404)
    errors = db.execute(
        "SELECT object_type, error_type, error_message, created_at FROM migration_errors WHERE migration_id = ? ORDER BY created_at DESC LIMIT 20",
        (migration_id,)
    ).fetchall()
    return {**dict(row), "recent_errors": [dict(e) for e in errors]}



@app.get("/api/migrations/{migration_id}/errors")
async def get_errors(migration_id: str):
    """Full error report with plain English summaries grouped by type."""
    db = get_db()
    row = db.execute("SELECT * FROM migrations WHERE id = ?", (migration_id,)).fetchone()
    if not row:
        raise HTTPException(404)
    m = dict(row)

    errors = db.execute(
        """SELECT id, object_type, source_id, error_type, error_message,
                  raw_record, resolved, resolved_note, retry_count, created_at
           FROM migration_errors
           WHERE migration_id = ?
           ORDER BY object_type, created_at""",
        (migration_id,)
    ).fetchall()

    errors_list = [dict(e) for e in errors]

    # Group by error type with plain English explanation
    ERROR_EXPLANATIONS = {
        "api": "The destination platform rejected the record. Usually a required field is missing or a value is in the wrong format.",
        "mapping": "A field value could not be transformed correctly before writing to the destination.",
        "auth": "Authentication failed mid-migration. The API key may have expired.",
        "rate_limit": "Too many requests sent too quickly. The record will retry automatically.",
        "not_found": "A related record (company or contact) was not found in the destination. Usually means it failed earlier in the migration.",
        "unknown": "An unexpected error occurred. Check the raw record for details.",
    }

    grouped = {}
    for e in errors_list:
        etype = e["error_type"]
        if etype not in grouped:
            grouped[etype] = {
                "error_type": etype,
                "plain_english": ERROR_EXPLANATIONS.get(etype, "An error occurred."),
                "count": 0,
                "unresolved": 0,
                "errors": []
            }
        grouped[etype]["count"] += 1
        if not e["resolved"]:
            grouped[etype]["unresolved"] += 1
        grouped[etype]["errors"].append(e)

    return {
        "migration_id": migration_id,
        "client_name": m["client_name"],
        "total_errors": len(errors_list),
        "unresolved": sum(1 for e in errors_list if not e["resolved"]),
        "resolved": sum(1 for e in errors_list if e["resolved"]),
        "grouped_by_type": list(grouped.values()),
    }


@app.post("/api/migrations/{migration_id}/errors/{error_id}/resolve")
async def resolve_error(migration_id: str, error_id: str, data: dict):
    """Mark an error as resolved with an optional note explaining what was done."""
    db = get_db()
    note = data.get("note", "")
    db.execute(
        "UPDATE migration_errors SET resolved = true, resolved_note = ? WHERE id = ? AND migration_id = ?",
        (note, error_id, migration_id)
    )
    db.commit()
    return {"ok": True}


@app.post("/api/migrations/{migration_id}/retry-failed")
async def retry_failed(migration_id: str):
    """
    Re-run failed records using the full executor pipeline —
    reference remapping, idempotency checks, and association rebuilding
    are all applied on retry just like the original run.
    """
    db = get_db()
    row = db.execute("SELECT * FROM migrations WHERE id = ?", (migration_id,)).fetchone()
    if not row:
        raise HTTPException(404)
    m = dict(row)

    if m["status"] not in ("complete_with_errors", "complete"):
        raise HTTPException(400, "Can only retry on migrations that have finished running")

    # Pull all unresolved errors that have raw_record saved
    failed_errors = db.execute(
        """SELECT id, object_type, source_id, raw_record
           FROM migration_errors
           WHERE migration_id = ? AND resolved = false AND raw_record IS NOT NULL""",
        (migration_id,)
    ).fetchall()

    if not len(failed_errors):
        return {"ok": True, "message": "No unresolved errors to retry", "retried": 0}

    src_creds = json.loads(m["source_credentials"])
    dst_creds = json.loads(m["dest_credentials"])
    src_conn = get_connector(m["source_platform"], src_creds)
    dst_conn = get_connector(m["dest_platform"], dst_creds)

    mappings = db.execute(
        "SELECT * FROM field_mappings WHERE migration_id = ? AND operator_action != 'skipped'",
        (migration_id,)
    ).fetchall()
    approved_mappings = [dict(r) for r in mappings]

    # Create a fresh executor instance so it loads reference maps properly
    executor = MigrationExecutor(
        src_conn, dst_conn, approved_mappings, migration_id, db,
        source_platform=m["source_platform"],
        dest_platform=m["dest_platform"]
    )
    await executor._load_reference_maps()

    retried = 0
    newly_succeeded = 0
    newly_failed = 0

    for err in failed_errors:
        err = dict(err)
        object_type = err["object_type"]
        raw = json.loads(err["raw_record"])
        source_id = str(err["source_id"])

        type_mappings = executor.mappings_by_type.get(object_type, [])
        if not type_mappings:
            continue

        dest_type = type_mappings[0].get("dest_object_type", object_type)
        canonical = __import__('core.executor', fromlist=['OBJECT_TYPE_ALIASES']).OBJECT_TYPE_ALIASES.get(object_type.lower(), object_type.lower())

        try:
            # Apply field mapping
            mapped = executor._apply_mapping(raw, type_mappings)
            mapped.pop("_id", None)
            mapped.pop("_object_type", None)
            mapped.pop("_associations", None)
            mapped = {k: v for k, v in mapped.items() if v not in ("", None)}

            # Apply reference remapping (owner/stage) — same as main run
            mapped = executor._remap_references(mapped, canonical)

            # Idempotency check — did this record already get created somehow?
            existing_id = executor._lookup_persisted_dest_id(canonical, source_id)
            if existing_id:
                db.execute(
                    "UPDATE migration_errors SET resolved = true, resolved_note = 'Already migrated — found in record map', retry_count = retry_count + 1 WHERE id = ?",
                    (err["id"],)
                )
                newly_succeeded += 1
                retried += 1
                db.commit()
                continue

            # Dedup check against destination
            dedup_id = await executor._check_existing_in_dest(dest_type, mapped)
            if dedup_id:
                executor._persist_record_mapping(canonical, source_id, dest_type, dedup_id, status="reused")
                executor._update_typed_map(canonical, source_id, dedup_id)
                db.execute(
                    "UPDATE migration_errors SET resolved = true, resolved_note = 'Record already exists in destination', retry_count = retry_count + 1 WHERE id = ?",
                    (err["id"],)
                )
                newly_succeeded += 1
                retried += 1
                db.commit()
                continue

            # Create the record
            new_id = await dst_conn.create_record(dest_type, mapped)
            executor._persist_record_mapping(canonical, source_id, dest_type, new_id,
                                             status="created", fingerprint=executor._fingerprint(mapped))
            executor._update_typed_map(canonical, source_id, new_id)

            # Rebuild associations for this specific record
            assocs = raw.get("_associations", [])
            if not assocs:
                try:
                    assocs = await src_conn.get_record_associations(object_type, source_id)
                except Exception:
                    assocs = []

            for assoc in assocs:
                to_canonical = __import__('core.executor', fromlist=['OBJECT_TYPE_ALIASES']).OBJECT_TYPE_ALIASES.get(
                    assoc["to_object_type"].lower(), assoc["to_object_type"].lower())
                to_dest_id = executor._lookup_dest_id(to_canonical, str(assoc["to_record_id"]))
                if to_dest_id:
                    await dst_conn.create_association(canonical, new_id, to_canonical, to_dest_id,
                                                      assoc.get("relationship_type", ""))

            db.execute(
                "UPDATE migration_errors SET resolved = true, resolved_note = 'Succeeded on retry', retry_count = retry_count + 1 WHERE id = ?",
                (err["id"],)
            )
            newly_succeeded += 1

        except Exception as e:
            db.execute(
                "UPDATE migration_errors SET retry_count = retry_count + 1, error_message = ? WHERE id = ?",
                (str(e), err["id"])
            )
            newly_failed += 1

        db.commit()
        retried += 1

    # Update migration counts
    current = db.execute(
        "SELECT migrated_records, failed_records FROM migrations WHERE id = ?",
        (migration_id,)
    ).fetchone()
    new_migrated = current["migrated_records"] + newly_succeeded
    new_failed = max(0, current["failed_records"] - newly_succeeded)
    remaining_unresolved = db.execute(
        "SELECT COUNT(*) as c FROM migration_errors WHERE migration_id = ? AND resolved = false",
        (migration_id,)
    ).fetchone()["c"]
    new_status = "complete" if remaining_unresolved == 0 else "complete_with_errors"
    db.execute(
        "UPDATE migrations SET migrated_records = ?, failed_records = ?, status = ? WHERE id = ?",
        (new_migrated, new_failed, new_status, migration_id)
    )
    db.commit()

    return {
        "ok": True,
        "retried": retried,
        "newly_succeeded": newly_succeeded,
        "still_failing": newly_failed,
        "status": new_status,
        "message": f"Retried {retried} records. {newly_succeeded} succeeded, {newly_failed} still failing."
    }









@app.get("/api/migrations/{migration_id}/report")
async def get_migration_report(migration_id: str):
    """
    Full migration reconciliation report.
    Shows record counts, association rebuild results,
    integrity checks, and anything that needs manual attention.
    """
    db = get_db()
    row = db.execute("SELECT * FROM migrations WHERE id = ?", (migration_id,)).fetchone()
    if not row:
        raise HTTPException(404)
    m = dict(row)

    # Parse stored report from operator_notes if available
    stored_report = {}
    if m.get("operator_notes"):
        try:
            stored_report = json.loads(m["operator_notes"])
        except Exception:
            pass

    # Record map summary
    record_map = db.execute(
        """SELECT source_object_type, dest_object_type, status, COUNT(*) as count
           FROM migration_record_map WHERE migration_id = ?
           GROUP BY source_object_type, dest_object_type, status""",
        (migration_id,)
    ).fetchall()

    # Association summary
    assoc_summary = db.execute(
        """SELECT status, COUNT(*) as count
           FROM migration_associations WHERE migration_id = ?
           GROUP BY status""",
        (migration_id,)
    ).fetchall()

    # Failed records detail
    failed_records = db.execute(
        """SELECT source_object_type, source_record_id, error_message
           FROM migration_record_map
           WHERE migration_id = ? AND status = 'failed'
           LIMIT 50""",
        (migration_id,)
    ).fetchall()

    # Skipped associations
    skipped_assocs = db.execute(
        """SELECT from_source_object_type, from_source_record_id,
                  to_source_object_type, to_source_record_id, error_message
           FROM migration_associations
           WHERE migration_id = ? AND status IN ('skipped', 'failed')
           LIMIT 50""",
        (migration_id,)
    ).fetchall()

    return {
        "migration_id": migration_id,
        "client_name": m["client_name"],
        "source_platform": m["source_platform"],
        "dest_platform": m["dest_platform"],
        "status": m["status"],
        "summary": {
            "total_records": m["total_records"],
            "migrated": m["migrated_records"],
            "failed": m["failed_records"],
            "reused": m.get("reused_records", 0),
            "created": m["migrated_records"] - m.get("reused_records", 0),
        },
        "record_map": [dict(r) for r in record_map],
        "association_summary": {r["status"]: r["count"] for r in assoc_summary},
        "failed_records": [dict(r) for r in failed_records],
        "skipped_associations": [dict(r) for r in skipped_assocs],
        "reconciliation": stored_report,
        "internal_notes": m.get("internal_notes", ""),
        "mapping_snapshot": json.loads(m["mapping_snapshot"]) if m.get("mapping_snapshot") else [],
    }


@app.post("/api/test-connection")
async def test_connection(data: dict):
    platform = data.get("platform")
    credentials = data.get("credentials", {})
    conn = get_connector(platform, credentials)
    ok = await conn.test_connection()
    return {"connected": ok}
