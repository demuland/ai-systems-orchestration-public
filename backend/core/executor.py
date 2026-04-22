import asyncio
import uuid
import json
import hashlib
from connectors.base import BaseConnector

# ─────────────────────────────────────────────────────────────────────────────
# MigrationExecutor — Multi-phase relationship-aware migration engine
#
# Phase 1: Create base records in dependency order
#   - Idempotency check before every create (prevents duplicates on retry)
#   - Persists source→destination ID mapping by object type to DB
#   - Remaps owner IDs and stage IDs inline during record creation
#   - Stores inline associations from source records for Phase 2
#
# Phase 2: Rebuild associations
#   - Reads all associations collected in Phase 1
#   - Resolves both ends through the persisted mapping table
#   - Creates associations in destination platform
#   - Logs any unresolvable references
#
# Phase 3: Validate and reconcile
#   - Compares source record counts vs destination
#   - Checks association rebuild coverage
#   - Flags orphaned records and unresolved references
#   - Produces a full migration report
# ─────────────────────────────────────────────────────────────────────────────

EXECUTION_ORDER = {
    "hubspot_to_pipedrive":  ["companies", "contacts", "deals"],
    "pipedrive_to_hubspot":  ["organizations", "persons", "deals"],
    "hubspot_to_zoho":       ["companies", "contacts", "deals"],
    "zoho_to_hubspot":       ["Accounts", "Contacts", "Deals"],
    "hubspot_to_monday":     ["companies", "contacts", "deals"],
    "hubspot_to_activecampaign": ["companies", "contacts", "deals"],
    "hubspot_to_freshsales": ["companies", "contacts", "deals"],
    "hubspot_to_copper":     ["companies", "contacts", "deals"],
    "pipedrive_to_zoho":     ["organizations", "persons", "deals"],
    "pipedrive_to_activecampaign": ["organizations", "persons", "deals"],
}

# Canonical object type aliases — normalize cross-platform names
OBJECT_TYPE_ALIASES = {
    "companies": "company",
    "company": "company",
    "organizations": "company",
    "organization": "company",
    "contacts": "contact",
    "contact": "contact",
    "persons": "contact",
    "person": "contact",
    "deals": "deal",
    "deal": "deal",
    "opportunities": "deal",
    "opportunity": "deal",
    "accounts": "company",
    "account": "company",
}

# Unique lookup fields per canonical object type
UNIQUE_FIELDS = {
    "contact": ["email"],
    "company": ["name", "domain"],
    "deal": ["dealname", "title", "name"],
}


class MigrationExecutor:

    def __init__(self, source: BaseConnector, dest: BaseConnector,
                 mappings: list[dict], migration_id: str, db,
                 source_platform: str = "", dest_platform: str = ""):
        self.source = source
        self.dest = dest
        self.migration_id = migration_id
        self.db = db
        self.source_platform = source_platform
        self.dest_platform = dest_platform

        # In-memory typed ID map for fast lookup during a run
        # {canonical_type: {source_id: dest_id}}
        self.typed_id_map: dict[str, dict[str, str]] = {}

        # Pending associations collected during Phase 1
        # [{from_type, from_source_id, to_type, to_source_id, relationship_type}]
        self.pending_associations: list[dict] = []

        # Owner and stage reference maps (populated at start of run)
        self.owner_map: dict[str, str] = {}   # {source_owner_id: dest_owner_id}
        self.stage_map: dict[str, str] = {}   # {source_stage_id: dest_stage_id}

        # Unresolved reference counters for reconciliation report
        self._unresolved_owner_refs = 0
        self._unresolved_stage_refs = 0

        # Build mappings by object type
        self.mappings_by_type: dict[str, list[dict]] = {}
        for m in mappings:
            obj = m["object_type"]
            if obj not in self.mappings_by_type:
                self.mappings_by_type[obj] = []
            self.mappings_by_type[obj].append(m)

    def _is_cancelled(self) -> bool:
        """Check DB for cancel flag — called between batches."""
        row = self.db.execute(
            "SELECT cancel_requested FROM migrations WHERE id = ?", (self.migration_id,)
        ).fetchone()
        return bool(row and row["cancel_requested"])

    def _classify_error(self, exc: Exception) -> str:
        """Map an exception to a typed error bucket."""
        msg = str(exc).lower()
        if any(k in msg for k in ("401", "403", "unauthorized", "forbidden", "authentication", "invalid token", "api key")):
            return "auth"
        if any(k in msg for k in ("429", "rate limit", "too many requests", "quota")):
            return "rate_limit"
        if any(k in msg for k in ("422", "400", "validation", "required", "invalid value", "unprocessable")):
            return "validation"
        if any(k in msg for k in ("duplicate", "already exists", "conflict", "409")):
            return "duplicate"
        if any(k in msg for k in ("404", "not found", "does not exist")):
            return "not_found"
        if any(k in msg for k in ("mapping", "transform", "field")):
            return "mapping"
        return "unknown"

    # ── Public entry point ────────────────────────────────────────────────────

    async def run(self):
        """Execute the full multi-phase migration."""
        try:
            # Load owner and stage maps before starting
            await self._load_reference_maps()

            if self._is_cancelled():
                self.db.execute("UPDATE migrations SET status = 'cancelled' WHERE id = ?", (self.migration_id,))
                self.db.commit()
                return

            # Phase 1: Create all base records
            await self._phase1_create_records()

            if self._is_cancelled():
                self.db.execute("UPDATE migrations SET status = 'cancelled' WHERE id = ?", (self.migration_id,))
                self.db.commit()
                return

            # Phase 2: Rebuild all associations
            await self._phase2_rebuild_associations()

            if self._is_cancelled():
                self.db.execute("UPDATE migrations SET status = 'cancelled' WHERE id = ?", (self.migration_id,))
                self.db.commit()
                return

            # Phase 3: Validate and produce final report
            report = await self._phase3_validate()

            # Write final status
            unresolved = report.get("unresolved_associations", 0)
            failed = self._count_failed_records()
            final_status = "complete" if (failed == 0 and unresolved == 0) else "complete_with_errors"
            self.db.execute(
                "UPDATE migrations SET status = ?, operator_notes = ? WHERE id = ?",
                (final_status, json.dumps(report), self.migration_id)
            )
            self.db.commit()

        except Exception as e:
            self.db.execute(
                "UPDATE migrations SET status = 'failed', operator_notes = ? WHERE id = ?",
                (str(e), self.migration_id)
            )
            self.db.commit()
            raise

    # ── Phase 1: Create base records ─────────────────────────────────────────

    async def _phase1_create_records(self):
        order = self._get_execution_order()
        total = 0
        migrated = 0
        failed = 0
        reused = 0

        for object_type in order:
            type_mappings = self.mappings_by_type.get(object_type, [])
            if not type_mappings:
                continue

            dest_type = type_mappings[0].get("dest_object_type", object_type)
            canonical = OBJECT_TYPE_ALIASES.get(object_type.lower(), object_type.lower())

            async for batch in self.source.get_records(object_type):
                # Cancel check between batches
                if self._is_cancelled():
                    return

                total += len(batch)
                self.db.execute(
                    "UPDATE migrations SET total_records = ? WHERE id = ?",
                    (total, self.migration_id)
                )
                self.db.commit()

                for record in batch:
                    source_id = str(record.get("_id", "unknown"))

                    # ── Idempotency check ──────────────────────────────────
                    existing_dest_id = self._lookup_persisted_dest_id(canonical, source_id)
                    if existing_dest_id:
                        self._update_typed_map(canonical, source_id, existing_dest_id)
                        migrated += 1
                        continue

                    # ── Collect associations — inline first, fallback to API ──
                    inline_assocs = record.get("_associations", [])
                    if not inline_assocs:
                        try:
                            inline_assocs = await self.source.get_record_associations(object_type, source_id)
                        except Exception:
                            inline_assocs = []

                    for assoc in inline_assocs:
                        self.pending_associations.append({
                            "from_type": canonical,
                            "from_source_id": source_id,
                            "to_type": OBJECT_TYPE_ALIASES.get(assoc["to_object_type"].lower(), assoc["to_object_type"].lower()),
                            "to_source_id": str(assoc["to_record_id"]),
                            "relationship_type": assoc.get("relationship_type", ""),
                        })

                    try:
                        # ── Apply field mappings ───────────────────────────
                        mapped = self._apply_mapping(record, type_mappings)
                        mapped.pop("_id", None)
                        mapped.pop("_object_type", None)
                        mapped.pop("_associations", None)
                        mapped = {k: v for k, v in mapped.items() if v not in ("", None)}

                        # ── Remap owner and stage references ───────────────
                        mapped = self._remap_references(mapped, canonical)

                        # ── Dedup check against destination ───────────────
                        dedup_id = await self._check_existing_in_dest(dest_type, mapped)
                        if dedup_id:
                            self._persist_record_mapping(canonical, source_id, dest_type, dedup_id, status="reused")
                            self._update_typed_map(canonical, source_id, dedup_id)
                            migrated += 1
                            reused += 1
                            self.db.execute(
                                "UPDATE migrations SET migrated_records = ?, reused_records = ? WHERE id = ?",
                                (migrated, reused, self.migration_id)
                            )
                            self.db.commit()
                            continue

                        # ── Generate fingerprint for change detection ─────
                        fingerprint = self._fingerprint(mapped)

                        # ── Create in destination ─────────────────────────
                        new_id = await self.dest.create_record(dest_type, mapped)
                        self._persist_record_mapping(canonical, source_id, dest_type, new_id, status="created", fingerprint=fingerprint)
                        self._update_typed_map(canonical, source_id, new_id)
                        migrated += 1
                        self.db.execute(
                            "UPDATE migrations SET migrated_records = ? WHERE id = ?",
                            (migrated, self.migration_id)
                        )
                        self.db.commit()

                    except Exception as e:
                        failed += 1
                        error_type = self._classify_error(e)
                        self._persist_record_mapping(canonical, source_id, dest_type, None, status="failed", error=str(e))
                        self._log_error(object_type, source_id, error_type, str(e), record)
                        self.db.execute(
                            "UPDATE migrations SET failed_records = ? WHERE id = ?",
                            (failed, self.migration_id)
                        )
                        self.db.commit()

    # ── Phase 2: Rebuild associations ─────────────────────────────────────────

    async def _phase2_rebuild_associations(self):
        """
        After all records exist in destination, rebuild every relationship
        using the persisted source→destination ID mappings.
        """
        created = 0
        failed = 0
        skipped = 0

        for assoc in self.pending_associations:
            from_type = assoc["from_type"]
            from_source_id = assoc["from_source_id"]
            to_type = assoc["to_type"]
            to_source_id = assoc["to_source_id"]
            rel_type = assoc.get("relationship_type", "")

            from_dest_id = self._lookup_dest_id(from_type, from_source_id)
            to_dest_id = self._lookup_dest_id(to_type, to_source_id)

            if not from_dest_id or not to_dest_id:
                skipped += 1
                self._log_missing_association(from_type, from_source_id, to_type, to_source_id)
                continue

            try:
                success = await self.dest.create_association(
                    from_type, from_dest_id,
                    to_type, to_dest_id,
                    rel_type
                )
                if success:
                    created += 1
                    self._persist_association(assoc, from_dest_id, to_dest_id, "created")
                    self._update_association_status(assoc["from_type"], assoc["from_source_id"], "partial")
                else:
                    skipped += 1
                    self._persist_association(assoc, from_dest_id, to_dest_id, "skipped")
            except Exception as e:
                failed += 1
                self._persist_association(assoc, from_dest_id, to_dest_id, "failed", str(e))

        print(f"[Phase 2] Associations: {created} created, {skipped} skipped, {failed} failed")

    # ── Phase 3: Validate and reconcile ──────────────────────────────────────

    async def _phase3_validate(self) -> dict:
        """
        Produce a reconciliation report with graph parity verification.
        For every successfully migrated record that had associations in the
        source, we verify the destination has the same number of linked records.
        """
        # Count by status from migration_record_map
        rows = self.db.execute(
            """SELECT source_object_type, status, COUNT(*) as count
               FROM migration_record_map
               WHERE migration_id = ?
               GROUP BY source_object_type, status""",
            (self.migration_id,)
        ).fetchall()

        record_summary = {}
        for row in rows:
            obj = row["source_object_type"]
            if obj not in record_summary:
                record_summary[obj] = {}
            record_summary[obj][row["status"]] = row["count"]

        # Count associations by status
        assoc_rows = self.db.execute(
            """SELECT status, COUNT(*) as count
               FROM migration_associations
               WHERE migration_id = ?
               GROUP BY status""",
            (self.migration_id,)
        ).fetchall()
        assoc_summary = {r["status"]: r["count"] for r in assoc_rows}

        # Unresolved references
        unresolved_assocs = self.db.execute(
            """SELECT COUNT(*) as c FROM migration_associations
               WHERE migration_id = ? AND status IN ('failed', 'skipped')""",
            (self.migration_id,)
        ).fetchone()["c"]

        # Duplicate prevention events
        reused = self.db.execute(
            """SELECT COUNT(*) as c FROM migration_record_map
               WHERE migration_id = ? AND status = 'reused'""",
            (self.migration_id,)
        ).fetchone()["c"]

        total_records = self.db.execute(
            "SELECT total_records, migrated_records, failed_records FROM migrations WHERE id = ?",
            (self.migration_id,)
        ).fetchone()

        # ── Graph parity check ────────────────────────────────────────────────
        # For each successfully migrated record that had source associations,
        # count how many associations were expected vs how many were created.
        # Surfaces records where relationship count doesn't match source.
        parity_check = await self._verify_graph_parity()

        report = {
            "migration_id": self.migration_id,
            "records": {
                "total_discovered": total_records["total_records"] if total_records else 0,
                "successfully_migrated": total_records["migrated_records"] if total_records else 0,
                "failed": total_records["failed_records"] if total_records else 0,
                "duplicates_prevented": reused,
                "by_object_type": record_summary,
            },
            "associations": {
                "total_pending": len(self.pending_associations),
                "created": assoc_summary.get("created", 0),
                "skipped": assoc_summary.get("skipped", 0),
                "failed": assoc_summary.get("failed", 0),
            },
            "unresolved_associations": unresolved_assocs,
            "graph_parity": parity_check,
            "integrity_check": {
                "orphaned_records": self._count_orphaned_records(),
                "missing_owner_refs": self._count_unresolved_refs("owner"),
                "missing_stage_refs": self._count_unresolved_refs("stage"),
                "parity_failures": parity_check["parity_failures"],
            }
        }
        return report

    async def _verify_graph_parity(self) -> dict:
        """
        Verify that every record's association count in the destination
        matches what it had in the source.

        For each source record:
          source_assoc_count = number of associations we collected in Phase 1
          dest_assoc_count   = number of associations successfully created in Phase 2

        A parity failure means: source had N relationships but destination has M < N.
        These are the records an operator needs to manually check.
        """
        # Build a map of source_record_id -> expected association count
        # from pending_associations collected in Phase 1
        source_counts: dict[str, int] = {}
        for assoc in self.pending_associations:
            key = f"{assoc['from_type']}:{assoc['from_source_id']}"
            source_counts[key] = source_counts.get(key, 0) + 1

        if not source_counts:
            return {
                "checked": 0,
                "passed": 0,
                "parity_failures": 0,
                "failures": [],
                "coverage_pct": 100,
            }

        # Count successfully created associations per source record
        created_rows = self.db.execute(
            """SELECT from_source_object_type, from_source_record_id, COUNT(*) as count
               FROM migration_associations
               WHERE migration_id = ? AND status = 'created'
               GROUP BY from_source_object_type, from_source_record_id""",
            (self.migration_id,)
        ).fetchall()

        dest_counts: dict[str, int] = {}
        for row in created_rows:
            key = f"{row['from_source_object_type']}:{row['from_source_record_id']}"
            dest_counts[key] = row["count"]

        # Compare
        failures = []
        passed = 0
        for key, expected in source_counts.items():
            actual = dest_counts.get(key, 0)
            if actual < expected:
                obj_type, record_id = key.split(":", 1)
                # Look up destination record ID for operator reference
                dest_row = self.db.execute(
                    """SELECT dest_record_id FROM migration_record_map
                       WHERE migration_id = ? AND source_object_type = ?
                       AND source_record_id = ?""",
                    (self.migration_id, obj_type, record_id)
                ).fetchone()
                dest_id = dest_row["dest_record_id"] if dest_row else "unknown"

                failures.append({
                    "object_type": obj_type,
                    "source_record_id": record_id,
                    "dest_record_id": dest_id,
                    "expected_associations": expected,
                    "actual_associations": actual,
                    "missing": expected - actual,
                    "action": f"Manually verify {obj_type} {dest_id} in destination — {expected - actual} relationship(s) missing",
                })
            else:
                passed += 1

        total_checked = len(source_counts)
        coverage_pct = round((passed / total_checked) * 100, 1) if total_checked > 0 else 100.0

        return {
            "checked": total_checked,
            "passed": passed,
            "parity_failures": len(failures),
            "failures": failures[:50],  # Cap at 50 for response size
            "coverage_pct": coverage_pct,
        }

    # ── Reference mapping helpers ─────────────────────────────────────────────

    async def _load_reference_maps(self):
        """Load owner and stage maps from both platforms before migration starts."""
        try:
            src_owners = await self.source.get_owner_reference_map()
            dst_owners = await self.dest.get_owner_reference_map()
            # Build cross-platform owner map by matching email or name
            for src_id, src_email in src_owners.items():
                for dst_id, dst_email in dst_owners.items():
                    if src_email and dst_email and src_email.lower() == dst_email.lower():
                        self.owner_map[src_id] = dst_id
                        break
        except Exception as e:
            print(f"[Executor] Owner map load failed (non-fatal): {e}")

        try:
            src_stages = await self.source.get_stage_reference_map()
            dst_stages = await self.dest.get_stage_reference_map()
            # Build cross-platform stage map by matching stage labels
            dst_by_label = {v.lower(): k for k, v in dst_stages.items()}
            for src_id, src_label in src_stages.items():
                dst_id = dst_by_label.get(src_label.lower())
                if dst_id:
                    self.stage_map[src_id] = dst_id
        except Exception as e:
            print(f"[Executor] Stage map load failed (non-fatal): {e}")

    def _remap_references(self, record: dict, canonical_type: str) -> dict:
        """
        Remap owner IDs and stage IDs in a record before creating in destination.
        Operates on known field name patterns across all supported platforms.
        """
        OWNER_FIELDS = {
            "hubspot": ["hubspot_owner_id", "hs_all_owner_ids"],
            "pipedrive": ["owner_id", "user_id"],
            "zoho": ["Owner", "owner"],
            "activecampaign": ["owner", "assignee_id"],
            "freshsales": ["owner_id"],
            "copper": ["assignee_id"],
        }
        STAGE_FIELDS = {
            "hubspot": ["dealstage", "hs_pipeline_stage"],
            "pipedrive": ["stage_id"],
            "zoho": ["Stage"],
            "activecampaign": ["deal_stage"],
            "freshsales": ["deal_stage_id"],
        }

        owner_fields = OWNER_FIELDS.get(self.source_platform, []) + ["owner_id", "hubspot_owner_id", "user_id"]
        stage_fields = STAGE_FIELDS.get(self.source_platform, []) + ["dealstage", "stage_id"]

        result = dict(record)
        for field in owner_fields:
            if field in result:
                val = str(result[field])
                if val in self.owner_map:
                    result[field] = self.owner_map[val]
                elif val and val not in ("", "None", "null"):
                    self._unresolved_owner_refs += 1
        for field in stage_fields:
            if field in result:
                val = str(result[field])
                if val in self.stage_map:
                    result[field] = self.stage_map[val]
                elif val and val not in ("", "None", "null"):
                    self._unresolved_stage_refs += 1
        return result

    # ── Persistence helpers ───────────────────────────────────────────────────

    def _persist_record_mapping(self, src_type: str, src_id: str, dst_type: str,
                                dst_id: str, status: str, error: str = None,
                                fingerprint: str = None):
        self.db.execute(
            """INSERT INTO migration_record_map
               (id, migration_id, source_object_type, source_record_id,
                dest_object_type, dest_record_id, source_fingerprint,
                status, error_message, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
               ON CONFLICT(migration_id, source_object_type, source_record_id)
               DO UPDATE SET dest_record_id=excluded.dest_record_id,
                             source_fingerprint=excluded.source_fingerprint,
                             status=excluded.status,
                             error_message=excluded.error_message,
                             updated_at=CURRENT_TIMESTAMP""",
            (str(uuid.uuid4()), self.migration_id, src_type, src_id,
             dst_type, dst_id, fingerprint, status, error)
        )
        self.db.commit()

    def _persist_association(self, assoc: dict, from_dest_id: str, to_dest_id: str,
                             status: str, error: str = None):
        self.db.execute(
            """INSERT OR IGNORE INTO migration_associations
               (id, migration_id, from_source_object_type, from_source_record_id,
                from_dest_record_id, to_source_object_type, to_source_record_id,
                to_dest_record_id, relationship_type, status, error_message)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (str(uuid.uuid4()), self.migration_id,
             assoc["from_type"], assoc["from_source_id"], from_dest_id,
             assoc["to_type"], assoc["to_source_id"], to_dest_id,
             assoc.get("relationship_type", ""), status, error)
        )
        self.db.commit()

    def _log_missing_association(self, from_type: str, from_source_id: str,
                                  to_type: str, to_source_id: str):
        self.db.execute(
            """INSERT OR IGNORE INTO migration_associations
               (id, migration_id, from_source_object_type, from_source_record_id,
                to_source_object_type, to_source_record_id, status, error_message)
               VALUES (?, ?, ?, ?, ?, ?, 'skipped', 'Destination ID not found — record may have failed in Phase 1')""",
            (str(uuid.uuid4()), self.migration_id,
             from_type, from_source_id, to_type, to_source_id)
        )
        self.db.commit()

    def _lookup_persisted_dest_id(self, canonical_type: str, source_id: str) -> str:
        """Check DB for a previously migrated record — for idempotent retries."""
        row = self.db.execute(
            """SELECT dest_record_id FROM migration_record_map
               WHERE migration_id = ? AND source_object_type = ?
               AND source_record_id = ? AND status IN ('created', 'reused')""",
            (self.migration_id, canonical_type, source_id)
        ).fetchone()
        return row["dest_record_id"] if row else None

    def _lookup_dest_id(self, canonical_type: str, source_id: str) -> str:
        """Look up destination ID from in-memory map first, then DB fallback."""
        mem = self.typed_id_map.get(canonical_type, {}).get(source_id)
        if mem:
            return mem
        row = self.db.execute(
            """SELECT dest_record_id FROM migration_record_map
               WHERE migration_id = ? AND source_object_type = ?
               AND source_record_id = ? AND dest_record_id IS NOT NULL""",
            (self.migration_id, canonical_type, source_id)
        ).fetchone()
        return row["dest_record_id"] if row else None

    def _update_typed_map(self, canonical_type: str, source_id: str, dest_id: str):
        if canonical_type not in self.typed_id_map:
            self.typed_id_map[canonical_type] = {}
        self.typed_id_map[canonical_type][source_id] = dest_id

    async def _check_existing_in_dest(self, dest_type: str, mapped: dict) -> str:
        """Check if record already exists in destination before creating."""
        canonical = OBJECT_TYPE_ALIASES.get(dest_type.lower(), dest_type.lower())
        unique_fields = UNIQUE_FIELDS.get(canonical, [])
        for field in unique_fields:
            if mapped.get(field):
                existing = await self.dest.find_existing_record(dest_type, {field: mapped[field]})
                if existing:
                    return existing
        return None

    # ── Validation helpers ────────────────────────────────────────────────────

    def _count_failed_records(self) -> int:
        row = self.db.execute(
            "SELECT failed_records FROM migrations WHERE id = ?",
            (self.migration_id,)
        ).fetchone()
        return row["failed_records"] if row else 0

    def _count_orphaned_records(self) -> int:
        """Count records created but with no associations when associations were expected."""
        row = self.db.execute(
            """SELECT COUNT(*) as c FROM migration_record_map m
               WHERE m.migration_id = ? AND m.status = 'created'
               AND m.source_object_type = 'deal'
               AND NOT EXISTS (
                   SELECT 1 FROM migration_associations a
                   WHERE a.migration_id = m.migration_id
                   AND a.from_source_record_id = m.source_record_id
                   AND a.status = 'created'
               )""",
            (self.migration_id,)
        ).fetchone()
        return row["c"] if row else 0

    def _count_unresolved_refs(self, ref_type: str) -> int:
        """Count owner or stage references that could not be remapped."""
        return self._unresolved_owner_refs if ref_type == "owner" else self._unresolved_stage_refs

    def _fingerprint(self, data: dict) -> str:
        """Generate a short hash of the mapped record for change detection."""
        cleaned = {k: v for k, v in sorted(data.items()) if not k.startswith("_")}
        return hashlib.md5(json.dumps(cleaned, sort_keys=True).encode()).hexdigest()[:16]

    def _update_association_status(self, canonical_type: str, source_id: str, status: str):
        """Update the association_status for a migrated record after Phase 2."""
        self.db.execute(
            """UPDATE migration_record_map
               SET association_status = ?, updated_at = CURRENT_TIMESTAMP
               WHERE migration_id = ? AND source_object_type = ? AND source_record_id = ?""",
            (status, self.migration_id, canonical_type, source_id)
        )
        self.db.commit()

    # ── Standard helpers ──────────────────────────────────────────────────────

    def _get_execution_order(self) -> list[str]:
        src_types = list(self.mappings_by_type.keys())
        pair_key = f"{self.source_platform}_to_{self.dest_platform}"
        ordered = EXECUTION_ORDER.get(pair_key, [])
        if ordered:
            return [t for t in ordered if t in src_types]
        # Fallback: companies/orgs first, then contacts/persons, then deals
        priority = ["companies", "organizations", "accounts", "contacts", "persons", "deals", "opportunities"]
        result = [t for t in priority if t in src_types]
        result += [t for t in src_types if t not in result]
        return result

    def _apply_mapping(self, record: dict, mappings: list[dict]) -> dict:
        result = {}
        for m in mappings:
            src_field = m["source_field"]
            dst_field = m.get("operator_override") or m.get("dest_field")
            if not dst_field:
                continue
            val = record.get(src_field)
            if val is not None:
                result[dst_field] = str(val) if not isinstance(val, (str, int, float, bool)) else val
        result["_id"] = record.get("_id")
        result["_object_type"] = record.get("_object_type")
        return result

    def _log_error(self, object_type: str, source_id: str, error_type: str,
                   message: str, record: dict = None):
        self.db.execute(
            """INSERT INTO migration_errors
               (id, migration_id, object_type, source_id, error_type, error_message, raw_record)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (str(uuid.uuid4()), self.migration_id, object_type, source_id,
             error_type, message, json.dumps(record) if record else None)
        )
        self.db.commit()
