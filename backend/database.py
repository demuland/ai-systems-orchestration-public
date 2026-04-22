import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "migrations.db")

def get_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def init_db():
    db = get_db()
    db.executescript("""
        CREATE TABLE IF NOT EXISTS migrations (
            id                TEXT PRIMARY KEY,
            client_name       TEXT NOT NULL,
            source_platform   TEXT NOT NULL,
            dest_platform     TEXT NOT NULL,
            source_credentials TEXT NOT NULL,
            dest_credentials  TEXT NOT NULL,
            status            TEXT DEFAULT 'pending',
            total_records     INTEGER DEFAULT 0,
            migrated_records  INTEGER DEFAULT 0,
            failed_records    INTEGER DEFAULT 0,
            reused_records    INTEGER DEFAULT 0,
            operator_notes    TEXT,
            internal_notes    TEXT,
            cancel_requested  BOOLEAN DEFAULT false,
            mapping_snapshot  TEXT,
            created_at        DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at        DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        -- Add new columns to existing DBs without breaking them
        CREATE TEMPORARY TABLE IF NOT EXISTS _noop (x INTEGER);
        DROP TABLE _noop;

        CREATE TABLE IF NOT EXISTS field_mappings (
            id                TEXT PRIMARY KEY,
            migration_id      TEXT REFERENCES migrations(id),
            object_type       TEXT NOT NULL,
            dest_object_type  TEXT,
            source_field      TEXT NOT NULL,
            source_type       TEXT,
            dest_field        TEXT,
            dest_type         TEXT,
            confidence        REAL DEFAULT 0.0,
            llm_reasoning     TEXT,
            operator_action   TEXT DEFAULT 'pending',
            operator_override TEXT,
            transform_fn      TEXT,
            created_at        DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS schema_cache (
            id            TEXT PRIMARY KEY,
            platform      TEXT NOT NULL,
            object_type   TEXT NOT NULL,
            schema_json   TEXT NOT NULL,
            fetched_at    DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS migration_record_map (
            id                  TEXT PRIMARY KEY,
            migration_id        TEXT REFERENCES migrations(id),
            source_object_type  TEXT NOT NULL,
            source_record_id    TEXT NOT NULL,
            dest_object_type    TEXT NOT NULL,
            dest_record_id      TEXT,
            source_fingerprint  TEXT,
            status              TEXT DEFAULT 'pending',
            -- pending | created | linked | failed | skipped
            association_status  TEXT DEFAULT 'pending',
            -- pending | complete | partial | failed | not_applicable
            error_message       TEXT,
            created_at          DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at          DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(migration_id, source_object_type, source_record_id)
        );

        CREATE TABLE IF NOT EXISTS migration_associations (
            id                      TEXT PRIMARY KEY,
            migration_id            TEXT REFERENCES migrations(id),
            from_source_object_type TEXT NOT NULL,
            from_source_record_id   TEXT NOT NULL,
            from_dest_record_id     TEXT,
            to_source_object_type   TEXT NOT NULL,
            to_source_record_id     TEXT NOT NULL,
            to_dest_record_id       TEXT,
            relationship_type       TEXT,
            status                  TEXT DEFAULT 'pending',
            -- pending | created | failed | skipped
            error_message           TEXT,
            created_at              DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS migration_errors (
            id            TEXT PRIMARY KEY,
            migration_id  TEXT REFERENCES migrations(id),
            object_type   TEXT NOT NULL,
            source_id     TEXT,
            error_type    TEXT NOT NULL,
            error_message TEXT NOT NULL,
            raw_record    TEXT,
            resolved      BOOLEAN DEFAULT false,
            resolved_note TEXT,
            retry_count   INTEGER DEFAULT 0,
            created_at    DATETIME DEFAULT CURRENT_TIMESTAMP
        );


        CREATE TABLE IF NOT EXISTS training_log (
            id                  TEXT PRIMARY KEY,
            migration_id        TEXT REFERENCES migrations(id),
            source_platform     TEXT NOT NULL,
            dest_platform       TEXT NOT NULL,
            object_type         TEXT NOT NULL,
            source_field        TEXT NOT NULL,
            source_type         TEXT,
            source_label        TEXT,
            source_sample       TEXT,
            source_context      TEXT,
            dest_field_proposed TEXT,
            dest_type_proposed  TEXT,
            confidence_proposed REAL,
            llm_reasoning       TEXT,
            dest_field_actual   TEXT,
            was_correct         BOOLEAN,
            correction_made     BOOLEAN,
            correction_delta    TEXT,
            operator_action     TEXT,
            operator_note       TEXT,
            created_at          DATETIME DEFAULT CURRENT_TIMESTAMP
        );
    """)
    db.commit()

    # Migrate existing DBs — safe to run repeatedly, errors ignored
    for col, defn in [
        ("reused_records",   "INTEGER DEFAULT 0"),
        ("internal_notes",   "TEXT"),
        ("cancel_requested", "BOOLEAN DEFAULT false"),
        ("mapping_snapshot", "TEXT"),
    ]:
        try:
            db.execute(f"ALTER TABLE migrations ADD COLUMN {col} {defn}")
            db.commit()
        except Exception:
            pass  # column already exists

    for col, defn in [
        ("dest_object_type", "TEXT"),
    ]:
        try:
            db.execute(f"ALTER TABLE field_mappings ADD COLUMN {col} {defn}")
            db.commit()
        except Exception:
            pass  # column already exists
