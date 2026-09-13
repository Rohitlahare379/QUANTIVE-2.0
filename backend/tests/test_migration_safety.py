"""Offline regressions for PostgreSQL migration transaction safety."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


BACKEND_ROOT = Path(__file__).resolve().parents[1]


def test_new_gap_repair_enum_value_commits_before_the_exclusion_constraint_uses_it():
    """PostgreSQL rejects an enum value used before its ADD VALUE commits."""
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head", "--sql"],
        cwd=BACKEND_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr

    sql = result.stdout
    enum_value = "ALTER TYPE gaprepairstatus ADD VALUE IF NOT EXISTS 'AWAITING_MERGE'"
    exclusion_constraint = "ADD CONSTRAINT exclude_active_gap_repair_jobs"
    enum_position = sql.index(enum_value)
    constraint_position = sql.index(exclusion_constraint, enum_position)
    # Alembic's autocommit block ends the surrounding DDL transaction *before*
    # ADD VALUE and begins a fresh one before the exclusion constraint.
    commit_position = sql.rindex("COMMIT;", 0, enum_position)
    begin_position = sql.index("BEGIN;", enum_position)

    assert commit_position < enum_position < begin_position < constraint_position


def test_sync_range_and_raw_delete_share_a_database_serialization_fence():
    """Direct SQL cannot interleave a raw delete with a coverage assertion."""
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head", "--sql"],
        cwd=BACKEND_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr

    migration_sql = result.stdout.rsplit(
        "CREATE OR REPLACE FUNCTION enforce_sync_range_raw_coverage()", 1
    )[1]
    lock = "pg_advisory_xact_lock(731415022, NEW.asset_id)"
    delete_lock = "pg_advisory_xact_lock(731415022, OLD.asset_id)"
    assert lock in migration_sql
    assert delete_lock in migration_sql
    assert migration_sql.index(lock) < migration_sql.index("SELECT count(*), min(timestamp), max(timestamp)")
    assert migration_sql.index(delete_lock) < migration_sql.index("DELETE FROM sync_ranges")


def test_export_durability_migration_installs_fenced_claim_state_before_processing_constraint():
    """A legacy processing export must be requeued before claim completeness is enforced."""
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head", "--sql"],
        cwd=BACKEND_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr

    sql = result.stdout
    migration_sql = sql.rsplit("UPDATE export_jobs", 1)[1]
    assert "requeued by export durability migration" in migration_sql
    assert "ck_export_jobs_processing_claim_complete" in migration_sql
    assert migration_sql.index("requeued by export durability migration") < migration_sql.index(
        "ck_export_jobs_processing_claim_complete"
    )


def test_websocket_shard_persistence_fence_migration_is_present_after_sync_range_hardening():
    """The durable WebSocket write fence must be part of the migration head."""
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head", "--sql"],
        cwd=BACKEND_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr

    sql = result.stdout
    create = "CREATE TABLE ws_shard_persistence_fences"
    assert create in sql
    assert "fencing_token BIGINT NOT NULL" in sql
    assert "ck_ws_shard_persistence_fence_positive CHECK (fencing_token > 0)" in sql


def test_continuous_aggregate_migration_commits_before_creating_materialized_views():
    """Timescale rejects continuous aggregates inside Alembic's DDL transaction."""
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head", "--sql"],
        cwd=BACKEND_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr

    sql = result.stdout
    cagg_position = sql.index("CREATE MATERIALIZED VIEW candles_5m")
    assert sql.rindex("COMMIT;", 0, cagg_position) < cagg_position


def test_explicitly_created_postgresql_enums_are_not_created_again_by_table_ddl():
    """Named enums created in a migration need create_type=False on table columns."""
    for revision in ("005_cagg_refresh.py", "006_export_jobs.py"):
        contents = (BACKEND_ROOT / "alembic" / "versions" / revision).read_text()
        assert "create_type=False" in contents


def test_raw_integrity_constraints_disable_timescale_compression_during_schema_change():
    """Timescale rejects adding raw-table constraints while compression is enabled."""
    contents = (
        BACKEND_ROOT / "alembic" / "versions" / "014_data_integrity_hardening.py"
    ).read_text()
    disabled = "ALTER TABLE raw_1m_candles SET (timescaledb.compress = false)"
    constraints = "_add_integrity_constraints(table_name)"
    restore_call = "    _restore_raw_compression()"
    assert contents.index(disabled) < contents.rindex(constraints) < contents.index(restore_call)


def test_alembic_version_column_is_widened_before_the_first_long_revision_id():
    """A fresh database must be able to record revision 018's 35-character ID."""
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head", "--sql"],
        cwd=BACKEND_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr

    sql = result.stdout
    widen = "ALTER TABLE alembic_version ALTER COLUMN version_num TYPE VARCHAR(64)"
    record_long_revision = "version_num='018_sync_range_delete_serialization'"
    assert widen in sql
    assert sql.index(widen) < sql.index(record_long_revision)
