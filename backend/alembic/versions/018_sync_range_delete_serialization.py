"""serialize raw deletion with sync-range assertions

Revision ID: 018_sync_range_delete_serialization
Revises: 017_candle_revision_immutability
Create Date: 2026-09-14 00:00:00.000000

``sync_ranges`` is an availability assertion over canonical raw rows.  The
coverage trigger from migration 014 proves that assertion at insert time, but
a concurrent raw-row delete could previously commit just after that proof and
before the range-creation transaction committed.  The delete trigger could
not see the uncommitted range, leaving a false coverage claim.

Both operations now take the same transaction-scoped, per-asset advisory lock.
The lock is deliberately acquired in the triggers themselves, so direct SQL
writers cannot bypass the serialization boundary.
"""

from alembic import op


revision = "018_sync_range_delete_serialization"
down_revision = "017_candle_revision_immutability"
branch_labels = None
depends_on = None


# Keep this separate from other advisory-lock domains (for example CAGG
# scheduling) while allowing every raw-delete/range-assertion operation for
# the same asset to serialize.
_SYNC_RANGE_ADVISORY_LOCK_NAMESPACE = 731_415_022


def _create_serialized_functions() -> None:
    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION enforce_sync_range_raw_coverage() RETURNS trigger AS $$
        DECLARE
            expected_count bigint;
            actual_count bigint;
            first_timestamp timestamptz;
            last_timestamp timestamptz;
        BEGIN
            PERFORM pg_advisory_xact_lock({_SYNC_RANGE_ADVISORY_LOCK_NAMESPACE}, NEW.asset_id);
            expected_count := floor(extract(epoch FROM (NEW.end_timestamp - NEW.start_timestamp)) / 60) + 1;
            SELECT count(*), min(timestamp), max(timestamp)
              INTO actual_count, first_timestamp, last_timestamp
              FROM raw_1m_candles
             WHERE asset_id = NEW.asset_id
               AND timestamp >= NEW.start_timestamp
               AND timestamp <= NEW.end_timestamp;
            IF actual_count <> expected_count
               OR first_timestamp IS DISTINCT FROM NEW.start_timestamp
               OR last_timestamp IS DISTINCT FROM NEW.end_timestamp THEN
                RAISE EXCEPTION 'sync_ranges can only claim complete canonical raw_1m_candles coverage';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION invalidate_sync_range_on_raw_candle_delete() RETURNS trigger AS $$
        BEGIN
            PERFORM pg_advisory_xact_lock({_SYNC_RANGE_ADVISORY_LOCK_NAMESPACE}, OLD.asset_id);
            DELETE FROM sync_ranges
             WHERE asset_id = OLD.asset_id
               AND start_timestamp <= OLD.timestamp
               AND end_timestamp >= OLD.timestamp;
            RETURN OLD;
        END;
        $$ LANGUAGE plpgsql;
        """
    )


def _create_pre_018_functions() -> None:
    op.execute(
        """
        CREATE OR REPLACE FUNCTION enforce_sync_range_raw_coverage() RETURNS trigger AS $$
        DECLARE
            expected_count bigint;
            actual_count bigint;
            first_timestamp timestamptz;
            last_timestamp timestamptz;
        BEGIN
            expected_count := floor(extract(epoch FROM (NEW.end_timestamp - NEW.start_timestamp)) / 60) + 1;
            SELECT count(*), min(timestamp), max(timestamp)
              INTO actual_count, first_timestamp, last_timestamp
              FROM raw_1m_candles
             WHERE asset_id = NEW.asset_id
               AND timestamp >= NEW.start_timestamp
               AND timestamp <= NEW.end_timestamp;
            IF actual_count <> expected_count
               OR first_timestamp IS DISTINCT FROM NEW.start_timestamp
               OR last_timestamp IS DISTINCT FROM NEW.end_timestamp THEN
                RAISE EXCEPTION 'sync_ranges can only claim complete canonical raw_1m_candles coverage';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE OR REPLACE FUNCTION invalidate_sync_range_on_raw_candle_delete() RETURNS trigger AS $$
        BEGIN
            DELETE FROM sync_ranges
             WHERE asset_id = OLD.asset_id
               AND start_timestamp <= OLD.timestamp
               AND end_timestamp >= OLD.timestamp;
            RETURN OLD;
        END;
        $$ LANGUAGE plpgsql;
        """
    )


def upgrade() -> None:
    _create_serialized_functions()


def downgrade() -> None:
    _create_pre_018_functions()
