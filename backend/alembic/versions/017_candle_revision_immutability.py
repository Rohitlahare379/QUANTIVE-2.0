"""make canonical candle revision history immutable

Revision ID: 017_candle_revision_immutability
Revises: 016_strategy_forensic_hardening
Create Date: 2026-09-14 00:00:00.000000

``candle_revisions`` is a forensic ledger, not a cache.  The canonical raw
row may be corrected, but every accepted value must remain independently
auditable.  Application-level code is not a sufficient boundary because
operational scripts and future services can issue direct SQL.
"""

from alembic import op
import sqlalchemy as sa


revision = "017_candle_revision_immutability"
down_revision = "016_strategy_forensic_hardening"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Alembic's default version table uses VARCHAR(32).  The next revision ID
    # is longer, so widen it before Alembic records this revision and proceeds
    # to migration 018.  This is intentionally in the last 32-character
    # revision that precedes the longer ID.
    op.alter_column(
        "alembic_version",
        "version_num",
        existing_type=sa.String(length=32),
        type_=sa.String(length=64),
        existing_nullable=False,
    )
    # A raw row can be deliberately removed by an operational repair/retention
    # action.  Its prior revisions remain for lineage, so a later re-ingest of
    # the same asset/minute must not restart at revision one and collide with
    # (or silently disappear from) that ledger.
    op.execute(
        """
        CREATE OR REPLACE FUNCTION enforce_raw_candle_revision() RETURNS trigger AS $$
        DECLARE
            old_priority integer;
            new_priority integer;
            prior_revision integer;
        BEGIN
            IF TG_OP = 'INSERT' THEN
                SELECT max(data_revision)
                  INTO prior_revision
                  FROM candle_revisions
                 WHERE asset_id = NEW.asset_id
                   AND timestamp = NEW.timestamp;
                IF prior_revision IS NULL THEN
                    IF NEW.data_revision <> 1 THEN
                        RAISE EXCEPTION 'initial canonical candle revision must be 1';
                    END IF;
                ELSIF NEW.data_revision = 1 THEN
                    -- ORM defaults use one for a new raw row.  Preserve the
                    -- historical ledger by promoting that re-ingest to the
                    -- next immutable canonical revision automatically.
                    NEW.data_revision := prior_revision + 1;
                ELSIF NEW.data_revision <> prior_revision + 1 THEN
                    RAISE EXCEPTION 'reingested canonical candle must advance data_revision exactly once';
                END IF;
            END IF;
            IF TG_OP = 'UPDATE' AND (
                NEW.asset_id IS DISTINCT FROM OLD.asset_id OR
                NEW.timestamp IS DISTINCT FROM OLD.timestamp
            ) THEN
                RAISE EXCEPTION 'canonical candle identity is immutable';
            END IF;
            IF TG_OP = 'UPDATE' AND (
                NEW.open IS DISTINCT FROM OLD.open OR
                NEW.high IS DISTINCT FROM OLD.high OR
                NEW.low IS DISTINCT FROM OLD.low OR
                NEW.close IS DISTINCT FROM OLD.close OR
                NEW.volume IS DISTINCT FROM OLD.volume OR
                NEW.source IS DISTINCT FROM OLD.source OR
                NEW.source_event_time IS DISTINCT FROM OLD.source_event_time OR
                NEW.source_received_at IS DISTINCT FROM OLD.source_received_at
            ) THEN
                old_priority := CASE OLD.source
                    WHEN 'binance_rest' THEN 30
                    WHEN 'binance_ws' THEN 20
                    WHEN 'legacy' THEN 0
                    ELSE 10
                END;
                new_priority := CASE NEW.source
                    WHEN 'binance_rest' THEN 30
                    WHEN 'binance_ws' THEN 20
                    WHEN 'legacy' THEN 0
                    ELSE 10
                END;
                IF new_priority < old_priority THEN
                    RAISE EXCEPTION 'lower-priority candle source cannot overwrite canonical data';
                END IF;
                IF NEW.data_revision <> OLD.data_revision + 1 THEN
                    RAISE EXCEPTION 'canonical candle correction or provenance update must advance data_revision exactly once';
                END IF;
            ELSIF TG_OP = 'UPDATE' AND NEW.data_revision IS DISTINCT FROM OLD.data_revision THEN
                RAISE EXCEPTION 'data_revision cannot change without a canonical candle or provenance update';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute("DROP TRIGGER IF EXISTS raw_1m_candles_revision_enforced ON raw_1m_candles;")
    op.execute(
        """
        CREATE TRIGGER raw_1m_candles_revision_enforced
        BEFORE INSERT OR UPDATE ON raw_1m_candles
        FOR EACH ROW EXECUTE FUNCTION enforce_raw_candle_revision();
        """
    )

    # Foreign-key cascades that delete an asset must still be able to remove its
    # dependent history.  They invoke this trigger from a nested RI trigger;
    # direct UPDATE/DELETE statements invoke it at depth one and are rejected.
    op.execute(
        """
        CREATE FUNCTION prevent_candle_revision_mutation() RETURNS trigger AS $$
        BEGIN
            IF pg_trigger_depth() > 1 THEN
                IF TG_OP = 'DELETE' THEN
                    RETURN OLD;
                END IF;
                RETURN NEW;
            END IF;
            RAISE EXCEPTION 'candle_revisions is append-only';
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER candle_revisions_append_only
        BEFORE UPDATE OR DELETE ON candle_revisions
        FOR EACH ROW EXECUTE FUNCTION prevent_candle_revision_mutation();
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS candle_revisions_append_only ON candle_revisions;")
    op.execute("DROP FUNCTION IF EXISTS prevent_candle_revision_mutation();")
    op.execute("DROP TRIGGER IF EXISTS raw_1m_candles_revision_enforced ON raw_1m_candles;")
    op.execute(
        """
        CREATE OR REPLACE FUNCTION enforce_raw_candle_revision() RETURNS trigger AS $$
        DECLARE
            old_priority integer;
            new_priority integer;
        BEGIN
            IF TG_OP = 'INSERT' AND NEW.data_revision <> 1 THEN
                RAISE EXCEPTION 'initial canonical candle revision must be 1';
            END IF;
            IF TG_OP = 'UPDATE' AND (
                NEW.asset_id IS DISTINCT FROM OLD.asset_id OR
                NEW.timestamp IS DISTINCT FROM OLD.timestamp
            ) THEN
                RAISE EXCEPTION 'canonical candle identity is immutable';
            END IF;
            IF TG_OP = 'UPDATE' AND (
                NEW.open IS DISTINCT FROM OLD.open OR
                NEW.high IS DISTINCT FROM OLD.high OR
                NEW.low IS DISTINCT FROM OLD.low OR
                NEW.close IS DISTINCT FROM OLD.close OR
                NEW.volume IS DISTINCT FROM OLD.volume OR
                NEW.source IS DISTINCT FROM OLD.source OR
                NEW.source_event_time IS DISTINCT FROM OLD.source_event_time OR
                NEW.source_received_at IS DISTINCT FROM OLD.source_received_at
            ) THEN
                old_priority := CASE OLD.source
                    WHEN 'binance_rest' THEN 30
                    WHEN 'binance_ws' THEN 20
                    WHEN 'legacy' THEN 0
                    ELSE 10
                END;
                new_priority := CASE NEW.source
                    WHEN 'binance_rest' THEN 30
                    WHEN 'binance_ws' THEN 20
                    WHEN 'legacy' THEN 0
                    ELSE 10
                END;
                IF new_priority < old_priority THEN
                    RAISE EXCEPTION 'lower-priority candle source cannot overwrite canonical data';
                END IF;
                IF NEW.data_revision <> OLD.data_revision + 1 THEN
                    RAISE EXCEPTION 'canonical candle correction or provenance update must advance data_revision exactly once';
                END IF;
            ELSIF TG_OP = 'UPDATE' AND NEW.data_revision IS DISTINCT FROM OLD.data_revision THEN
                RAISE EXCEPTION 'data_revision cannot change without a canonical candle or provenance update';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    # The current revision ID is still 32 characters while this downgrade runs,
    # so returning the version table to its original shape is safe.
    op.alter_column(
        "alembic_version",
        "version_num",
        existing_type=sa.String(length=64),
        type_=sa.String(length=32),
        existing_nullable=False,
    )
    op.execute(
        """
        CREATE TRIGGER raw_1m_candles_revision_enforced
        BEFORE UPDATE ON raw_1m_candles
        FOR EACH ROW EXECUTE FUNCTION enforce_raw_candle_revision();
        """
    )
