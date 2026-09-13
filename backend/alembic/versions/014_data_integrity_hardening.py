"""canonical candle data integrity and lineage

Revision ID: 014_data_integrity_hardening
Revises: 013_strategy_monitoring
Create Date: 2026-09-13 00:00:00.000000

``sync_ranges`` is a statement about canonical raw coverage.  This migration
hardens the raw/staging boundary so each accepted canonical correction has
source provenance and an append-only revision record.
"""

from alembic import op
import sqlalchemy as sa


revision = "014_data_integrity_hardening"
down_revision = "013_strategy_monitoring"
branch_labels = None
depends_on = None


_TABLES = ("raw_1m_candles", "gap_staging_candles")


def _add_integrity_columns(table_name: str) -> None:
    # Existing rows predate source capture.  Preserve that fact rather than
    # misrepresenting them as received from a live connector.
    op.add_column(
        table_name,
        sa.Column("source", sa.String(length=64), nullable=False, server_default=sa.text("'legacy'")),
    )
    op.add_column(table_name, sa.Column("source_event_time", sa.DateTime(timezone=True), nullable=True))
    op.add_column(table_name, sa.Column("source_received_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column(
        table_name,
        sa.Column("data_revision", sa.Integer(), nullable=False, server_default=sa.text("1")),
    )
    # New direct SQL writers cannot silently masquerade as historical data.
    op.alter_column(table_name, "source", server_default=sa.text("'unknown'"))


def _add_integrity_constraints(table_name: str) -> None:
    finite = " AND ".join(
        f"{column} > '-Infinity'::double precision AND {column} < 'Infinity'::double precision"
        for column in ("open", "high", "low", "close", "volume")
    )
    ohlcv = (
        f"{finite} AND open > 0 AND high > 0 AND low > 0 AND close > 0 AND volume >= 0 "
        "AND high >= low AND open >= low AND open <= high AND close >= low AND close <= high"
    )
    op.create_check_constraint(f"ck_{table_name}_ohlcv_valid", table_name, ohlcv)
    op.create_check_constraint(
        f"ck_{table_name}_timestamp_minute",
        table_name,
        "date_trunc('minute', timestamp) = timestamp",
    )
    op.create_check_constraint(
        f"ck_{table_name}_source_nonempty", table_name, "length(btrim(source)) > 0"
    )
    op.create_check_constraint(
        f"ck_{table_name}_positive_revision", table_name, "data_revision > 0"
    )


def upgrade() -> None:
    for table_name in _TABLES:
        _add_integrity_columns(table_name)
        _add_integrity_constraints(table_name)

    op.create_table(
        "candle_revisions",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("asset_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("data_revision", sa.Integer(), nullable=False),
        sa.Column("open", sa.Float(), nullable=False),
        sa.Column("high", sa.Float(), nullable=False),
        sa.Column("low", sa.Float(), nullable=False),
        sa.Column("close", sa.Float(), nullable=False),
        sa.Column("volume", sa.Float(), nullable=False),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("source_event_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("source_received_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint(
            "open > '-Infinity'::double precision AND open < 'Infinity'::double precision "
            "AND high > '-Infinity'::double precision AND high < 'Infinity'::double precision "
            "AND low > '-Infinity'::double precision AND low < 'Infinity'::double precision "
            "AND close > '-Infinity'::double precision AND close < 'Infinity'::double precision "
            "AND volume > '-Infinity'::double precision AND volume < 'Infinity'::double precision "
            "AND open > 0 AND high > 0 AND low > 0 AND close > 0 AND volume >= 0 "
            "AND high >= low AND open >= low AND open <= high AND close >= low AND close <= high",
            name="ck_candle_revisions_ohlcv_valid",
        ),
        sa.CheckConstraint(
            "date_trunc('minute', timestamp) = timestamp",
            name="ck_candle_revisions_timestamp_minute",
        ),
        sa.CheckConstraint("length(btrim(source)) > 0", name="ck_candle_revisions_source_nonempty"),
        sa.CheckConstraint("data_revision > 0", name="ck_candle_revisions_positive_revision"),
        sa.ForeignKeyConstraint(["asset_id"], ["asset_registry.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "asset_id",
            "timestamp",
            "data_revision",
            name="uq_candle_revisions_asset_timestamp_revision",
        ),
    )
    op.create_index(
        "ix_candle_revisions_asset_timestamp_revision",
        "candle_revisions",
        ["asset_id", "timestamp", "data_revision"],
        unique=False,
    )

    # Seed lineage for rows that were canonical before this migration.  Their
    # source remains explicitly ``legacy`` rather than being inferred.
    op.execute(
        """
        INSERT INTO candle_revisions (
            asset_id, timestamp, data_revision, open, high, low, close, volume,
            source, source_event_time, source_received_at
        )
        SELECT
            asset_id, timestamp, data_revision, open, high, low, close, volume,
            source, source_event_time, source_received_at
        FROM raw_1m_candles
        ON CONFLICT (asset_id, timestamp, data_revision) DO NOTHING
        """
    )

    # Keep the lineage invariant at the database boundary too.  Application code
    # is deliberately idempotent, but direct SQL writers must not be able to
    # overwrite canonical data without both source precedence and a new revision.
    op.execute(
        """
        CREATE FUNCTION enforce_raw_candle_revision() RETURNS trigger AS $$
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
    op.execute(
        """
        CREATE TRIGGER raw_1m_candles_revision_enforced
        BEFORE UPDATE ON raw_1m_candles
        FOR EACH ROW EXECUTE FUNCTION enforce_raw_candle_revision();
        """
    )
    op.execute(
        """
        CREATE FUNCTION append_raw_candle_revision() RETURNS trigger AS $$
        BEGIN
            INSERT INTO candle_revisions (
                asset_id, timestamp, data_revision, open, high, low, close, volume,
                source, source_event_time, source_received_at
            ) VALUES (
                NEW.asset_id, NEW.timestamp, NEW.data_revision,
                NEW.open, NEW.high, NEW.low, NEW.close, NEW.volume,
                NEW.source, NEW.source_event_time, NEW.source_received_at
            ) ON CONFLICT (asset_id, timestamp, data_revision) DO NOTHING;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER raw_1m_candles_revision_appended
        AFTER INSERT OR UPDATE ON raw_1m_candles
        FOR EACH ROW EXECUTE FUNCTION append_raw_candle_revision();
        """
    )
    # A raw delete can happen through an operational repair or retention action.
    # It is safer to discard affected coverage metadata than let sync_ranges lie.
    op.execute(
        """
        CREATE FUNCTION invalidate_sync_range_on_raw_candle_delete() RETURNS trigger AS $$
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
    op.execute(
        """
        CREATE TRIGGER raw_1m_candles_delete_invalidates_coverage
        AFTER DELETE ON raw_1m_candles
        FOR EACH ROW EXECUTE FUNCTION invalidate_sync_range_on_raw_candle_delete();
        """
    )
    # Earlier releases could advance sync_ranges while a historical payload was
    # still only staged.  Rebuild the materialized assertion from canonical raw
    # rows once during upgrade, preserving every truly contiguous raw interval
    # and dropping every claim that cannot be proven.
    op.execute("DELETE FROM sync_ranges")
    op.execute(
        """
        WITH sequenced AS (
            SELECT
                asset_id,
                timestamp,
                timestamp - (row_number() OVER (
                    PARTITION BY asset_id ORDER BY timestamp
                ) * INTERVAL '1 minute') AS contiguous_group
            FROM raw_1m_candles
        )
        INSERT INTO sync_ranges (asset_id, start_timestamp, end_timestamp)
        SELECT asset_id, min(timestamp), max(timestamp)
        FROM sequenced
        GROUP BY asset_id, contiguous_group
        """
    )
    # `sync_ranges` is a high-value availability assertion consumed by repair,
    # query, and strategy systems.  Enforce its meaning even for direct ORM/SQL
    # writers that bypass IngestionService.
    op.execute(
        """
        CREATE FUNCTION enforce_sync_range_raw_coverage() RETURNS trigger AS $$
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
        CREATE TRIGGER sync_ranges_raw_coverage_enforced
        BEFORE INSERT OR UPDATE OF asset_id, start_timestamp, end_timestamp ON sync_ranges
        FOR EACH ROW EXECUTE FUNCTION enforce_sync_range_raw_coverage();
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS sync_ranges_raw_coverage_enforced ON sync_ranges;")
    op.execute("DROP FUNCTION IF EXISTS enforce_sync_range_raw_coverage();")
    op.execute("DROP TRIGGER IF EXISTS raw_1m_candles_delete_invalidates_coverage ON raw_1m_candles;")
    op.execute("DROP FUNCTION IF EXISTS invalidate_sync_range_on_raw_candle_delete();")
    op.execute("DROP TRIGGER IF EXISTS raw_1m_candles_revision_appended ON raw_1m_candles;")
    op.execute("DROP FUNCTION IF EXISTS append_raw_candle_revision();")
    op.execute("DROP TRIGGER IF EXISTS raw_1m_candles_revision_enforced ON raw_1m_candles;")
    op.execute("DROP FUNCTION IF EXISTS enforce_raw_candle_revision();")
    op.drop_index("ix_candle_revisions_asset_timestamp_revision", table_name="candle_revisions")
    op.drop_table("candle_revisions")

    for table_name in reversed(_TABLES):
        op.drop_constraint(f"ck_{table_name}_positive_revision", table_name, type_="check")
        op.drop_constraint(f"ck_{table_name}_source_nonempty", table_name, type_="check")
        op.drop_constraint(f"ck_{table_name}_timestamp_minute", table_name, type_="check")
        op.drop_constraint(f"ck_{table_name}_ohlcv_valid", table_name, type_="check")
        op.drop_column(table_name, "data_revision")
        op.drop_column(table_name, "source_received_at")
        op.drop_column(table_name, "source_event_time")
        op.drop_column(table_name, "source")
