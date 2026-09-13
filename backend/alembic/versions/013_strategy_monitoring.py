"""continuous strategy monitoring

Revision ID: 013_strategy_monitoring
Revises: 012_strategy_divergence
Create Date: 2026-09-13 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "013_strategy_monitoring"
down_revision = "012_strategy_divergence"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "strategy_monitoring_bindings",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("activation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("reference_run_id", sa.String(length=64), nullable=False),
        sa.Column("comparison_policy_id", sa.String(length=128), nullable=False),
        sa.Column("comparison_policy_revision", sa.String(length=128), nullable=False),
        sa.Column("comparison_policy_hash", sa.String(length=64), nullable=False),
        sa.Column("policy_id", sa.String(length=128), nullable=False),
        sa.Column("policy_revision", sa.String(length=128), nullable=False),
        sa.Column("policy_hash", sa.String(length=64), nullable=False),
        sa.Column("policy", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, server_default=sa.text("'active'")),
        sa.Column("registered_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("deactivated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_worker_id", sa.String(length=128), nullable=True),
        sa.Column("lease_token", sa.String(length=128), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("claim_attempt_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint(
            "policy_hash ~ '^[0-9a-f]{64}$' AND comparison_policy_hash ~ '^[0-9a-f]{64}$'",
            name="ck_monitoring_bindings_policy_hashes",
        ),
        sa.CheckConstraint(
            "status IN ('active', 'deactivated', 'error')",
            name="ck_monitoring_bindings_status",
        ),
        sa.CheckConstraint("claim_attempt_count >= 0", name="ck_monitoring_bindings_nonnegative_claims"),
        sa.CheckConstraint(
            "(lease_worker_id IS NULL AND lease_token IS NULL AND lease_expires_at IS NULL) "
            "OR (lease_worker_id IS NOT NULL AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)",
            name="ck_monitoring_bindings_complete_lease",
        ),
        sa.CheckConstraint(
            "deactivated_at IS NULL OR deactivated_at >= registered_at",
            name="ck_monitoring_bindings_valid_deactivation",
        ),
        sa.ForeignKeyConstraint(["activation_id"], ["live_strategy_activations.id"]),
        sa.ForeignKeyConstraint(["reference_run_id"], ["backtest_results.run_id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    for name, columns in (
        ("ix_strategy_monitoring_bindings_activation_id", ["activation_id"]),
        ("ix_strategy_monitoring_bindings_reference_run_id", ["reference_run_id"]),
        ("ix_strategy_monitoring_bindings_status", ["status"]),
        ("ix_strategy_monitoring_bindings_lease_expires_at", ["lease_expires_at"]),
        ("ix_strategy_monitoring_bindings_last_processed_at", ["last_processed_at"]),
        ("ix_monitoring_bindings_claimable", ["status", "lease_expires_at", "last_processed_at"]),
    ):
        op.create_index(name, "strategy_monitoring_bindings", columns, unique=False)
    op.create_index(
        "uq_monitoring_bindings_active_activation",
        "strategy_monitoring_bindings",
        ["activation_id"],
        unique=True,
        postgresql_where=sa.text("status = 'active'"),
    )

    op.create_table(
        "strategy_health_states",
        sa.Column("activation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("binding_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, server_default=sa.text("'unknown'")),
        sa.Column("cause", sa.String(length=32), nullable=True),
        sa.Column("severity", sa.String(length=16), nullable=True),
        sa.Column("last_comparison_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_complete_comparison_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_complete_window_end", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_healthy_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_evaluated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("consecutive_healthy_checks", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("last_error", sa.String(length=2000), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint(
            "status IN ('unknown', 'healthy', 'degraded', 'stale', 'error')",
            name="ck_strategy_health_states_status",
        ),
        sa.CheckConstraint(
            "cause IS NULL OR cause IN "
            "('data_quality', 'execution', 'strategy_behavior', 'strategy_failure', 'reference', 'runtime')",
            name="ck_strategy_health_states_cause",
        ),
        sa.CheckConstraint(
            "severity IS NULL OR severity IN ('info', 'warning', 'critical')",
            name="ck_strategy_health_states_severity",
        ),
        sa.CheckConstraint(
            "consecutive_healthy_checks >= 0",
            name="ck_strategy_health_states_nonnegative_recovery",
        ),
        sa.CheckConstraint(
            "(status IN ('unknown', 'healthy') AND cause IS NULL AND severity IS NULL) "
            "OR (status NOT IN ('unknown', 'healthy') AND cause IS NOT NULL AND severity IS NOT NULL)",
            name="ck_strategy_health_states_status_evidence",
        ),
        sa.ForeignKeyConstraint(["activation_id"], ["live_strategy_activations.id"]),
        sa.ForeignKeyConstraint(["binding_id"], ["strategy_monitoring_bindings.id"]),
        sa.PrimaryKeyConstraint("activation_id"),
        sa.UniqueConstraint("binding_id", name="uq_strategy_health_states_binding"),
    )

    op.create_table(
        "strategy_incidents",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("dedup_key", sa.String(length=64), nullable=False),
        sa.Column("binding_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("activation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("reference_run_id", sa.String(length=64), nullable=False),
        sa.Column("monitoring_policy_hash", sa.String(length=64), nullable=False),
        sa.Column("strategy_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("strategy_version_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("strategy_version_number", sa.Integer(), nullable=False),
        sa.Column("strategy_definition_hash", sa.String(length=64), nullable=False),
        sa.Column("asset_id", sa.Integer(), nullable=True),
        sa.Column("divergence_type", sa.String(length=64), nullable=False),
        sa.Column("category", sa.String(length=128), nullable=False),
        sa.Column("cause", sa.String(length=32), nullable=False),
        sa.Column("severity", sa.String(length=16), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, server_default=sa.text("'open'")),
        sa.Column("first_detected_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_detected_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("occurrence_count", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column("acknowledged_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("acknowledged_by", sa.String(length=128), nullable=True),
        sa.Column("acknowledgement_note", sa.String(length=2000), nullable=True),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resolved_by", sa.String(length=128), nullable=True),
        sa.Column("resolution_note", sa.String(length=2000), nullable=True),
        sa.Column("latest_comparison_run_id", sa.String(length=64), nullable=True),
        sa.Column("latest_finding_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("source_reference", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("evidence_summary", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["binding_id"], ["strategy_monitoring_bindings.id"]),
        sa.ForeignKeyConstraint(["activation_id"], ["live_strategy_activations.id"]),
        sa.ForeignKeyConstraint(["reference_run_id"], ["backtest_results.run_id"]),
        sa.ForeignKeyConstraint(["strategy_id"], ["strategies.id"]),
        sa.ForeignKeyConstraint(
            ["strategy_version_id", "strategy_id"],
            ["strategy_versions.id", "strategy_versions.strategy_id"],
            name="fk_strategy_incidents_strategy_version_strategy",
        ),
        sa.ForeignKeyConstraint(["asset_id"], ["asset_registry.id"]),
        sa.ForeignKeyConstraint(["latest_comparison_run_id"], ["strategy_comparison_runs.run_id"]),
        sa.ForeignKeyConstraint(["latest_finding_id"], ["strategy_divergence_findings.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("dedup_key", name="uq_strategy_incidents_dedup_key"),
        sa.CheckConstraint("dedup_key ~ '^[0-9a-f]{64}$'", name="ck_strategy_incidents_dedup_key"),
        sa.CheckConstraint(
            "monitoring_policy_hash ~ '^[0-9a-f]{64}$'",
            name="ck_strategy_incidents_policy_hash",
        ),
        sa.CheckConstraint("strategy_version_number > 0", name="ck_strategy_incidents_positive_version"),
        sa.CheckConstraint(
            "strategy_definition_hash ~ '^[0-9a-f]{64}$'",
            name="ck_strategy_incidents_definition_hash",
        ),
        sa.CheckConstraint(
            "cause IN ('data_quality', 'execution', 'strategy_behavior', 'strategy_failure', 'reference', 'runtime')",
            name="ck_strategy_incidents_cause",
        ),
        sa.CheckConstraint("severity IN ('info', 'warning', 'critical')", name="ck_strategy_incidents_severity"),
        sa.CheckConstraint("status IN ('open', 'acknowledged', 'resolved')", name="ck_strategy_incidents_status"),
        sa.CheckConstraint("occurrence_count > 0", name="ck_strategy_incidents_positive_occurrences"),
        sa.CheckConstraint(
            "last_detected_at >= first_detected_at",
            name="ck_strategy_incidents_valid_detection_window",
        ),
        sa.CheckConstraint(
            "acknowledged_at IS NULL OR acknowledged_by IS NOT NULL",
            name="ck_strategy_incidents_ack_actor",
        ),
        sa.CheckConstraint(
            "status <> 'acknowledged' OR acknowledged_at IS NOT NULL",
            name="ck_strategy_incidents_ack_state",
        ),
        sa.CheckConstraint(
            "status <> 'resolved' OR resolved_at IS NOT NULL",
            name="ck_strategy_incidents_resolution_state",
        ),
    )
    for name, columns in (
        ("ix_strategy_incidents_binding_id", ["binding_id"]),
        ("ix_strategy_incidents_activation_id", ["activation_id"]),
        ("ix_strategy_incidents_reference_run_id", ["reference_run_id"]),
        ("ix_strategy_incidents_strategy_id", ["strategy_id"]),
        ("ix_strategy_incidents_strategy_version_id", ["strategy_version_id"]),
        ("ix_strategy_incidents_asset_id", ["asset_id"]),
        ("ix_strategy_incidents_divergence_type", ["divergence_type"]),
        ("ix_strategy_incidents_status", ["status"]),
        ("ix_strategy_incidents_last_detected_at", ["last_detected_at"]),
        ("ix_strategy_incidents_activation_status", ["activation_id", "status"]),
        ("ix_strategy_incidents_strategy_status", ["strategy_id", "status"]),
    ):
        op.create_index(name, "strategy_incidents", columns, unique=False)

    op.create_table(
        "strategy_incident_evidence",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("incident_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("finding_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("comparison_run_id", sa.String(length=64), nullable=True),
        sa.Column("evidence_key", sa.String(length=64), nullable=False),
        sa.Column("evidence_kind", sa.String(length=32), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_reference", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["incident_id"], ["strategy_incidents.id"]),
        sa.ForeignKeyConstraint(["finding_id"], ["strategy_divergence_findings.id"]),
        sa.ForeignKeyConstraint(["comparison_run_id"], ["strategy_comparison_runs.run_id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("incident_id", "evidence_key", name="uq_strategy_incident_evidence_key"),
        sa.UniqueConstraint("finding_id", name="uq_strategy_incident_evidence_finding"),
        sa.CheckConstraint("evidence_key ~ '^[0-9a-f]{64}$'", name="ck_strategy_incident_evidence_key"),
        sa.CheckConstraint(
            "evidence_kind IN ('divergence_finding', 'stale_strategy_state', 'stale_comparison_evidence', 'runtime_state')",
            name="ck_strategy_incident_evidence_kind",
        ),
        sa.CheckConstraint(
            "(evidence_kind = 'divergence_finding' AND finding_id IS NOT NULL AND comparison_run_id IS NOT NULL) "
            "OR (evidence_kind IN ('stale_strategy_state', 'stale_comparison_evidence', 'runtime_state') AND finding_id IS NULL AND comparison_run_id IS NULL)",
            name="ck_strategy_incident_evidence_source_shape",
        ),
    )
    for name, columns in (
        ("ix_strategy_incident_evidence_incident_id", ["incident_id"]),
        ("ix_strategy_incident_evidence_comparison_run_id", ["comparison_run_id"]),
    ):
        op.create_index(name, "strategy_incident_evidence", columns, unique=False)

    op.create_table(
        "strategy_monitoring_assessments",
        sa.Column("comparison_run_id", sa.String(length=64), nullable=False),
        sa.Column("binding_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("activation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("reference_run_id", sa.String(length=64), nullable=False),
        sa.Column("comparison_status", sa.String(length=32), nullable=False),
        sa.Column("finding_count", sa.Integer(), nullable=False),
        sa.Column("processor_worker_id", sa.String(length=128), nullable=True),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["comparison_run_id"], ["strategy_comparison_runs.run_id"]),
        sa.ForeignKeyConstraint(["binding_id"], ["strategy_monitoring_bindings.id"]),
        sa.ForeignKeyConstraint(["activation_id"], ["live_strategy_activations.id"]),
        sa.ForeignKeyConstraint(["reference_run_id"], ["backtest_results.run_id"]),
        sa.PrimaryKeyConstraint("comparison_run_id"),
        sa.CheckConstraint(
            "comparison_run_id ~ '^[0-9a-f]{64}$'",
            name="ck_monitoring_assessments_run_id",
        ),
        sa.CheckConstraint(
            "comparison_status IN ('healthy', 'divergent', 'insufficient_reference')",
            name="ck_monitoring_assessments_status",
        ),
        sa.CheckConstraint("finding_count >= 0", name="ck_monitoring_assessments_nonnegative_findings"),
    )
    for name, columns in (
        ("ix_strategy_monitoring_assessments_binding_id", ["binding_id"]),
        ("ix_strategy_monitoring_assessments_activation_id", ["activation_id"]),
    ):
        op.create_index(name, "strategy_monitoring_assessments", columns, unique=False)

    op.execute(
        """
        CREATE FUNCTION validate_strategy_monitoring_binding_reference() RETURNS trigger AS $$
        DECLARE
            activation_strategy uuid;
            reference_strategy uuid;
        BEGIN
            SELECT strategy_id INTO activation_strategy
              FROM live_strategy_activations WHERE id = NEW.activation_id;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'monitoring binding references an unknown live activation';
            END IF;
            SELECT strategy_id INTO reference_strategy
              FROM backtest_results WHERE run_id = NEW.reference_run_id;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'monitoring binding references an unknown backtest result';
            END IF;
            IF activation_strategy <> reference_strategy THEN
                RAISE EXCEPTION 'monitoring binding activation and reference must belong to the same strategy';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER strategy_monitoring_binding_reference_valid
        BEFORE INSERT ON strategy_monitoring_bindings
        FOR EACH ROW EXECUTE FUNCTION validate_strategy_monitoring_binding_reference();
        """
    )
    op.execute(
        """
        CREATE FUNCTION prevent_strategy_monitoring_binding_identity_mutation() RETURNS trigger AS $$
        BEGIN
            IF NEW.activation_id IS DISTINCT FROM OLD.activation_id
               OR NEW.reference_run_id IS DISTINCT FROM OLD.reference_run_id
               OR NEW.comparison_policy_id IS DISTINCT FROM OLD.comparison_policy_id
               OR NEW.comparison_policy_revision IS DISTINCT FROM OLD.comparison_policy_revision
               OR NEW.comparison_policy_hash IS DISTINCT FROM OLD.comparison_policy_hash
               OR NEW.policy_id IS DISTINCT FROM OLD.policy_id
               OR NEW.policy_revision IS DISTINCT FROM OLD.policy_revision
               OR NEW.policy_hash IS DISTINCT FROM OLD.policy_hash
               OR NEW.policy IS DISTINCT FROM OLD.policy THEN
                RAISE EXCEPTION 'strategy monitoring binding policy and scope are immutable';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER strategy_monitoring_binding_identity_immutable
        BEFORE UPDATE ON strategy_monitoring_bindings
        FOR EACH ROW EXECUTE FUNCTION prevent_strategy_monitoring_binding_identity_mutation();
        """
    )
    op.execute(
        """
        CREATE FUNCTION validate_strategy_health_state_binding() RETURNS trigger AS $$
        DECLARE
            binding_activation uuid;
        BEGIN
            SELECT activation_id INTO binding_activation
              FROM strategy_monitoring_bindings WHERE id = NEW.binding_id;
            IF NOT FOUND OR binding_activation <> NEW.activation_id THEN
                RAISE EXCEPTION 'strategy health state must match its monitoring binding activation';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER strategy_health_state_binding_valid
        BEFORE INSERT OR UPDATE OF activation_id, binding_id ON strategy_health_states
        FOR EACH ROW EXECUTE FUNCTION validate_strategy_health_state_binding();
        """
    )
    op.execute(
        """
        CREATE FUNCTION validate_strategy_incident_identity() RETURNS trigger AS $$
        DECLARE
            activation_strategy uuid;
            activation_version uuid;
            activation_version_number integer;
            activation_definition_hash text;
            binding_activation uuid;
            binding_reference text;
            binding_policy_hash text;
            finding_run text;
            finding_activation uuid;
            finding_reference text;
        BEGIN
            SELECT strategy_id, strategy_version_id, strategy_version_number, strategy_definition_hash
              INTO activation_strategy, activation_version, activation_version_number, activation_definition_hash
              FROM live_strategy_activations WHERE id = NEW.activation_id;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'strategy incident references an unknown live activation';
            END IF;
            SELECT activation_id, reference_run_id, policy_hash
              INTO binding_activation, binding_reference, binding_policy_hash
              FROM strategy_monitoring_bindings WHERE id = NEW.binding_id;
            IF NOT FOUND
               OR binding_activation <> NEW.activation_id
               OR binding_reference <> NEW.reference_run_id
               OR binding_policy_hash <> NEW.monitoring_policy_hash THEN
                RAISE EXCEPTION 'strategy incident does not match its monitoring binding scope or policy';
            END IF;
            IF NEW.strategy_id <> activation_strategy
               OR NEW.strategy_version_id <> activation_version
               OR NEW.strategy_version_number <> activation_version_number
               OR NEW.strategy_definition_hash <> activation_definition_hash THEN
                RAISE EXCEPTION 'strategy incident identity does not match its live activation';
            END IF;
            IF (NEW.latest_comparison_run_id IS NULL) <> (NEW.latest_finding_id IS NULL) THEN
                RAISE EXCEPTION 'strategy incident latest comparison/finding references must be paired';
            END IF;
            IF NEW.latest_finding_id IS NOT NULL THEN
                SELECT finding.comparison_run_id, run.activation_id, run.reference_run_id
                  INTO finding_run, finding_activation, finding_reference
                  FROM strategy_divergence_findings AS finding
                  JOIN strategy_comparison_runs AS run ON run.run_id = finding.comparison_run_id
                 WHERE finding.id = NEW.latest_finding_id;
                IF NOT FOUND
                   OR finding_run <> NEW.latest_comparison_run_id
                   OR finding_activation <> NEW.activation_id
                   OR finding_reference <> NEW.reference_run_id THEN
                    RAISE EXCEPTION 'strategy incident latest evidence does not match its scope';
                END IF;
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER strategy_incident_identity_valid
        BEFORE INSERT OR UPDATE ON strategy_incidents
        FOR EACH ROW EXECUTE FUNCTION validate_strategy_incident_identity();
        """
    )
    op.execute(
        """
        CREATE FUNCTION prevent_strategy_incident_identity_mutation() RETURNS trigger AS $$
        BEGIN
            IF NEW.dedup_key IS DISTINCT FROM OLD.dedup_key
               OR NEW.binding_id IS DISTINCT FROM OLD.binding_id
               OR NEW.activation_id IS DISTINCT FROM OLD.activation_id
               OR NEW.reference_run_id IS DISTINCT FROM OLD.reference_run_id
               OR NEW.monitoring_policy_hash IS DISTINCT FROM OLD.monitoring_policy_hash
               OR NEW.strategy_id IS DISTINCT FROM OLD.strategy_id
               OR NEW.strategy_version_id IS DISTINCT FROM OLD.strategy_version_id
               OR NEW.strategy_version_number IS DISTINCT FROM OLD.strategy_version_number
               OR NEW.strategy_definition_hash IS DISTINCT FROM OLD.strategy_definition_hash
               OR NEW.asset_id IS DISTINCT FROM OLD.asset_id
               OR NEW.divergence_type IS DISTINCT FROM OLD.divergence_type
               OR NEW.category IS DISTINCT FROM OLD.category
               OR NEW.cause IS DISTINCT FROM OLD.cause
               OR NEW.first_detected_at IS DISTINCT FROM OLD.first_detected_at THEN
                RAISE EXCEPTION 'strategy incident identity is immutable';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER strategy_incident_identity_immutable
        BEFORE UPDATE ON strategy_incidents
        FOR EACH ROW EXECUTE FUNCTION prevent_strategy_incident_identity_mutation();
        """
    )
    op.execute(
        """
        CREATE FUNCTION validate_strategy_incident_evidence_reference() RETURNS trigger AS $$
        DECLARE
            finding_run_id text;
            finding_activation uuid;
            finding_reference text;
            incident_activation uuid;
            incident_reference text;
        BEGIN
            IF NEW.evidence_kind = 'divergence_finding' THEN
                SELECT finding.comparison_run_id, run.activation_id, run.reference_run_id
                  INTO finding_run_id, finding_activation, finding_reference
                  FROM strategy_divergence_findings AS finding
                  JOIN strategy_comparison_runs AS run ON run.run_id = finding.comparison_run_id
                 WHERE finding.id = NEW.finding_id;
                IF NOT FOUND OR finding_run_id <> NEW.comparison_run_id THEN
                    RAISE EXCEPTION 'incident evidence finding and comparison run do not match';
                END IF;
                SELECT activation_id, reference_run_id INTO incident_activation, incident_reference
                  FROM strategy_incidents WHERE id = NEW.incident_id;
                IF NOT FOUND OR incident_activation <> finding_activation OR incident_reference <> finding_reference THEN
                    RAISE EXCEPTION 'incident evidence must match the activation and reference scope of its finding';
                END IF;
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER strategy_incident_evidence_reference_valid
        BEFORE INSERT ON strategy_incident_evidence
        FOR EACH ROW EXECUTE FUNCTION validate_strategy_incident_evidence_reference();
        """
    )
    op.execute(
        """
        CREATE FUNCTION prevent_strategy_incident_evidence_mutation() RETURNS trigger AS $$
        BEGIN
            RAISE EXCEPTION 'strategy incident evidence is immutable';
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER strategy_incident_evidence_immutable
        BEFORE UPDATE OR DELETE ON strategy_incident_evidence
        FOR EACH ROW EXECUTE FUNCTION prevent_strategy_incident_evidence_mutation();
        """
    )
    op.execute(
        """
        CREATE FUNCTION validate_strategy_monitoring_assessment_scope() RETURNS trigger AS $$
        DECLARE
            binding_activation uuid;
            binding_reference text;
            binding_comparison_policy_hash text;
            run_activation uuid;
            run_reference text;
            run_policy_hash text;
        BEGIN
            SELECT activation_id, reference_run_id, comparison_policy_hash
              INTO binding_activation, binding_reference, binding_comparison_policy_hash
              FROM strategy_monitoring_bindings WHERE id = NEW.binding_id;
            IF NOT FOUND OR binding_activation <> NEW.activation_id OR binding_reference <> NEW.reference_run_id THEN
                RAISE EXCEPTION 'monitoring assessment does not match its binding scope';
            END IF;
            SELECT activation_id, reference_run_id, policy_hash
              INTO run_activation, run_reference, run_policy_hash
              FROM strategy_comparison_runs WHERE run_id = NEW.comparison_run_id;
            IF NOT FOUND OR run_activation <> NEW.activation_id OR run_reference <> NEW.reference_run_id
               OR run_policy_hash <> binding_comparison_policy_hash THEN
                RAISE EXCEPTION 'monitoring assessment does not match its comparison scope';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER strategy_monitoring_assessment_scope_valid
        BEFORE INSERT ON strategy_monitoring_assessments
        FOR EACH ROW EXECUTE FUNCTION validate_strategy_monitoring_assessment_scope();
        """
    )
    op.execute(
        """
        CREATE FUNCTION prevent_strategy_monitoring_assessment_mutation() RETURNS trigger AS $$
        BEGIN
            RAISE EXCEPTION 'strategy monitoring assessments are immutable';
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER strategy_monitoring_assessment_immutable
        BEFORE UPDATE OR DELETE ON strategy_monitoring_assessments
        FOR EACH ROW EXECUTE FUNCTION prevent_strategy_monitoring_assessment_mutation();
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS strategy_monitoring_assessment_immutable ON strategy_monitoring_assessments;")
    op.execute("DROP FUNCTION IF EXISTS prevent_strategy_monitoring_assessment_mutation();")
    op.execute("DROP TRIGGER IF EXISTS strategy_monitoring_assessment_scope_valid ON strategy_monitoring_assessments;")
    op.execute("DROP FUNCTION IF EXISTS validate_strategy_monitoring_assessment_scope();")
    op.execute("DROP TRIGGER IF EXISTS strategy_incident_evidence_immutable ON strategy_incident_evidence;")
    op.execute("DROP FUNCTION IF EXISTS prevent_strategy_incident_evidence_mutation();")
    op.execute("DROP TRIGGER IF EXISTS strategy_incident_evidence_reference_valid ON strategy_incident_evidence;")
    op.execute("DROP FUNCTION IF EXISTS validate_strategy_incident_evidence_reference();")
    op.execute("DROP TRIGGER IF EXISTS strategy_incident_identity_immutable ON strategy_incidents;")
    op.execute("DROP FUNCTION IF EXISTS prevent_strategy_incident_identity_mutation();")
    op.execute("DROP TRIGGER IF EXISTS strategy_incident_identity_valid ON strategy_incidents;")
    op.execute("DROP FUNCTION IF EXISTS validate_strategy_incident_identity();")
    op.execute("DROP TRIGGER IF EXISTS strategy_health_state_binding_valid ON strategy_health_states;")
    op.execute("DROP FUNCTION IF EXISTS validate_strategy_health_state_binding();")
    op.execute("DROP TRIGGER IF EXISTS strategy_monitoring_binding_identity_immutable ON strategy_monitoring_bindings;")
    op.execute("DROP FUNCTION IF EXISTS prevent_strategy_monitoring_binding_identity_mutation();")
    op.execute("DROP TRIGGER IF EXISTS strategy_monitoring_binding_reference_valid ON strategy_monitoring_bindings;")
    op.execute("DROP FUNCTION IF EXISTS validate_strategy_monitoring_binding_reference();")

    for name in (
        "ix_strategy_monitoring_assessments_activation_id",
        "ix_strategy_monitoring_assessments_binding_id",
    ):
        op.drop_index(name, table_name="strategy_monitoring_assessments")
    op.drop_table("strategy_monitoring_assessments")
    for name in (
        "ix_strategy_incident_evidence_comparison_run_id",
        "ix_strategy_incident_evidence_incident_id",
    ):
        op.drop_index(name, table_name="strategy_incident_evidence")
    op.drop_table("strategy_incident_evidence")
    for name in (
        "ix_strategy_incidents_strategy_status",
        "ix_strategy_incidents_activation_status",
        "ix_strategy_incidents_last_detected_at",
        "ix_strategy_incidents_status",
        "ix_strategy_incidents_divergence_type",
        "ix_strategy_incidents_asset_id",
        "ix_strategy_incidents_strategy_version_id",
        "ix_strategy_incidents_strategy_id",
        "ix_strategy_incidents_reference_run_id",
        "ix_strategy_incidents_activation_id",
        "ix_strategy_incidents_binding_id",
    ):
        op.drop_index(name, table_name="strategy_incidents")
    op.drop_table("strategy_incidents")
    op.drop_table("strategy_health_states")
    for name in (
        "uq_monitoring_bindings_active_activation",
        "ix_monitoring_bindings_claimable",
        "ix_strategy_monitoring_bindings_last_processed_at",
        "ix_strategy_monitoring_bindings_lease_expires_at",
        "ix_strategy_monitoring_bindings_status",
        "ix_strategy_monitoring_bindings_reference_run_id",
        "ix_strategy_monitoring_bindings_activation_id",
    ):
        op.drop_index(name, table_name="strategy_monitoring_bindings")
    op.drop_table("strategy_monitoring_bindings")
