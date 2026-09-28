ALTER TABLE event_projections
    ADD COLUMN sync_meta JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN local_revision BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN last_reconciled_at TIMESTAMPTZ;

CREATE INDEX idx_event_projections_reconciliation_due
    ON event_projections (last_reconciled_at NULLS FIRST, updated_at, event_id);

CREATE TABLE reconciliation_applied (
    merge_id UUID PRIMARY KEY,
    event_id UUID NOT NULL REFERENCES event_projections(event_id),
    applied_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE reconciliation_conflicts (
    conflict_key VARCHAR(64) PRIMARY KEY,
    merge_id UUID NOT NULL,
    event_id UUID NOT NULL,
    field_name VARCHAR(50) NOT NULL,
    left_value JSONB,
    right_value JSONB,
    winner_value JSONB,
    resolution VARCHAR(50) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_reconciliation_conflicts_event
    ON reconciliation_conflicts (event_id, created_at);
