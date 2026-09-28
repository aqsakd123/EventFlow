ALTER TABLE events
    ADD COLUMN sync_meta JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE TABLE reconciliation_applied (
    merge_id UUID PRIMARY KEY,
    event_id UUID NOT NULL REFERENCES events(id),
    applied_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_event_reconciliation_applied_event
    ON reconciliation_applied (event_id, applied_at);
