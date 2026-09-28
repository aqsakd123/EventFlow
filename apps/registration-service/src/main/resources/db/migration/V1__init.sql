CREATE TABLE event_projections (
    event_id UUID PRIMARY KEY,
    workspace_id VARCHAR(100) NOT NULL,
    event_status VARCHAR(20) NOT NULL,
    registration_open BOOLEAN NOT NULL,
    capacity INTEGER NOT NULL CHECK (capacity > 0),
    confirmed_count INTEGER NOT NULL DEFAULT 0 CHECK (confirmed_count >= 0),
    event_version BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE registrations (
    id UUID PRIMARY KEY,
    event_id UUID NOT NULL REFERENCES event_projections(event_id),
    workspace_id VARCHAR(100) NOT NULL,
    participant_id VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('CONFIRMED', 'CANCELLED')),
    registered_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ,
    version BIGINT NOT NULL DEFAULT 0,
    UNIQUE (event_id, participant_id)
);

CREATE INDEX idx_registrations_event_status ON registrations (event_id, status);
CREATE INDEX idx_registrations_participant ON registrations (participant_id, event_id);

CREATE TABLE idempotency_keys (
    participant_id VARCHAR(100) NOT NULL,
    idempotency_key VARCHAR(200) NOT NULL,
    request_hash VARCHAR(200) NOT NULL,
    state VARCHAR(20) NOT NULL CHECK (state IN ('IN_PROGRESS', 'SUCCEEDED')),
    response_body JSONB,
    created_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (participant_id, idempotency_key)
);

CREATE TABLE inbox_messages (
    message_id UUID PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    received_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE attendance (
    registration_id UUID PRIMARY KEY REFERENCES registrations(id),
    event_id UUID NOT NULL,
    workspace_id VARCHAR(100) NOT NULL,
    participant_id VARCHAR(100) NOT NULL,
    checked_in_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_attendance_event ON attendance (event_id, workspace_id);

CREATE TABLE outbox_messages (
    id UUID PRIMARY KEY,
    channel VARCHAR(20) NOT NULL CHECK (channel = 'KAFKA'),
    event_type VARCHAR(100) NOT NULL,
    aggregate_id UUID NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('PENDING', 'PROCESSING', 'SENT')),
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error VARCHAR(500),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    sent_at TIMESTAMPTZ
);

CREATE INDEX idx_registration_outbox_pending ON outbox_messages (status, created_at);

CREATE TABLE analytics_event_ledger (
    message_id VARCHAR(100) PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    workspace_id VARCHAR(100) NOT NULL,
    received_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE analytics_projection (
    metric_key VARCHAR(100) PRIMARY KEY,
    metric_value BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
