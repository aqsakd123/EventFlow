CREATE TABLE events (
    id UUID PRIMARY KEY,
    workspace_id VARCHAR(100) NOT NULL,
    organizer_id VARCHAR(100) NOT NULL,
    title VARCHAR(200) NOT NULL,
    description VARCHAR(4000),
    starts_at TIMESTAMPTZ NOT NULL,
    ends_at TIMESTAMPTZ NOT NULL,
    timezone VARCHAR(80) NOT NULL,
    capacity INTEGER NOT NULL CHECK (capacity > 0),
    confirmed_count INTEGER NOT NULL DEFAULT 0 CHECK (confirmed_count >= 0),
    status VARCHAR(20) NOT NULL CHECK (status IN ('DRAFT', 'PUBLISHED', 'ENDED', 'CANCELLED')),
    registration_open BOOLEAN NOT NULL DEFAULT false,
    version BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CHECK (ends_at > starts_at),
    CHECK (confirmed_count <= capacity)
);

CREATE INDEX idx_events_workspace_starts ON events (workspace_id, starts_at);
CREATE INDEX idx_events_public ON events (workspace_id, status, starts_at);

CREATE TABLE event_media (
    id UUID PRIMARY KEY,
    event_id UUID NOT NULL REFERENCES events(id),
    workspace_id VARCHAR(100) NOT NULL,
    object_key VARCHAR(500) NOT NULL UNIQUE,
    expected_content_type VARCHAR(100) NOT NULL,
    expected_size BIGINT NOT NULL CHECK (expected_size > 0),
    actual_content_type VARCHAR(100),
    actual_size BIGINT,
    state VARCHAR(30) NOT NULL CHECK (state IN ('PENDING_UPLOAD', 'VALIDATING', 'READY', 'REJECTED', 'EXPIRED', 'DELETING', 'DELETED')),
    rejected_reason VARCHAR(200),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ
);

CREATE INDEX idx_event_media_tenant_event ON event_media (workspace_id, event_id);

CREATE TABLE audit_logs (
    id UUID PRIMARY KEY,
    workspace_id VARCHAR(100) NOT NULL,
    actor_id VARCHAR(100) NOT NULL,
    aggregate_id UUID NOT NULL,
    action VARCHAR(100) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE outbox_messages (
    id UUID PRIMARY KEY,
    channel VARCHAR(20) NOT NULL CHECK (channel IN ('RABBIT', 'KAFKA')),
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

CREATE INDEX idx_outbox_pending ON outbox_messages (status, created_at);
