CREATE INDEX IF NOT EXISTS idx_outbox_delivery_channel_due
    ON outbox_messages (channel, status, next_attempt_at, created_at, id);

CREATE INDEX IF NOT EXISTS idx_outbox_delivery_channel_lease
    ON outbox_messages (channel, status, lease_until);

