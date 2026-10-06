ALTER TABLE outbox_messages
    ADD COLUMN next_attempt_at TIMESTAMPTZ,
    ADD COLUMN lease_token UUID,
    ADD COLUMN lease_until TIMESTAMPTZ,
    ADD COLUMN quarantined_at TIMESTAMPTZ,
    ADD COLUMN quarantine_reason VARCHAR(500);

UPDATE outbox_messages
SET next_attempt_at = COALESCE(updated_at, created_at)
WHERE next_attempt_at IS NULL;

ALTER TABLE outbox_messages
    ALTER COLUMN next_attempt_at SET DEFAULT now(),
    ALTER COLUMN next_attempt_at SET NOT NULL;

ALTER TABLE outbox_messages DROP CONSTRAINT outbox_messages_status_check;
ALTER TABLE outbox_messages
    ADD CONSTRAINT outbox_messages_status_check
    CHECK (status IN ('PENDING', 'PROCESSING', 'SENT', 'QUARANTINED'));

CREATE INDEX idx_registration_outbox_delivery_due
    ON outbox_messages (status, next_attempt_at, created_at);
CREATE INDEX idx_registration_outbox_delivery_lease
    ON outbox_messages (status, lease_until);
