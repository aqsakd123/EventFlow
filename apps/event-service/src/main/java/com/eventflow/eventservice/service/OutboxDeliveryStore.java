package com.eventflow.eventservice.service;

import java.util.List;
import java.util.UUID;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class OutboxDeliveryStore {
    private final JdbcTemplate jdbc;

    public OutboxDeliveryStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public ClaimedOutbox claim(String channel, int maxAttempts, long leaseMs, UUID leaseToken) {
        List<ClaimedOutbox> rows = jdbc.query("""
                WITH candidate AS (
                    SELECT id
                    FROM outbox_messages
                    WHERE channel = ?
                      AND status = 'PENDING'
                      AND next_attempt_at <= now()
                      AND attempts < ?
                    ORDER BY created_at, id
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                UPDATE outbox_messages outbox
                SET status = 'PROCESSING',
                    attempts = outbox.attempts + 1,
                    lease_token = ?,
                    lease_until = now() + (? * interval '1 millisecond'),
                    updated_at = now()
                FROM candidate
                WHERE outbox.id = candidate.id
                RETURNING outbox.id, outbox.channel, outbox.event_type,
                          outbox.aggregate_id, outbox.payload, outbox.attempts, outbox.lease_token
                """, (rs, rowNum) -> new ClaimedOutbox(
                UUID.fromString(rs.getString("id")),
                rs.getString("channel"),
                rs.getString("event_type"),
                UUID.fromString(rs.getString("aggregate_id")),
                rs.getString("payload"),
                rs.getInt("attempts"),
                UUID.fromString(rs.getString("lease_token"))),
                channel, maxAttempts, leaseToken, leaseMs);
        return rows.isEmpty() ? null : rows.get(0);
    }

    public int markSent(ClaimedOutbox row) {
        return jdbc.update("""
                UPDATE outbox_messages
                SET status = 'SENT', sent_at = now(), updated_at = now(),
                    lease_token = NULL, lease_until = NULL
                WHERE id = ? AND status = 'PROCESSING' AND lease_token = ?
                """, row.id(), row.leaseToken());
    }

    public int releaseClaim(ClaimedOutbox row) {
        return jdbc.update("""
                UPDATE outbox_messages
                SET status = 'PENDING', next_attempt_at = now(), updated_at = now(),
                    lease_token = NULL, lease_until = NULL
                WHERE id = ? AND status = 'PROCESSING' AND lease_token = ?
                """, row.id(), row.leaseToken());
    }

    public int retryOrQuarantine(ClaimedOutbox row, String error, int maxAttempts, long delayMs) {
        if (row.attempts() >= maxAttempts) {
            return jdbc.update("""
                    UPDATE outbox_messages
                    SET status = 'QUARANTINED', quarantined_at = now(),
                        quarantine_reason = ?, last_error = ?, updated_at = now(),
                        lease_token = NULL, lease_until = NULL
                    WHERE id = ? AND status = 'PROCESSING' AND lease_token = ?
                    """, error, error, row.id(), row.leaseToken());
        }

        return jdbc.update("""
                UPDATE outbox_messages
                SET status = 'PENDING',
                    next_attempt_at = now() + (? * interval '1 millisecond'),
                    last_error = ?, updated_at = now(),
                    lease_token = NULL, lease_until = NULL
                WHERE id = ? AND status = 'PROCESSING' AND lease_token = ?
                """, delayMs, error, row.id(), row.leaseToken());
    }

    public int recoverExpiredLeases(int maxAttempts) {
        return jdbc.update("""
                UPDATE outbox_messages
                SET status = 'PENDING', next_attempt_at = now(),
                    lease_token = NULL, lease_until = NULL, updated_at = now()
                WHERE status = 'PROCESSING'
                  AND (lease_until IS NULL OR lease_until < now())
                  AND attempts < ?
                """, maxAttempts);
    }

    public int quarantineExhausted(int maxAttempts) {
        int pending = jdbc.update("""
                UPDATE outbox_messages
                SET status = 'QUARANTINED', quarantined_at = now(),
                    quarantine_reason = COALESCE(last_error, 'maximum outbox attempts exceeded'),
                    updated_at = now()
                WHERE status = 'PENDING' AND attempts >= ?
                """, maxAttempts);
        int processing = jdbc.update("""
                UPDATE outbox_messages
                SET status = 'QUARANTINED', quarantined_at = now(),
                    quarantine_reason = COALESCE(last_error, 'maximum outbox attempts exceeded'),
                    lease_token = NULL, lease_until = NULL, updated_at = now()
                WHERE status = 'PROCESSING' AND attempts >= ?
                  AND (lease_until IS NULL OR lease_until < now())
                """, maxAttempts);
        return pending + processing;
    }

    public record ClaimedOutbox(
            UUID id,
            String channel,
            String eventType,
            UUID aggregateId,
            String payload,
            int attempts,
            UUID leaseToken) {
    }
}

