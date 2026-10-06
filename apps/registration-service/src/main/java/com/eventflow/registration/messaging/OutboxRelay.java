package com.eventflow.registration.messaging;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.eventflow.registration.metrics.OutboxMetrics;

@Component
public class OutboxRelay {
    private final JdbcTemplate jdbc;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topic;
    private final OutboxMetrics metrics;
    private final int maxAttempts;
    private final long baseDelayMs;
    private final long maxDelayMs;
    private final double jitterRatio;
    private final long leaseMs;

    public OutboxRelay(
            JdbcTemplate jdbc,
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("$" + "{eventflow.kafka.topic:eventflow.domain-events}") String topic,
            OutboxMetrics metrics,
            @Value("$" + "{eventflow.outbox.max-attempts:8}") int maxAttempts,
            @Value("$" + "{eventflow.outbox.base-delay-ms:250}") long baseDelayMs,
            @Value("$" + "{eventflow.outbox.max-delay-ms:300000}") long maxDelayMs,
            @Value("$" + "{eventflow.outbox.jitter-ratio:0.2}") double jitterRatio,
            @Value("$" + "{eventflow.outbox.lease-ms:30000}") long leaseMs) {
        this.jdbc = jdbc;
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
        this.metrics = metrics;
        this.maxAttempts = Math.max(1, maxAttempts);
        this.baseDelayMs = Math.max(1, baseDelayMs);
        this.maxDelayMs = Math.max(this.baseDelayMs, maxDelayMs);
        this.jitterRatio = Math.max(0, Math.min(1, jitterRatio));
        this.leaseMs = Math.max(1, leaseMs);
    }

    @Scheduled(fixedDelayString = "$" + "{eventflow.outbox.poll-ms:1000}")
    public void relay() {
        recoverExpiredLeases();
        quarantineExhausted();

        List<OutboxRow> rows = jdbc.query("""
                SELECT id, aggregate_id, payload, attempts
                FROM outbox_messages
                WHERE status = 'PENDING' AND next_attempt_at <= now() AND attempts < ?
                ORDER BY created_at
                LIMIT 20
                """, (rs, rowNum) -> new OutboxRow(
                UUID.fromString(rs.getString("id")),
                UUID.fromString(rs.getString("aggregate_id")),
                rs.getString("payload"),
                rs.getInt("attempts")), maxAttempts);

        for (OutboxRow row : rows) {
            UUID leaseToken = UUID.randomUUID();
            int claimed = jdbc.update("""
                    UPDATE outbox_messages
                    SET status = 'PROCESSING', attempts = attempts + 1,
                        lease_token = ?, lease_until = now() + (? * interval '1 millisecond'),
                        updated_at = now()
                    WHERE id = ? AND status = 'PENDING'
                      AND next_attempt_at <= now() AND attempts < ?
                    """, leaseToken, leaseMs, row.id(), maxAttempts);
            if (claimed != 1) {
                metrics.markClaimConflict();
                continue;
            }

            int attempt = row.attempts() + 1;
            try {
                kafkaTemplate.send(topic, row.aggregateId().toString(), row.payload()).get(5, TimeUnit.SECONDS);
                int finalized = jdbc.update("""
                        UPDATE outbox_messages
                        SET status = 'SENT', sent_at = now(), updated_at = now(),
                            lease_token = NULL, lease_until = NULL
                        WHERE id = ? AND status = 'PROCESSING' AND lease_token = ?
                        """, row.id(), leaseToken);
                if (finalized == 1) {
                    metrics.markSent();
                } else {
                    metrics.markClaimConflict();
                }
            } catch (Exception exception) {
                metrics.markFailed();
                String error = trim(exception.getMessage());
                int updated;
                if (attempt >= maxAttempts) {
                    updated = jdbc.update("""
                            UPDATE outbox_messages
                            SET status = 'QUARANTINED', quarantined_at = now(),
                                quarantine_reason = ?, last_error = ?, updated_at = now(),
                                lease_token = NULL, lease_until = NULL
                            WHERE id = ? AND status = 'PROCESSING' AND lease_token = ?
                            """, error, error, row.id(), leaseToken);
                    if (updated == 1) metrics.markQuarantined();
                } else {
                    updated = jdbc.update("""
                            UPDATE outbox_messages
                            SET status = 'PENDING',
                                next_attempt_at = now() + (? * interval '1 millisecond'),
                                last_error = ?, updated_at = now(),
                                lease_token = NULL, lease_until = NULL
                            WHERE id = ? AND status = 'PROCESSING' AND lease_token = ?
                            """, backoffMs(attempt), error, row.id(), leaseToken);
                }
                if (updated == 0) metrics.markClaimConflict();
            }
        }
    }

    private void recoverExpiredLeases() {
        int recovered = jdbc.update("""
                UPDATE outbox_messages
                SET status = 'PENDING', next_attempt_at = now(),
                    lease_token = NULL, lease_until = NULL, updated_at = now()
                WHERE status = 'PROCESSING' AND (lease_until IS NULL OR lease_until < now())
                  AND attempts < ?
                """, maxAttempts);
        metrics.markRecovered(recovered);
    }

    private void quarantineExhausted() {
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
        metrics.markQuarantined(pending + processing);
    }

    private long backoffMs(int attempt) {
        long delay = baseDelayMs;
        for (int i = 1; i < attempt; i++) {
            delay = delay >= maxDelayMs / 2 ? maxDelayMs : Math.min(maxDelayMs, delay * 2);
        }
        long jitter = (long) (delay * jitterRatio);
        if (jitter == 0) return delay;
        long sampled = ThreadLocalRandom.current().nextLong(-jitter, jitter + 1);
        return Math.max(1, Math.min(maxDelayMs, delay + sampled));
    }

    private String trim(String value) {
        if (value == null) return "unknown";
        return value.length() > 500 ? value.substring(0, 500) : value;
    }

    private record OutboxRow(UUID id, UUID aggregateId, String payload, int attempts) { }
}
