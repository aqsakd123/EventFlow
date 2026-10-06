package com.eventflow.eventservice.metrics;

import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component("outbox")
public class OutboxHealthIndicator implements HealthIndicator {
    private final JdbcTemplate jdbc;
    private final long alertAgeSeconds;

    public OutboxHealthIndicator(JdbcTemplate jdbc,
                                 @Value("${eventflow.metrics.outbox-alert-age-seconds:60}") long alertAgeSeconds) {
        this.jdbc = jdbc;
        this.alertAgeSeconds = alertAgeSeconds;
    }

    @Override
    public Health health() {
        try {
            Long pending = jdbc.queryForObject(
                    "SELECT count(*) FROM outbox_messages WHERE status = 'PENDING'", Long.class);
            Long processing = jdbc.queryForObject(
                    "SELECT count(*) FROM outbox_messages WHERE status = 'PROCESSING'", Long.class);
            Long quarantined = jdbc.queryForObject(
                    "SELECT count(*) FROM outbox_messages WHERE status = 'QUARANTINED'", Long.class);
            Double age = jdbc.queryForObject("""
                    SELECT COALESCE(EXTRACT(EPOCH FROM (now() - min(created_at))), 0)
                    FROM outbox_messages WHERE status IN ('PENDING', 'PROCESSING')
                    """, (rs, rowNum) -> rs.getDouble(1));
            long ageSeconds = age == null ? 0 : Math.max(0, Math.round(age));
            long quarantinedCount = quarantined == null ? 0 : quarantined;
            String state = quarantinedCount > 0 ? "QUARANTINED_MESSAGES"
                    : ageSeconds > alertAgeSeconds ? "BACKLOG_OLDER_THAN_THRESHOLD" : "OK";
            return Health.up().withDetails(Map.of(
                    "pending", pending == null ? 0 : pending,
                    "processing", processing == null ? 0 : processing,
                    "quarantined", quarantinedCount,
                    "oldestAgeSeconds", ageSeconds,
                    "alertAgeSeconds", alertAgeSeconds,
                    "state", state)).build();
        } catch (RuntimeException exception) {
            return Health.down(exception).build();
        }
    }
}
