package com.eventflow.registration.metrics;

import java.util.concurrent.atomic.AtomicLong;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class OutboxMetrics {
    private final JdbcTemplate jdbc;
    private final AtomicLong pending = new AtomicLong();
    private final AtomicLong processing = new AtomicLong();
    private final AtomicLong oldestAgeSeconds = new AtomicLong();
    private final Counter sent;
    private final Counter failed;

    public OutboxMetrics(JdbcTemplate jdbc, MeterRegistry registry) {
        this.jdbc = jdbc;
        this.sent = Counter.builder("eventflow.outbox.sent")
                .description("Outbox messages successfully published")
                .register(registry);
        this.failed = Counter.builder("eventflow.outbox.failed")
                .description("Outbox publish attempts that failed")
                .register(registry);
        Gauge.builder("eventflow.outbox.pending", pending, AtomicLong::doubleValue)
                .description("Pending outbox messages")
                .register(registry);
        Gauge.builder("eventflow.outbox.processing", processing, AtomicLong::doubleValue)
                .description("Outbox messages currently being processed")
                .register(registry);
        Gauge.builder("eventflow.outbox.oldest.age.seconds", oldestAgeSeconds, AtomicLong::doubleValue)
                .description("Age of the oldest unsent outbox message")
                .register(registry);
    }

    @Scheduled(fixedDelayString = "${eventflow.metrics.refresh-ms:5000}",
               initialDelayString = "${eventflow.metrics.refresh-ms:5000}")
    public void refresh() {
        try {
            pending.set(count("PENDING"));
            processing.set(count("PROCESSING"));
            oldestAgeSeconds.set(oldestAgeSeconds());
        } catch (RuntimeException ignored) {
            // Health remains responsible for reporting a database failure.
        }
    }

    public void markSent() {
        sent.increment();
    }

    public void markFailed() {
        failed.increment();
    }

    private long count(String status) {
        Long value = jdbc.queryForObject(
                "SELECT count(*) FROM outbox_messages WHERE status = ?", Long.class, status);
        return value == null ? 0 : value;
    }

    private long oldestAgeSeconds() {
        Double value = jdbc.queryForObject("""
                SELECT COALESCE(EXTRACT(EPOCH FROM (now() - min(created_at))), 0)
                FROM outbox_messages WHERE status <> 'SENT'
                """, (rs, rowNum) -> rs.getDouble(1));
        return value == null ? 0 : Math.max(0, Math.round(value));
    }
}
