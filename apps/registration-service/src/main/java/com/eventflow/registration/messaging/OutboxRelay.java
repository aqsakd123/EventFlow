package com.eventflow.registration.messaging;

import java.util.List;
import java.util.UUID;
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

    public OutboxRelay(JdbcTemplate jdbc, KafkaTemplate<String, String> kafkaTemplate,
                       @Value("${eventflow.kafka.topic:eventflow.domain-events}") String topic,
                       OutboxMetrics metrics) {
        this.jdbc = jdbc;
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
        this.metrics = metrics;
    }

    @Scheduled(fixedDelayString = "${eventflow.outbox.poll-ms:1000}")
    public void relay() {
        jdbc.update("UPDATE outbox_messages SET status = 'PENDING', updated_at = now() WHERE status = 'PROCESSING' AND updated_at < now() - interval '30 seconds'");
        List<OutboxRow> rows = jdbc.query("SELECT id, aggregate_id, payload FROM outbox_messages WHERE status = 'PENDING' ORDER BY created_at LIMIT 20",
                (rs, rowNum) -> new OutboxRow(UUID.fromString(rs.getString("id")), UUID.fromString(rs.getString("aggregate_id")), rs.getString("payload")));
        for (OutboxRow row : rows) {
            if (jdbc.update("UPDATE outbox_messages SET status = 'PROCESSING', attempts = attempts + 1, updated_at = now() WHERE id = ? AND status = 'PENDING'", row.id()) != 1) continue;
            try {
                kafkaTemplate.send(topic, row.aggregateId().toString(), row.payload()).get(5, TimeUnit.SECONDS);
                jdbc.update("UPDATE outbox_messages SET status = 'SENT', sent_at = now(), updated_at = now() WHERE id = ?", row.id());
                metrics.markSent();
            } catch (Exception exception) {
                metrics.markFailed();
                jdbc.update("UPDATE outbox_messages SET status = 'PENDING', last_error = ?, updated_at = now() WHERE id = ?",
                        exception.getMessage() == null ? "unknown" : exception.getMessage().substring(0, Math.min(500, exception.getMessage().length())), row.id());
            }
        }
    }

    private record OutboxRow(UUID id, UUID aggregateId, String payload) { }
}
