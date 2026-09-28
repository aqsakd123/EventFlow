package com.eventflow.eventservice.service;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.eventflow.eventservice.metrics.OutboxMetrics;

@Component
public class OutboxRelay {
    private final JdbcTemplate jdbc;
    private final RabbitTemplate rabbitTemplate;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String kafkaTopic;
    private final OutboxMetrics metrics;

    public OutboxRelay(JdbcTemplate jdbc, RabbitTemplate rabbitTemplate, KafkaTemplate<String, String> kafkaTemplate,
                       @Value("${eventflow.kafka.topic:eventflow.domain-events}") String kafkaTopic,
                       OutboxMetrics metrics) {
        this.jdbc = jdbc;
        this.rabbitTemplate = rabbitTemplate;
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaTopic = kafkaTopic;
        this.metrics = metrics;
    }

    @Scheduled(fixedDelayString = "${eventflow.outbox.poll-ms:1000}")
    public void relay() {
        jdbc.update("UPDATE outbox_messages SET status = 'PENDING', updated_at = now() WHERE status = 'PROCESSING' AND updated_at < now() - interval '30 seconds'");
        List<OutboxRow> rows = jdbc.query("""
                SELECT id, channel, event_type, aggregate_id, payload FROM outbox_messages
                WHERE status = 'PENDING' ORDER BY created_at LIMIT 20
                """, (rs, rowNum) -> new OutboxRow(UUID.fromString(rs.getString("id")), rs.getString("channel"),
                rs.getString("event_type"), UUID.fromString(rs.getString("aggregate_id")), rs.getString("payload")));
        for (OutboxRow row : rows) {
            if (jdbc.update("UPDATE outbox_messages SET status = 'PROCESSING', attempts = attempts + 1, updated_at = now() WHERE id = ? AND status = 'PENDING'", row.id()) != 1) {
                continue;
            }
            try {
                if ("RABBIT".equals(row.channel())) {
                    CorrelationData correlation = new CorrelationData(row.id().toString());
                    rabbitTemplate.convertAndSend("eventflow.events", row.eventType(), row.payload(), correlation);
                    var confirm = correlation.getFuture().get(5, TimeUnit.SECONDS);
                    if (!confirm.isAck()) {
                        throw new IllegalStateException("Rabbit publish was not confirmed: " + confirm.getReason());
                    }
                    if (correlation.getReturned() != null) {
                        throw new IllegalStateException("Rabbit publish was returned: " + correlation.getReturned().getReplyText());
                    }
                } else {
                    kafkaTemplate.send(kafkaTopic, row.aggregateId().toString(), row.payload()).get(5, TimeUnit.SECONDS);
                }
                jdbc.update("UPDATE outbox_messages SET status = 'SENT', sent_at = now(), updated_at = now() WHERE id = ?", row.id());
                metrics.markSent();
            } catch (Exception exception) {
                metrics.markFailed();
                jdbc.update("UPDATE outbox_messages SET status = 'PENDING', last_error = ?, updated_at = now() WHERE id = ?",
                        trim(exception.getMessage()), row.id());
            }
        }
    }

    private String trim(String value) {
        if (value == null) return "unknown";
        return value.length() > 500 ? value.substring(0, 500) : value;
    }

    private record OutboxRow(UUID id, String channel, String eventType, UUID aggregateId, String payload) { }
}
