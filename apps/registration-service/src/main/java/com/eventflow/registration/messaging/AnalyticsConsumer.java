package com.eventflow.registration.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class AnalyticsConsumer {
    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;

    public AnalyticsConsumer(JdbcTemplate jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "${eventflow.kafka.topic:eventflow.domain-events}", groupId = "${spring.kafka.consumer.group-id:eventflow-analytics}")
    @Transactional
    public void consume(String payload) throws Exception {
        JsonNode event = objectMapper.readTree(payload);
        String messageId = event.path("messageId").asText(null);
        String eventType = event.path("eventType").asText("UNKNOWN");
        String workspaceId = event.path("workspaceId").asText("unknown");
        if (messageId == null) throw new IllegalArgumentException("messageId is required");
        int inserted = jdbc.update("INSERT INTO analytics_event_ledger (message_id, event_type, workspace_id, received_at) VALUES (?, ?, ?, now()) ON CONFLICT DO NOTHING",
                messageId, eventType, workspaceId);
        if (inserted == 1) {
            jdbc.update("INSERT INTO analytics_projection (metric_key, metric_value, updated_at) VALUES (?, 1, now()) ON CONFLICT (metric_key) DO UPDATE SET metric_value = analytics_projection.metric_value + 1, updated_at = now()",
                    eventType);
        }
    }
}
