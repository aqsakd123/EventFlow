package com.eventflow.registration.messaging;

import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class EventProjectionConsumer {
    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;

    public EventProjectionConsumer(JdbcTemplate jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    @RabbitListener(queues = "eventflow.registration.events")
    @Transactional
    public void consume(String payload) throws Exception {
        EventMessage message = objectMapper.readValue(payload, EventMessage.class);
        int inserted = jdbc.update("""
                INSERT INTO inbox_messages (message_id, event_type, received_at)
                VALUES (?, ?, now()) ON CONFLICT (message_id) DO NOTHING
                """, message.messageId(), message.eventType());
        if (inserted != 1) return;
        jdbc.update("""
                INSERT INTO event_projections (event_id, workspace_id, event_status, registration_open, capacity,
                    confirmed_count, event_version, sync_meta, updated_at)
                VALUES (?, ?, ?, ?, ?, 0, ?, ?::jsonb, now())
                ON CONFLICT (event_id) DO UPDATE SET workspace_id = EXCLUDED.workspace_id,
                    event_status = EXCLUDED.event_status, registration_open = EXCLUDED.registration_open,
                    capacity = EXCLUDED.capacity, event_version = EXCLUDED.event_version,
                    sync_meta = event_projections.sync_meta || EXCLUDED.sync_meta,
                    local_revision = event_projections.local_revision + 1, updated_at = now()
                WHERE event_projections.event_version < EXCLUDED.event_version
                """, message.eventId(), message.workspaceId(), message.status(), message.registrationOpen(), message.capacity(),
                message.eventVersion(), objectMapper.writeValueAsString(
                        message.fieldClocks() == null ? Map.of() : message.fieldClocks()));
    }
}
