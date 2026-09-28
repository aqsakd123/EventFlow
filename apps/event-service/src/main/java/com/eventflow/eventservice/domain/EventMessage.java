package com.eventflow.eventservice.domain;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record EventMessage(
        UUID messageId,
        String eventType,
        int schemaVersion,
        UUID eventId,
        String workspaceId,
        String status,
        boolean registrationOpen,
        int capacity,
        long eventVersion,
        Map<String, String> fieldClocks,
        Instant occurredAt,
        String correlationId) {
}
