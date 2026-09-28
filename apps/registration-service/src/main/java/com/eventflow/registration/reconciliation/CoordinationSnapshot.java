package com.eventflow.registration.reconciliation;

import java.util.Map;
import java.util.UUID;

public record CoordinationSnapshot(
        UUID eventId,
        String workspaceId,
        String status,
        boolean registrationOpen,
        int capacity,
        int confirmedCount,
        long version,
        Map<String, String> fieldClocks) {
}
