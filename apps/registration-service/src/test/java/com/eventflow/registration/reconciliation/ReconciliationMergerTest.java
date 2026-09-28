package com.eventflow.registration.reconciliation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class ReconciliationMergerTest {
    private final ReconciliationMerger merger = new ReconciliationMerger();
    private final UUID eventId = UUID.randomUUID();

    @Test
    void mergesIndependentFields() {
        CoordinationSnapshot source = snapshot("PUBLISHED", 10, 0, Map.of(
                "status", "100:0:event", "registrationOpen", "100:0:event",
                "capacity", "110:0:event", "confirmedCount", "100:0:event"));
        CoordinationSnapshot projection = snapshot("PUBLISHED", 8, 3, Map.of(
                "status", "100:0:event", "registrationOpen", "100:0:event",
                "capacity", "100:0:event", "confirmedCount", "120:0:registration"));

        ReconciliationMerger.MergeResult result = merger.merge(source, projection);

        assertFalse(result.quarantined());
        assertEquals(10, result.snapshot().capacity());
        assertEquals(3, result.snapshot().confirmedCount());
    }

    @Test
    void resolvesSameFieldByPerFieldLastWriterWins() {
        CoordinationSnapshot source = snapshot("PUBLISHED", 10, 0, Map.of(
                "status", "100:0:event", "registrationOpen", "100:0:event",
                "capacity", "110:0:event", "confirmedCount", "100:0:event"));
        CoordinationSnapshot projection = snapshot("PUBLISHED", 12, 0, Map.of(
                "status", "100:0:event", "registrationOpen", "100:0:event",
                "capacity", "120:0:registration", "confirmedCount", "100:0:event"));

        ReconciliationMerger.MergeResult result = merger.merge(source, projection);

        assertEquals(12, result.snapshot().capacity());
        assertTrue(result.conflicts().stream().anyMatch(conflict ->
                conflict.field().equals("capacity") && conflict.resolution().equals("FIELD_LWW")));
    }

    @Test
    void terminalStateCannotBeReopenedByNewerClock() {
        CoordinationSnapshot source = snapshot("CANCELLED", 10, 0, Map.of(
                "status", "110:0:event", "registrationOpen", "110:0:event",
                "capacity", "100:0:event", "confirmedCount", "100:0:event"));
        CoordinationSnapshot projection = snapshot("PUBLISHED", 10, 0, Map.of(
                "status", "999:0:registration", "registrationOpen", "999:0:registration",
                "capacity", "100:0:event", "confirmedCount", "100:0:event"));

        ReconciliationMerger.MergeResult result = merger.merge(source, projection);

        assertEquals("CANCELLED", result.snapshot().status());
        assertFalse(result.snapshot().registrationOpen());
    }

    @Test
    void terminalStateAuditsRegistrationOpenOverride() {
        CoordinationSnapshot source = snapshot("CANCELLED", 10, 0, Map.of(
                "status", "110:0:event", "registrationOpen", "110:0:event",
                "capacity", "100:0:event", "confirmedCount", "100:0:event"));
        CoordinationSnapshot projection = new CoordinationSnapshot(eventId, "workspace-1", "CANCELLED", true,
                10, 0, 4, Map.of(
                "status", "110:0:event", "registrationOpen", "999:0:registration",
                "capacity", "100:0:event", "confirmedCount", "100:0:event"));

        ReconciliationMerger.MergeResult result = merger.merge(source, projection);

        assertFalse(result.snapshot().registrationOpen());
        assertTrue(result.conflicts().stream().anyMatch(conflict ->
                conflict.field().equals("registrationOpen")
                        && conflict.resolution().equals("TERMINAL_STATE_CLOSED")
                        && Boolean.FALSE.equals(conflict.winner())));
    }

    @Test
    void quarantinesCapacityBelowConfirmedCount() {
        CoordinationSnapshot source = snapshot("PUBLISHED", 2, 0, Map.of(
                "status", "100:0:event", "registrationOpen", "100:0:event",
                "capacity", "200:0:event", "confirmedCount", "100:0:event"));
        CoordinationSnapshot projection = snapshot("PUBLISHED", 10, 3, Map.of(
                "status", "100:0:event", "registrationOpen", "100:0:event",
                "capacity", "100:0:event", "confirmedCount", "150:0:registration"));

        ReconciliationMerger.MergeResult result = merger.merge(source, projection);

        assertTrue(result.quarantined());
        assertEquals("CAPACITY_BELOW_CONFIRMED", result.quarantineReason());
    }

    private CoordinationSnapshot snapshot(String state, int capacity, int confirmed, Map<String, String> clocks) {
        return new CoordinationSnapshot(eventId, "workspace-1", state, "PUBLISHED".equals(state),
                capacity, confirmed, 4, clocks);
    }
}
