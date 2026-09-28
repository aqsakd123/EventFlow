package com.eventflow.registration.reconciliation;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import com.eventflow.registration.api.ApiException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@ConditionalOnProperty(name = "eventflow.reconciliation.chaos-writes-enabled", havingValue = "true")
public class ReconciliationChaosService {
    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;
    private final HybridLogicalClock clock;

    public ReconciliationChaosService(JdbcTemplate jdbc, ObjectMapper objectMapper, HybridLogicalClock clock) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
        this.clock = clock;
    }

    @Transactional
    public CoordinationSnapshot patch(UUID eventId, String workspaceId, ChaosPatch patch) {
        CoordinationSnapshot current = find(eventId, true);
        if (!current.workspaceId().equals(workspaceId)) {
            throw ApiException.forbidden("TENANT_ACCESS_DENIED", "The projection belongs to another workspace");
        }
        int capacity = patch.capacity() == null ? current.capacity() : patch.capacity();
        String state = patch.status() == null ? current.status() : patch.status();
        boolean open = patch.registrationOpen() == null ? current.registrationOpen() : patch.registrationOpen();
        if (capacity < current.confirmedCount()) {
            throw ApiException.conflict("CAPACITY_BELOW_CONFIRMED", "Chaos write cannot break capacity invariant");
        }
        requireMonotonic(current.status(), state);
        if (!"PUBLISHED".equals(state)) open = false;

        Map<String, String> clocks = new LinkedHashMap<>(current.fieldClocks());
        String stamp = clock.next(clocks.values());
        if (patch.capacity() != null) clocks.put("capacity", stamp);
        if (patch.status() != null) clocks.put("status", stamp);
        if (patch.registrationOpen() != null) clocks.put("registrationOpen", stamp);
        jdbc.update("""
                UPDATE event_projections
                SET event_status = ?, registration_open = ?, capacity = ?, sync_meta = ?::jsonb,
                    local_revision = local_revision + 1, updated_at = now()
                WHERE event_id = ?
                """, state, open, capacity, json(clocks), eventId);
        return find(eventId, false);
    }

    private void requireMonotonic(String current, String requested) {
        if (current.equals(requested)) return;
        if ("DRAFT".equals(current) && ("PUBLISHED".equals(requested) || terminal(requested))) return;
        if ("PUBLISHED".equals(current) && terminal(requested)) return;
        throw ApiException.conflict("STATE_REGRESSION", "Chaos fixture still enforces irreversible state");
    }

    private boolean terminal(String value) {
        return "ENDED".equals(value) || "CANCELLED".equals(value);
    }

    private CoordinationSnapshot find(UUID id, boolean lock) {
        String suffix = lock ? " FOR UPDATE" : "";
        return jdbc.query("""
                SELECT event_id, workspace_id, event_status, registration_open, capacity,
                       confirmed_count, event_version, sync_meta
                FROM event_projections WHERE event_id = ?
                """ + suffix, (rs, rowNum) -> new CoordinationSnapshot(
                        UUID.fromString(rs.getString("event_id")), rs.getString("workspace_id"),
                        rs.getString("event_status"), rs.getBoolean("registration_open"),
                        rs.getInt("capacity"), rs.getInt("confirmed_count"), rs.getLong("event_version"),
                        parse(rs.getString("sync_meta"))), id)
                .stream().findFirst()
                .orElseThrow(() -> ApiException.notFound("EVENT_PROJECTION_NOT_READY", "Projection was not found"));
    }

    private String json(Map<String, String> value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Cannot serialize chaos clocks", exception);
        }
    }

    private Map<String, String> parse(String value) {
        try {
            return value == null ? Map.of() : objectMapper.readValue(value, new TypeReference<>() { });
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Invalid chaos clocks", exception);
        }
    }

    public record ChaosPatch(Integer capacity, String status, Boolean registrationOpen) { }
}
