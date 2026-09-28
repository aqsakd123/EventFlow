package com.eventflow.eventservice.reconciliation;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.eventflow.eventservice.api.ApiException;
import com.eventflow.eventservice.domain.EventMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@ConditionalOnProperty(name = "eventflow.reconciliation.enabled", havingValue = "true")
public class EventReconciliationService {
    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;

    public EventReconciliationService(JdbcTemplate jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    public Snapshot get(UUID eventId) {
        return find(eventId, false);
    }

    public List<SourceRef> listIds(String workspaceId, UUID after, int requestedLimit) {
        int limit = Math.max(1, Math.min(requestedLimit, 200));
        StringBuilder sql = new StringBuilder("SELECT id, workspace_id FROM events WHERE true");
        List<Object> arguments = new ArrayList<>();
        if (workspaceId != null && !workspaceId.isBlank()) {
            sql.append(" AND workspace_id = ?");
            arguments.add(workspaceId);
        }
        if (after != null) {
            sql.append(" AND id > ?");
            arguments.add(after);
        }
        sql.append(" ORDER BY id LIMIT ?");
        arguments.add(limit);
        return jdbc.query(sql.toString(), (rs, rowNum) -> new SourceRef(
                UUID.fromString(rs.getString("id")), rs.getString("workspace_id")), arguments.toArray());
    }

    @Transactional
    public Snapshot apply(UUID eventId, ApplyRequest request) {
        Snapshot current = find(eventId, true);
        if (!current.workspaceId().equals(request.workspaceId())) {
            throw ApiException.conflict("RECONCILIATION_WORKSPACE_CONFLICT", "Workspace cannot change during reconciliation");
        }
        if (request.confirmedCount() < 0 || request.confirmedCount() > request.capacity()) {
            throw ApiException.conflict("RECONCILIATION_CAPACITY_CONFLICT", "Confirmed count must fit capacity");
        }
        requireMonotonicState(current.status(), request.status());

        int firstApply = jdbc.update("""
                INSERT INTO reconciliation_applied (merge_id, event_id, applied_at)
                VALUES (?, ?, now()) ON CONFLICT (merge_id) DO NOTHING
                """, request.mergeId(), eventId);
        if (firstApply == 0) return find(eventId, false);
        if (current.version() != request.expectedVersion()) {
            throw ApiException.conflict("RECONCILIATION_VERSION_CONFLICT", "Event changed while reconciliation was running");
        }

        boolean registrationOpen = "PUBLISHED".equals(request.status()) && request.registrationOpen();
        int changed = jdbc.update("""
                UPDATE events
                SET status = ?, registration_open = ?, capacity = ?, confirmed_count = ?,
                    sync_meta = ?::jsonb, version = version + 1, updated_at = now()
                WHERE id = ? AND version = ?
                """, request.status(), registrationOpen, request.capacity(), request.confirmedCount(),
                json(request.fieldClocks()), eventId, request.expectedVersion());
        if (changed != 1) {
            throw ApiException.conflict("RECONCILIATION_VERSION_CONFLICT", "Event changed while reconciliation was running");
        }

        jdbc.update("""
                INSERT INTO audit_logs (id, workspace_id, actor_id, aggregate_id, action, created_at)
                VALUES (?, ?, 'reconciliation-worker', ?, 'EVENT_RECONCILED', now())
                """, UUID.randomUUID(), current.workspaceId(), eventId);
        Snapshot resolved = find(eventId, false);
        enqueue(resolved);
        return resolved;
    }

    private void requireMonotonicState(String current, String requested) {
        if (current.equals(requested)) return;
        if ("DRAFT".equals(current) && ("PUBLISHED".equals(requested) || terminal(requested))) return;
        if ("PUBLISHED".equals(current) && terminal(requested)) return;
        throw ApiException.conflict("RECONCILIATION_STATE_REGRESSION",
                "Reconciliation cannot reverse or replace a terminal event state");
    }

    private boolean terminal(String state) {
        return "ENDED".equals(state) || "CANCELLED".equals(state);
    }

    private Snapshot find(UUID eventId, boolean lock) {
        String suffix = lock ? " FOR UPDATE" : "";
        return jdbc.query("""
                SELECT id, workspace_id, status, registration_open, capacity, confirmed_count, version, sync_meta
                FROM events WHERE id = ?
                """ + suffix, (rs, rowNum) -> new Snapshot(
                        UUID.fromString(rs.getString("id")),
                        rs.getString("workspace_id"),
                        rs.getString("status"),
                        rs.getBoolean("registration_open"),
                        rs.getInt("capacity"),
                        rs.getInt("confirmed_count"),
                        rs.getLong("version"),
                        parse(rs.getString("sync_meta"))), eventId)
                .stream().findFirst()
                .orElseThrow(() -> ApiException.notFound("EVENT_NOT_FOUND", "Event was not found"));
    }

    private void enqueue(Snapshot snapshot) {
        try {
            Map<String, String> projectionClocks = new LinkedHashMap<>(snapshot.fieldClocks());
            projectionClocks.remove("confirmedCount");
            EventMessage message = new EventMessage(UUID.randomUUID(), "EVENT_RECONCILED", 1,
                    snapshot.eventId(), snapshot.workspaceId(), snapshot.status(), snapshot.registrationOpen(),
                    snapshot.capacity(), snapshot.version(), projectionClocks, Instant.now(), "reconciliation-worker");
            String payload = objectMapper.writeValueAsString(message);
            jdbc.update("""
                    INSERT INTO outbox_messages (id, channel, event_type, aggregate_id, payload, status, attempts, created_at, updated_at)
                    VALUES (?, 'RABBIT', 'EVENT_RECONCILED', ?, ?::jsonb, 'PENDING', 0, now(), now()),
                           (?, 'KAFKA', 'EVENT_RECONCILED', ?, ?::jsonb, 'PENDING', 0, now(), now())
                    """, message.messageId(), snapshot.eventId(), payload, UUID.randomUUID(), snapshot.eventId(), payload);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Cannot serialize reconciliation event", exception);
        }
    }

    private String json(Map<String, String> clocks) {
        try {
            return objectMapper.writeValueAsString(clocks);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Cannot serialize reconciliation clocks", exception);
        }
    }

    private Map<String, String> parse(String json) {
        try {
            return json == null ? Map.of() : objectMapper.readValue(json, new TypeReference<>() { });
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Invalid reconciliation clocks", exception);
        }
    }

    public record Snapshot(UUID eventId, String workspaceId, String status, boolean registrationOpen,
                           int capacity, int confirmedCount, long version, Map<String, String> fieldClocks) { }

    public record SourceRef(UUID eventId, String workspaceId) { }

    public record ApplyRequest(UUID mergeId, String workspaceId, String status, boolean registrationOpen,
                               int capacity, int confirmedCount, long expectedVersion,
                               Map<String, String> fieldClocks) { }
}
