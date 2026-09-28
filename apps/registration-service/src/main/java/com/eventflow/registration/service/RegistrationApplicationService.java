package com.eventflow.registration.service;

import java.time.Instant;
import java.sql.Timestamp;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import com.eventflow.registration.api.Actor;
import com.eventflow.registration.api.ApiException;
import com.eventflow.registration.api.RegistrationDtos;
import com.eventflow.registration.reconciliation.HybridLogicalClock;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.servlet.http.HttpServletRequest;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class RegistrationApplicationService {
    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;
    private final EventStateClient eventStateClient;
    private final HybridLogicalClock hybridClock;

    public RegistrationApplicationService(JdbcTemplate jdbc, ObjectMapper objectMapper, EventStateClient eventStateClient,
                                          HybridLogicalClock hybridClock) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
        this.eventStateClient = eventStateClient;
        this.hybridClock = hybridClock;
    }

    @Transactional
    public RegistrationDtos.RegistrationResponse register(UUID eventId, String idempotencyKey, HttpServletRequest request) {
        Actor actor = actor(request);
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw ApiException.badRequest("IDEMPOTENCY_KEY_REQUIRED", "Idempotency-Key is required");
        }
        String requestHash = eventId + ":" + actor.userId();
        jdbc.update("""
                INSERT INTO idempotency_keys (participant_id, idempotency_key, request_hash, state, created_at)
                VALUES (?, ?, ?, 'IN_PROGRESS', now())
                ON CONFLICT (participant_id, idempotency_key) DO NOTHING
                """, actor.userId(), idempotencyKey, requestHash);
        IdempotencyRow idem = findIdempotency(actor.userId(), idempotencyKey);
        if (!requestHash.equals(idem.requestHash())) {
            throw ApiException.conflict("IDEMPOTENCY_KEY_REUSED", "The key was used with another request");
        }
        if ("SUCCEEDED".equals(idem.state())) return parseResponse(idem.responseBody());

        EventStateClient.SourceState source = eventStateClient.requireRegistrationAllowed(eventId, actor.workspaceId());
        Projection projection = findProjectionForUpdate(eventId);
        if (!projection.workspaceId().equals(actor.workspaceId())) {
            throw ApiException.forbidden("TENANT_ACCESS_DENIED", "The resource belongs to another workspace");
        }
        if (projection.eventVersion() < source.version()) {
            throw ApiException.notFound("EVENT_PROJECTION_NOT_READY", "Event projection is behind the source event");
        }
        RegistrationRow current = findRegistrationForUpdate(eventId, actor.userId());
        if (current != null && hasAttendance(current.id())) {
            throw ApiException.conflict("ALREADY_CHECKED_IN", "A checked-in participant cannot register again");
        }
        if (current != null && "CONFIRMED".equals(current.status())) {
            RegistrationDtos.RegistrationResponse response = response(current, false);
            saveIdempotency(actor.userId(), idempotencyKey, response);
            return response;
        }
        if (!"PUBLISHED".equals(projection.status()) || !projection.registrationOpen()) {
            throw ApiException.conflict("REGISTRATION_CLOSED", "Registration is not open");
        }
        Map<String, String> projectionClocks = stampConfirmedCount(projection);
        int changed = jdbc.update("""
                UPDATE event_projections SET confirmed_count = confirmed_count + 1,
                    sync_meta = ?::jsonb, local_revision = local_revision + 1, updated_at = now()
                WHERE event_id = ? AND event_status = 'PUBLISHED' AND registration_open = true AND confirmed_count < capacity
                """, clocksJson(projectionClocks), eventId);
        if (changed != 1) throw ApiException.conflict("EVENT_FULL", "The event has no remaining capacity");

        UUID registrationId = current == null ? UUID.randomUUID() : current.id();
        if (current == null) {
            jdbc.update("""
                    INSERT INTO registrations (id, event_id, workspace_id, participant_id, status, registered_at, version)
                    VALUES (?, ?, ?, ?, 'CONFIRMED', now(), 0)
                    """, registrationId, eventId, projection.workspaceId(), actor.userId());
        } else {
            jdbc.update("UPDATE registrations SET status = 'CONFIRMED', version = version + 1, updated_at = now() WHERE id = ?",
                    registrationId);
        }
        RegistrationRow saved = findRegistrationForUpdate(eventId, actor.userId());
        RegistrationDtos.RegistrationResponse response = response(saved, false);
        saveIdempotency(actor.userId(), idempotencyKey, response);
        enqueue(saved, "REGISTRATION_CONFIRMED", projection.workspaceId(), actor.userId());
        return response;
    }

    @Transactional
    public RegistrationDtos.RegistrationResponse cancel(UUID eventId, HttpServletRequest request) {
        Actor actor = actor(request);
        Projection projection = findProjectionForUpdate(eventId);
        if (!projection.workspaceId().equals(actor.workspaceId())) {
            throw ApiException.forbidden("TENANT_ACCESS_DENIED", "The resource belongs to another workspace");
        }
        RegistrationRow current = findRegistrationForUpdate(eventId, actor.userId());
        if (current == null) throw ApiException.notFound("REGISTRATION_NOT_FOUND", "Registration was not found");
        if (hasAttendance(current.id())) throw ApiException.conflict("ALREADY_CHECKED_IN", "A checked-in participant cannot cancel");
        if ("CANCELLED".equals(current.status())) return response(current, false);
        jdbc.update("UPDATE registrations SET status = 'CANCELLED', version = version + 1, updated_at = now() WHERE id = ?", current.id());
        Map<String, String> projectionClocks = stampConfirmedCount(projection);
        jdbc.update("""
                UPDATE event_projections SET confirmed_count = confirmed_count - 1,
                    sync_meta = ?::jsonb, local_revision = local_revision + 1, updated_at = now()
                WHERE event_id = ? AND confirmed_count > 0
                """, clocksJson(projectionClocks), eventId);
        RegistrationRow saved = findRegistrationForUpdate(eventId, actor.userId());
        enqueue(saved, "REGISTRATION_CANCELLED", projection.workspaceId(), actor.userId());
        return response(saved, false);
    }

    public RegistrationDtos.RegistrationResponse getMine(UUID eventId, HttpServletRequest request) {
        Actor actor = actor(request);
        Projection projection = findProjection(eventId);
        if (!projection.workspaceId().equals(actor.workspaceId())) {
            throw ApiException.forbidden("TENANT_ACCESS_DENIED", "The resource belongs to another workspace");
        }
        RegistrationRow row = findRegistration(eventId, actor.userId());
        if (row == null) throw ApiException.notFound("REGISTRATION_NOT_FOUND", "Registration was not found");
        return response(row, hasAttendance(row.id()));
    }

    @Transactional
    public RegistrationDtos.CheckInResponse checkIn(UUID eventId, RegistrationDtos.CheckInRequest checkIn, HttpServletRequest request) {
        Actor actor = actor(request);
        if (!actor.hasAny("OWNER", "ORGANIZER", "CHECKIN_STAFF")) {
            throw ApiException.forbidden("CHECKIN_STAFF_REQUIRED", "Check-in permission is required");
        }
        eventStateClient.requireCheckinAllowed(eventId, actor.workspaceId());
        Projection projection = findProjectionForUpdate(eventId);
        if (!projection.workspaceId().equals(actor.workspaceId())) {
            throw ApiException.forbidden("TENANT_ACCESS_DENIED", "The resource belongs to another workspace");
        }
        RegistrationRow row = findRegistrationForUpdate(eventId, checkIn.participantId());
        if (row == null) throw ApiException.notFound("REGISTRATION_NOT_FOUND", "Registration was not found");
        if (!"PUBLISHED".equals(projection.status())) throw ApiException.conflict("EVENT_NOT_CHECKINABLE", "Event is not active");
        if (!"CONFIRMED".equals(row.status())) throw ApiException.conflict("REGISTRATION_NOT_CONFIRMED", "Only confirmed registrations can check in");
        AttendanceRow existing = jdbc.query("SELECT participant_id, checked_in_at FROM attendance WHERE registration_id = ?",
                (rs, rowNum) -> new AttendanceRow(rs.getString("participant_id"), rs.getTimestamp("checked_in_at").toInstant()), row.id())
                .stream().findFirst().orElse(null);
        if (existing != null) return new RegistrationDtos.CheckInResponse(eventId, existing.participantId(), "CHECKED_IN", existing.checkedInAt());
        Instant now = Instant.now();
        jdbc.update("INSERT INTO attendance (registration_id, event_id, workspace_id, participant_id, checked_in_at) VALUES (?, ?, ?, ?, ?)",
                row.id(), eventId, projection.workspaceId(), row.participantId(), Timestamp.from(now));
        enqueue(row, "ATTENDANCE_CHECKED_IN", projection.workspaceId(), actor.userId());
        return new RegistrationDtos.CheckInResponse(eventId, row.participantId(), "CHECKED_IN", now);
    }

    public List<RegistrationDtos.AttendanceRow> attendance(UUID eventId, HttpServletRequest request) {
        Actor actor = actor(request);
        Projection projection = findProjection(eventId);
        if (!projection.workspaceId().equals(actor.workspaceId()) || !actor.hasAny("OWNER", "ORGANIZER", "CHECKIN_STAFF")) {
            throw ApiException.forbidden("ATTENDANCE_ACCESS_DENIED", "Attendance permission is required");
        }
        return jdbc.query("""
                SELECT r.participant_id, CASE WHEN a.registration_id IS NULL THEN 'NOT_CHECKED_IN' ELSE 'CHECKED_IN' END AS state,
                    a.checked_in_at
                FROM registrations r LEFT JOIN attendance a ON a.registration_id = r.id
                WHERE r.event_id = ? ORDER BY r.registered_at
                """, (rs, rowNum) -> new RegistrationDtos.AttendanceRow(rs.getString("participant_id"), rs.getString("state"),
                rs.getTimestamp("checked_in_at") == null ? null : rs.getTimestamp("checked_in_at").toInstant()), eventId);
    }

    private Actor actor(HttpServletRequest request) {
        Actor actor = Actor.from(request);
        actor.requireIdentity();
        return actor;
    }

    private Projection findProjection(UUID eventId) {
        return jdbc.query("SELECT * FROM event_projections WHERE event_id = ?", projectionMapper(), eventId).stream().findFirst()
                .orElseThrow(() -> ApiException.notFound("EVENT_PROJECTION_NOT_READY", "Event has not reached registration service yet"));
    }

    private Projection findProjectionForUpdate(UUID eventId) {
        return jdbc.query("SELECT * FROM event_projections WHERE event_id = ? FOR UPDATE", projectionMapper(), eventId).stream().findFirst()
                .orElseThrow(() -> ApiException.notFound("EVENT_PROJECTION_NOT_READY", "Event has not reached registration service yet"));
    }

    private RegistrationRow findRegistration(UUID eventId, String participantId) {
        return jdbc.query("SELECT * FROM registrations WHERE event_id = ? AND participant_id = ?", registrationMapper(), eventId, participantId)
                .stream().findFirst().orElse(null);
    }

    private RegistrationRow findRegistrationForUpdate(UUID eventId, String participantId) {
        return jdbc.query("SELECT * FROM registrations WHERE event_id = ? AND participant_id = ? FOR UPDATE", registrationMapper(), eventId, participantId)
                .stream().findFirst().orElse(null);
    }

    private boolean hasAttendance(UUID registrationId) {
        return Boolean.TRUE.equals(jdbc.queryForObject("SELECT EXISTS (SELECT 1 FROM attendance WHERE registration_id = ?)", Boolean.class, registrationId));
    }

    private RegistrationDtos.RegistrationResponse response(RegistrationRow row, boolean checkedIn) {
        return new RegistrationDtos.RegistrationResponse(row.id(), row.eventId(), row.participantId(), row.status(), row.registeredAt(), checkedIn, row.version());
    }

    private void saveIdempotency(String participantId, String key, RegistrationDtos.RegistrationResponse response) {
        try {
            jdbc.update("UPDATE idempotency_keys SET state = 'SUCCEEDED', response_body = ?::jsonb, completed_at = now() WHERE participant_id = ? AND idempotency_key = ?",
                    objectMapper.writeValueAsString(response), participantId, key);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Cannot save idempotency response", exception);
        }
    }

    private RegistrationDtos.RegistrationResponse parseResponse(String responseBody) {
        try {
            return objectMapper.readValue(responseBody, RegistrationDtos.RegistrationResponse.class);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Stored idempotency response is invalid", exception);
        }
    }

    private IdempotencyRow findIdempotency(String participantId, String key) {
        return jdbc.query("SELECT request_hash, state, response_body FROM idempotency_keys WHERE participant_id = ? AND idempotency_key = ?",
                (rs, rowNum) -> new IdempotencyRow(rs.getString("request_hash"), rs.getString("state"), rs.getString("response_body")), participantId, key)
                .stream().findFirst().orElseThrow(() -> new IllegalStateException("Idempotency row disappeared"));
    }

    private void enqueue(RegistrationRow row, String eventType, String workspaceId, String actorId) {
        try {
            var payload = objectMapper.createObjectNode();
            payload.put("messageId", UUID.randomUUID().toString());
            payload.put("eventType", eventType);
            payload.put("schemaVersion", 1);
            payload.put("eventId", row.eventId().toString());
            payload.put("registrationId", row.id().toString());
            payload.put("participantId", row.participantId());
            payload.put("workspaceId", workspaceId);
            payload.put("actorId", actorId);
            payload.put("occurredAt", Instant.now().toString());
            jdbc.update("""
                    INSERT INTO outbox_messages (id, channel, event_type, aggregate_id, payload, status, attempts, created_at, updated_at)
                    VALUES (?, 'KAFKA', ?, ?, ?::jsonb, 'PENDING', 0, now(), now())
                    """, UUID.fromString(payload.get("messageId").asText()), eventType, row.eventId(), payload.toString());
        } catch (Exception exception) {
            throw new IllegalStateException("Cannot enqueue registration event", exception);
        }
    }

    private org.springframework.jdbc.core.RowMapper<Projection> projectionMapper() {
        return (rs, rowNum) -> new Projection(UUID.fromString(rs.getString("event_id")), rs.getString("workspace_id"),
                rs.getString("event_status"), rs.getBoolean("registration_open"), rs.getInt("capacity"),
                rs.getInt("confirmed_count"), rs.getLong("event_version"), parseClocks(rs.getString("sync_meta")));
    }

    private org.springframework.jdbc.core.RowMapper<RegistrationRow> registrationMapper() {
        return (rs, rowNum) -> new RegistrationRow(UUID.fromString(rs.getString("id")), UUID.fromString(rs.getString("event_id")),
                rs.getString("workspace_id"), rs.getString("participant_id"), rs.getString("status"),
                rs.getTimestamp("registered_at").toInstant(), rs.getLong("version"));
    }

    private record Projection(UUID eventId, String workspaceId, String status, boolean registrationOpen, int capacity,
                              int confirmedCount, long eventVersion, Map<String, String> syncMeta) { }
    private record RegistrationRow(UUID id, UUID eventId, String workspaceId, String participantId, String status,
                                   Instant registeredAt, long version) { }
    private record IdempotencyRow(String requestHash, String state, String responseBody) { }
    private record AttendanceRow(String participantId, Instant checkedInAt) { }

    private Map<String, String> stampConfirmedCount(Projection projection) {
        Map<String, String> clocks = new LinkedHashMap<>(projection.syncMeta());
        clocks.put("confirmedCount", hybridClock.next(clocks.values()));
        return clocks;
    }

    private String clocksJson(Map<String, String> clocks) {
        try {
            return objectMapper.writeValueAsString(clocks);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Cannot serialize projection field clocks", exception);
        }
    }

    private Map<String, String> parseClocks(String json) {
        try {
            return json == null ? Map.of() : objectMapper.readValue(json, new TypeReference<>() { });
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Invalid projection field clocks", exception);
        }
    }
}
