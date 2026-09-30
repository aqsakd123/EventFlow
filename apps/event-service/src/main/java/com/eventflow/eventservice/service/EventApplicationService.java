package com.eventflow.eventservice.service;

import java.time.Duration;
import java.time.Instant;
import java.sql.Timestamp;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.eventflow.eventservice.api.Actor;
import com.eventflow.eventservice.api.ApiException;
import com.eventflow.eventservice.api.EventDtos;
import com.eventflow.eventservice.domain.EventMessage;
import com.eventflow.eventservice.domain.StateRules;
import com.eventflow.eventservice.reconciliation.HybridLogicalClock;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.servlet.http.HttpServletRequest;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;

@Service
public class EventApplicationService {
    private static final long MAX_MEDIA_SIZE = 5 * 1024 * 1024;
    private static final List<String> MEDIA_TYPES = List.of("image/png", "image/jpeg", "image/webp", "application/pdf");

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;
    private final S3Client s3Client;
    private final S3Presigner s3Presigner;
    private final RegistrationCapacityClient registrationCapacityClient;
    private final EventReadRouter readRouter;
    private final ConsistencyPinRegistry consistencyPins;
    private final HybridLogicalClock hybridClock;
    private final String bucket;
    private final Duration presignTtl;

    public EventApplicationService(
            JdbcTemplate jdbc,
            ObjectMapper objectMapper,
            S3Client s3Client,
            S3Presigner s3Presigner,
            RegistrationCapacityClient registrationCapacityClient,
            EventReadRouter readRouter,
            ConsistencyPinRegistry consistencyPins,
            HybridLogicalClock hybridClock,
            @org.springframework.beans.factory.annotation.Value("${eventflow.s3.bucket:eventflow-media}") String bucket,
            @org.springframework.beans.factory.annotation.Value("${eventflow.s3.presign-minutes:10}") long presignMinutes) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
        this.s3Client = s3Client;
        this.s3Presigner = s3Presigner;
        this.registrationCapacityClient = registrationCapacityClient;
        this.readRouter = readRouter;
        this.consistencyPins = consistencyPins;
        this.hybridClock = hybridClock;
        this.bucket = bucket;
        this.presignTtl = Duration.ofMinutes(presignMinutes);
    }

    @Transactional
    public EventDtos.EventResponse create(EventDtos.CreateEventRequest request, HttpServletRequest servletRequest) {
        Actor actor = actor(servletRequest);
        requireOrganizer(actor);
        StateRules.validateWindow(request.startsAt(), request.endsAt());
        UUID id = UUID.randomUUID();
        Instant now = Instant.now();
        String initialClock = hybridClock.next(List.of());
        Map<String, String> clocks = new LinkedHashMap<>();
        Set.of("status", "registrationOpen", "capacity", "confirmedCount")
                .forEach(field -> clocks.put(field, initialClock));
        jdbc.update("""
                INSERT INTO events (id, workspace_id, organizer_id, title, description, starts_at, ends_at,
                    timezone, capacity, confirmed_count, status, registration_open, version, sync_meta, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 'DRAFT', false, 0, ?::jsonb, ?, ?)
                """, id, actor.workspaceId(), actor.userId(), request.title(), request.description(),
                Timestamp.from(request.startsAt()), Timestamp.from(request.endsAt()), request.timezone(), request.capacity(),
                clocksJson(clocks), Timestamp.from(now), Timestamp.from(now));
        audit(id, actor, "EVENT_CREATED");
        return toResponse(find(id));
    }

    public List<EventDtos.EventResponse> list(HttpServletRequest servletRequest) {
        Actor actor = actor(servletRequest);
        actor.requireWorkspace();
        String visibility = actor.hasAny("OWNER", "ORGANIZER") ? "" : " AND status = 'PUBLISHED'";
        EventReadRouter.Selection selection = readRouter.select(servletRequest, false);
        return selection.jdbc().query("SELECT * FROM events WHERE workspace_id = ?" + visibility + " ORDER BY starts_at",
                        eventRowMapper(), actor.workspaceId())
                .stream().map(this::toResponse).toList();
    }

    public EventDtos.EventResponse get(UUID id, HttpServletRequest servletRequest) {
        Actor actor = actor(servletRequest);
        actor.requireWorkspace();
        EventReadRouter.Selection selection = readRouter.select(servletRequest, true);
        var requiredVersion = consistencyPins.minimumVersion(servletRequest);
        EventRow row = findOrNull(id, selection.jdbc());
        if (row == null && !selection.primary() && requiredVersion.isPresent()) {
            row = findOrNull(id, readRouter.fallbackToWriter(servletRequest, "entity-not-replayed").jdbc());
        }
        if (row == null) throw ApiException.notFound("EVENT_NOT_FOUND", "Event was not found");
        if (!selection.primary() && requiredVersion.isPresent() && row.version() < requiredVersion.getAsLong()) {
            row = find(id, readRouter.fallbackToWriter(servletRequest, "entity-version-behind").jdbc());
        }
        assertTenant(row, actor);
        if (!"PUBLISHED".equals(row.status()) && !actor.hasAny("OWNER", "ORGANIZER")) {
            throw ApiException.forbidden("EVENT_NOT_PUBLIC", "Only published events are visible to participants");
        }
        return toResponse(row);
    }

    @Transactional
    public EventDtos.EventResponse update(UUID id, EventDtos.UpdateEventRequest request, HttpServletRequest servletRequest) {
        Actor actor = actor(servletRequest);
        requireOrganizer(actor);
        StateRules.validateWindow(request.startsAt(), request.endsAt());
        EventRow current = ownedForUpdate(id, actor);
        StateRules.requireEditable(current.status());
        registrationCapacityClient.validate(id, request.capacity());
        StateRules.requireCapacity(request.capacity(), current.confirmedCount());
        Map<String, String> clocks = stamped(current.syncMeta(), Set.of("capacity"));
        int changed = jdbc.update("""
                UPDATE events SET title = ?, description = ?, starts_at = ?, ends_at = ?, timezone = ?,
                    capacity = ?, version = version + 1, sync_meta = ?::jsonb, updated_at = now()
                WHERE id = ? AND version = ?
                """, request.title(), request.description(), Timestamp.from(request.startsAt()), Timestamp.from(request.endsAt()), request.timezone(),
                request.capacity(), clocksJson(clocks), id, request.version());
        if (changed == 0) throw ApiException.conflict("EVENT_VERSION_CONFLICT", "Event changed; reload before updating");
        audit(id, actor, "EVENT_UPDATED");
        enqueue(id, "EVENT_UPDATED", actor, find(id));
        return toResponse(find(id));
    }

    @Transactional
    public EventDtos.EventResponse publish(UUID id, HttpServletRequest servletRequest) {
        Actor actor = actor(servletRequest);
        requireOrganizer(actor);
        EventRow current = ownedForUpdate(id, actor);
        StateRules.requirePublishable(current.status());
        Map<String, String> clocks = stamped(current.syncMeta(), Set.of("status", "registrationOpen"));
        int changed = jdbc.update("""
                UPDATE events SET status = 'PUBLISHED', registration_open = true, version = version + 1,
                    sync_meta = ?::jsonb, updated_at = now()
                WHERE id = ? AND status = 'DRAFT'
                """, clocksJson(clocks), id);
        if (changed == 0) throw ApiException.conflict("EVENT_STATE_CONFLICT", "Event is no longer a draft");
        EventRow published = find(id);
        audit(id, actor, "EVENT_PUBLISHED");
        enqueue(id, "EVENT_PUBLISHED", actor, published);
        return toResponse(published);
    }

    @Transactional
    public EventDtos.EventResponse cancel(UUID id, HttpServletRequest servletRequest) {
        Actor actor = actor(servletRequest);
        requireOrganizer(actor);
        EventRow current = ownedForUpdate(id, actor);
        StateRules.requireEditable(current.status());
        Map<String, String> clocks = stamped(current.syncMeta(), Set.of("status", "registrationOpen"));
        int changed = jdbc.update("""
                UPDATE events SET status = 'CANCELLED', registration_open = false, version = version + 1,
                    sync_meta = ?::jsonb, updated_at = now()
                WHERE id = ? AND status IN ('DRAFT', 'PUBLISHED')
                """, clocksJson(clocks), id);
        if (changed == 0) throw ApiException.conflict("EVENT_STATE_CONFLICT", "Event cannot be cancelled in its current state");
        EventRow cancelled = find(id);
        audit(id, actor, "EVENT_CANCELLED");
        enqueue(id, "EVENT_CANCELLED", actor, cancelled);
        return toResponse(cancelled);
    }

    @Transactional
    public void markEnded(UUID id) {
        EventRow current = findForUpdate(id);
        Map<String, String> clocks = stamped(current.syncMeta(), Set.of("status", "registrationOpen"));
        int changed = jdbc.update("""
                UPDATE events SET status = 'ENDED', registration_open = false, version = version + 1,
                    sync_meta = ?::jsonb, updated_at = now()
                WHERE id = ? AND status = 'PUBLISHED' AND ends_at <= now()
                """, clocksJson(clocks), id);
        if (changed != 1) return;
        EventRow ended = find(id);
        jdbc.update("INSERT INTO audit_logs (id, workspace_id, actor_id, aggregate_id, action, created_at) VALUES (?, ?, 'system', ?, 'EVENT_ENDED', now())",
                UUID.randomUUID(), ended.workspaceId(), id);
        enqueue(id, "EVENT_ENDED", new Actor("system", ended.workspaceId(), java.util.Set.of("SYSTEM")), ended);
    }

    @Transactional
    public EventDtos.UploadSessionResponse createUploadSession(UUID eventId, EventDtos.UploadSessionRequest request,
                                                               HttpServletRequest servletRequest) {
        Actor actor = actor(servletRequest);
        requireOrganizer(actor);
        owned(eventId, actor);
        if (request.size() > MAX_MEDIA_SIZE || !MEDIA_TYPES.contains(request.contentType().toLowerCase())) {
            throw ApiException.badRequest("MEDIA_NOT_ALLOWED", "Only supported media types up to 5 MiB are allowed");
        }
        UUID mediaId = UUID.randomUUID();
        String key = "tenant/%s/events/%s/%s".formatted(actor.workspaceId(), eventId, mediaId);
        jdbc.update("""
                INSERT INTO event_media (id, event_id, workspace_id, object_key, expected_content_type, expected_size, state, created_at)
                VALUES (?, ?, ?, ?, ?, ?, 'PENDING_UPLOAD', now())
                """, mediaId, eventId, actor.workspaceId(), key, request.contentType().toLowerCase(), request.size());
        PutObjectRequest put = PutObjectRequest.builder().bucket(bucket).key(key).contentType(request.contentType()).build();
        PutObjectPresignRequest presign = PutObjectPresignRequest.builder().signatureDuration(presignTtl).putObjectRequest(put).build();
        PresignedPutObjectRequest signed = s3Presigner.presignPutObject(presign);
        return new EventDtos.UploadSessionResponse(mediaId, key, signed.url().toString(), Instant.now().plus(presignTtl));
    }

    public EventDtos.FinalizeMediaResponse finalizeMedia(UUID eventId, UUID mediaId, HttpServletRequest servletRequest) {
        Actor actor = actor(servletRequest);
        requireOrganizer(actor);
        owned(eventId, actor);
        MediaRow media = jdbc.query("SELECT * FROM event_media WHERE id = ? AND event_id = ? AND workspace_id = ?",
                (rs, rowNum) -> new MediaRow(rs.getString("object_key"), rs.getString("expected_content_type"),
                        rs.getLong("expected_size")), mediaId, eventId, actor.workspaceId())
                .stream().findFirst().orElseThrow(() -> ApiException.notFound("MEDIA_NOT_FOUND", "Media was not found"));
        try {
            var head = s3Client.headObject(HeadObjectRequest.builder().bucket(bucket).key(media.objectKey()).build());
            String actualType = head.contentType() == null ? "" : head.contentType().toLowerCase();
            if (head.contentLength() != media.expectedSize() || !actualType.equals(media.expectedContentType())) {
                jdbc.update("UPDATE event_media SET state = 'REJECTED', rejected_reason = ?, updated_at = now() WHERE id = ?",
                        "OBJECT_METADATA_MISMATCH", mediaId);
                return new EventDtos.FinalizeMediaResponse(mediaId, "REJECTED", media.objectKey(), head.contentLength(), actualType);
            }
            jdbc.update("UPDATE event_media SET state = 'READY', actual_size = ?, actual_content_type = ?, updated_at = now() WHERE id = ?",
                    head.contentLength(), actualType, mediaId);
            return new EventDtos.FinalizeMediaResponse(mediaId, "READY", media.objectKey(), head.contentLength(), actualType);
        } catch (NoSuchKeyException exception) {
            jdbc.update("UPDATE event_media SET state = 'REJECTED', rejected_reason = ?, updated_at = now() WHERE id = ?",
                    "OBJECT_NOT_FOUND", mediaId);
            throw ApiException.conflict("MEDIA_NOT_READY", "Uploaded object is not available for validation");
        } catch (SdkException exception) {
            jdbc.update("UPDATE event_media SET state = 'VALIDATING', rejected_reason = ?, updated_at = now() WHERE id = ?",
                    "STORAGE_UNAVAILABLE", mediaId);
            throw ApiException.serviceUnavailable("MEDIA_VALIDATION_UNAVAILABLE", "Media storage is temporarily unavailable");
        }
    }

    private Actor actor(HttpServletRequest request) {
        Actor actor = Actor.from(request);
        actor.requireUser();
        return actor;
    }

    private void requireOrganizer(Actor actor) {
        actor.requireUser();
        actor.requireWorkspace();
        if (!actor.hasAny("OWNER", "ORGANIZER")) {
            throw ApiException.forbidden("ORGANIZER_REQUIRED", "Organizer permission is required");
        }
    }

    private EventRow owned(UUID id, Actor actor) {
        EventRow row = find(id);
        assertTenant(row, actor);
        return row;
    }

    private EventRow ownedForUpdate(UUID id, Actor actor) {
        EventRow row = findForUpdate(id);
        assertTenant(row, actor);
        return row;
    }

    private void assertTenant(EventRow row, Actor actor) {
        if (!row.workspaceId().equals(actor.workspaceId())) {
            throw ApiException.forbidden("TENANT_ACCESS_DENIED", "The resource belongs to another workspace");
        }
    }

    private EventRow find(UUID id) {
        return find(id, jdbc);
    }

    private EventRow findForUpdate(UUID id) {
        return jdbc.query("SELECT * FROM events WHERE id = ? FOR UPDATE", eventRowMapper(), id).stream().findFirst()
                .orElseThrow(() -> ApiException.notFound("EVENT_NOT_FOUND", "Event was not found"));
    }

    private EventRow find(UUID id, JdbcTemplate source) {
        EventRow row = findOrNull(id, source);
        if (row == null) throw ApiException.notFound("EVENT_NOT_FOUND", "Event was not found");
        return row;
    }

    private EventRow findOrNull(UUID id, JdbcTemplate source) {
        return source.query("SELECT * FROM events WHERE id = ?", eventRowMapper(), id).stream().findFirst()
                .orElse(null);
    }

    private org.springframework.jdbc.core.RowMapper<EventRow> eventRowMapper() {
        return (rs, rowNum) -> new EventRow(
                UUID.fromString(rs.getString("id")), rs.getString("workspace_id"), rs.getString("organizer_id"),
                rs.getString("title"), rs.getString("description"), rs.getTimestamp("starts_at").toInstant(),
                rs.getTimestamp("ends_at").toInstant(), rs.getString("timezone"), rs.getInt("capacity"),
                rs.getInt("confirmed_count"), rs.getString("status"), rs.getBoolean("registration_open"),
                rs.getLong("version"), parseClocks(rs.getString("sync_meta")));
    }

    private EventDtos.EventResponse toResponse(EventRow row) {
        return new EventDtos.EventResponse(row.id(), row.workspaceId(), row.organizerId(), row.title(), row.description(),
                row.startsAt(), row.endsAt(), row.timezone(), row.capacity(), row.confirmedCount(), row.status(),
                row.registrationOpen(), row.version());
    }

    private void audit(UUID eventId, Actor actor, String action) {
        jdbc.update("INSERT INTO audit_logs (id, workspace_id, actor_id, aggregate_id, action, created_at) VALUES (?, ?, ?, ?, ?, now())",
                UUID.randomUUID(), actor.workspaceId(), actor.userId(), eventId, action);
    }

    private void enqueue(UUID eventId, String type, Actor actor, EventRow row) {
        try {
            Instant now = Instant.now();
            Map<String, String> projectionClocks = new LinkedHashMap<>(row.syncMeta());
            // Registration Service owns confirmedCount changes; event delivery
            // must not overwrite a newer local counter clock.
            projectionClocks.remove("confirmedCount");
            EventMessage message = new EventMessage(UUID.randomUUID(), type, 1, eventId, row.workspaceId(), row.status(),
                    row.registrationOpen(), row.capacity(), row.version(), projectionClocks, now, actor.userId());
            String payload = objectMapper.writeValueAsString(message);
            jdbc.update("""
                    INSERT INTO outbox_messages (id, channel, event_type, aggregate_id, payload, status, attempts, created_at, updated_at)
                    VALUES (?, 'RABBIT', ?, ?, ?::jsonb, 'PENDING', 0, now(), now()),
                           (?, 'KAFKA', ?, ?, ?::jsonb, 'PENDING', 0, now(), now())
                    """, message.messageId(), type, eventId, payload, UUID.randomUUID(), type, eventId, payload);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Cannot serialize domain event", exception);
        }
    }

    private record EventRow(UUID id, String workspaceId, String organizerId, String title, String description,
                            Instant startsAt, Instant endsAt, String timezone, int capacity, int confirmedCount,
                            String status, boolean registrationOpen, long version, Map<String, String> syncMeta) { }

    private record MediaRow(String objectKey, String expectedContentType, long expectedSize) { }

    private Map<String, String> stamped(Map<String, String> current, Set<String> fields) {
        Map<String, String> updated = new LinkedHashMap<>(current);
        String stamp = hybridClock.next(current.values());
        fields.forEach(field -> updated.put(field, stamp));
        return updated;
    }

    private String clocksJson(Map<String, String> clocks) {
        try {
            return objectMapper.writeValueAsString(clocks);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Cannot serialize field clocks", exception);
        }
    }

    private Map<String, String> parseClocks(String json) {
        try {
            return json == null ? Map.of() : objectMapper.readValue(json, new TypeReference<>() { });
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Invalid event field clocks", exception);
        }
    }
}
