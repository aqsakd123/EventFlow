package com.eventflow.eventservice.api;

import java.time.Instant;
import java.util.UUID;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

public final class EventDtos {
    private EventDtos() { }

    public record CreateEventRequest(
            @NotBlank @Size(max = 200) String title,
            @Size(max = 4000) String description,
            @NotNull Instant startsAt,
            @NotNull Instant endsAt,
            @NotBlank String timezone,
            @Min(1) int capacity) { }

    public record UpdateEventRequest(
            @NotBlank @Size(max = 200) String title,
            @Size(max = 4000) String description,
            @NotNull Instant startsAt,
            @NotNull Instant endsAt,
            @NotBlank String timezone,
            @Min(1) int capacity,
            @NotNull Long version) { }

    public record EventResponse(
            UUID id,
            String workspaceId,
            String organizerId,
            String title,
            String description,
            Instant startsAt,
            Instant endsAt,
            String timezone,
            int capacity,
            int confirmedCount,
            String status,
            boolean registrationOpen,
            long version) { }

    public record UploadSessionRequest(@NotBlank String fileName, @NotBlank String contentType, @Min(1) long size) { }

    public record UploadSessionResponse(UUID mediaId, String objectKey, String uploadUrl, Instant expiresAt) { }

    public record FinalizeMediaResponse(UUID mediaId, String state, String objectKey, long actualSize, String contentType) { }
}
