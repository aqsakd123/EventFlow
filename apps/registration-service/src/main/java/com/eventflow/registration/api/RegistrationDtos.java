package com.eventflow.registration.api;

import java.time.Instant;
import java.util.UUID;

import jakarta.validation.constraints.NotBlank;

public final class RegistrationDtos {
    private RegistrationDtos() { }

    public record RegistrationResponse(UUID registrationId, UUID eventId, String participantId, String status,
                                       Instant registeredAt, boolean checkedIn, long version) { }

    public record CheckInRequest(@NotBlank String participantId) { }

    public record CheckInResponse(UUID eventId, String participantId, String status, Instant checkedInAt) { }

    public record AttendanceRow(String participantId, String status, Instant checkedInAt) { }
}
