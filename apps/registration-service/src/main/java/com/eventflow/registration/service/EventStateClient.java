package com.eventflow.registration.service;

import java.util.UUID;

import com.eventflow.registration.api.ApiException;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;

/**
 * Reads Event Service's source state immediately before mutations that must not
 * rely on a stale Rabbit projection.
 */
@Component
public class EventStateClient {
    private final RestClient client;

    public EventStateClient(RestClient.Builder builder,
                            @Value("${eventflow.event-service.url:http://localhost:8081}") String baseUrl) {
        this.client = builder.baseUrl(baseUrl).build();
    }

    public SourceState read(UUID eventId) {
        try {
            SourceState state = client.get()
                    .uri("/internal/events/{eventId}/registration-state", eventId)
                    .retrieve()
                    .body(SourceState.class);
            if (state == null) {
                throw ApiException.serviceUnavailable("EVENT_SOURCE_UNAVAILABLE", "Event source returned no state");
            }
            return state;
        } catch (RestClientResponseException exception) {
            HttpStatusCode status = exception.getStatusCode();
            if (status.value() == 404) {
                throw ApiException.notFound("EVENT_NOT_FOUND", "Event was not found");
            }
            throw ApiException.serviceUnavailable("EVENT_SOURCE_UNAVAILABLE", "Event source state is unavailable");
        } catch (RestClientException exception) {
            throw ApiException.serviceUnavailable("EVENT_SOURCE_UNAVAILABLE", "Event source state is unavailable");
        }
    }

    public SourceState requireRegistrationAllowed(UUID eventId, String workspaceId) {
        SourceState state = read(eventId);
        requireWorkspace(state, workspaceId);
        if (!"PUBLISHED".equals(state.status()) || !state.registrationOpen()) {
            throw ApiException.conflict("REGISTRATION_CLOSED", "Registration is not open");
        }
        return state;
    }

    public SourceState requireCheckinAllowed(UUID eventId, String workspaceId) {
        SourceState state = read(eventId);
        requireWorkspace(state, workspaceId);
        if (!"PUBLISHED".equals(state.status())) {
            throw ApiException.conflict("EVENT_NOT_CHECKINABLE", "Event is not active");
        }
        return state;
    }

    private void requireWorkspace(SourceState state, String workspaceId) {
        if (!state.workspaceId().equals(workspaceId)) {
            throw ApiException.forbidden("TENANT_ACCESS_DENIED", "The resource belongs to another workspace");
        }
    }

    public record SourceState(UUID eventId, String workspaceId, String status,
                              boolean registrationOpen, long version) { }
}
