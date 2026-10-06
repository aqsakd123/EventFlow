package com.eventflow.registration.reconciliation;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

import com.eventflow.registration.resilience.InternalHttpResilience;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;

@Component
@ConditionalOnProperty(name = "eventflow.reconciliation.enabled", havingValue = "true")
public class EventReconciliationClient {
    private final RestClient client;
    private final String internalKey;
    private final CircuitBreaker circuitBreaker;

    public EventReconciliationClient(
            RestClient.Builder builder,
            InternalHttpResilience resilience,
            @Value("$" + "{eventflow.event-service.url:http://localhost:8081}") String url,
            @Value("$" + "{eventflow.reconciliation.internal-key:}") String internalKey) {
        this.client = builder.requestFactory(resilience.requestFactory()).baseUrl(url).build();
        this.internalKey = internalKey;
        this.circuitBreaker = resilience.circuitBreaker(
                "event-service-reconciliation",
                exception -> !(exception instanceof NonTransientResponseException));
    }

    public CoordinationSnapshot get(UUID eventId, String workspaceId) {
        Snapshot response = execute(() -> client.get().uri("/internal/reconciliation/events/{id}", eventId)
                .header(InternalReconciliationAuth.KEY_HEADER, internalKey)
                .header(InternalReconciliationAuth.WORKSPACE_HEADER, workspaceId)
                .retrieve().body(Snapshot.class));
        if (response == null) throw new IllegalStateException("Event reconciliation source returned no body");
        return response.toCoordination();
    }

    public CoordinationSnapshot apply(CoordinationSnapshot snapshot, UUID mergeId, long expectedVersion) {
        ApplyRequest request = new ApplyRequest(mergeId, snapshot.workspaceId(), snapshot.status(),
                snapshot.registrationOpen(), snapshot.capacity(), snapshot.confirmedCount(),
                expectedVersion, snapshot.fieldClocks());
        Snapshot response = execute(() -> client.post().uri("/internal/reconciliation/events/{id}/apply", snapshot.eventId())
                .header(InternalReconciliationAuth.KEY_HEADER, internalKey)
                .header(InternalReconciliationAuth.WORKSPACE_HEADER, snapshot.workspaceId())
                .body(request).retrieve().body(Snapshot.class));
        if (response == null) throw new IllegalStateException("Event reconciliation apply returned no body");
        return response.toCoordination();
    }

    public List<SourceRef> list(String workspaceId, UUID after, int limit) {
        SourceRef[] response = execute(() -> client.get().uri(builder -> {
                    builder.path("/internal/reconciliation/events/ids").queryParam("limit", limit);
                    if (after != null) builder.queryParam("after", after);
                    return builder.build();
                })
                .headers(headers -> {
                    headers.set(InternalReconciliationAuth.KEY_HEADER, internalKey);
                    if (workspaceId != null && !workspaceId.isBlank()) {
                        headers.set(InternalReconciliationAuth.WORKSPACE_HEADER, workspaceId);
                    }
                })
                .retrieve().body(SourceRef[].class));
        return response == null ? List.of() : Arrays.asList(response);
    }

    private <T> T execute(Supplier<T> operation) {
        try {
            return circuitBreaker.executeSupplier(() -> {
                try {
                    return operation.get();
                } catch (RestClientResponseException exception) {
                    int status = exception.getStatusCode().value();
                    if (status == 404 || status == 409) {
                        throw new NonTransientResponseException(exception);
                    }
                    throw exception;
                }
            });
        } catch (NonTransientResponseException exception) {
            throw exception.response();
        } catch (CallNotPermittedException | RestClientException exception) {
            throw new IllegalStateException("Event reconciliation dependency unavailable", exception);
        }
    }

    private static final class NonTransientResponseException extends RuntimeException {
        private NonTransientResponseException(RestClientResponseException response) {
            super(response);
        }

        private RestClientResponseException response() {
            return (RestClientResponseException) getCause();
        }
    }

    private record Snapshot(UUID eventId, String workspaceId, String status, boolean registrationOpen,
                            int capacity, int confirmedCount, long version, Map<String, String> fieldClocks) {
        CoordinationSnapshot toCoordination() {
            return new CoordinationSnapshot(eventId, workspaceId, status, registrationOpen, capacity,
                    confirmedCount, version, fieldClocks == null ? Map.of() : fieldClocks);
        }
    }

    private record ApplyRequest(UUID mergeId, String workspaceId, String status, boolean registrationOpen,
                                int capacity, int confirmedCount, long expectedVersion,
                                Map<String, String> fieldClocks) { }

    public record SourceRef(UUID eventId, String workspaceId) { }
}
