package com.eventflow.registration.reconciliation;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

@Component
@ConditionalOnProperty(name = "eventflow.reconciliation.enabled", havingValue = "true")
public class EventReconciliationClient {
    private final RestClient client;
    private final String internalKey;

    public EventReconciliationClient(RestClient.Builder builder,
                                     @Value("${eventflow.event-service.url:http://localhost:8081}") String url,
                                     @Value("${eventflow.reconciliation.internal-key:}") String internalKey) {
        this.client = builder.baseUrl(url).build();
        this.internalKey = internalKey;
    }

    public CoordinationSnapshot get(UUID eventId, String workspaceId) {
        Snapshot response = client.get().uri("/internal/reconciliation/events/{id}", eventId)
                .header(InternalReconciliationAuth.KEY_HEADER, internalKey)
                .header(InternalReconciliationAuth.WORKSPACE_HEADER, workspaceId)
                .retrieve().body(Snapshot.class);
        if (response == null) throw new IllegalStateException("Event reconciliation source returned no body");
        return response.toCoordination();
    }

    public CoordinationSnapshot apply(CoordinationSnapshot snapshot, UUID mergeId, long expectedVersion) {
        ApplyRequest request = new ApplyRequest(mergeId, snapshot.workspaceId(), snapshot.status(),
                snapshot.registrationOpen(), snapshot.capacity(), snapshot.confirmedCount(),
                expectedVersion, snapshot.fieldClocks());
        Snapshot response = client.post().uri("/internal/reconciliation/events/{id}/apply", snapshot.eventId())
                .header(InternalReconciliationAuth.KEY_HEADER, internalKey)
                .header(InternalReconciliationAuth.WORKSPACE_HEADER, snapshot.workspaceId())
                .body(request).retrieve().body(Snapshot.class);
        if (response == null) throw new IllegalStateException("Event reconciliation apply returned no body");
        return response.toCoordination();
    }

    public List<SourceRef> list(String workspaceId, UUID after, int limit) {
        SourceRef[] response = client.get().uri(builder -> {
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
                .retrieve().body(SourceRef[].class);
        return response == null ? List.of() : Arrays.asList(response);
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
