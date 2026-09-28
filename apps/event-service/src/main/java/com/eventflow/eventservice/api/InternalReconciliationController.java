package com.eventflow.eventservice.api;

import java.util.UUID;

import com.eventflow.eventservice.reconciliation.EventReconciliationService;
import com.eventflow.eventservice.reconciliation.InternalReconciliationAuth;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/internal/reconciliation/events")
@ConditionalOnProperty(name = "eventflow.reconciliation.enabled", havingValue = "true")
public class InternalReconciliationController {
    private final EventReconciliationService service;
    private final InternalReconciliationAuth auth;

    public InternalReconciliationController(EventReconciliationService service, InternalReconciliationAuth auth) {
        this.service = service;
        this.auth = auth;
    }

    @GetMapping("/ids")
    java.util.List<EventReconciliationService.SourceRef> ids(
            @RequestHeader(value = InternalReconciliationAuth.KEY_HEADER, required = false) String key,
            @RequestHeader(value = InternalReconciliationAuth.WORKSPACE_HEADER, required = false) String workspaceId,
            @RequestParam(required = false) UUID after,
            @RequestParam(defaultValue = "200") int limit) {
        auth.require(key);
        return service.listIds(workspaceId, after, limit);
    }

    @GetMapping("/{eventId}")
    EventReconciliationService.Snapshot get(
            @PathVariable UUID eventId,
            @RequestHeader(value = InternalReconciliationAuth.KEY_HEADER, required = false) String key,
            @RequestHeader(value = InternalReconciliationAuth.WORKSPACE_HEADER, required = false) String workspaceId) {
        auth.require(key);
        EventReconciliationService.Snapshot snapshot = service.get(eventId);
        auth.requireWorkspace(workspaceId, snapshot.workspaceId());
        return snapshot;
    }

    @PostMapping("/{eventId}/apply")
    EventReconciliationService.Snapshot apply(
                                              @PathVariable UUID eventId,
                                              @RequestHeader(value = InternalReconciliationAuth.KEY_HEADER, required = false) String key,
                                              @RequestHeader(value = InternalReconciliationAuth.WORKSPACE_HEADER, required = false) String workspaceId,
                                              @RequestBody EventReconciliationService.ApplyRequest request) {
        auth.require(key);
        auth.requireWorkspace(workspaceId, request.workspaceId());
        return service.apply(eventId, request);
    }
}
