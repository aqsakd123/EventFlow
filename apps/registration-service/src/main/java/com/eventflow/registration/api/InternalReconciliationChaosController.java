package com.eventflow.registration.api;

import java.util.UUID;

import com.eventflow.registration.reconciliation.CoordinationSnapshot;
import com.eventflow.registration.reconciliation.InternalReconciliationAuth;
import com.eventflow.registration.reconciliation.ReconciliationChaosService;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/internal/reconciliation/events")
@ConditionalOnProperty(name = "eventflow.reconciliation.chaos-writes-enabled", havingValue = "true")
public class InternalReconciliationChaosController {
    private final ReconciliationChaosService service;
    private final InternalReconciliationAuth auth;

    public InternalReconciliationChaosController(ReconciliationChaosService service, InternalReconciliationAuth auth) {
        this.service = service;
        this.auth = auth;
    }

    @PatchMapping("/{eventId}/chaos")
    CoordinationSnapshot patch(@PathVariable UUID eventId,
                               @RequestHeader(value = InternalReconciliationAuth.KEY_HEADER, required = false) String key,
                               @RequestHeader(value = InternalReconciliationAuth.WORKSPACE_HEADER, required = false) String workspaceId,
                               @RequestBody ReconciliationChaosService.ChaosPatch request) {
        auth.require(key);
        return service.patch(eventId, auth.requireWorkspace(workspaceId), request);
    }
}
