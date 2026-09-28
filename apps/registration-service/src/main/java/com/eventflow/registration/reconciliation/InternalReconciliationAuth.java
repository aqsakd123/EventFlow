package com.eventflow.registration.reconciliation;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import com.eventflow.registration.api.ApiException;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "eventflow.reconciliation.enabled", havingValue = "true")
public class InternalReconciliationAuth {
    public static final String KEY_HEADER = "X-EventFlow-Reconciliation-Key";
    public static final String WORKSPACE_HEADER = "X-Workspace-Id";

    private final byte[] expected;

    public InternalReconciliationAuth(
            @Value("${eventflow.reconciliation.internal-key:}") String internalKey) {
        if (internalKey == null || internalKey.isBlank()) {
            throw new IllegalStateException("eventflow.reconciliation.internal-key is required when reconciliation is enabled");
        }
        this.expected = internalKey.getBytes(StandardCharsets.UTF_8);
    }

    public void require(String presented) {
        byte[] actual = presented == null ? new byte[0] : presented.getBytes(StandardCharsets.UTF_8);
        if (!MessageDigest.isEqual(expected, actual)) {
            throw ApiException.unauthorized("RECONCILIATION_AUTH_REQUIRED",
                    "Valid reconciliation service credentials are required");
        }
    }

    public String requireWorkspace(String workspaceId) {
        if (workspaceId == null || workspaceId.isBlank()) {
            throw ApiException.forbidden("TENANT_SCOPE_REQUIRED", "A workspace scope is required");
        }
        return workspaceId;
    }
}
