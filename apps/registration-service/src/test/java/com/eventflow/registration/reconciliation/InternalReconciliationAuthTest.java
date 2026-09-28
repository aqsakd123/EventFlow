package com.eventflow.registration.reconciliation;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.eventflow.registration.api.ApiException;

import org.junit.jupiter.api.Test;

class InternalReconciliationAuthTest {
    private final InternalReconciliationAuth auth = new InternalReconciliationAuth("secret");

    @Test
    void rejectsMissingOrWrongCredential() {
        assertThrows(ApiException.class, () -> auth.require(null));
        assertThrows(ApiException.class, () -> auth.require("wrong"));
    }

    @Test
    void acceptsExactCredentialAndRequiresWorkspaceScope() {
        assertDoesNotThrow(() -> auth.require("secret"));
        assertDoesNotThrow(() -> auth.requireWorkspace("workspace-1"));
        assertThrows(ApiException.class, () -> auth.requireWorkspace(" "));
    }
}
