package com.eventflow.eventservice.domain;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;

import org.junit.jupiter.api.Test;

class StateRulesTest {
    @Test
    void rejectsNonIncreasingWindow() {
        assertThrows(RuntimeException.class, () -> StateRules.validateWindow(Instant.MAX, Instant.MIN));
    }

    @Test
    void protectsConfirmedCapacity() {
        assertThrows(RuntimeException.class, () -> StateRules.requireCapacity(9, 10));
        assertDoesNotThrow(() -> StateRules.requireCapacity(10, 10));
    }
}
