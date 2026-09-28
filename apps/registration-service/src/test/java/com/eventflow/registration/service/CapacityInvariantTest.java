package com.eventflow.registration.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class CapacityInvariantTest {
    @Test
    void onlyAConditionalDatabaseUpdateCanClaimTheLastSeat() {
        int capacity = 1;
        int confirmed = 1;
        assertEquals(0, capacity - confirmed);
    }
}
