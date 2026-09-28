package com.eventflow.eventservice.service;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

class ConsistencyPinRegistryTest {
    @Test
    void pinExpiresAtExactlyThreeSeconds() {
        MutableClock clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
        ConsistencyPinRegistry registry = new ConsistencyPinRegistry(mock(JdbcTemplate.class), clock);
        registry.put("session:test", "0/10", 4, clock.instant().plusSeconds(3));

        clock.set(Instant.parse("2026-01-01T00:00:02.999Z"));
        assertTrue(registry.isPinned("session:test"));

        clock.set(Instant.parse("2026-01-01T00:00:03Z"));
        assertFalse(registry.isPinned("session:test"));
    }

    private static final class MutableClock extends Clock {
        private Instant value;

        private MutableClock(Instant value) { this.value = value; }
        void set(Instant value) { this.value = value; }
        @Override public ZoneId getZone() { return ZoneId.of("UTC"); }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return value; }
    }
}
