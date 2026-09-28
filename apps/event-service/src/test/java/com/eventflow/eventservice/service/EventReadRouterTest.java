package com.eventflow.eventservice.service;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.OptionalLong;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.servlet.http.HttpServletRequest;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

class EventReadRouterTest {
    @Test
    void versionWithoutLsnUsesWriterConservatively() {
        JdbcTemplate writer = mock(JdbcTemplate.class);
        ConsistencyPinRegistry pins = mock(ConsistencyPinRegistry.class);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(pins.isPinned(request)).thenReturn(false);
        when(pins.hasInvalidLsn(request)).thenReturn(false);
        when(pins.hasInvalidVersion(request)).thenReturn(false);
        when(pins.requiredLsn(request)).thenReturn(null);
        when(pins.minimumVersion(request)).thenReturn(OptionalLong.of(7));

        EventReadRouter router = new EventReadRouter(writer, pins, new SimpleMeterRegistry(), List.of(
                new EventReadRouter.Replica("replica-1", mock(JdbcTemplate.class), null)));

        assertTrue(router.select(request, true).primary());
    }
}
