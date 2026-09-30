package com.eventflow.eventservice.service;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.servlet.http.HttpServletRequest;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

class EventReadRouterTest {
    @Test
    void offModeIgnoresCausalHeadersAndRoutesToHealthyReplica() {
        JdbcTemplate writer = mock(JdbcTemplate.class);
        JdbcTemplate replicaJdbc = mock(JdbcTemplate.class);
        ConsistencyPinRegistry pins = mock(ConsistencyPinRegistry.class);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getHeader(ConsistencyPinRegistry.MIN_LSN_HEADER)).thenReturn("0/FFFF");
        when(request.getAttribute(ReadConsistencyPolicy.REQUEST_ATTRIBUTE)).thenReturn(ReadConsistencyPolicy.off());
        when(replicaJdbc.queryForMap(anyString())).thenReturn(Map.of(
                "major", 16, "recovery", true, "replay_lsn", "0/100", "caught_up", true));

        EventReadRouter router = new EventReadRouter(writer, pins, new SimpleMeterRegistry(), List.of(
                new EventReadRouter.Replica("replica-1", replicaJdbc, null)));

        assertFalse(router.select(request, true, "event-a").primary());
    }

    @Test
    void versionLsnWithoutRequiredLsnUsesPrimary() {
        JdbcTemplate writer = mock(JdbcTemplate.class);
        ConsistencyPinRegistry pins = mock(ConsistencyPinRegistry.class);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getAttribute(ReadConsistencyPolicy.REQUEST_ATTRIBUTE))
                .thenReturn(new ReadConsistencyPolicy(ReadRoutingMode.VERSION_LSN, "feature", true));
        when(pins.requiredLsn(request)).thenReturn(null);

        EventReadRouter router = new EventReadRouter(writer, pins, new SimpleMeterRegistry(), List.of(
                new EventReadRouter.Replica("replica-1", mock(JdbcTemplate.class), null)));

        assertTrue(router.select(request, true, "event-a").primary());
    }

    @Test
    void featureThresholdsCanRouteAgainstDifferentReplicaReplayPoints() {
        JdbcTemplate writer = mock(JdbcTemplate.class);
        JdbcTemplate replicaJdbc = mock(JdbcTemplate.class);
        ConsistencyPinRegistry pins = mock(ConsistencyPinRegistry.class);
        HttpServletRequest featureA = mock(HttpServletRequest.class);
        HttpServletRequest featureB = mock(HttpServletRequest.class);
        ReadConsistencyPolicy policy = new ReadConsistencyPolicy(ReadRoutingMode.VERSION_LSN, "feature", true);
        when(featureA.getAttribute(ReadConsistencyPolicy.REQUEST_ATTRIBUTE)).thenReturn(policy);
        when(featureB.getAttribute(ReadConsistencyPolicy.REQUEST_ATTRIBUTE)).thenReturn(policy);
        when(pins.hasInvalidLsn(featureA)).thenReturn(false);
        when(pins.hasInvalidVersion(featureA)).thenReturn(false);
        when(pins.hasInvalidLsn(featureB)).thenReturn(false);
        when(pins.hasInvalidVersion(featureB)).thenReturn(false);
        when(pins.matchesReceiptScope(featureA, "feature", "feature-a", true)).thenReturn(true);
        when(pins.matchesReceiptScope(featureB, "feature", "feature-b", true)).thenReturn(true);
        when(pins.requiredLsn(featureA)).thenReturn("0/3E9");
        when(pins.requiredLsn(featureB)).thenReturn("0/3E8");
        when(pins.minimumVersion(featureA)).thenReturn(java.util.OptionalLong.of(1));
        when(pins.minimumVersion(featureB)).thenReturn(java.util.OptionalLong.of(1));
        when(replicaJdbc.queryForMap(anyString(), any()))
                .thenReturn(Map.of("major", 16, "recovery", true, "replay_lsn", "0/3E8", "caught_up", false))
                .thenReturn(Map.of("major", 16, "recovery", true, "replay_lsn", "0/3E8", "caught_up", true));

        EventReadRouter router = new EventReadRouter(writer, pins, new SimpleMeterRegistry(), List.of(
                new EventReadRouter.Replica("replica-1", replicaJdbc, null)));

        assertTrue(router.select(featureA, true, "feature-a").primary());
        assertFalse(router.select(featureB, true, "feature-b").primary());
    }

    @Test
    void primaryPinModeHonorsScopedPin() {
        JdbcTemplate writer = mock(JdbcTemplate.class);
        ConsistencyPinRegistry pins = mock(ConsistencyPinRegistry.class);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getAttribute(ReadConsistencyPolicy.REQUEST_ATTRIBUTE))
                .thenReturn(new ReadConsistencyPolicy(ReadRoutingMode.PRIMARY_PIN, "feature", true));
        when(pins.isPinned(request, "feature", "event-a")).thenReturn(true);

        EventReadRouter router = new EventReadRouter(writer, pins, new SimpleMeterRegistry(), List.of(
                new EventReadRouter.Replica("replica-1", mock(JdbcTemplate.class), null)));

        assertTrue(router.select(request, true, "event-a").primary());
    }
}