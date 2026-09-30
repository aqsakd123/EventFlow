package com.eventflow.eventservice.service;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Keeps the short server-side primary pin and emits the causal values that a
 * client can echo when requests are load-balanced across application nodes.
 */
@Component
public class ConsistencyPinRegistry {
    public static final Duration WRITE_PIN = Duration.ofSeconds(3);
    public static final String MIN_LSN_HEADER = "X-EventFlow-Min-LSN";
    public static final String MIN_VERSION_HEADER = "X-EventFlow-Min-Version";
    public static final String COMMIT_LSN_HEADER = "X-EventFlow-Commit-LSN";
    public static final String ENTITY_VERSION_HEADER = "X-EventFlow-Entity-Version";
    public static final String PIN_UNTIL_HEADER = "X-EventFlow-Write-Pin-Until";
    public static final String SESSION_HEADER = "X-EventFlow-Consistency-Session";
    public static final String SCOPE_HEADER = "X-EventFlow-Consistency-Scope";
    public static final String ENTITY_KEY_HEADER = "X-EventFlow-Entity-Key";

    private static final Pattern LSN = Pattern.compile("(?i)^[0-9a-f]{1,16}/[0-9a-f]{1,16}$");

    private final JdbcTemplate writer;
    private final Clock clock;
    private final Map<String, Pin> pins = new ConcurrentHashMap<>();

    @Autowired
    public ConsistencyPinRegistry(JdbcTemplate writer) {
        this(writer, Clock.systemUTC());
    }

    ConsistencyPinRegistry(JdbcTemplate writer, Clock clock) {
        this.writer = writer;
        this.clock = clock;
    }

    public Receipt recordCommittedWrite(HttpServletRequest request, String scope, String entityKey, long entityVersion) {
        Instant pinUntil = clock.instant().plus(WRITE_PIN);
        String lsn = null;
        try {
            // The service transaction has returned (and committed) before the
            // controller calls this method, so this LSN includes its commit.
            lsn = writer.queryForObject("SELECT pg_current_wal_flush_lsn()::text", String.class);
        } catch (RuntimeException ignored) {
            // A successful business commit must not become HTTP 500 merely
            // because its optional consistency receipt could not be created.
        }
        pins.put(pinKey(request, scope, entityKey), new Pin(lsn, entityVersion, pinUntil));
        return new Receipt(lsn, entityVersion, pinUntil, scope, entityKey);
    }

    public boolean isPinned(HttpServletRequest request, String scope, String entityKey) {
        if (headerPinIsActive(request, scope, entityKey)) return true;
        String key = pinKey(request, scope, entityKey);
        Pin pin = pins.get(key);
        if (pin == null) return false;
        if (clock.instant().isBefore(pin.until())) return true;
        pins.remove(key, pin);
        return false;
    }

    private boolean headerPinIsActive(HttpServletRequest request, String scope, String entityKey) {
        String value = request.getHeader(PIN_UNTIL_HEADER);
        if (value == null || requiredLsn(request) == null || minimumVersion(request).isEmpty()) return false;
        if (!matchesReceiptScope(request, scope, entityKey, entityKey != null)) return false;
        try {
            Instant now = clock.instant();
            Instant until = Instant.parse(value);
            // A consistency hint can move traffic to the writer, never grant
            // data access. Clamp it so a forged header cannot create a long pin.
            return now.isBefore(until) && !until.isAfter(now.plus(WRITE_PIN));
        } catch (RuntimeException ignored) {
            return false;
        }
    }

    public boolean matchesReceiptScope(HttpServletRequest request, String scope, String entityKey,
                                       boolean requireEntityKey) {
        if (!java.util.Objects.equals(scope, request.getHeader(SCOPE_HEADER))) return false;
        return !requireEntityKey || (entityKey != null && java.util.Objects.equals(entityKey,
                request.getHeader(ENTITY_KEY_HEADER)));
    }
    public String requiredLsn(HttpServletRequest request) {
        String value = request.getHeader(MIN_LSN_HEADER);
        return value != null && LSN.matcher(value.trim()).matches() ? value.trim().toUpperCase() : null;
    }

    public boolean hasInvalidLsn(HttpServletRequest request) {
        String value = request.getHeader(MIN_LSN_HEADER);
        return value != null && !value.isBlank() && !LSN.matcher(value.trim()).matches();
    }

    public OptionalLong minimumVersion(HttpServletRequest request) {
        String value = request.getHeader(MIN_VERSION_HEADER);
        if (value == null || value.isBlank()) return OptionalLong.empty();
        try {
            long parsed = Long.parseLong(value);
            return parsed >= 0 ? OptionalLong.of(parsed) : OptionalLong.empty();
        } catch (NumberFormatException ignored) {
            return OptionalLong.empty();
        }
    }

    public boolean hasInvalidVersion(HttpServletRequest request) {
        String value = request.getHeader(MIN_VERSION_HEADER);
        if (value == null || value.isBlank()) return false;
        try {
            return Long.parseLong(value) < 0;
        } catch (NumberFormatException ignored) {
            return true;
        }
    }

    boolean isPinned(String key) {
        Pin pin = pins.get(key);
        return pin != null && clock.instant().isBefore(pin.until());
    }

    void put(String key, String lsn, long version, Instant until) {
        pins.put(key, new Pin(lsn, version, until));
    }

    private String sessionKey(HttpServletRequest request) {
        String explicit = request.getHeader(SESSION_HEADER);
        if (explicit != null && !explicit.isBlank()) return "session:" + explicit.trim();
        return "actor:" + value(request, "X-Workspace-Id") + ":" + value(request, "X-User-Id");
    }

    private String value(HttpServletRequest request, String name) {
        String value = request.getHeader(name);
        return value == null ? "anonymous" : value;
    }

    private String pinKey(HttpServletRequest request, String scope, String entityKey) {
        return sessionKey(request) + ":scope:" + (scope == null ? "global" : scope)
                + ":entity:" + (entityKey == null ? "*" : entityKey);
    }

    private record Pin(String lsn, long version, Instant until) { }

    public record Receipt(String lsn, long version, Instant pinUntil, String scope, String entityKey) {
        public void writeTo(HttpServletResponse response) {
            if (lsn != null) response.setHeader(COMMIT_LSN_HEADER, lsn);
            response.setHeader(SCOPE_HEADER, scope);
            if (entityKey != null) response.setHeader(ENTITY_KEY_HEADER, entityKey);
            response.setHeader(ENTITY_VERSION_HEADER, Long.toString(version));
            response.setHeader(PIN_UNTIL_HEADER, pinUntil.toString());
        }
    }
}
