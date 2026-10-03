package com.eventflow.eventservice.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/** Explicit routing for safe Event read APIs. All other JdbcTemplate users keep the writer. */
@Component
public class EventReadRouter {
    public static final String ROUTE_HEADER = "X-EventFlow-DB-Route";
    public static final String REPLAY_LSN_HEADER = "X-EventFlow-Replay-LSN";

    private final JdbcTemplate writer;
    private final ConsistencyPinRegistry pins;
    private final List<Replica> replicas;
    private final AtomicInteger cursor = new AtomicInteger();
    private final MeterRegistry meterRegistry;

    @Autowired
    public EventReadRouter(
            DataSource writerDataSource,
            ConsistencyPinRegistry pins,
            MeterRegistry meterRegistry,
            @Value("${eventflow.database.read-replica-urls:}") String replicaUrls,
            @Value("${eventflow.database.replica-username:${spring.datasource.username}}") String username,
            @Value("${eventflow.database.replica-password:${spring.datasource.password}}") String password,
            @Value("${eventflow.database.replica-pool-size:5}") int poolSize) {
        this.writer = new JdbcTemplate(writerDataSource);
        this.pins = pins;
        this.meterRegistry = meterRegistry;
        this.replicas = createReplicas(replicaUrls, username, password, poolSize);
    }

    EventReadRouter(JdbcTemplate writer, ConsistencyPinRegistry pins, MeterRegistry meterRegistry,
                    List<Replica> replicas) {
        this.writer = writer;
        this.pins = pins;
        this.meterRegistry = meterRegistry;
        this.replicas = replicas;
    }

    public Selection select(HttpServletRequest request, boolean singleAggregateRead) {
        return select(request, singleAggregateRead, null);
    }

    public Selection select(HttpServletRequest request, boolean singleAggregateRead, String entityKey) {
        if (replicas.isEmpty()) return writer(request, "routing-disabled");

        ReadConsistencyPolicy policy = ReadConsistencyPolicy.from(request);
        if (policy.mode() == ReadRoutingMode.PRIMARY_PIN
                && pins.isPinned(request, policy.scope(), entityKey)) {
            return writer(request, "write-pin");
        }

        String requiredLsn = null;
        if (policy.mode() == ReadRoutingMode.VERSION_LSN) {
            if (pins.hasInvalidLsn(request)) return writer(request, "invalid-lsn");
            if (pins.hasInvalidVersion(request)) return writer(request, "invalid-version");
            if (pins.requiredLsn(request) == null) return writer(request, "missing-lsn");
            if (!pins.matchesReceiptScope(request, policy.scope(), entityKey, policy.requireEntityVersion())) {
                return writer(request, "scope-mismatch");
            }
            if (policy.requireEntityVersion() && pins.minimumVersion(request).isEmpty()) {
                return writer(request, "missing-version");
            }
            if (policy.requireEntityVersion() && !singleAggregateRead) {
                return writer(request, "version-not-applicable");
            }
            requiredLsn = pins.requiredLsn(request);
        }
        int start = Math.floorMod(cursor.getAndIncrement(), replicas.size());
        for (int offset = 0; offset < replicas.size(); offset++) {
            Replica replica = replicas.get((start + offset) % replicas.size());
            Probe probe = probe(replica, requiredLsn);
            if (probe.eligible()) {
                Selection selected = new Selection(replica.jdbc(), replica.name(), probe.replayLsn(), false);
                remember(request, selected, "replica");
                return selected;
            }
        }
        return writer(request, requiredLsn == null ? "no-healthy-replica" : "lsn-not-replayed");
    }

    public Selection fallbackToWriter(HttpServletRequest request, String reason) {
        return writer(request, reason);
    }

    public void writeResponseHeaders(HttpServletRequest request, HttpServletResponse response) {
        Object selection = request.getAttribute(Selection.class.getName());
        if (!(selection instanceof Selection selected)) return;
        response.setHeader(ROUTE_HEADER, selected.route());
        if (selected.replayLsn() != null) response.setHeader(REPLAY_LSN_HEADER, selected.replayLsn());
    }

    private Selection writer(HttpServletRequest request, String reason) {
        Selection selected = new Selection(writer, "primary", null, true);
        remember(request, selected, reason);
        return selected;
    }

    private void remember(HttpServletRequest request, Selection selection, String reason) {
        request.setAttribute(Selection.class.getName(), selection);
        Counter.builder("eventflow.database.read.routes")
                .tag("target", selection.primary() ? "primary" : "replica")
                .tag("reason", reason)
                .register(meterRegistry)
                .increment();
    }

    private Probe probe(Replica replica, String requiredLsn) {
        try {
            Map<String, Object> row;
            if (requiredLsn == null) {
                row = replica.jdbc().queryForMap("""
                        SELECT current_setting('server_version_num')::int / 10000 AS major,
                               pg_is_in_recovery() AS recovery,
                               pg_last_wal_replay_lsn()::text AS replay_lsn,
                               true AS caught_up
                        """);
            } else {
                row = replica.jdbc().queryForMap("""
                        SELECT current_setting('server_version_num')::int / 10000 AS major,
                               pg_is_in_recovery() AS recovery,
                               pg_last_wal_replay_lsn()::text AS replay_lsn,
                               COALESCE(pg_wal_lsn_diff(pg_last_wal_replay_lsn(), ?::pg_lsn) >= 0, false) AS caught_up
                        """, requiredLsn);
            }
            boolean eligible = number(row.get("major")) == 16
                    && Boolean.TRUE.equals(row.get("recovery"))
                    && Boolean.TRUE.equals(row.get("caught_up"));
            return new Probe(eligible, (String) row.get("replay_lsn"));
        } catch (RuntimeException ignored) {
            return new Probe(false, null);
        }
    }

    private int number(Object value) {
        return value instanceof Number number ? number.intValue() : -1;
    }

    private List<Replica> createReplicas(String urls, String username, String password, int poolSize) {
        if (urls == null || urls.isBlank()) return List.of();
        String[] split = urls.split(",");
        if (split.length != 1) {
            throw new IllegalArgumentException("EVENT_DB_READ_REPLICA_URLS must contain exactly 1 comma-separated URL");
        }
        List<Replica> created = new ArrayList<>();
        for (int index = 0; index < split.length; index++) {
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(split[index].trim());
            config.setUsername(username);
            config.setPassword(password);
            config.setReadOnly(true);
            config.setMaximumPoolSize(poolSize);
            config.setMinimumIdle(0);
            config.setConnectionTimeout(1_000);
            config.setInitializationFailTimeout(-1);
            config.setPoolName("event-read-replica-" + (index + 1));
            HikariDataSource dataSource = new HikariDataSource(config);
            created.add(new Replica("replica-" + (index + 1), new JdbcTemplate(dataSource), dataSource));
        }
        return List.copyOf(created);
    }

    @PreDestroy
    void close() {
        replicas.stream().map(Replica::dataSource).filter(dataSource -> dataSource != null)
                .forEach(HikariDataSource::close);
    }

    public record Selection(JdbcTemplate jdbc, String route, String replayLsn, boolean primary) { }
    record Replica(String name, JdbcTemplate jdbc, HikariDataSource dataSource) { }
    private record Probe(boolean eligible, String replayLsn) { }
}
