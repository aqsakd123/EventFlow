package com.eventflow.registration.reconciliation;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@Component
@ConditionalOnProperty(name = "eventflow.reconciliation.enabled", havingValue = "true")
public class EventProjectionReconciliationWorker {
    private static final Logger log = LoggerFactory.getLogger(EventProjectionReconciliationWorker.class);
    private static final int BATCH_SIZE = 200;

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;
    private final EventReconciliationClient client;
    private final ReconciliationMerger merger;
    private final TransactionTemplate transactions;
    private final boolean automatic;
    private final Map<String, UUID> sourceCursors = new ConcurrentHashMap<>();
    private final Counter resolvedMetric;
    private final Counter conflictMetric;
    private final Counter quarantineMetric;
    private final Counter failedMetric;

    public EventProjectionReconciliationWorker(
            JdbcTemplate jdbc,
            ObjectMapper objectMapper,
            EventReconciliationClient client,
            ReconciliationMerger merger,
            PlatformTransactionManager transactionManager,
            MeterRegistry registry,
            @Value("${eventflow.reconciliation.auto:false}") boolean automatic) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
        this.client = client;
        this.merger = merger;
        this.transactions = new TransactionTemplate(transactionManager);
        this.automatic = automatic;
        this.resolvedMetric = registry.counter("eventflow.reconciliation.resolved");
        this.conflictMetric = registry.counter("eventflow.reconciliation.conflicts");
        this.quarantineMetric = registry.counter("eventflow.reconciliation.quarantined");
        this.failedMetric = registry.counter("eventflow.reconciliation.failed");
    }

    @Scheduled(fixedDelayString = "${eventflow.reconciliation.poll-ms:5000}")
    public void scheduled() {
        if (automatic) runOnce(null);
    }

    public Report runOnce() {
        return runOnce(null);
    }

    public synchronized Report runOnce(String workspaceId) {
        LinkedHashMap<UUID, String> candidates = new LinkedHashMap<>();
        localCandidates(workspaceId).forEach(candidate ->
                candidates.put(candidate.eventId(), candidate.workspaceId()));

        String cursorKey = workspaceId == null ? "*" : workspaceId;
        List<EventReconciliationClient.SourceRef> sourcePage =
                client.list(workspaceId, sourceCursors.get(cursorKey), BATCH_SIZE);
        sourcePage.forEach(source -> candidates.putIfAbsent(source.eventId(), source.workspaceId()));
        if (sourcePage.size() < BATCH_SIZE) {
            sourceCursors.remove(cursorKey);
        } else {
            sourceCursors.put(cursorKey, sourcePage.get(sourcePage.size() - 1).eventId());
        }

        int resolved = 0;
        int conflicts = 0;
        int quarantined = 0;
        int failed = 0;
        for (Map.Entry<UUID, String> candidate : candidates.entrySet()) {
            try {
                Outcome outcome = reconcile(candidate.getKey(), candidate.getValue());
                resolved += outcome.resolved() ? 1 : 0;
                conflicts += outcome.conflicts();
                quarantined += outcome.quarantined() ? 1 : 0;
            } catch (RuntimeException exception) {
                failed++;
                failedMetric.increment();
                log.warn("Reconciliation failed eventId={} error={}", candidate.getKey(), exception.getMessage());
            } finally {
                markAttempted(candidate.getKey());
            }
        }
        return new Report(candidates.size(), resolved, conflicts, quarantined, failed);
    }

    private List<Candidate> localCandidates(String workspaceId) {
        String select = """
                SELECT event_id, workspace_id
                FROM event_projections
                """;
        String order = " ORDER BY last_reconciled_at NULLS FIRST, updated_at, event_id LIMIT " + BATCH_SIZE;
        if (workspaceId == null) {
            return jdbc.query(select + order, (rs, rowNum) -> new Candidate(
                    UUID.fromString(rs.getString("event_id")), rs.getString("workspace_id")));
        }
        return jdbc.query(select + " WHERE workspace_id = ?" + order, (rs, rowNum) -> new Candidate(
                UUID.fromString(rs.getString("event_id")), rs.getString("workspace_id")), workspaceId);
    }

    private Outcome reconcile(UUID eventId, String workspaceHint) {
        LocalProjection local = local(eventId);
        String workspaceId = local == null ? workspaceHint : local.snapshot().workspaceId();
        CoordinationSnapshot source = client.get(eventId, workspaceId);
        if (local == null) {
            boolean inserted = insertMissingProjection(source);
            if (!inserted) throw new IllegalStateException("Projection appeared while reconciliation was applying");
            resolvedMetric.increment();
            return new Outcome(true, 0, false);
        }

        CoordinationSnapshot projection = local.snapshot();
        ReconciliationMerger.MergeResult merge = merger.merge(source, projection);
        if (merge.quarantined()) {
            quarantineMetric.increment();
            log.warn("Reconciliation quarantined eventId={} reason={}", eventId, merge.quarantineReason());
            return new Outcome(false, merge.conflicts().size(), true);
        }

        CoordinationSnapshot canonical = merge.snapshot();
        UUID mergeId = mergeId(canonical);
        int recordedConflicts = recordConflicts(eventId, mergeId, merge.conflicts());
        if (recordedConflicts > 0) conflictMetric.increment(recordedConflicts);

        CoordinationSnapshot resolvedSource = source;
        if (!sameData(canonical, source)) {
            resolvedSource = client.apply(canonical, mergeId, source.version());
        }
        CoordinationSnapshot localTarget = new CoordinationSnapshot(eventId, canonical.workspaceId(),
                resolvedSource.status(), resolvedSource.registrationOpen(), resolvedSource.capacity(),
                resolvedSource.confirmedCount(), resolvedSource.version(), resolvedSource.fieldClocks());
        boolean changed = !sameData(localTarget, projection) || localTarget.version() != projection.version();
        if (changed || !merge.conflicts().isEmpty()) {
            applyLocal(local, localTarget, mergeId);
        }
        if (changed) resolvedMetric.increment();
        return new Outcome(changed, recordedConflicts, false);
    }

    private boolean insertMissingProjection(CoordinationSnapshot source) {
        return jdbc.update("""
                INSERT INTO event_projections
                    (event_id, workspace_id, event_status, registration_open, capacity, confirmed_count,
                     event_version, sync_meta, local_revision, last_reconciled_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?::jsonb, 0, now(), now())
                ON CONFLICT (event_id) DO NOTHING
                """, source.eventId(), source.workspaceId(), source.status(), source.registrationOpen(),
                source.capacity(), source.confirmedCount(), source.version(), json(source.fieldClocks())) == 1;
    }

    private void applyLocal(LocalProjection expected, CoordinationSnapshot target, UUID mergeId) {
        transactions.executeWithoutResult(status -> {
            int applied = jdbc.update("""
                    INSERT INTO reconciliation_applied (merge_id, event_id, applied_at)
                    VALUES (?, ?, now()) ON CONFLICT (merge_id) DO NOTHING
                    """, mergeId, target.eventId());
            if (applied == 0) return;
            int changed = jdbc.update("""
                    UPDATE event_projections
                    SET event_status = ?, registration_open = ?, capacity = ?, confirmed_count = ?,
                        event_version = ?, sync_meta = ?::jsonb, local_revision = local_revision + 1,
                        last_reconciled_at = now(), updated_at = now()
                    WHERE event_id = ? AND event_version = ? AND local_revision = ?
                    """, target.status(), target.registrationOpen(), target.capacity(), target.confirmedCount(),
                    target.version(), json(target.fieldClocks()), target.eventId(),
                    expected.snapshot().version(), expected.localRevision());
            if (changed != 1) {
                throw new IllegalStateException("Projection changed while reconciliation was applying");
            }
        });
    }

    private int recordConflicts(UUID eventId, UUID mergeId, List<ReconciliationMerger.Conflict> conflicts) {
        int inserted = 0;
        for (ReconciliationMerger.Conflict conflict : conflicts) {
            String key = conflictKey(mergeId, conflict);
            inserted += jdbc.update("""
                    INSERT INTO reconciliation_conflicts
                        (conflict_key, merge_id, event_id, field_name, left_value, right_value,
                         winner_value, resolution, created_at)
                    VALUES (?, ?, ?, ?, ?::jsonb, ?::jsonb, ?::jsonb, ?, now())
                    ON CONFLICT (conflict_key) DO NOTHING
                    """, key, mergeId, eventId, conflict.field(), jsonValue(conflict.left()),
                    jsonValue(conflict.right()), jsonValue(conflict.winner()), conflict.resolution());
        }
        return inserted;
    }

    private LocalProjection local(UUID eventId) {
        return jdbc.query("""
                SELECT event_id, workspace_id, event_status, registration_open, capacity,
                       confirmed_count, event_version, sync_meta, local_revision
                FROM event_projections WHERE event_id = ?
                """, (rs, rowNum) -> new LocalProjection(new CoordinationSnapshot(
                        UUID.fromString(rs.getString("event_id")),
                        rs.getString("workspace_id"),
                        rs.getString("event_status"),
                        rs.getBoolean("registration_open"),
                        rs.getInt("capacity"),
                        rs.getInt("confirmed_count"),
                        rs.getLong("event_version"),
                        parse(rs.getString("sync_meta"))), rs.getLong("local_revision")), eventId)
                .stream().findFirst().orElse(null);
    }

    private void markAttempted(UUID eventId) {
        jdbc.update("UPDATE event_projections SET last_reconciled_at = now() WHERE event_id = ?", eventId);
    }

    private boolean sameData(CoordinationSnapshot left, CoordinationSnapshot right) {
        return left.workspaceId().equals(right.workspaceId())
                && left.status().equals(right.status())
                && left.registrationOpen() == right.registrationOpen()
                && left.capacity() == right.capacity()
                && left.confirmedCount() == right.confirmedCount()
                && left.fieldClocks().equals(right.fieldClocks());
    }

    private UUID mergeId(CoordinationSnapshot snapshot) {
        String stable = snapshot.eventId() + "|" + snapshot.workspaceId() + "|" + snapshot.status() + "|"
                + snapshot.registrationOpen() + "|" + snapshot.capacity() + "|" + snapshot.confirmedCount()
                + "|" + new TreeMap<>(snapshot.fieldClocks());
        return UUID.nameUUIDFromBytes(stable.getBytes(StandardCharsets.UTF_8));
    }

    private String conflictKey(UUID mergeId, ReconciliationMerger.Conflict conflict) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            String value = mergeId + "|" + conflict.field() + "|" + conflict.left() + "|"
                    + conflict.right() + "|" + conflict.winner() + "|" + conflict.resolution();
            return HexFormat.of().formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception exception) {
            throw new IllegalStateException("Cannot hash reconciliation conflict", exception);
        }
    }

    private String json(Map<String, String> value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Cannot serialize reconciliation clocks", exception);
        }
    }

    private String jsonValue(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Cannot serialize reconciliation conflict", exception);
        }
    }

    private Map<String, String> parse(String value) {
        try {
            return value == null ? Map.of() : objectMapper.readValue(value, new TypeReference<>() { });
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Invalid projection clocks", exception);
        }
    }

    private record Candidate(UUID eventId, String workspaceId) { }
    private record LocalProjection(CoordinationSnapshot snapshot, long localRevision) { }
    private record Outcome(boolean resolved, int conflicts, boolean quarantined) { }
    public record Report(int scanned, int resolved, int conflicts, int quarantined, int failed) { }
}
