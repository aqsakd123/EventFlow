package com.eventflow.registration.api;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.eventflow.registration.reconciliation.EventProjectionReconciliationWorker;
import com.eventflow.registration.reconciliation.InternalReconciliationAuth;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/internal/reconciliation")
@ConditionalOnProperty(name = "eventflow.reconciliation.enabled", havingValue = "true")
public class InternalReconciliationController {
    private final EventProjectionReconciliationWorker worker;
    private final JdbcTemplate jdbc;
    private final InternalReconciliationAuth auth;

    public InternalReconciliationController(EventProjectionReconciliationWorker worker, JdbcTemplate jdbc,
                                            InternalReconciliationAuth auth) {
        this.worker = worker;
        this.jdbc = jdbc;
        this.auth = auth;
    }

    @PostMapping("/run")
    EventProjectionReconciliationWorker.Report run(
            @RequestHeader(value = InternalReconciliationAuth.KEY_HEADER, required = false) String key,
            @RequestHeader(value = InternalReconciliationAuth.WORKSPACE_HEADER, required = false) String workspaceId) {
        auth.require(key);
        return worker.runOnce(auth.requireWorkspace(workspaceId));
    }

    @GetMapping("/conflicts")
    List<Map<String, Object>> conflicts(
            @RequestHeader(value = InternalReconciliationAuth.KEY_HEADER, required = false) String key,
            @RequestHeader(value = InternalReconciliationAuth.WORKSPACE_HEADER, required = false) String workspaceId) {
        auth.require(key);
        String scope = auth.requireWorkspace(workspaceId);
        return jdbc.queryForList("""
                SELECT c.conflict_key, c.merge_id, c.event_id, c.field_name, c.left_value, c.right_value,
                       c.winner_value, c.resolution, c.created_at
                FROM reconciliation_conflicts c
                JOIN event_projections e ON e.event_id = c.event_id
                WHERE e.workspace_id = ?
                ORDER BY c.created_at DESC LIMIT 100
                """, scope);
    }
}
