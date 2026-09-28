package com.eventflow.eventservice.api;

import java.util.UUID;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Internal source-of-truth read used to close the stale projection window before
 * a registration mutation. It is reachable only on the private service network.
 */
@RestController
@RequestMapping("/internal/events")
public class InternalEventStateController {
    private final JdbcTemplate jdbc;

    public InternalEventStateController(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @GetMapping("/{eventId}/registration-state")
    RegistrationState registrationState(@PathVariable UUID eventId) {
        return jdbc.query("""
                SELECT id, workspace_id, status, registration_open, version
                FROM events WHERE id = ?
                """, (rs, rowNum) -> new RegistrationState(
                        UUID.fromString(rs.getString("id")),
                        rs.getString("workspace_id"),
                        rs.getString("status"),
                        rs.getBoolean("registration_open"),
                        rs.getLong("version")), eventId)
                .stream().findFirst()
                .orElseThrow(() -> ApiException.notFound("EVENT_NOT_FOUND", "Event was not found"));
    }

    record RegistrationState(UUID eventId, String workspaceId, String status,
                             boolean registrationOpen, long version) { }
}
