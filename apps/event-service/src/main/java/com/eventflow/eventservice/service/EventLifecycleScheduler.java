package com.eventflow.eventservice.service;

import java.util.List;
import java.util.UUID;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class EventLifecycleScheduler {
    private final JdbcTemplate jdbc;
    private final EventApplicationService eventService;

    public EventLifecycleScheduler(JdbcTemplate jdbc, EventApplicationService eventService) {
        this.jdbc = jdbc;
        this.eventService = eventService;
    }

    @Scheduled(fixedDelayString = "${eventflow.lifecycle.poll-ms:1000}")
    @Transactional
    public void markEndedEvents() {
        List<UUID> ids = jdbc.query("SELECT id FROM events WHERE status = 'PUBLISHED' AND ends_at <= now() FOR UPDATE",
                (rs, rowNum) -> UUID.fromString(rs.getString("id")));
        for (UUID id : ids) eventService.markEnded(id);
    }
}
