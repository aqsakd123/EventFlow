package com.eventflow.registration.api;

import java.util.UUID;

import jakarta.validation.constraints.Min;

import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/internal/events")
public class InternalCapacityController {
    private final JdbcTemplate jdbc;

    public InternalCapacityController(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    @GetMapping("/{eventId}/capacity")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    void validateCapacity(@PathVariable UUID eventId, @RequestParam @Min(1) int capacity) {
        Integer confirmed = jdbc.query("SELECT confirmed_count FROM event_projections WHERE event_id = ? FOR SHARE",
                (rs, rowNum) -> rs.getInt("confirmed_count"), eventId).stream().findFirst().orElse(0);
        if (capacity < confirmed) {
            throw ApiException.conflict("CAPACITY_BELOW_CONFIRMED", "Capacity cannot be below confirmed registrations");
        }
    }
}
