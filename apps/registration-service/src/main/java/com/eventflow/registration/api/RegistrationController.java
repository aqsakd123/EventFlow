package com.eventflow.registration.api;

import java.util.List;
import java.util.UUID;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;

import com.eventflow.registration.service.RegistrationApplicationService;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/events/{eventId}")
public class RegistrationController {
    private final RegistrationApplicationService service;

    public RegistrationController(RegistrationApplicationService service) { this.service = service; }

    @PostMapping("/registrations")
    @ResponseStatus(HttpStatus.CREATED)
    RegistrationDtos.RegistrationResponse register(@PathVariable UUID eventId,
                                                    @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
                                                    HttpServletRequest request) {
        return service.register(eventId, idempotencyKey, request);
    }

    @DeleteMapping("/registrations/me")
    RegistrationDtos.RegistrationResponse cancel(@PathVariable UUID eventId, HttpServletRequest request) {
        return service.cancel(eventId, request);
    }

    @GetMapping("/registrations/me")
    RegistrationDtos.RegistrationResponse getMine(@PathVariable UUID eventId, HttpServletRequest request) {
        return service.getMine(eventId, request);
    }

    @PostMapping("/check-ins")
    RegistrationDtos.CheckInResponse checkIn(@PathVariable UUID eventId, @Valid @RequestBody RegistrationDtos.CheckInRequest checkIn,
                                             HttpServletRequest request) {
        return service.checkIn(eventId, checkIn, request);
    }

    @GetMapping("/attendance")
    List<RegistrationDtos.AttendanceRow> attendance(@PathVariable UUID eventId, HttpServletRequest request) {
        return service.attendance(eventId, request);
    }
}
