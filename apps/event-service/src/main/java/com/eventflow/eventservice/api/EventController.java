package com.eventflow.eventservice.api;

import java.util.List;
import java.util.UUID;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.validation.Valid;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.eventflow.eventservice.service.EventApplicationService;
import com.eventflow.eventservice.service.ConsistencyPinRegistry;
import com.eventflow.eventservice.service.EventReadRouter;

@RestController
@RequestMapping("/api/v1/events")
public class EventController {
    private final EventApplicationService service;
    private final ConsistencyPinRegistry consistencyPins;
    private final EventReadRouter readRouter;

    public EventController(EventApplicationService service, ConsistencyPinRegistry consistencyPins,
                           EventReadRouter readRouter) {
        this.service = service;
        this.consistencyPins = consistencyPins;
        this.readRouter = readRouter;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    EventDtos.EventResponse create(@Valid @RequestBody EventDtos.CreateEventRequest request,
                                   HttpServletRequest servletRequest, HttpServletResponse response) {
        EventDtos.EventResponse created = service.create(request, servletRequest);
        consistencyPins.recordCommittedWrite(servletRequest, created.version()).writeTo(response);
        return created;
    }

    @GetMapping
    List<EventDtos.EventResponse> list(HttpServletRequest servletRequest, HttpServletResponse response) {
        List<EventDtos.EventResponse> events = service.list(servletRequest);
        readRouter.writeResponseHeaders(servletRequest, response);
        return events;
    }

    @GetMapping("/{eventId}")
    EventDtos.EventResponse get(@PathVariable UUID eventId, HttpServletRequest servletRequest,
                                HttpServletResponse response) {
        EventDtos.EventResponse event = service.get(eventId, servletRequest);
        readRouter.writeResponseHeaders(servletRequest, response);
        return event;
    }

    @PatchMapping("/{eventId}")
    EventDtos.EventResponse update(@PathVariable UUID eventId, @Valid @RequestBody EventDtos.UpdateEventRequest request,
                                   HttpServletRequest servletRequest, HttpServletResponse response) {
        EventDtos.EventResponse updated = service.update(eventId, request, servletRequest);
        consistencyPins.recordCommittedWrite(servletRequest, updated.version()).writeTo(response);
        return updated;
    }

    @PostMapping("/{eventId}/publish")
    EventDtos.EventResponse publish(@PathVariable UUID eventId, HttpServletRequest servletRequest,
                                    HttpServletResponse response) {
        EventDtos.EventResponse published = service.publish(eventId, servletRequest);
        consistencyPins.recordCommittedWrite(servletRequest, published.version()).writeTo(response);
        return published;
    }

    @PostMapping("/{eventId}/cancel")
    EventDtos.EventResponse cancel(@PathVariable UUID eventId, HttpServletRequest servletRequest,
                                   HttpServletResponse response) {
        EventDtos.EventResponse cancelled = service.cancel(eventId, servletRequest);
        consistencyPins.recordCommittedWrite(servletRequest, cancelled.version()).writeTo(response);
        return cancelled;
    }

    @PostMapping("/{eventId}/media/upload-session")
    @ResponseStatus(HttpStatus.CREATED)
    EventDtos.UploadSessionResponse uploadSession(@PathVariable UUID eventId, @Valid @RequestBody EventDtos.UploadSessionRequest request,
                                                  HttpServletRequest servletRequest) {
        return service.createUploadSession(eventId, request, servletRequest);
    }

    @PostMapping("/{eventId}/media/{mediaId}/finalize")
    EventDtos.FinalizeMediaResponse finalizeMedia(@PathVariable UUID eventId, @PathVariable UUID mediaId,
                                                  HttpServletRequest servletRequest) {
        return service.finalizeMedia(eventId, mediaId, servletRequest);
    }
}
