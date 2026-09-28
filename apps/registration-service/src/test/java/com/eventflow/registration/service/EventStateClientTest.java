package com.eventflow.registration.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;

import com.eventflow.registration.api.ApiException;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withResourceNotFound;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

class EventStateClientTest {
    @Test
    void readsSourceStateFromEventService() {
        RestClient.Builder builder = RestClient.builder();
        MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
        UUID eventId = UUID.randomUUID();
        server.expect(requestTo("http://event-service/internal/events/" + eventId + "/registration-state"))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("""
                        {"eventId":"%s","workspaceId":"workspace-1","status":"PUBLISHED",
                         "registrationOpen":true,"version":4}
                        """.formatted(eventId), MediaType.APPLICATION_JSON));

        EventStateClient.SourceState state = new EventStateClient(builder, "http://event-service").read(eventId);

        assertEquals("workspace-1", state.workspaceId());
        assertEquals("PUBLISHED", state.status());
        assertEquals(4, state.version());
        server.verify();
    }

    @Test
    void mapsUnavailableSourceToServiceUnavailable() {
        RestClient.Builder builder = RestClient.builder();
        MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
        UUID eventId = UUID.randomUUID();
        server.expect(requestTo("http://event-service/internal/events/" + eventId + "/registration-state"))
                .andRespond(withResourceNotFound());

        ApiException error = assertThrows(ApiException.class,
                () -> new EventStateClient(builder, "http://event-service").read(eventId));

        assertEquals(404, error.status().value());
        assertEquals("EVENT_NOT_FOUND", error.code());
        server.verify();
    }
}
