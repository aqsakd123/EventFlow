package com.eventflow.eventservice.service;

import java.util.UUID;

import com.eventflow.eventservice.api.ApiException;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;

@Component
public class RegistrationCapacityClient {
    private final RestClient client;

    public RegistrationCapacityClient(RestClient.Builder builder,
                                      @Value("${eventflow.registration-service.url:http://localhost:8082}") String baseUrl) {
        this.client = builder.baseUrl(baseUrl).build();
    }

    public void validate(UUID eventId, int capacity) {
        try {
            client.get()
                    .uri(uriBuilder -> uriBuilder.path("/internal/events/{eventId}/capacity")
                            .queryParam("capacity", capacity).build(eventId))
                    .retrieve()
                    .toBodilessEntity();
        } catch (RestClientResponseException exception) {
            HttpStatusCode status = exception.getStatusCode();
            if (status.value() == 409) {
                throw ApiException.conflict("CAPACITY_BELOW_CONFIRMED", "Capacity cannot be below confirmed registrations");
            }
            throw ApiException.serviceUnavailable("CAPACITY_CHECK_UNAVAILABLE", "Registration capacity check failed");
        } catch (RestClientException exception) {
            throw ApiException.serviceUnavailable("CAPACITY_CHECK_UNAVAILABLE", "Registration capacity check failed");
        }
    }
}
