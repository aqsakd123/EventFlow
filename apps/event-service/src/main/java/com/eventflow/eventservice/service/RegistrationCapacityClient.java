package com.eventflow.eventservice.service;

import java.util.UUID;

import com.eventflow.eventservice.api.ApiException;
import com.eventflow.eventservice.resilience.InternalHttpResilience;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;

@Component
public class RegistrationCapacityClient {
    private final RestClient client;
    private final CircuitBreaker circuitBreaker;

    public RegistrationCapacityClient(
            RestClient.Builder builder,
            InternalHttpResilience resilience,
            @Value("$" + "{eventflow.registration-service.url:http://localhost:8082}") String baseUrl) {
        this.client = builder.requestFactory(resilience.requestFactory()).baseUrl(baseUrl).build();
        this.circuitBreaker = resilience.circuitBreaker(
                "registration-service-capacity",
                exception -> !(exception instanceof BusinessConflictException));
    }

    public void validate(UUID eventId, int capacity) {
        try {
            circuitBreaker.executeSupplier(() -> {
                try {
                    client.get()
                            .uri(uriBuilder -> uriBuilder.path("/internal/events/{eventId}/capacity")
                                    .queryParam("capacity", capacity).build(eventId))
                            .retrieve()
                            .toBodilessEntity();
                    return null;
                } catch (RestClientResponseException exception) {
                    if (exception.getStatusCode().value() == 409) {
                        throw new BusinessConflictException(exception);
                    }
                    throw exception;
                }
            });
        } catch (BusinessConflictException exception) {
            throw ApiException.conflict("CAPACITY_BELOW_CONFIRMED", "Capacity cannot be below confirmed registrations");
        } catch (CallNotPermittedException | RestClientException exception) {
            throw ApiException.serviceUnavailable("CAPACITY_CHECK_UNAVAILABLE", "Registration capacity check failed");
        }
    }

    private static final class BusinessConflictException extends RuntimeException {
        private BusinessConflictException(RestClientResponseException cause) {
            super(cause);
        }
    }
}
