package com.eventflow.eventservice.resilience;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.function.Predicate;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;

@Component
public class InternalHttpResilience {
    private final int connectTimeoutMs;
    private final int readTimeoutMs;
    private final int slidingWindowSize;
    private final int minimumNumberOfCalls;
    private final float failureRateThreshold;
    private final long openStateWaitMs;
    private final int halfOpenPermittedCalls;

    public InternalHttpResilience(
            @Value("$" + "{eventflow.internal-http.connect-timeout-ms:1000}") int connectTimeoutMs,
            @Value("$" + "{eventflow.internal-http.read-timeout-ms:3000}") int readTimeoutMs,
            @Value("$" + "{eventflow.circuit-breaker.sliding-window-size:20}") int slidingWindowSize,
            @Value("$" + "{eventflow.circuit-breaker.minimum-number-of-calls:5}") int minimumNumberOfCalls,
            @Value("$" + "{eventflow.circuit-breaker.failure-rate-threshold:50}") float failureRateThreshold,
            @Value("$" + "{eventflow.circuit-breaker.open-state-wait-ms:10000}") long openStateWaitMs,
            @Value("$" + "{eventflow.circuit-breaker.half-open-permitted-calls:2}") int halfOpenPermittedCalls) {
        this.connectTimeoutMs = Math.max(1, connectTimeoutMs);
        this.readTimeoutMs = Math.max(1, readTimeoutMs);
        this.slidingWindowSize = Math.max(2, slidingWindowSize);
        this.minimumNumberOfCalls = Math.max(1, Math.min(this.slidingWindowSize, minimumNumberOfCalls));
        this.failureRateThreshold = Math.max(1, Math.min(100, failureRateThreshold));
        this.openStateWaitMs = Math.max(1, openStateWaitMs);
        this.halfOpenPermittedCalls = Math.max(1, halfOpenPermittedCalls);
    }

    public ClientHttpRequestFactory requestFactory() {
        HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(connectTimeoutMs))
                .build();
        JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(httpClient);
        factory.setReadTimeout(Duration.ofMillis(readTimeoutMs));
        return factory;
    }

    public CircuitBreaker circuitBreaker(String name, Predicate<Throwable> recordException) {
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .slidingWindowSize(slidingWindowSize)
                .minimumNumberOfCalls(minimumNumberOfCalls)
                .failureRateThreshold(failureRateThreshold)
                .waitDurationInOpenState(Duration.ofMillis(openStateWaitMs))
                .permittedNumberOfCallsInHalfOpenState(halfOpenPermittedCalls)
                .recordException(recordException)
                .build();
        return CircuitBreaker.of(name, config);
    }
}
