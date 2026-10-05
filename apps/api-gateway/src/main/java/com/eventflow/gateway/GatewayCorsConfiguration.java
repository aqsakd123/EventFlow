package com.eventflow.gateway;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;

import reactor.core.publisher.Mono;

@Configuration
class GatewayCorsConfiguration {
    private static final List<String> METHODS = List.of("GET", "POST", "PATCH", "DELETE", "OPTIONS");
    private static final List<String> ALLOWED_HEADERS = List.of(
            "Authorization", "Content-Type", "Idempotency-Key", "X-User-Id", "X-Workspace-Id", "X-Roles",
            "X-Request-Id", "X-Correlation-Id");
    private static final List<String> EXPOSED_HEADERS = List.of(
            "X-Request-Id", "X-Correlation-Id", "X-Trace-Id", "X-Processed-At", "X-EventFlow-Db-Route",
            "X-EventFlow-Replay-Lsn", "X-EventFlow-Commit-Lsn", "X-EventFlow-Entity-Version",
            "X-EventFlow-Write-Pin-Until");

    @Bean
    WebFilter gatewayCorsWebFilter(@Value("${eventflow.cors.allowed-origins}") String allowedOrigins) {
        Set<String> origins = Set.copyOf(Arrays.stream(allowedOrigins.split(","))
                .map(String::trim)
                .filter(origin -> !origin.isEmpty())
                .toList());
        return new GatewayCorsWebFilter(origins);
    }

    private static final class GatewayCorsWebFilter implements WebFilter, Ordered {
        private final Set<String> allowedOrigins;

        private GatewayCorsWebFilter(Set<String> allowedOrigins) {
            this.allowedOrigins = allowedOrigins;
        }

        @Override
        public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
            HttpHeaders requestHeaders = exchange.getRequest().getHeaders();
            String origin = requestHeaders.getOrigin();
            if (origin == null || !allowedOrigins.contains(origin)) {
                return isPreflight(exchange) ? reject(exchange) : chain.filter(exchange);
            }

            HttpHeaders responseHeaders = exchange.getResponse().getHeaders();
            responseHeaders.setAccessControlAllowOrigin(origin);
            responseHeaders.setAccessControlExposeHeaders(EXPOSED_HEADERS);
            addVaryHeaders(responseHeaders);

            if (!isPreflight(exchange)) {
                return chain.filter(exchange);
            }

            String requestedMethod = requestHeaders.getFirst(HttpHeaders.ACCESS_CONTROL_REQUEST_METHOD);
            List<String> requestedHeaders = requestHeaders.getAccessControlRequestHeaders();
            if (requestedMethod == null || !METHODS.contains(requestedMethod.toUpperCase(Locale.ROOT))
                    || !requestedHeadersAllowed(requestedHeaders)) {
                return reject(exchange);
            }

            responseHeaders.set(HttpHeaders.ACCESS_CONTROL_ALLOW_METHODS, String.join(", ", METHODS));
            responseHeaders.set(HttpHeaders.ACCESS_CONTROL_ALLOW_HEADERS, String.join(", ", ALLOWED_HEADERS));
            responseHeaders.setAccessControlMaxAge(3600L);
            return exchange.getResponse().setComplete();
        }

        @Override
        public int getOrder() {
            return Ordered.HIGHEST_PRECEDENCE;
        }

        private boolean isPreflight(ServerWebExchange exchange) {
            HttpHeaders headers = exchange.getRequest().getHeaders();
            return HttpMethod.OPTIONS.equals(exchange.getRequest().getMethod())
                    && headers.containsKey(HttpHeaders.ORIGIN)
                    && headers.containsKey(HttpHeaders.ACCESS_CONTROL_REQUEST_METHOD);
        }

        private boolean requestedHeadersAllowed(List<String> requestedHeaders) {
            return requestedHeaders == null || requestedHeaders.stream().allMatch(requested -> ALLOWED_HEADERS.stream()
                    .anyMatch(allowed -> allowed.equalsIgnoreCase(requested)));
        }

        private Mono<Void> reject(ServerWebExchange exchange) {
            exchange.getResponse().setStatusCode(HttpStatus.FORBIDDEN);
            return exchange.getResponse().setComplete();
        }

        private void addVaryHeaders(HttpHeaders responseHeaders) {
            responseHeaders.add(HttpHeaders.VARY, HttpHeaders.ORIGIN);
            responseHeaders.add(HttpHeaders.VARY, HttpHeaders.ACCESS_CONTROL_REQUEST_METHOD);
            responseHeaders.add(HttpHeaders.VARY, HttpHeaders.ACCESS_CONTROL_REQUEST_HEADERS);
        }
    }
}
