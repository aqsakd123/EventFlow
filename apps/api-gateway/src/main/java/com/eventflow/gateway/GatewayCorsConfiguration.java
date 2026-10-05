package com.eventflow.gateway;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.reactive.CorsConfigurationSource;
import org.springframework.web.cors.reactive.CorsWebFilter;
import org.springframework.web.cors.reactive.UrlBasedCorsConfigurationSource;
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
    CorsConfigurationSource gatewayCorsConfigurationSource(
            @Value("${eventflow.cors.allowed-origins}") String allowedOrigins) {
        CorsConfiguration configuration = new CorsConfiguration();
        configuration.setAllowedOrigins(Arrays.stream(allowedOrigins.split(","))
                .map(String::trim)
                .filter(origin -> !origin.isEmpty())
                .toList());
        configuration.setAllowedMethods(METHODS);
        configuration.setAllowedHeaders(ALLOWED_HEADERS);
        configuration.setExposedHeaders(EXPOSED_HEADERS);
        configuration.setAllowCredentials(false);
        configuration.setMaxAge(3600L);

        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/**", configuration);
        return source;
    }

    @Bean
    WebFilter gatewayCorsWebFilter(CorsConfigurationSource gatewayCorsConfigurationSource) {
        return new HighestPrecedenceCorsFilter(new CorsWebFilter(gatewayCorsConfigurationSource));
    }

    private static final class HighestPrecedenceCorsFilter implements WebFilter, Ordered {
        private final CorsWebFilter delegate;

        private HighestPrecedenceCorsFilter(CorsWebFilter delegate) {
            this.delegate = delegate;
        }

        @Override
        public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
            return delegate.filter(exchange, chain);
        }

        @Override
        public int getOrder() {
            return Ordered.HIGHEST_PRECEDENCE;
        }
    }
}
