package com.eventflow.gateway;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.http.HttpHeaders;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpRequestDecorator;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.web.server.ServerWebExchange;

import reactor.core.publisher.Mono;

@Configuration
public class GatewayFilters {

    @Bean
    GlobalFilter correlationHeadersFilter() {
        return (exchange, chain) -> {
            String requestId = valueOrNew(exchange.getRequest().getHeaders().getFirst("X-Request-Id"));
            String correlationId = valueOrNew(exchange.getRequest().getHeaders().getFirst("X-Correlation-Id"));
            ServerHttpRequest request = withHeaders(exchange.getRequest(), headers -> {
                headers.set("X-Request-Id", requestId);
                headers.set("X-Correlation-Id", correlationId);
                headers.set("X-Trace-Id", requestId);
            });
            return chain.filter(exchange.mutate().request(request).build())
                    .then(Mono.fromRunnable(() -> {
                        exchange.getResponse().getHeaders().set("X-Request-Id", requestId);
                        exchange.getResponse().getHeaders().set("X-Correlation-Id", correlationId);
                        exchange.getResponse().getHeaders().set("X-Processed-At", Instant.now().toString());
                    }));
        };
    }

    @Bean
    @org.springframework.core.annotation.Order(Ordered.HIGHEST_PRECEDENCE + 10)
    GlobalFilter verifiedIdentityHeadersFilter() {
        return (exchange, chain) -> exchange.getPrincipal()
                .cast(Authentication.class)
                .filter(authentication -> authentication instanceof JwtAuthenticationToken)
                .cast(JwtAuthenticationToken.class)
                .flatMap(authentication -> {
                    var jwt = authentication.getToken();
                    String userId = jwt.getClaimAsString("sub");
                    String workspaceId = firstNonBlank(jwt.getClaimAsString("workspace_id"),
                            jwt.getClaimAsString("workspaceId"));
                    String roles = String.join(",", rolesFrom(jwt));
                    ServerHttpRequest request = withHeaders(exchange.getRequest(), headers -> {
                        headers.remove("X-User-Id");
                        headers.remove("X-Workspace-Id");
                        headers.remove("X-Roles");
                        if (userId != null) headers.set("X-User-Id", userId);
                        if (workspaceId != null) headers.set("X-Workspace-Id", workspaceId);
                        if (!roles.isBlank()) headers.set("X-Roles", roles);
                    });
                    return chain.filter(exchange.mutate().request(request).build());
                })
                .switchIfEmpty(chain.filter(exchange));
    }

    private static String valueOrNew(String value) {
        return value == null || value.isBlank() ? UUID.randomUUID().toString() : value;
    }

    private static String firstNonBlank(String first, String second) {
        return first != null && !first.isBlank() ? first : second;
    }

    private static List<String> rolesFrom(org.springframework.security.oauth2.jwt.Jwt jwt) {
        List<String> flatRoles = jwt.getClaimAsStringList("roles");
        if (flatRoles != null) return flatRoles;
        Object realmAccess = jwt.getClaims().get("realm_access");
        if (realmAccess instanceof Map<?, ?> realmAccessMap
                && realmAccessMap.get("roles") instanceof Collection<?> nestedRoles) {
            return nestedRoles.stream().map(String::valueOf).toList();
        }
        return List.of();
    }

    private static ServerHttpRequest withHeaders(ServerHttpRequest original,
                                                  java.util.function.Consumer<HttpHeaders> mutator) {
        HttpHeaders headers = new HttpHeaders();
        headers.putAll(original.getHeaders());
        mutator.accept(headers);
        return new ServerHttpRequestDecorator(original) {
            @Override
            public HttpHeaders getHeaders() {
                return headers;
            }
        };
    }
}
