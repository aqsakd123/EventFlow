package com.eventflow.gateway;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.springframework.http.HttpMethod;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.oauth2.server.resource.authentication.ReactiveJwtAuthenticationConverterAdapter;
import org.springframework.security.web.server.SecurityWebFilterChain;

final class SecurityConfig {

    @Configuration
    @EnableWebFluxSecurity
    @ConditionalOnProperty(name = "eventflow.security.enabled", havingValue = "false", matchIfMissing = true)
    static class LocalSecurity {
        @Bean
        SecurityWebFilterChain localChain(ServerHttpSecurity http) {
            return http.csrf(ServerHttpSecurity.CsrfSpec::disable)
                    .authorizeExchange(exchange -> exchange.anyExchange().permitAll())
                    .build();
        }
    }

    @Configuration
    @EnableWebFluxSecurity
    @ConditionalOnProperty(name = "eventflow.security.enabled", havingValue = "true")
    static class OidcSecurity {
        @Bean
        SecurityWebFilterChain oidcChain(ServerHttpSecurity http) {
            return http.csrf(ServerHttpSecurity.CsrfSpec::disable)
                    .authorizeExchange(exchange -> exchange
                            .pathMatchers(HttpMethod.OPTIONS, "/**").permitAll()
                            .pathMatchers("/actuator/health/**").permitAll()
                            .anyExchange().authenticated())
                    .oauth2ResourceServer(oauth2 -> oauth2.jwt(jwt -> jwt
                            .jwtAuthenticationConverter(new ReactiveJwtAuthenticationConverterAdapter(jwtAuthenticationConverter()))))
                    .build();
        }

        private JwtAuthenticationConverter jwtAuthenticationConverter() {
            JwtAuthenticationConverter converter = new JwtAuthenticationConverter();
            converter.setJwtGrantedAuthoritiesConverter(jwt -> rolesFrom(jwt).stream()
                    .map(role -> (GrantedAuthority) new SimpleGrantedAuthority("ROLE_" + role))
                    .toList());
            return converter;
        }

        private Collection<String> rolesFrom(Jwt jwt) {
            Object realmAccess = jwt.getClaims().get("realm_access");
            if (realmAccess instanceof Map<?, ?> realmAccessMap
                    && realmAccessMap.get("roles") instanceof Collection<?> roles) {
                return roles.stream().map(String::valueOf).map(String::toUpperCase).toList();
            }
            List<String> flatRoles = jwt.getClaimAsStringList("roles");
            return flatRoles == null ? List.of() : flatRoles;
        }
    }
}
