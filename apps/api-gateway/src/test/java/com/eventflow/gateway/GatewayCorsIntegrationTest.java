package com.eventflow.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;
import static org.springframework.http.HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN;
import static org.springframework.http.HttpHeaders.ACCESS_CONTROL_REQUEST_HEADERS;
import static org.springframework.http.HttpHeaders.ACCESS_CONTROL_REQUEST_METHOD;
import static org.springframework.http.HttpHeaders.ORIGIN;

import com.fasterxml.jackson.databind.node.NullNode;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.reactive.CorsConfigurationSource;

import reactor.core.publisher.Mono;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "eventflow.security.enabled=false",
                "eventflow.cors.allowed-origins=http://d2d0kbqgkjj8pl.cloudfront.net,https://d2d0kbqgkjj8pl.cloudfront.net"
        })
class GatewayCorsIntegrationTest {
    private static final String ORIGIN_VALUE = "http://d2d0kbqgkjj8pl.cloudfront.net";

    private WebTestClient client;

    @LocalServerPort
    private int port;

    @BeforeEach
    void setUp() {
        client = WebTestClient.bindToServer().baseUrl("http://localhost:" + port).build();
        when(weatherService.lookup()).thenReturn(Mono.just(new PublicWeatherService.PublicWeatherResponse(
                "198.51.100.20", "Test City", "Test Region", "Test Country", 10.0, 20.0, "UTC",
                NullNode.getInstance(), NullNode.getInstance())));
    }

    @Autowired
    private CorsConfigurationSource gatewayCorsConfigurationSource;

    @MockBean
    private PublicWeatherService weatherService;

    @Test
    void acceptsWeatherPreflightWhenOidcIsDisabled() {
        MockServerWebExchange sourceExchange = MockServerWebExchange.from(MockServerHttpRequest
                .options("http://gateway.internal/api/v1/network/weather")
                .header(ORIGIN, ORIGIN_VALUE));
        CorsConfiguration resolved = gatewayCorsConfigurationSource.getCorsConfiguration(sourceExchange);
        assertNotNull(resolved);
        assertEquals(ORIGIN_VALUE, resolved.checkOrigin(ORIGIN_VALUE));

        client.options()
                .uri("/api/v1/network/weather")
                .header(ORIGIN, ORIGIN_VALUE)
                .header(ACCESS_CONTROL_REQUEST_METHOD, "GET")
                .header(ACCESS_CONTROL_REQUEST_HEADERS,
                        "X-User-Id,X-Workspace-Id,X-Roles,Idempotency-Key,Content-Type")
                .exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals(ACCESS_CONTROL_ALLOW_ORIGIN, ORIGIN_VALUE)
                .expectHeader().exists("Access-Control-Allow-Headers");
    }

    @Test
    void addsCorsHeadersToWeatherGet() {
        client.get()
                .uri("/api/v1/network/weather")
                .header(ORIGIN, ORIGIN_VALUE)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals(ACCESS_CONTROL_ALLOW_ORIGIN, ORIGIN_VALUE)
                .expectHeader().valueMatches("Access-Control-Expose-Headers", ".*X-Request-Id.*");
    }
}
