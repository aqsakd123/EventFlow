package com.eventflow.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static org.springframework.http.HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN;
import static org.springframework.http.HttpHeaders.ACCESS_CONTROL_REQUEST_HEADERS;
import static org.springframework.http.HttpHeaders.ACCESS_CONTROL_REQUEST_METHOD;
import static org.springframework.http.HttpHeaders.ORIGIN;

import com.fasterxml.jackson.databind.node.NullNode;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.server.WebFilter;

import reactor.core.publisher.Mono;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "eventflow.security.enabled=false",
                "eventflow.cors.allowed-origins=http://d2d0kbqgkjj8pl.cloudfront.net,https://d2d0kbqgkj8pl.cloudfront.net"
        })
class GatewayCorsIntegrationTest {
    private static final String ORIGIN_VALUE = "http://d2d0kbqgkjj8pl.cloudfront.net";

    private WebTestClient client;

    @LocalServerPort
    private int port;

    @Autowired
    @Qualifier("gatewayCorsWebFilter")
    private WebFilter gatewayCorsWebFilter;

    @MockBean
    private PublicWeatherService weatherService;

    @BeforeEach
    void setUp() {
        client = WebTestClient.bindToServer().baseUrl("http://localhost:" + port).build();
        when(weatherService.lookup()).thenReturn(Mono.just(new PublicWeatherService.PublicWeatherResponse(
                "198.51.100.20", "Test City", "Test Region", "Test Country", 10.0, 20.0, "UTC",
                NullNode.getInstance(), NullNode.getInstance())));
    }

    @Test
    void handlesAllowedOriginWithoutInspectingRequestUri() {
        MockServerWebExchange exchange = MockServerWebExchange.from(MockServerHttpRequest
                .get("/api/v1/network/weather")
                .header(ORIGIN, ORIGIN_VALUE));

        gatewayCorsWebFilter.filter(exchange, ignored -> Mono.empty()).block();

        assertEquals(ORIGIN_VALUE, exchange.getResponse().getHeaders().getFirst(ACCESS_CONTROL_ALLOW_ORIGIN));
    }

    @Test
    void acceptsWeatherPreflightWhenOidcIsDisabled() {
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
