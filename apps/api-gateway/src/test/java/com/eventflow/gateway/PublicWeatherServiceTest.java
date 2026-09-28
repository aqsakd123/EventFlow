package com.eventflow.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.function.client.WebClient;

class PublicWeatherServiceTest {
    private HttpServer server;

    @BeforeEach
    void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/geo", exchange -> respond(exchange, 200, """
                {"ip":"198.51.100.20","city":"Test City","region":"Test Region",
                 "country_name":"Test Country","latitude":10.5,"longitude":106.7}
                """));
        server.createContext("/weather", exchange -> {
            URI requestUri = exchange.getRequestURI();
            assertTrue(requestUri.getQuery().contains("latitude=10.5"));
            assertTrue(requestUri.getQuery().contains("longitude=106.7"));
            respond(exchange, 200, """
                    {"timezone":"Asia/Ho_Chi_Minh","current":{"temperature_2m":30.2},
                     "current_units":{"temperature_2m":"°C"}}
                    """);
        });
        server.start();
    }

    @AfterEach
    void tearDown() {
        server.stop(0);
    }

    @Test
    void resolvesPublicLocationThenFetchesWeather() {
        String baseUrl = "http://localhost:" + server.getAddress().getPort();
        PublicWeatherService service = new PublicWeatherService(
                WebClient.builder(), baseUrl + "/geo", baseUrl + "/weather", java.time.Duration.ofSeconds(2));

        PublicWeatherService.PublicWeatherResponse result = service.lookup().block(java.time.Duration.ofSeconds(2));

        assertEquals("198.51.100.20", result.ip());
        assertEquals("Test City", result.city());
        assertEquals("Asia/Ho_Chi_Minh", result.timezone());
        assertEquals(30.2, result.current().get("temperature_2m").doubleValue());
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] response = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, response.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(response);
        }
    }
}
