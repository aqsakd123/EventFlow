package com.eventflow.gateway;

import java.net.URI;
import java.time.Duration;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponentsBuilder;

import reactor.core.publisher.Mono;

@Service
class PublicWeatherService {
    private static final String CURRENT_WEATHER_FIELDS =
            "temperature_2m,relative_humidity_2m,apparent_temperature,weather_code,wind_speed_10m";

    private final WebClient client;
    private final String geolocationUrl;
    private final String weatherUrl;
    private final Duration timeout;

    PublicWeatherService(WebClient.Builder builder,
                         @Value("${eventflow.public-weather.geolocation-url:https://ipwho.is/}") String geolocationUrl,
                         @Value("${eventflow.public-weather.weather-url:https://api.open-meteo.com/v1/forecast}") String weatherUrl,
                         @Value("${eventflow.public-weather.timeout:5s}") Duration timeout) {
        this.client = builder.build();
        this.geolocationUrl = geolocationUrl;
        this.weatherUrl = weatherUrl;
        this.timeout = timeout;
    }

    Mono<PublicWeatherResponse> lookup() {
        return client.get()
                .uri(geolocationUrl)
                .retrieve()
                .bodyToMono(PublicLocation.class)
                .flatMap(location -> {
                    URI weatherRequest = UriComponentsBuilder.fromUriString(weatherUrl)
                            .queryParam("latitude", location.latitude())
                            .queryParam("longitude", location.longitude())
                            .queryParam("current", CURRENT_WEATHER_FIELDS)
                            .queryParam("timezone", "auto")
                            .build()
                            .toUri();
                    return client.get()
                            .uri(weatherRequest)
                            .retrieve()
                            .bodyToMono(WeatherPayload.class)
                            .map(weather -> new PublicWeatherResponse(
                                    location.ip(),
                                    location.city(),
                                    location.region(),
                                    location.countryName(),
                                    location.latitude(),
                                    location.longitude(),
                                    weather.timezone(),
                                    weather.current(),
                                    weather.currentUnits()));
                })
                .timeout(timeout);
    }

    record PublicWeatherResponse(
            String ip,
            String city,
            String region,
            String country,
            double latitude,
            double longitude,
            String timezone,
            JsonNode current,
            JsonNode currentUnits) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record PublicLocation(
            String ip,
            String city,
            String region,
            @JsonAlias({"country", "country_name"}) String countryName,
            double latitude,
            double longitude) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record WeatherPayload(
            String timezone,
            JsonNode current,
            @com.fasterxml.jackson.annotation.JsonProperty("current_units") JsonNode currentUnits) {
    }
}
