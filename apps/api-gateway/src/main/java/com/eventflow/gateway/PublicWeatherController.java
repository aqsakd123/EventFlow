package com.eventflow.gateway;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/v1/network")
class PublicWeatherController {
    private final PublicWeatherService service;

    PublicWeatherController(PublicWeatherService service) {
        this.service = service;
    }

    @GetMapping("/weather")
    Mono<PublicWeatherService.PublicWeatherResponse> weather() {
        return service.lookup()
                .onErrorMap(exception -> new ResponseStatusException(
                        HttpStatus.BAD_GATEWAY, "Public location/weather lookup failed", exception));
    }
}
