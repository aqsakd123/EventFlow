package com.eventflow.eventservice.api;

import java.time.Instant;

public record ApiError(String code, String message, String requestId, Instant timestamp) {
}
