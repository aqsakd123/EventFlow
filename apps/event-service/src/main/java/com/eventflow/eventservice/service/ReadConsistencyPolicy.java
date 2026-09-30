package com.eventflow.eventservice.service;

import jakarta.servlet.http.HttpServletRequest;

public record ReadConsistencyPolicy(ReadRoutingMode mode, String scope, boolean requireEntityVersion) {
    public static final String REQUEST_ATTRIBUTE = ReadConsistencyPolicy.class.getName();

    public static ReadConsistencyPolicy off() {
        return new ReadConsistencyPolicy(ReadRoutingMode.OFF, "global", false);
    }

    public static ReadConsistencyPolicy from(HttpServletRequest request) {
        Object value = request.getAttribute(REQUEST_ATTRIBUTE);
        return value instanceof ReadConsistencyPolicy policy ? policy : off();
    }

    public static ReadConsistencyPolicy from(ReadConsistency annotation) {
        return new ReadConsistencyPolicy(annotation.mode(), annotation.scope(), annotation.requireEntityVersion());
    }
}
