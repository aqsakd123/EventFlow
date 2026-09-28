package com.eventflow.registration.api;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import jakarta.servlet.http.HttpServletRequest;

public record Actor(String userId, String workspaceId, Set<String> roles) {
    public static Actor from(HttpServletRequest request) {
        String userId = value(request, "X-User-Id", "anonymous");
        String workspaceId = value(request, "X-Workspace-Id", "");
        Set<String> roles = Arrays.stream(value(request, "X-Roles", "PARTICIPANT").split(","))
                .map(String::trim).filter(role -> !role.isBlank()).map(String::toUpperCase).collect(Collectors.toUnmodifiableSet());
        return new Actor(userId, workspaceId, roles);
    }

    public boolean hasAny(String... required) {
        return Arrays.stream(required).map(String::toUpperCase).anyMatch(roles::contains);
    }

    public void requireIdentity() {
        if (userId.isBlank() || "anonymous".equals(userId)) {
            throw ApiException.unauthorized("AUTHENTICATION_REQUIRED", "Authentication is required");
        }
        if (workspaceId.isBlank()) {
            throw ApiException.forbidden("WORKSPACE_REQUIRED", "A verified workspace is required");
        }
    }

    private static String value(HttpServletRequest request, String name, String fallback) {
        String value = request.getHeader(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
