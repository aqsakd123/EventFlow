package com.eventflow.eventservice.api;

import java.time.Instant;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.ConstraintViolationException;

import org.springframework.http.ResponseEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.http.converter.HttpMessageNotReadableException;

@RestControllerAdvice
public class GlobalExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(ApiException.class)
    ResponseEntity<ApiError> handleApi(ApiException exception, HttpServletRequest request) {
        return ResponseEntity.status(exception.status()).body(error(exception.code(), exception.getMessage(), request));
    }

    @ExceptionHandler({MethodArgumentNotValidException.class, ConstraintViolationException.class, HttpMessageNotReadableException.class})
    ResponseEntity<ApiError> handleValidation(Exception exception, HttpServletRequest request) {
        return ResponseEntity.badRequest().body(error("VALIDATION_ERROR", exception.getMessage(), request));
    }

    @ExceptionHandler(Exception.class)
    ResponseEntity<ApiError> handleUnexpected(Exception exception, HttpServletRequest request) {
        log.error("Unhandled request failure requestId={} path={}", request.getHeader("X-Request-Id"),
                request.getRequestURI(), exception);
        return ResponseEntity.internalServerError().body(error("INTERNAL_ERROR", "Unexpected server error", request));
    }

    private ApiError error(String code, String message, HttpServletRequest request) {
        return new ApiError(code, message, request.getHeader("X-Request-Id"), Instant.now());
    }
}
