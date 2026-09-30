package com.eventflow.eventservice.config;

import com.eventflow.eventservice.service.ReadConsistency;
import com.eventflow.eventservice.service.ReadConsistencyPolicy;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.HandlerInterceptor;

public class ReadConsistencyInterceptor implements HandlerInterceptor {
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        ReadConsistency annotation = null;
        if (handler instanceof HandlerMethod method) {
            annotation = AnnotatedElementUtils.findMergedAnnotation(method.getMethod(), ReadConsistency.class);
            if (annotation == null) {
                annotation = AnnotatedElementUtils.findMergedAnnotation(method.getBeanType(), ReadConsistency.class);
            }
        }
        request.setAttribute(ReadConsistencyPolicy.REQUEST_ATTRIBUTE,
                annotation == null ? ReadConsistencyPolicy.off() : ReadConsistencyPolicy.from(annotation));
        return true;
    }
}
