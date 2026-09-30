package com.eventflow.eventservice.service;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface ReadConsistency {
    ReadRoutingMode mode() default ReadRoutingMode.OFF;

    String scope() default "global";

    boolean requireEntityVersion() default false;
}
