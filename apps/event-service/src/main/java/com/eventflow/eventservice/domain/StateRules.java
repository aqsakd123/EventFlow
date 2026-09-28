package com.eventflow.eventservice.domain;

import java.time.Instant;

import com.eventflow.eventservice.api.ApiException;

public final class StateRules {
    private StateRules() { }

    public static void validateWindow(Instant startsAt, Instant endsAt) {
        if (!endsAt.isAfter(startsAt)) {
            throw ApiException.badRequest("INVALID_EVENT_WINDOW", "endsAt must be after startsAt");
        }
    }

    public static void requireEditable(String status) {
        if ("ENDED".equals(status) || "CANCELLED".equals(status)) {
            throw ApiException.conflict("EVENT_NOT_EDITABLE", "Ended or cancelled events cannot be edited");
        }
    }

    public static void requirePublishable(String status) {
        if (!"DRAFT".equals(status)) {
            throw ApiException.conflict("EVENT_NOT_PUBLISHABLE", "Only draft events can be published");
        }
    }

    public static void requireCapacity(int capacity, int confirmedCount) {
        if (capacity < confirmedCount) {
            throw ApiException.conflict("CAPACITY_BELOW_CONFIRMED", "Capacity cannot be below confirmed count");
        }
    }
}
