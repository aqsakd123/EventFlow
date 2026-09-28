package com.eventflow.eventservice.reconciliation;

import java.time.Clock;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Small hybrid logical clock used only to establish a deterministic per-field
 * last-writer order across reconciliation participants.
 */
@Component
public class HybridLogicalClock {
    private final Clock clock;
    private final String site;
    private final AtomicReference<Stamp> last = new AtomicReference<>(new Stamp(0, 0, ""));

    @Autowired
    public HybridLogicalClock(@Value("${eventflow.reconciliation.site-id:event}") String site) {
        this(Clock.systemUTC(), site);
    }

    HybridLogicalClock(Clock clock, String site) {
        if (site == null || !site.matches("[A-Za-z0-9_-]{1,32}")) {
            throw new IllegalArgumentException("Reconciliation site id must be 1-32 safe characters");
        }
        this.clock = clock;
        this.site = site;
    }

    public synchronized String next(Collection<String> observed) {
        Stamp maximum = last.get();
        for (String value : observed) {
            if (value != null && !value.isBlank()) maximum = max(maximum, parse(value));
        }
        long now = clock.millis();
        long physical = Math.max(now, maximum.physicalMillis());
        long logical = physical == maximum.physicalMillis() ? maximum.logical() + 1 : 0;
        Stamp next = new Stamp(physical, logical, site);
        last.set(next);
        return next.encode();
    }

    public static int compare(String left, String right) {
        if (left == null) return right == null ? 0 : -1;
        if (right == null) return 1;
        return parse(left).compareTo(parse(right));
    }

    private static Stamp max(Stamp left, Stamp right) {
        return left.compareTo(right) >= 0 ? left : right;
    }

    private static Stamp parse(String value) {
        String[] parts = value.split(":", 3);
        if (parts.length != 3) throw new IllegalArgumentException("Invalid HLC");
        return new Stamp(Long.parseLong(parts[0]), Long.parseLong(parts[1]), parts[2]);
    }

    private record Stamp(long physicalMillis, long logical, String site) implements Comparable<Stamp> {
        String encode() { return physicalMillis + ":" + logical + ":" + site; }

        @Override
        public int compareTo(Stamp other) {
            int physical = Long.compare(physicalMillis, other.physicalMillis);
            if (physical != 0) return physical;
            int counter = Long.compare(logical, other.logical);
            if (counter != 0) return counter;
            return site.compareTo(other.site);
        }
    }
}
