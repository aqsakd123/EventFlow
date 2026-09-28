package com.eventflow.registration.reconciliation;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.springframework.stereotype.Component;

@Component
public class ReconciliationMerger {
    public MergeResult merge(CoordinationSnapshot source, CoordinationSnapshot projection) {
        if (!source.eventId().equals(projection.eventId())) {
            return MergeResult.quarantined("EVENT_ID_MISMATCH");
        }
        if (!source.workspaceId().equals(projection.workspaceId())) {
            return MergeResult.quarantined("WORKSPACE_MISMATCH");
        }

        List<Conflict> conflicts = new ArrayList<>();
        Choice<String> status = chooseStatus(source, projection, conflicts);
        Choice<Integer> capacity = choose("capacity", source.capacity(), projection.capacity(),
                source.fieldClocks().get("capacity"), projection.fieldClocks().get("capacity"),
                Side.SOURCE, conflicts);
        Choice<Integer> confirmed = choose("confirmedCount", source.confirmedCount(), projection.confirmedCount(),
                source.fieldClocks().get("confirmedCount"), projection.fieldClocks().get("confirmedCount"),
                Side.PROJECTION, conflicts);

        int conflictCountBeforeOpen = conflicts.size();
        Choice<Boolean> requestedOpen = choose("registrationOpen", source.registrationOpen(),
                projection.registrationOpen(), source.fieldClocks().get("registrationOpen"),
                projection.fieldClocks().get("registrationOpen"), Side.SOURCE, conflicts);
        boolean registrationOpen = "PUBLISHED".equals(status.value()) && requestedOpen.value();
        if (requestedOpen.value() && !registrationOpen) {
            if (conflicts.size() > conflictCountBeforeOpen
                    && "registrationOpen".equals(conflicts.get(conflicts.size() - 1).field())) {
                conflicts.remove(conflicts.size() - 1);
            }
            conflicts.add(new Conflict("registrationOpen", source.registrationOpen(),
                    projection.registrationOpen(), false, "TERMINAL_STATE_CLOSED"));
        }
        if (confirmed.value() < 0 || confirmed.value() > capacity.value()) {
            return new MergeResult(null, conflicts, "CAPACITY_BELOW_CONFIRMED");
        }

        Map<String, String> clocks = new LinkedHashMap<>();
        putClock(clocks, "status", status.clock());
        putClock(clocks, "registrationOpen", requestedOpen.clock());
        putClock(clocks, "capacity", capacity.clock());
        putClock(clocks, "confirmedCount", confirmed.clock());
        CoordinationSnapshot merged = new CoordinationSnapshot(source.eventId(), source.workspaceId(),
                status.value(), registrationOpen, capacity.value(), confirmed.value(),
                source.version(), Map.copyOf(clocks));
        return new MergeResult(merged, List.copyOf(conflicts), null);
    }

    private Choice<String> chooseStatus(CoordinationSnapshot source, CoordinationSnapshot projection,
                                        List<Conflict> conflicts) {
        String left = source.status();
        String right = projection.status();
        String leftClock = source.fieldClocks().get("status");
        String rightClock = projection.fieldClocks().get("status");
        if (left.equals(right)) return new Choice<>(left, newest(leftClock, rightClock), Side.SOURCE);

        String winner;
        Side side;
        String resolution;
        if (terminal(left)) {
            winner = left;
            side = Side.SOURCE;
            resolution = terminal(right) ? "TERMINAL_SOURCE_WINS" : "IRREVERSIBLE_STATE";
        } else if (terminal(right)) {
            winner = right;
            side = Side.PROJECTION;
            resolution = "IRREVERSIBLE_STATE";
        } else if (rank(left) != rank(right)) {
            side = rank(left) > rank(right) ? Side.SOURCE : Side.PROJECTION;
            winner = side == Side.SOURCE ? left : right;
            resolution = "MONOTONIC_STATE";
        } else {
            Choice<String> lww = choose("status", left, right, leftClock, rightClock, Side.SOURCE, conflicts);
            return lww;
        }
        conflicts.add(new Conflict("status", left, right, winner, resolution));
        return new Choice<>(winner, side == Side.SOURCE ? leftClock : rightClock, side);
    }

    private <T> Choice<T> choose(String field, T left, T right, String leftClock, String rightClock,
                                 Side missingClockPreference, List<Conflict> conflicts) {
        if (Objects.equals(left, right)) {
            return HybridLogicalClock.compare(leftClock, rightClock) >= 0
                    ? new Choice<>(left, leftClock, Side.SOURCE)
                    : new Choice<>(right, rightClock, Side.PROJECTION);
        }
        int compared;
        if (leftClock == null && rightClock == null) {
            compared = missingClockPreference == Side.SOURCE ? 1 : -1;
        } else {
            compared = HybridLogicalClock.compare(leftClock, rightClock);
        }
        Side side = compared >= 0 ? Side.SOURCE : Side.PROJECTION;
        T winner = side == Side.SOURCE ? left : right;
        String winnerClock = side == Side.SOURCE ? leftClock : rightClock;
        conflicts.add(new Conflict(field, left, right, winner, "FIELD_LWW"));
        return new Choice<>(winner, winnerClock, side);
    }

    private String newest(String left, String right) {
        return HybridLogicalClock.compare(left, right) >= 0 ? left : right;
    }

    private void putClock(Map<String, String> clocks, String field, String value) {
        if (value != null) clocks.put(field, value);
    }

    private boolean terminal(String value) {
        return "ENDED".equals(value) || "CANCELLED".equals(value);
    }

    private int rank(String value) {
        return switch (value) {
            case "DRAFT" -> 0;
            case "PUBLISHED" -> 1;
            case "ENDED", "CANCELLED" -> 2;
            default -> throw new IllegalArgumentException("Unknown event state " + value);
        };
    }

    private enum Side { SOURCE, PROJECTION }
    private record Choice<T>(T value, String clock, Side side) { }

    public record Conflict(String field, Object left, Object right, Object winner, String resolution) { }
    public record MergeResult(CoordinationSnapshot snapshot, List<Conflict> conflicts, String quarantineReason) {
        static MergeResult quarantined(String reason) { return new MergeResult(null, List.of(), reason); }
        public boolean quarantined() { return quarantineReason != null; }
    }
}
