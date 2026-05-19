/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.id.indexed;

import static java.lang.Math.max;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tracks two pieces of state used by {@link FreeIdScanner} to decide whether and how to scan for free IDs.
 * <p>
 * <b>Notification counter</b>: incremented each time one or more IDs are freed. The scanner records the
 * counter value at the start of each scan round and, if a round ends with nothing found, checks whether
 * the counter has advanced since then via {@link #hasPendingFrees()}. If it has, new IDs may have been
 * freed during the scan, and another round is warranted; if not, scanning is suspended until new IDs are
 * freed.
 * <p>
 * <b>Largest-observed slot size</b>: an upper bound on the slot size of free IDs currently in the tree.
 * Updated whenever IDs are freed with a known slot size, and reset to zero at the start of each scan
 * round. Allows allocation requests that are larger than any known free slot to skip the scan entirely.
 */
class FreeIdFindState {
    private final AtomicReference<Snapshot> state = new AtomicReference<>(new Snapshot(0, 0));

    /**
     * The notification count acknowledged by the scanner — the counter value at the start of the most
     * recently completed (or abandoned) scan round. Updated via {@link #setAcknowledgedNotificationCount}.
     */
    private final AtomicInteger acknowledgedNotificationCount = new AtomicInteger();

    /**
     * Returns {@code true} if IDs have been freed since the scanner last acknowledged them,
     * meaning a new scan round may find free IDs.
     */
    boolean hasPendingFrees() {
        return acknowledgedNotificationCount.get() != state.get().notificationCount();
    }

    /**
     * Records that one or more IDs of the given slot size have been freed.
     * Increments the notification counter and updates the largest-observed-slot-size hint.
     */
    void recordFreedIds(int largestObservedSlotSize) {
        assert largestObservedSlotSize > 0;
        state.updateAndGet(s -> s.next(largestObservedSlotSize));
    }

    /**
     * Marks the start of a new scan round: resets the largest-observed-slot-size hint to 0 so the scan
     * considers all slot sizes.
     */
    void beginNewScanRound() {
        state.updateAndGet(Snapshot::beginNewScanRound);
    }

    /**
     * Increments the notification counter so that any frees arriving
     * during this round will be detected by {@link #hasPendingFrees()} afterward.
     */
    void bumpNotificationCount() {
        state.updateAndGet(Snapshot::bumpNotificationCount);
    }

    /**
     * Returns a consistent snapshot of the current notification count and slot-size hint.
     */
    Snapshot snapshot() {
        return state.get();
    }

    /**
     * Records that a scan round which started at {@code notificationCount} has completed without finding
     * any free IDs. {@link #hasPendingFrees()} will return {@code false} until the next
     * {@link #recordFreedIds} call increments the counter past this value.
     */
    void setAcknowledgedNotificationCount(int notificationCount) {
        acknowledgedNotificationCount.set(notificationCount);
    }

    record Snapshot(int notificationCount, int largestObservedSlotSize) {
        /**
         * Update the snapshot with a new observation.
         * @param observedSlotSize the newly observed slot size.
         */
        Snapshot next(int observedSlotSize) {
            return new Snapshot(notificationCount + 1, max(largestObservedSlotSize, observedSlotSize));
        }

        /**
         * Begins a new scan round by resetting {@code largestObservedSlotSize} to zero.
         */
        Snapshot beginNewScanRound() {
            return new Snapshot(notificationCount, 0);
        }

        /**
         * Increments the notification counter so that frees arriving during the round are detectable afterward.
         */
        Snapshot bumpNotificationCount() {
            return new Snapshot(notificationCount + 1, largestObservedSlotSize);
        }

        /**
         * Returns {@code true} if any freed IDs have been observed since the last scan round began,
         * meaning {@link #largestObservedSlotSize()} is a meaningful upper bound. When {@code false},
         * the slot-size constraint should not be applied.
         */
        boolean hasObservedFrees() {
            return largestObservedSlotSize != 0;
        }
    }
}
