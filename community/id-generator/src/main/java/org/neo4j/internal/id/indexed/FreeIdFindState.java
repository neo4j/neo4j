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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Captures whether there have been freed IDs since last exhausted seek (I'd guess) and also the size
 * of the largest possible ID slotSize that a seek can find (actual largest can be lower though).
 */
class FreeIdFindState {
    private static final int UNDECIDED_LARGEST_POSSIBLE_SLOT_SIZE = Integer.MAX_VALUE;

    private final AtomicReference<Snapshot> freeIdsNotifier =
            new AtomicReference<>(new Snapshot(0, UNDECIDED_LARGEST_POSSIBLE_SLOT_SIZE));
    private final AtomicInteger seenFreeIdsNotification = new AtomicInteger();

    boolean hasSeenMoreFrees() {
        return seenFreeIdsNotification.get() != freeIdsNotifier.get().freeIdsNotification;
    }

    void notifySeenFreedId(int largestPossibleSlotSize) {
        freeIdsNotifier.updateAndGet(current -> new Snapshot(
                current.freeIdsNotification + 1,
                largestPossibleSlotSize(current.largestPossibleSlotSize, largestPossibleSlotSize)));
    }

    private int largestPossibleSlotSize(int currentValue, int newValue) {
        if (newValue == 0) {
            return currentValue;
        }
        if (currentValue == UNDECIDED_LARGEST_POSSIBLE_SLOT_SIZE) {
            return newValue;
        }
        return Math.max(currentValue, newValue);
    }

    void resetLargestPossibleSlotSize() {
        freeIdsNotifier.updateAndGet(
                current -> new Snapshot(current.freeIdsNotification, UNDECIDED_LARGEST_POSSIBLE_SLOT_SIZE));
    }

    Snapshot snapshot() {
        return freeIdsNotifier.get();
    }

    void catchupFreeIdsNotification(int freeIdsNotificationBeforeScan) {
        seenFreeIdsNotification.updateAndGet(operand -> Math.max(operand, freeIdsNotificationBeforeScan));
    }

    record Snapshot(int freeIdsNotification, int largestPossibleSlotSize) {}
}
