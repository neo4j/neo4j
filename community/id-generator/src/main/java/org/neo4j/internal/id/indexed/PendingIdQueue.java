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

import static java.lang.Integer.min;

import java.util.Arrays;
import org.eclipse.collections.api.list.primitive.LongList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.neo4j.internal.id.IdSlotDistribution;

/**
 * Queue for keeping IDs which will be handed to {@link IdCache} after being marked as reserved.
 * IDs are queued in slots based on the number of consecutive IDs they provide.
 */
class PendingIdQueue {
    private final IdSlotDistribution.Slot[] slots;
    final MutableLongList[] queues;
    private final int[] slotSizes;
    private final int[] slotIndexBySize;

    PendingIdQueue(IdSlotDistribution.Slot... slots) {
        this.slots = slots;
        this.queues = new MutableLongList[slots.length];
        this.slotSizes = new int[slots.length];
        this.slotIndexBySize = new int[slots[slots.length - 1].slotSize()];
        for (int i = 0; i < slots.length; i++) {
            queues[i] = LongLists.mutable.empty();
            slotSizes[i] = slots[i].slotSize();
            Arrays.fill(slotIndexBySize, i == 0 ? 0 : slotSizes[i - 1] - 1, slotSizes[i] - 1, i - 1);
        }
        slotIndexBySize[slotIndexBySize.length - 1] = slots.length - 1;
    }

    private boolean cache(int slotIndex, long startId) {
        if (queues[slotIndex].size() >= slots[slotIndex].capacity()) {
            return false;
        }
        return queues[slotIndex].add(startId);
    }

    int offer(long id, int slotSize) {
        int slotIndex = largestSlotIndex(slotSize);
        int acceptedSlots = 0;
        while (slotSize > 0 && slotIndex >= 0) {
            int thisSlotSize = slotSizes[slotIndex];
            boolean added = cache(slotIndex, id);
            if (added) {
                id += thisSlotSize;
                acceptedSlots += thisSlotSize;
                slotSize -= thisSlotSize;
                while (slotIndex >= 0 && slotSize < slotSizes[slotIndex]) {
                    slotIndex--;
                }
            } else {
                slotIndex--;
            }
        }
        return acceptedSlots;
    }

    void accept(IdVisitor visitor) {
        for (int i = 0; i < slotSizes.length; i++) {
            visitor.ids(i, slotSizes[i], queues[i]);
        }
    }

    private int largestSlotIndex(int slotSize) {
        return slotIndexBySize[min(slotSize, slotIndexBySize.length) - 1];
    }

    boolean isEmpty() {
        for (MutableLongList queue : queues) {
            if (!queue.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    interface IdVisitor {
        void ids(int slotIndex, int slotSize, LongList ids);
    }
}
