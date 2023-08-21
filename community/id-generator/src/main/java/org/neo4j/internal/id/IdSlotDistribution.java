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
package org.neo4j.internal.id;

import static org.neo4j.internal.helpers.Numbers.ceilingPowerOfTwo;
import static org.neo4j.internal.id.indexed.IndexedIdGenerator.IDS_PER_ENTRY;
import static org.neo4j.util.Preconditions.requirePowerOfTwo;

import org.neo4j.io.pagecache.context.CursorContext;

/**
 * Defines which slot sizes for IDs to use, e.g. a slot size of 4 contains IDs where the ID + the 3 next IDs are available.
 */
public interface IdSlotDistribution {
    IdSlotDistribution SINGLE_IDS = new IdSlotDistribution() {
        @Override
        public Slot[] slots(int capacity) {
            return new Slot[] {new Slot(capacity, 1)};
        }

        @Override
        public int maxSlotSize() {
            return 1;
        }
    };

    static IdSlotDistribution evenSlotDistribution(int... slotSizes) {
        return evenSlotDistribution(IDS_PER_ENTRY, slotSizes);
    }

    static IdSlotDistribution evenSlotDistribution(int idsPerEntry, int... slotSizes) {
        return new BaseIdSlotDistribution(idsPerEntry, slotSizes) {
            @Override
            public Slot[] slots(int capacity) {
                Slot[] slots = new Slot[slotSizes.length];
                int capacityPerSlot = ceilingPowerOfTwo(capacity / slotSizes.length);
                for (int i = 0; i < slotSizes.length; i++) {
                    slots[i] = new Slot(capacityPerSlot, slotSizes[i]);
                }
                return slots;
            }
        };
    }

    static IdSlotDistribution diminishingSlotDistribution(int... slotSizes) {
        return diminishingSlotDistribution(IDS_PER_ENTRY, slotSizes);
    }

    static IdSlotDistribution diminishingSlotDistribution(int idsPerEntry, int... slotSizes) {
        return new BaseIdSlotDistribution(idsPerEntry, slotSizes) {
            @Override
            public Slot[] slots(int capacity) {
                Slot[] slots = new Slot[slotSizes.length];
                for (int i = 0; i < slotSizes.length; i++) {
                    slots[i] = new Slot(capacity / (1 << (i + 1)), slotSizes[i]);
                }
                return slots;
            }
        };
    }

    static int[] powerTwoSlotSizesDownwards(int highSlotSize) {
        requirePowerOfTwo(highSlotSize);
        int[] slots = new int[Integer.numberOfTrailingZeros(highSlotSize) + 1];
        for (int i = 0; i < slots.length; i++) {
            slots[i] = 1 << i;
        }
        return slots;
    }

    Slot[] slots(int capacity);

    /**
     * @return max ID slot size.
     */
    int maxSlotSize();

    /**
     * This affects high ID and how {@link IdGenerator#nextConsecutiveIdRange(int, boolean, CursorContext)}
     * decides when to cross a store "page"
     */
    default int idsPerEntry() {
        return IDS_PER_ENTRY;
    }

    record Slot(int capacity, int slotSize) {}

    abstract class BaseIdSlotDistribution implements IdSlotDistribution {
        private final int idsPerEntry;
        private final int[] slotSizes;

        BaseIdSlotDistribution(int idsPerEntry, int[] slotSizes) {
            this.idsPerEntry = idsPerEntry;
            this.slotSizes = slotSizes;
        }

        @Override
        public int idsPerEntry() {
            return idsPerEntry;
        }

        @Override
        public int maxSlotSize() {
            return slotSizes[slotSizes.length - 1];
        }
    }
}
