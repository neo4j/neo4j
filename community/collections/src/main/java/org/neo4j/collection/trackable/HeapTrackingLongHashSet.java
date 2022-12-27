/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.collection.trackable;

import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.VisibleForTesting;

import static java.util.Objects.requireNonNull;
import static org.neo4j.memory.HeapEstimator.ARRAY_HEADER_BYTES;
import static org.neo4j.memory.HeapEstimator.alignObjectSize;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

@SuppressWarnings("ExternalizableWithoutPublicNoArgConstructor")
public class HeapTrackingLongHashSet extends LongHashSet implements AutoCloseable {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(HeapTrackingLongHashSet.class);
    private static final int DEFAULT_INITIAL_CAPACITY = 16;

    private final MemoryTracker memoryTracker;
    private int trackedCapacity;

    @VisibleForTesting
    public static HeapTrackingLongHashSet createLongHashSet(MemoryTracker memoryTracker) {
        memoryTracker.allocateHeap(SHALLOW_SIZE + arrayHeapSize(DEFAULT_INITIAL_CAPACITY));
        return new HeapTrackingLongHashSet(memoryTracker);
    }

    static HeapTrackingLongHashSet createLongHashSet(MemoryTracker memoryTracker, HeapTrackingLongHashSet set) {
        memoryTracker.allocateHeap(SHALLOW_SIZE + arrayHeapSize(set.trackedCapacity));
        return new HeapTrackingLongHashSet(memoryTracker, set);
    }

    static HeapTrackingLongHashSet createLongHashSet(MemoryTracker memoryTracker, LongSet set) {
        memoryTracker.allocateHeap(SHALLOW_SIZE);
        return new HeapTrackingLongHashSet(memoryTracker, set);
    }

    static HeapTrackingLongHashSet createLongHashSet(MemoryTracker memoryTracker, int initialCapacity) {
        int capacity = smallestPowerOfTwoGreaterThan(initialCapacity << 1);
        memoryTracker.allocateHeap(SHALLOW_SIZE + arrayHeapSize(capacity));
        return new HeapTrackingLongHashSet(memoryTracker, initialCapacity, capacity);
    }

    private HeapTrackingLongHashSet(MemoryTracker memoryTracker) {
        this.memoryTracker = requireNonNull(memoryTracker);
        this.trackedCapacity = DEFAULT_INITIAL_CAPACITY;
    }

    private HeapTrackingLongHashSet(MemoryTracker memoryTracker, int initialCapacity, int actualCapacity) {
        super(initialCapacity);
        this.memoryTracker = requireNonNull(memoryTracker);
        this.trackedCapacity = actualCapacity;
    }

    private HeapTrackingLongHashSet(MemoryTracker memoryTracker, HeapTrackingLongHashSet set) {
        super(set);
        this.memoryTracker = requireNonNull(memoryTracker);
        this.trackedCapacity = set.trackedCapacity;
    }

    private HeapTrackingLongHashSet(MemoryTracker memoryTracker, LongSet set) {
        super(set);
        this.memoryTracker = requireNonNull(memoryTracker);
    }

    private static int smallestPowerOfTwoGreaterThan(int n) {
        return n > 1 ? Integer.highestOneBit(n - 1) << 1 : 1;
    }

    @Override
    protected void allocateTable(int sizeToAllocate) {
        if (memoryTracker != null) {
            memoryTracker.allocateHeap(arrayHeapSize(sizeToAllocate));
            memoryTracker.releaseHeap(arrayHeapSize(trackedCapacity));
            trackedCapacity = sizeToAllocate;
        }
        super.allocateTable(sizeToAllocate);
    }

    @Override
    public void close() {
        memoryTracker.releaseHeap(arrayHeapSize(trackedCapacity) + SHALLOW_SIZE);
    }

    @VisibleForTesting
    public static long arrayHeapSize(int arrayLength) {
        return alignObjectSize(ARRAY_HEADER_BYTES + (long) arrayLength * Long.BYTES);
    }
}
