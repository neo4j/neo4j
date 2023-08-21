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
package org.neo4j.collection.trackable;

import static java.util.Objects.requireNonNull;
import static org.neo4j.internal.helpers.Numbers.ceilingPowerOfTwo;
import static org.neo4j.memory.HeapEstimator.ARRAY_HEADER_BYTES;
import static org.neo4j.memory.HeapEstimator.alignObjectSize;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.VisibleForTesting;

@SuppressWarnings("ExternalizableWithoutPublicNoArgConstructor")
public final class HeapTrackingIntHashSet extends IntHashSet implements AutoCloseable {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(HeapTrackingIntHashSet.class);
    private static final int DEFAULT_INITIAL_CAPACITY = 16;

    private final MemoryTracker memoryTracker;
    private int trackedCapacity;

    public static HeapTrackingIntHashSet createIntHashSet(MemoryTracker memoryTracker) {
        return createIntHashSet(memoryTracker, DEFAULT_INITIAL_CAPACITY);
    }

    static HeapTrackingIntHashSet createIntHashSet(MemoryTracker memoryTracker, HeapTrackingIntHashSet set) {
        memoryTracker.allocateHeap(SHALLOW_SIZE + arrayHeapSize(set.trackedCapacity));
        return new HeapTrackingIntHashSet(memoryTracker, set);
    }

    static HeapTrackingIntHashSet createIntHashSet(MemoryTracker memoryTracker, IntSet set) {
        memoryTracker.allocateHeap(SHALLOW_SIZE);
        return new HeapTrackingIntHashSet(memoryTracker, set);
    }

    static HeapTrackingIntHashSet createIntHashSet(MemoryTracker memoryTracker, int initialCapacity) {
        int capacity = ceilingPowerOfTwo(initialCapacity << 1);
        memoryTracker.allocateHeap(SHALLOW_SIZE + arrayHeapSize(capacity));
        return new HeapTrackingIntHashSet(memoryTracker, initialCapacity, capacity);
    }

    private HeapTrackingIntHashSet(MemoryTracker memoryTracker, int initialCapacity, int actualCapacity) {
        super(initialCapacity);
        this.memoryTracker = requireNonNull(memoryTracker);
        this.trackedCapacity = actualCapacity;
    }

    private HeapTrackingIntHashSet(MemoryTracker memoryTracker, HeapTrackingIntHashSet set) {
        super(set);
        this.memoryTracker = requireNonNull(memoryTracker);
        this.trackedCapacity = set.trackedCapacity;
    }

    private HeapTrackingIntHashSet(MemoryTracker memoryTracker, IntSet set) {
        super(set);
        this.memoryTracker = requireNonNull(memoryTracker);
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
        return alignObjectSize(ARRAY_HEADER_BYTES + (long) arrayLength * Integer.BYTES);
    }
}
