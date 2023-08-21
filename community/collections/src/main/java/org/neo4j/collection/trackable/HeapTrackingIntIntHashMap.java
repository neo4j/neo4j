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
import static org.neo4j.memory.HeapEstimator.ARRAY_HEADER_BYTES;
import static org.neo4j.memory.HeapEstimator.alignObjectSize;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

import org.eclipse.collections.impl.map.mutable.primitive.IntIntHashMap;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.VisibleForTesting;

@SuppressWarnings("ExternalizableWithoutPublicNoArgConstructor")
public class HeapTrackingIntIntHashMap extends IntIntHashMap implements AutoCloseable {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(HeapTrackingIntIntHashMap.class);
    static final int DEFAULT_INITIAL_CAPACITY = 8;

    final MemoryTracker memoryTracker;
    private int trackedCapacity;

    public static HeapTrackingIntIntHashMap createIntIntHashMap(MemoryTracker memoryTracker) {
        memoryTracker.allocateHeap(SHALLOW_SIZE + arraysHeapSize(DEFAULT_INITIAL_CAPACITY << 2));
        return new HeapTrackingIntIntHashMap(memoryTracker, DEFAULT_INITIAL_CAPACITY << 1);
    }

    private HeapTrackingIntIntHashMap(MemoryTracker memoryTracker, int trackedCapacity) {
        this.memoryTracker = requireNonNull(memoryTracker);
        this.trackedCapacity = trackedCapacity;
    }

    @Override
    protected void allocateTable(int sizeToAllocate) {
        if (memoryTracker != null) {
            memoryTracker.allocateHeap(arraysHeapSize(sizeToAllocate << 1));
            memoryTracker.releaseHeap(arraysHeapSize(trackedCapacity << 1));
            trackedCapacity = sizeToAllocate;
        }
        super.allocateTable(sizeToAllocate);
    }

    @Override
    public void close() {
        memoryTracker.releaseHeap(arraysHeapSize(trackedCapacity << 1) + SHALLOW_SIZE);
    }

    /**
     * Make size() "thread-safer" by grabbing a reference to the values.
     */
    @Override
    public int size() {
        SentinelValues sentinelValues = getSentinelValues();
        return getOccupiedWithData() + (sentinelValues == null ? 0 : sentinelValues.size());
    }

    @VisibleForTesting
    public static long arraysHeapSize(int arrayLength) {
        return alignObjectSize(ARRAY_HEADER_BYTES + (long) arrayLength * Integer.BYTES);
    }
}
