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
import static org.neo4j.memory.HeapEstimator.OBJECT_REFERENCE_BYTES;
import static org.neo4j.memory.HeapEstimator.alignObjectSize;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.VisibleForTesting;

@SuppressWarnings("ExternalizableWithoutPublicNoArgConstructor")
public class HeapTrackingLongObjectHashMap<V> extends LongObjectHashMap<V> implements AutoCloseable {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(HeapTrackingLongObjectHashMap.class);
    protected static final int DEFAULT_INITIAL_CAPACITY = 16;

    protected final MemoryTracker memoryTracker;
    private int trackedCapacity;

    public static <V> HeapTrackingLongObjectHashMap<V> createLongObjectHashMap(MemoryTracker memoryTracker) {
        return createLongObjectHashMap(memoryTracker, DEFAULT_INITIAL_CAPACITY);
    }

    static <V> HeapTrackingLongObjectHashMap<V> createLongObjectHashMap(
            MemoryTracker memoryTracker, int initialCapacity) {
        memoryTracker.allocateHeap(SHALLOW_SIZE + arraysHeapSize(initialCapacity));
        return new HeapTrackingLongObjectHashMap<>(memoryTracker, initialCapacity);
    }

    protected HeapTrackingLongObjectHashMap(MemoryTracker memoryTracker, int trackedCapacity) {
        this.memoryTracker = requireNonNull(memoryTracker);
        this.trackedCapacity = trackedCapacity;
    }

    @Override
    protected void allocateTable(int sizeToAllocate) {
        if (memoryTracker != null) {
            memoryTracker.allocateHeap(arraysHeapSize(sizeToAllocate));
            memoryTracker.releaseHeap(arraysHeapSize(trackedCapacity));
            trackedCapacity = sizeToAllocate;
        }
        super.allocateTable(sizeToAllocate);
    }

    @Override
    public void close() {
        memoryTracker.releaseHeap(arraysHeapSize(trackedCapacity) + SHALLOW_SIZE);
    }

    @VisibleForTesting
    public static long arraysHeapSize(int arrayLength) {
        long keyArray = alignObjectSize(ARRAY_HEADER_BYTES + (long) arrayLength * Long.BYTES);
        long valueArray = alignObjectSize(ARRAY_HEADER_BYTES + (long) arrayLength * OBJECT_REFERENCE_BYTES);
        return keyArray + valueArray;
    }
}
