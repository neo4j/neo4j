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
package org.neo4j.collection;

import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

import org.neo4j.collection.trackable.HeapTrackingLongObjectHashMap;
import org.neo4j.memory.Measurable;
import org.neo4j.memory.MemoryTracker;

@SuppressWarnings("ExternalizableWithoutPublicNoArgConstructor")
public class HeapTrackingObjectMap<T extends Measurable> extends HeapTrackingLongObjectHashMap<T> {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(HeapTrackingObjectMap.class);
    private long valuesHeapSize;

    public static <T extends Measurable> HeapTrackingObjectMap<T> createObjectMap(MemoryTracker memoryTracker) {
        memoryTracker.allocateHeap(SHALLOW_SIZE + arraysHeapSize(DEFAULT_INITIAL_CAPACITY));
        return new HeapTrackingObjectMap<>(memoryTracker, DEFAULT_INITIAL_CAPACITY);
    }

    private HeapTrackingObjectMap(MemoryTracker memoryTracker, int trackedCapacity) {
        super(memoryTracker, trackedCapacity);
    }

    @Override
    public T put(long key, T value) {
        allocate(value);
        T old = super.put(key, value);
        if (old != null) {
            release(old);
        }
        return old;
    }

    @Override
    public T remove(long key) {
        T remove = super.remove(key);
        if (remove != null) {
            release(remove);
        }
        return remove;
    }

    @Override
    public void clear() {
        super.clear();
        memoryTracker.releaseHeap(valuesHeapSize);
        valuesHeapSize = 0;
    }

    private void allocate(T value) {
        long valueHeapSize = value.estimatedHeapUsage();
        valuesHeapSize += valueHeapSize;
        memoryTracker.allocateHeap(valueHeapSize);
    }

    private void release(T old) {
        long oldHeapSize = old.estimatedHeapUsage();
        valuesHeapSize -= oldHeapSize;
        memoryTracker.releaseHeap(oldHeapSize);
    }
}
