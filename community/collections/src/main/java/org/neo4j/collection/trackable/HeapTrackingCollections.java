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

import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.neo4j.memory.MemoryTracker;

public final class HeapTrackingCollections {
    private HeapTrackingCollections() {}

    public static HeapTrackingIntHashSet newIntSet(MemoryTracker memoryTracker) {
        return HeapTrackingIntHashSet.createIntHashSet(memoryTracker);
    }

    public static HeapTrackingIntHashSet newIntSet(MemoryTracker memoryTracker, int initialCapacity) {
        return HeapTrackingIntHashSet.createIntHashSet(memoryTracker, initialCapacity);
    }

    public static HeapTrackingIntHashSet newIntSet(MemoryTracker memoryTracker, HeapTrackingIntHashSet set) {
        return HeapTrackingIntHashSet.createIntHashSet(memoryTracker, set);
    }

    public static HeapTrackingIntHashSet newIntSet(MemoryTracker memoryTracker, IntSet set) {
        return HeapTrackingIntHashSet.createIntHashSet(memoryTracker, set);
    }

    public static <V> HeapTrackingIntObjectHashMap<V> newIntObjectHashMap(MemoryTracker memoryTracker) {
        return HeapTrackingIntObjectHashMap.createIntObjectHashMap(memoryTracker);
    }

    public static HeapTrackingLongHashSet newLongSet(MemoryTracker memoryTracker) {
        return HeapTrackingLongHashSet.createLongHashSet(memoryTracker);
    }

    public static HeapTrackingLongHashSet newLongSet(MemoryTracker memoryTracker, int initialCapacity) {
        return HeapTrackingLongHashSet.createLongHashSet(memoryTracker, initialCapacity);
    }

    public static HeapTrackingLongHashSet newLongSet(MemoryTracker memoryTracker, HeapTrackingLongHashSet set) {
        return HeapTrackingLongHashSet.createLongHashSet(memoryTracker, set);
    }

    public static HeapTrackingLongHashSet newLongSet(MemoryTracker memoryTracker, LongSet set) {
        return HeapTrackingLongHashSet.createLongHashSet(memoryTracker, set);
    }

    public static <V> HeapTrackingLongObjectHashMap<V> newLongObjectMap(MemoryTracker memoryTracker) {
        return HeapTrackingLongObjectHashMap.createLongObjectHashMap(memoryTracker);
    }

    public static <V> HeapTrackingLongObjectHashMap<V> newLongObjectMap(
            MemoryTracker memoryTracker, int initialCapacity) {
        return HeapTrackingLongObjectHashMap.createLongObjectHashMap(memoryTracker, initialCapacity);
    }

    public static HeapTrackingLongIntHashMap newLongIntMap(MemoryTracker memoryTracker) {
        return HeapTrackingLongIntHashMap.createLongIntHashMap(memoryTracker);
    }

    public static HeapTrackingIntIntHashMap newIntIntMap(MemoryTracker memoryTracker) {
        return HeapTrackingIntIntHashMap.createIntIntHashMap(memoryTracker);
    }

    public static <K, V> HeapTrackingUnifiedMap<K, V> newMap(MemoryTracker memoryTracker) {
        return HeapTrackingUnifiedMap.createUnifiedMap(memoryTracker);
    }

    public static HeapTrackingLongLongHashMap newLongLongMap(MemoryTracker memoryTracker) {
        return HeapTrackingLongLongHashMap.createLongLongHashMap(memoryTracker);
    }

    public static <T> HeapTrackingUnifiedSet<T> newSet(MemoryTracker memoryTracker) {
        return HeapTrackingUnifiedSet.createUnifiedSet(memoryTracker);
    }

    public static <T> HeapTrackingArrayList<T> newArrayList(int initialSize, MemoryTracker memoryTracker) {
        return HeapTrackingArrayList.newArrayList(initialSize, memoryTracker);
    }

    public static <T> HeapTrackingArrayList<T> newArrayList(MemoryTracker memoryTracker) {
        return HeapTrackingArrayList.newArrayList(memoryTracker);
    }

    public static HeapTrackingLongStack newLongStack(MemoryTracker memoryTracker) {
        return new HeapTrackingLongStack(HeapTrackingLongArrayList.newLongArrayList(memoryTracker));
    }

    public static <T> HeapTrackingArrayDeque<T> newArrayDeque(MemoryTracker memoryTracker) {
        return HeapTrackingArrayDeque.newArrayDeque(memoryTracker);
    }
}
