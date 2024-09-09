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

import static org.neo4j.collection.trackable.HeapTrackingArrayList.newCapacity;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.memory.HeapEstimator.sizeOfLongArray;
import static org.neo4j.util.Preconditions.requireNonNegative;

import java.util.Arrays;
import java.util.Objects;
import org.neo4j.collection.PrimitiveLongResourceCollections;
import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.graphdb.Resource;
import org.neo4j.memory.MemoryTracker;

public class HeapTrackingLongArrayList implements Resource {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(HeapTrackingLongArrayList.class);

    private final MemoryTracker memoryTracker;

    private long trackedSize;
    private int size;
    private long[] elementData;

    /**
     * @return a new heap tracking long array list with initial size 1
     */
    public static HeapTrackingLongArrayList newLongArrayList(MemoryTracker memoryTracker) {
        return newLongArrayList(1, memoryTracker);
    }

    /**
     * @return a new heap tracking long array list with the specified initial size
     */
    public static HeapTrackingLongArrayList newLongArrayList(int initialSize, MemoryTracker memoryTracker) {
        requireNonNegative(initialSize);
        long trackedSize = sizeOfLongArray(initialSize);
        memoryTracker.allocateHeap(SHALLOW_SIZE + trackedSize);
        return new HeapTrackingLongArrayList(initialSize, memoryTracker, trackedSize);
    }

    private HeapTrackingLongArrayList(int initialSize, MemoryTracker memoryTracker, long trackedSize) {
        this.trackedSize = trackedSize;
        this.elementData = new long[initialSize];
        this.memoryTracker = memoryTracker;
    }

    public boolean add(long item) {
        add(item, elementData, size);
        return true;
    }

    public void add(int index, long element) {
        rangeCheckForAdd(index);
        final int s = size;
        long[] elementData = this.elementData;
        if (s == elementData.length) {
            elementData = grow(size + 1);
        }
        System.arraycopy(elementData, index, elementData, index + 1, s - index);
        elementData[index] = element;
        size = s + 1;
    }

    public long get(int index) {
        Objects.checkIndex(index, size);
        return elementData[index];
    }

    public long set(int index, long element) {
        Objects.checkIndex(index, size);
        long oldValue = elementData[index];
        elementData[index] = element;
        return oldValue;
    }

    private void add(long e, long[] elementData, int s) {
        if (s == elementData.length) {
            elementData = grow(size + 1);
        }
        elementData[s] = e;
        size = s + 1;
    }

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public boolean notEmpty() {
        return size != 0;
    }

    public void clear() {
        Arrays.fill(this.elementData, 0, size, 0L);
        this.size = 0;
    }

    @Override
    public void close() {
        if (elementData != null) {
            memoryTracker.releaseHeap(trackedSize + SHALLOW_SIZE);
            elementData = null;
        }
    }

    public PrimitiveLongResourceIterator iterator() {
        return new PrimitiveLongResourceCollections.AbstractPrimitiveLongBaseResourceIterator(this) {
            private int index = -1;

            @Override
            protected boolean fetchNext() {
                index++;
                return index < size && next(elementData[index]);
            }
        };
    }

    public long removeLast() {
        long previous = elementData[size - 1];
        --size;
        elementData[size] = 0L;
        return previous;
    }

    public boolean addAll(long... longs) {
        return addAll0(longs, longs.length);
    }

    private boolean addAll0(long[] longs, int numNew) {
        if (numNew == 0) {
            return false;
        }
        final int s = size;
        long[] elementData = this.elementData;
        if (numNew > elementData.length - s) {
            elementData = grow(s + numNew);
        }
        System.arraycopy(longs, 0, elementData, s, numNew);
        size = s + numNew;
        return true;
    }

    public void addAll(HeapTrackingLongArrayList other) {
        addAll0(other.elementData, other.size);
    }
    /**
     * Grow and report size change to tracker
     */
    private long[] grow(int minimumCapacity) {
        int newCapacity = newCapacity(minimumCapacity, elementData.length);
        long oldHeapUsage = trackedSize;
        trackedSize = sizeOfLongArray(newCapacity);
        memoryTracker.allocateHeap(trackedSize);
        long[] newItems = new long[newCapacity];
        System.arraycopy(elementData, 0, newItems, 0, Math.min(size, newCapacity));
        elementData = newItems;
        memoryTracker.releaseHeap(oldHeapUsage);
        return elementData;
    }

    private void rangeCheckForAdd(int index) {
        if (index > size || index < 0) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
        }
    }
}
