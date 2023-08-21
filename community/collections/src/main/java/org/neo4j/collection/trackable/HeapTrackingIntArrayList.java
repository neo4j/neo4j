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
import static org.neo4j.memory.HeapEstimator.shallowSizeOfObjectArray;
import static org.neo4j.memory.HeapEstimator.sizeOfIntArray;
import static org.neo4j.util.Preconditions.requireNonNegative;

import java.util.Arrays;
import java.util.Objects;
import org.neo4j.graphdb.Resource;
import org.neo4j.memory.MemoryTracker;

public class HeapTrackingIntArrayList implements Resource {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(HeapTrackingIntArrayList.class);

    private final MemoryTracker memoryTracker;

    private long trackedSize;
    private int size;
    private int[] elementData;

    /**
     * @return a new heap tracking int array list with initial size 1
     */
    public static HeapTrackingIntArrayList newIntArrayList(MemoryTracker memoryTracker) {
        return newIntArrayList(1, memoryTracker);
    }

    @SuppressWarnings("CopyConstructorMissesField")
    private HeapTrackingIntArrayList(HeapTrackingIntArrayList other) {
        int otherSize = other.size;
        this.size = otherSize;
        this.elementData = new int[otherSize];
        System.arraycopy(other.elementData, 0, this.elementData, 0, otherSize);
        this.memoryTracker = other.memoryTracker;
        this.trackedSize = shallowSizeOfObjectArray(otherSize);
        memoryTracker.allocateHeap(SHALLOW_SIZE + trackedSize);
    }

    /**
     * @return a new heap tracking int array list with the specified initial size
     */
    public static HeapTrackingIntArrayList newIntArrayList(int initialSize, MemoryTracker memoryTracker) {
        requireNonNegative(initialSize);
        long trackedSize = sizeOfIntArray(initialSize);
        memoryTracker.allocateHeap(SHALLOW_SIZE + trackedSize);
        return new HeapTrackingIntArrayList(initialSize, memoryTracker, trackedSize);
    }

    private HeapTrackingIntArrayList(int initialSize, MemoryTracker memoryTracker, long trackedSize) {
        this.trackedSize = trackedSize;
        this.elementData = new int[initialSize];
        this.memoryTracker = memoryTracker;
    }

    public boolean add(int item) {
        add(item, elementData, size);
        return true;
    }

    public void add(int index, int element) {
        rangeCheckForAdd(index);
        final int s = size;
        int[] elementData = this.elementData;
        if (s == elementData.length) {
            elementData = grow(size + 1);
        }
        System.arraycopy(elementData, index, elementData, index + 1, s - index);
        elementData[index] = element;
        size = s + 1;
    }

    public int get(int index) {
        Objects.checkIndex(index, size);
        return elementData[index];
    }

    public int set(int index, int element) {
        Objects.checkIndex(index, size);
        int oldValue = elementData[index];
        elementData[index] = element;
        return oldValue;
    }

    private void add(int e, int[] elementData, int s) {
        if (s == elementData.length) {
            elementData = grow(size + 1);
        }
        elementData[s] = e;
        size = s + 1;
    }

    public boolean contains(int e) {
        for (int i = 0; i < size; i++) {
            if (elementData[i] == e) {
                return true;
            }
        }
        return false;
    }

    public int indexOf(int e) {
        for (int i = 0; i < size; i++) {
            if (elementData[i] == e) {
                return i;
            }
        }
        return -1;
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
        this.size = 0;
    }

    @Override
    public void close() {
        if (elementData != null) {
            memoryTracker.releaseHeap(trackedSize + SHALLOW_SIZE);
            elementData = null;
        }
    }

    public boolean addAll(int... values) {
        int numNew = values.length;
        if (numNew == 0) {
            return false;
        }
        final int s = size;
        int[] elementData = this.elementData;
        if (numNew > elementData.length - s) {
            elementData = grow(s + numNew);
        }
        System.arraycopy(values, 0, elementData, s, numNew);
        size = s + numNew;
        return true;
    }

    public int[] toArray() {
        return Arrays.copyOf(elementData, size);
    }

    @Override
    public HeapTrackingIntArrayList clone() {
        return new HeapTrackingIntArrayList(this);
    }

    public void truncate(int size) {
        if (size >= this.size) {
            return;
        }

        this.size = size;
    }

    /**
     * Grow and report size change to tracker
     */
    private int[] grow(int minimumCapacity) {
        int newCapacity = newCapacity(minimumCapacity, elementData.length);
        long oldHeapUsage = trackedSize;
        trackedSize = sizeOfIntArray(newCapacity);
        memoryTracker.allocateHeap(trackedSize);
        int[] newItems = new int[newCapacity];
        System.arraycopy(this.elementData, 0, newItems, 0, Math.min(size, newCapacity));
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
