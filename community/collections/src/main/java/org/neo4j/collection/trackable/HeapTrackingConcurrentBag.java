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

import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

import java.util.concurrent.atomic.AtomicReferenceArray;
import org.neo4j.memory.MemoryTracker;

@SuppressWarnings({"unchecked", "NullableProblems"})
public final class HeapTrackingConcurrentBag<E> extends HeapTrackingConcurrentHashCollection<E>
        implements AutoCloseable {
    private static final long SHALLOW_SIZE_THIS = shallowSizeOfInstance(HeapTrackingConcurrentBag.class);

    private HeapTrackingConcurrentBag(MemoryTracker memoryTracker) {
        super(memoryTracker, DEFAULT_INITIAL_CAPACITY);
    }

    private HeapTrackingConcurrentBag(MemoryTracker memoryTracker, int initialCapacity) {
        super(memoryTracker, initialCapacity);
    }

    public static <E> HeapTrackingConcurrentBag<E> newBag(MemoryTracker memoryTracker) {
        memoryTracker.allocateHeap(SHALLOW_SIZE_THIS);
        return new HeapTrackingConcurrentBag<>(memoryTracker);
    }

    public static <E> HeapTrackingConcurrentBag<E> newBag(MemoryTracker memoryTracker, int size) {
        memoryTracker.allocateHeap(SHALLOW_SIZE_THIS);
        return new HeapTrackingConcurrentBag<>(memoryTracker, size);
    }

    public static long staticSizeOfWrapperObject() {
        return SHALLOW_SIZE_WRAPPER;
    }

    @Override
    int hash(Object key) {
        return this.hash(System.identityHashCode(key));
    }

    public void add(E value) {
        int hash = this.hash(value);
        AtomicReferenceArray<Object> currentArray = this.table;
        int length = currentArray.length();
        int index = indexFor(hash, length);
        Object o = currentArray.get(index);
        if (o == null) {
            Node<E> newNode = new Node<>(value, null);
            this.addToSize(1);
            if (currentArray.compareAndSet(index, null, newNode)) {
                return;
            }
            addToSize(-1);
        }
        slowAdd(value, hash, currentArray);
    }

    private void slowAdd(E value, int hash, AtomicReferenceArray<Object> currentArray) {
        while (true) {
            int length = currentArray.length();
            int index = HeapTrackingConcurrentHashMap.indexFor(hash, length);
            Object o = currentArray.get(index);
            if (o == RESIZED || o == RESIZING) {
                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
            } else {
                Node<E> newNode = new Node<>(value, (Node<E>) o);
                if (currentArray.compareAndSet(index, o, newNode)) {
                    this.incrementSizeAndPossiblyResize(currentArray, length, o);
                    return;
                }
            }
        }
    }
}
