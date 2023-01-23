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
import static org.neo4j.memory.HeapEstimator.shallowSizeOfObjectArray;
import static org.neo4j.memory.HeapEstimator.sizeOfIntArray;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.neo4j.memory.MemoryTracker;

/**
 * This class contains a fork of org.eclipse.collections.impl.map.mutable.ConcurrentHashMap extending
 * it with memory tracking capabilities.
 * <p>
 * Modifications are marked in the code as following:
 * <pre>{@code
 *   //BEGIN MODIFICATION
 *   (modified lines)
 *   //END MODIFICATION
 * }</pre>
 *
 * NOTE: this class only tracks the memory of the internal structures, it will not track the individual entries.
 * The user of this class can use {@link #sizeOfWrapperObject()} to improve the estimation.
 */
public abstract class AbstractHeapTrackingConcurrentHash {

    static final Object RESIZE_SENTINEL = new Object();
    static final int DEFAULT_INITIAL_CAPACITY = 16;

    /**
     * The maximum capacity, used if a higher value is implicitly specified
     * by either of the constructors with arguments.
     * MUST be a power of two <= 1<<30.
     */
    static final int MAXIMUM_CAPACITY = 1 << 30;

    @SuppressWarnings("unchecked")
    private static final AtomicReferenceFieldUpdater<AbstractHeapTrackingConcurrentHash, AtomicReferenceArray<Object>>
            TABLE_UPDATER = AtomicReferenceFieldUpdater.newUpdater(
                    AbstractHeapTrackingConcurrentHash.class,
                    (Class<AtomicReferenceArray<Object>>) (Class<?>) AtomicReferenceArray.class,
                    "table");

    private static final AtomicIntegerFieldUpdater<AbstractHeapTrackingConcurrentHash> SIZE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(AbstractHeapTrackingConcurrentHash.class, "size");
    static final Object RESIZED = new Object();
    static final Object RESIZING = new Object();
    static final int PARTITIONED_SIZE_THRESHOLD = 4096; // chosen to keep size below 1% of the total size of the map
    static final int SIZE_BUCKETS = 7;
    static final int PARTITIONED_SIZE = SIZE_BUCKETS * 16;
    static final long SHALLOW_SIZE_ATOMIC_REFERENCE_ARRAY = shallowSizeOfInstance(AtomicReferenceArray.class);
    static final long SIZE_INTEGER_REFERENCE_ARRAY =
            shallowSizeOfInstance(AtomicIntegerArray.class) + sizeOfIntArray(PARTITIONED_SIZE);

    /**
     * The table, resized as necessary. Length MUST Always be a power of two.
     */
    volatile AtomicReferenceArray<Object> table;

    private AtomicIntegerArray partitionedSize;

    @SuppressWarnings("UnusedDeclaration")
    private volatile int size; // updated via atomic field updater

    final MemoryTracker memoryTracker;
    private volatile int trackedCapacity;

    AbstractHeapTrackingConcurrentHash(MemoryTracker memoryTracker, int initialCapacity) {
        if (initialCapacity < 0) {
            throw new IllegalArgumentException("Illegal Initial Capacity: " + initialCapacity);
        }
        if (initialCapacity > MAXIMUM_CAPACITY) {
            initialCapacity = MAXIMUM_CAPACITY;
        }

        int threshold = initialCapacity;
        threshold += threshold >> 1; // threshold = length * 0.75

        int capacity = 1;
        while (capacity < threshold) {
            capacity <<= 1;
        }
        // NOTE: adds memory tracking of internal structures
        // BEGIN MODIFICATION
        if (capacity >= PARTITIONED_SIZE_THRESHOLD) {
            this.partitionedSize = allocateAtomicIntegerArray();
        }
        this.memoryTracker = memoryTracker;
        this.table = allocateAtomicReferenceArray(capacity + 1);
        // END MODIFICATION
    }

    static int indexFor(int h, int length) {
        return h & length - 2;
    }

    public abstract long sizeOfWrapperObject();

    private AtomicReferenceArray<Object> allocateAtomicReferenceArray(int newSize) {
        memoryTracker.allocateHeap(shallowSizeOfAtomicReferenceArray(newSize));
        memoryTracker.releaseHeap(shallowSizeOfAtomicReferenceArray(trackedCapacity));
        trackedCapacity = newSize;
        return new AtomicReferenceArray<>(newSize);
    }

    /**
     * NOTE: this method is only (potentially) called from constructor and from within a synchronized block
     */
    private AtomicIntegerArray allocateAtomicIntegerArray() {
        memoryTracker.allocateHeap(SIZE_INTEGER_REFERENCE_ARRAY);
        return new AtomicIntegerArray(PARTITIONED_SIZE);
    }

    private static long shallowSizeOfAtomicReferenceArray(int size) {
        return size == 0 ? 0 : shallowSizeOfObjectArray(size) + SHALLOW_SIZE_ATOMIC_REFERENCE_ARRAY;
    }

    void incrementSizeAndPossiblyResize(AtomicReferenceArray<Object> currentArray, int length, Object prev) {
        this.addToSize(1);
        if (prev != null) {
            int localSize = this.size();
            int threshold = (length >> 1) + (length >> 2); // threshold = length * 0.75
            if (localSize + 1 > threshold) {
                this.resize(currentArray);
            }
        }
    }

    int hash(Object key) {
        return hash(key.hashCode());
    }

    final int hash(long key) {
        return hash(Long.hashCode(key));
    }

    final int hash(int h) {
        h ^= h >>> 20 ^ h >>> 12;
        h ^= h >>> 7 ^ h >>> 4;
        return h;
    }

    final AtomicReferenceArray<Object> helpWithResizeWhileCurrentIndex(
            AtomicReferenceArray<Object> currentArray, int index) {
        AtomicReferenceArray<Object> newArray = this.helpWithResize(currentArray);
        int helpCount = 0;
        while (currentArray.get(index) != RESIZED) {
            helpCount++;
            newArray = this.helpWithResize(currentArray);
            if ((helpCount & 7) == 0) {
                Thread.yield();
            }
        }
        return newArray;
    }

    final AtomicReferenceArray<Object> helpWithResize(AtomicReferenceArray<Object> currentArray) {
        ResizeContainer resizeContainer = (ResizeContainer) currentArray.get(currentArray.length() - 1);
        AtomicReferenceArray<Object> newTable = resizeContainer.nextArray;
        if (resizeContainer.getQueuePosition() > ResizeContainer.QUEUE_INCREMENT) {
            resizeContainer.incrementResizer();
            this.reverseTransfer(currentArray, resizeContainer);
            resizeContainer.decrementResizerAndNotify();
        }
        return newTable;
    }

    private void resize(AtomicReferenceArray<Object> oldTable) {
        this.resize(oldTable, (oldTable.length() - 1 << 1) + 1);
    }

    // newSize must be a power of 2 + 1
    @SuppressWarnings("JLM_JSR166_UTILCONCURRENT_MONITORENTER")
    private void resize(AtomicReferenceArray<Object> oldTable, int newSize) {
        int oldCapacity = oldTable.length();
        int end = oldCapacity - 1;
        Object last = oldTable.get(end);
        int localSize = this.size();
        if (localSize < end && last == RESIZE_SENTINEL) {
            return;
        }
        if (oldCapacity >= MAXIMUM_CAPACITY) {
            throw new RuntimeException("index is too large!");
        }
        ResizeContainer resizeContainer = null;
        boolean ownResize = false;
        if (last == null || last == RESIZE_SENTINEL) {
            synchronized (oldTable) // allocating a new array is too expensive to make this an atomic operation
            {
                if (oldTable.get(end) == null) {
                    oldTable.set(end, RESIZE_SENTINEL);
                    if (this.partitionedSize == null && newSize >= PARTITIONED_SIZE_THRESHOLD) {
                        // BEGIN MODIFICATION
                        this.partitionedSize = allocateAtomicIntegerArray();
                        // END MODIFICATION
                    }
                    // BEGIN MODIFICATION
                    resizeContainer = new ResizeContainer(allocateAtomicReferenceArray(newSize), oldTable.length() - 1);
                    // END MODIFICATION
                    oldTable.set(end, resizeContainer);
                    ownResize = true;
                }
            }
        }
        if (ownResize) {
            transfer(oldTable, resizeContainer);
            AtomicReferenceArray<Object> src = this.table;
            while (!TABLE_UPDATER.compareAndSet(this, oldTable, resizeContainer.nextArray)) {
                // we're in a double resize situation; we'll have to go help until it's our turn to set the table
                if (src != oldTable) {
                    this.helpWithResize(src);
                }
            }
        } else {
            this.helpWithResize(oldTable);
        }
    }

    /*
     * Transfer all entries from src to dest tables
     */
    abstract void transfer(
            AtomicReferenceArray<Object> src, HeapTrackingConcurrentHashMap.ResizeContainer resizeContainer);

    abstract void reverseTransfer(AtomicReferenceArray<Object> src, ResizeContainer resizeContainer);

    public int size() {
        int localSize = this.size;
        if (this.partitionedSize != null) {
            for (int i = 0; i < SIZE_BUCKETS; i++) {
                localSize += this.partitionedSize.get(i << 4);
            }
        }
        return localSize;
    }

    public boolean isEmpty() {
        return this.size == 0;
    }

    public boolean notEmpty() {
        return this.size > 0;
    }

    final void addToSize(int value) {
        if (this.partitionedSize != null) {
            if (this.incrementPartitionedSize(value)) {
                return;
            }
        }
        this.incrementLocalSize(value);
    }

    private boolean incrementPartitionedSize(int value) {
        int h = (int) Thread.currentThread().getId();
        h ^= (h >>> 18) ^ (h >>> 12);
        h = (h ^ (h >>> 10)) & SIZE_BUCKETS;
        if (h != 0) {
            h = (h - 1) << 4;
            while (true) {
                int localSize = this.partitionedSize.get(h);
                if (this.partitionedSize.compareAndSet(h, localSize, localSize + value)) {
                    return true;
                }
            }
        }
        return false;
    }

    private void incrementLocalSize(int value) {
        while (true) {
            int localSize = this.size;
            if (SIZE_UPDATER.compareAndSet(this, localSize, localSize + value)) {
                break;
            }
        }
    }

    public void releaseHeap() {
        memoryTracker.releaseHeap(shallowSizeOfAtomicReferenceArray(trackedCapacity));
        if (partitionedSize != null) {
            memoryTracker.releaseHeap(SIZE_INTEGER_REFERENCE_ARRAY);
        }
    }

    static final class ResizeContainer {
        static final int QUEUE_INCREMENT =
                Math.min(1 << 10, Integer.highestOneBit(Runtime.getRuntime().availableProcessors()) << 4);
        final AtomicInteger resizers = new AtomicInteger(1);
        final AtomicReferenceArray<Object> nextArray;
        final AtomicInteger queuePosition;

        ResizeContainer(AtomicReferenceArray<Object> nextArray, int oldSize) {
            this.nextArray = nextArray;
            this.queuePosition = new AtomicInteger(oldSize);
        }

        public void incrementResizer() {
            this.resizers.incrementAndGet();
        }

        public void decrementResizerAndNotify() {
            int remaining = this.resizers.decrementAndGet();
            if (remaining == 0) {
                synchronized (this) {
                    this.notifyAll();
                }
            }
        }

        public int getQueuePosition() {
            return this.queuePosition.get();
        }

        public int subtractAndGetQueuePosition() {
            return this.queuePosition.addAndGet(-QUEUE_INCREMENT);
        }

        public void waitForAllResizers() {
            if (this.resizers.get() > 0) {
                for (int i = 0; i < 16; i++) {
                    if (this.resizers.get() == 0) {
                        break;
                    }
                }
                for (int i = 0; i < 16; i++) {
                    if (this.resizers.get() == 0) {
                        break;
                    }
                    Thread.yield();
                }
            }
            if (this.resizers.get() > 0) {
                synchronized (this) {
                    while (this.resizers.get() > 0) {
                        try {
                            this.wait();
                        } catch (InterruptedException e) {
                            // ignore
                        }
                    }
                }
            }
        }

        public boolean isNotDone() {
            return this.resizers.get() > 0;
        }

        public void zeroOutQueuePosition() {
            this.queuePosition.set(0);
        }
    }

    static final class IteratorState {
        AtomicReferenceArray<Object> currentTable;
        int start;
        int end;

        IteratorState(AtomicReferenceArray<Object> currentTable) {
            this.currentTable = currentTable;
            this.end = this.currentTable.length() - 1;
        }

        IteratorState(AtomicReferenceArray<Object> currentTable, int start, int end) {
            this.currentTable = currentTable;
            this.start = start;
            this.end = end;
        }
    }

    interface Wrapper<W> {
        W getNext();
    }

    abstract class HashIterator<WRAPPER extends Wrapper<?>> {
        private List<IteratorState> todo;
        private IteratorState currentState;
        WRAPPER next;
        WRAPPER current;
        private int index;

        protected HashIterator() {
            this.currentState = new IteratorState(table);
            this.findNext();
        }

        final void findNext() {
            while (this.index < this.currentState.end) {
                Object o = this.currentState.currentTable.get(this.index);
                if (o == RESIZED || o == RESIZING) {
                    AtomicReferenceArray<Object> nextArray =
                            helpWithResizeWhileCurrentIndex(this.currentState.currentTable, this.index);
                    int endResized = this.index + 1;
                    while (endResized < this.currentState.end) {
                        if (this.currentState.currentTable.get(endResized) != RESIZED) {
                            break;
                        }
                        endResized++;
                    }
                    if (this.todo == null) {
                        this.todo = new FastList<>(4);
                    }
                    if (endResized < this.currentState.end) {
                        this.todo.add(
                                new IteratorState(this.currentState.currentTable, endResized, this.currentState.end));
                    }
                    int powerTwoLength = this.currentState.currentTable.length() - 1;
                    this.todo.add(
                            new IteratorState(nextArray, this.index + powerTwoLength, endResized + powerTwoLength));
                    this.currentState.currentTable = nextArray;
                    this.currentState.end = endResized;
                    this.currentState.start = this.index;
                } else if (o != null) {
                    this.next = (WRAPPER) o;
                    this.index++;
                    break;
                } else {
                    this.index++;
                }
            }
            if (this.next == null && this.index == this.currentState.end && this.todo != null && !this.todo.isEmpty()) {
                this.currentState = this.todo.remove(this.todo.size() - 1);
                this.index = this.currentState.start;
                this.findNext();
            }
        }

        public boolean hasNext() {
            return this.next != null;
        }
    }
}
