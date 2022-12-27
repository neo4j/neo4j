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

import static org.neo4j.collection.trackable.HeapTrackingConcurrentHashMap.MAXIMUM_CAPACITY;
import static org.neo4j.collection.trackable.HeapTrackingConcurrentHashMap.PARTITIONED_SIZE;
import static org.neo4j.collection.trackable.HeapTrackingConcurrentHashMap.PARTITIONED_SIZE_THRESHOLD;
import static org.neo4j.collection.trackable.HeapTrackingConcurrentHashMap.RESIZED;
import static org.neo4j.collection.trackable.HeapTrackingConcurrentHashMap.RESIZE_SENTINEL;
import static org.neo4j.collection.trackable.HeapTrackingConcurrentHashMap.RESIZING;
import static org.neo4j.collection.trackable.HeapTrackingConcurrentHashMap.SHALLOW_SIZE_ATOMIC_REFERENCE_ARRAY;
import static org.neo4j.collection.trackable.HeapTrackingConcurrentHashMap.SIZE_BUCKETS;
import static org.neo4j.collection.trackable.HeapTrackingConcurrentHashMap.SIZE_INTEGER_REFERENCE_ARRAY;
import static org.neo4j.collection.trackable.HeapTrackingLongObjectHashMap.DEFAULT_INITIAL_CAPACITY;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfObjectArray;

import java.util.AbstractSet;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.neo4j.collection.trackable.HeapTrackingConcurrentHashMap.IteratorState;
import org.neo4j.collection.trackable.HeapTrackingConcurrentHashMap.ResizeContainer;
import org.neo4j.memory.MemoryTracker;

@SuppressWarnings({"rawtypes", "ObjectEquality"})
public final class HeapTrackingConcurrentHashSet<E> extends AbstractSet<E> implements Set<E>, AutoCloseable {
    private static final AtomicReferenceFieldUpdater<HeapTrackingConcurrentHashSet, AtomicReferenceArray>
            TABLE_UPDATER = AtomicReferenceFieldUpdater.newUpdater(
                    HeapTrackingConcurrentHashSet.class, AtomicReferenceArray.class, "table");
    private static final AtomicIntegerFieldUpdater<HeapTrackingConcurrentHashSet> SIZE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(HeapTrackingConcurrentHashSet.class, "size");
    private static final long SHALLOW_SIZE_THIS = shallowSizeOfInstance(HeapTrackingConcurrentHashSet.class);

    /**
     * The table, resized as necessary. Length MUST Always be a power of two.
     */
    private volatile AtomicReferenceArray table;

    private AtomicIntegerArray partitionedSize;

    @SuppressWarnings("UnusedDeclaration")
    private volatile int size; // updated via atomic field updater

    private final MemoryTracker memoryTracker;
    private volatile int trackedCapacity;

    private HeapTrackingConcurrentHashSet(MemoryTracker memoryTracker) {
        this(memoryTracker, DEFAULT_INITIAL_CAPACITY);
    }

    public HeapTrackingConcurrentHashSet(MemoryTracker memoryTracker, int initialCapacity) {
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
        if (capacity >= PARTITIONED_SIZE_THRESHOLD) {
            this.partitionedSize = allocateAtomicIntegerArray();
        }
        this.memoryTracker = memoryTracker;
        this.table = allocateAtomicReferenceArray(capacity + 1);
    }

    public static <E> HeapTrackingConcurrentHashSet<E> newSet(MemoryTracker memoryTracker) {
        memoryTracker.allocateHeap(SHALLOW_SIZE_THIS);
        return new HeapTrackingConcurrentHashSet<>(memoryTracker);
    }

    public static <E> HeapTrackingConcurrentHashSet<E> newSet(MemoryTracker memoryTracker, int size) {
        memoryTracker.allocateHeap(SHALLOW_SIZE_THIS);
        return new HeapTrackingConcurrentHashSet<>(memoryTracker, size);
    }

    private AtomicReferenceArray allocateAtomicReferenceArray(int newSize) {
        long toAllocate = shallowSizeOfAtomicReferenceArray(newSize);
        long toRelease = shallowSizeOfAtomicReferenceArray(trackedCapacity);
        // TODO: Do we need to synchronize interaction with memoryTracker?
        memoryTracker.allocateHeap(toAllocate);
        memoryTracker.releaseHeap(toRelease);
        trackedCapacity = newSize;
        return new AtomicReferenceArray(newSize);
    }

    private AtomicIntegerArray allocateAtomicIntegerArray() {
        // TODO: Do we need to synchronize interaction with memoryTracker?
        memoryTracker.allocateHeap(SIZE_INTEGER_REFERENCE_ARRAY);
        return new AtomicIntegerArray(PARTITIONED_SIZE);
    }

    private static long shallowSizeOfAtomicReferenceArray(int size) {
        return size == 0 ? 0 : shallowSizeOfObjectArray(size) + SHALLOW_SIZE_ATOMIC_REFERENCE_ARRAY;
    }

    @Override
    public boolean add(E value) {
        int hash = this.hash(value);
        AtomicReferenceArray currentArray = this.table;
        int length = currentArray.length();
        int index = HeapTrackingConcurrentHashMap.indexFor(hash, length);
        Object o = currentArray.get(index);
        if (o == null) {
            Node<E> newNode = new Node<>(value, null);
            this.addToSize(1);
            if (currentArray.compareAndSet(index, null, newNode)) {
                return true;
            }
            addToSize(-1);
        }
        return slowAdd(value, hash, currentArray);
    }

    private boolean slowAdd(E value, int hash, AtomicReferenceArray currentArray) {
        //noinspection LabeledStatement
        outer:
        while (true) {
            int length = currentArray.length();
            int index = HeapTrackingConcurrentHashMap.indexFor(hash, length);
            Object o = currentArray.get(index);
            if (o == RESIZED || o == RESIZING) {
                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
            } else {
                Node<E> e = (Node<E>) o;
                while (e != null) {
                    Object candidate = e.value;
                    if (candidate.equals(value)) {
                        return false;
                    }
                    e = e.getNext();
                }
                Node<E> newNode = new Node<>(value, (Node<E>) o);
                if (currentArray.compareAndSet(index, o, newNode)) {
                    this.incrementSizeAndPossiblyResize(currentArray, length, o);
                    return true;
                }
            }
        }
    }

    @Override
    public Iterator<E> iterator() {
        return new HashIterator<E>();
    }

    private void incrementSizeAndPossiblyResize(AtomicReferenceArray currentArray, int length, Object prev) {
        this.addToSize(1);
        if (prev != null) {
            int localSize = this.size();
            int threshold = (length >> 1) + (length >> 2); // threshold = length * 0.75
            if (localSize + 1 > threshold) {
                this.resize(currentArray);
            }
        }
    }

    private int hash(Object key) {
        int h = key.hashCode();
        h ^= h >>> 20 ^ h >>> 12;
        h ^= h >>> 7 ^ h >>> 4;
        return h;
    }

    private AtomicReferenceArray helpWithResizeWhileCurrentIndex(AtomicReferenceArray currentArray, int index) {
        AtomicReferenceArray newArray = this.helpWithResize(currentArray);
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

    private AtomicReferenceArray helpWithResize(AtomicReferenceArray currentArray) {
        ResizeContainer resizeContainer = (ResizeContainer) currentArray.get(currentArray.length() - 1);
        AtomicReferenceArray newTable = resizeContainer.nextArray;
        if (resizeContainer.getQueuePosition() > ResizeContainer.QUEUE_INCREMENT) {
            resizeContainer.incrementResizer();
            this.reverseTransfer(currentArray, resizeContainer);
            resizeContainer.decrementResizerAndNotify();
        }
        return newTable;
    }

    private void resize(AtomicReferenceArray oldTable) {
        this.resize(oldTable, (oldTable.length() - 1 << 1) + 1);
    }

    // newSize must be a power of 2 + 1
    @SuppressWarnings("JLM_JSR166_UTILCONCURRENT_MONITORENTER")
    private void resize(AtomicReferenceArray oldTable, int newSize) {
        int oldCapacity = oldTable.length();
        int end = oldCapacity - 1;
        Object last = oldTable.get(end);
        if (this.size() < end && last == RESIZE_SENTINEL) {
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
                        this.partitionedSize = allocateAtomicIntegerArray();
                    }
                    resizeContainer = new ResizeContainer(allocateAtomicReferenceArray(newSize), oldTable.length() - 1);
                    oldTable.set(end, resizeContainer);
                    ownResize = true;
                }
            }
        }
        if (ownResize) {
            this.transfer(oldTable, resizeContainer);
            AtomicReferenceArray src = this.table;
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
    private void transfer(AtomicReferenceArray src, ResizeContainer resizeContainer) {
        AtomicReferenceArray dest = resizeContainer.nextArray;

        for (int j = 0; j < src.length() - 1; ) {
            Object o = src.get(j);
            if (o == null) {
                if (src.compareAndSet(j, null, RESIZED)) {
                    j++;
                }
            } else if (o == RESIZED || o == RESIZING) {
                j = (j & ~(ResizeContainer.QUEUE_INCREMENT - 1)) + ResizeContainer.QUEUE_INCREMENT;
                if (resizeContainer.resizers.get() == 1) {
                    break;
                }
            } else {
                Node<E> e = (Node<E>) o;
                if (src.compareAndSet(j, o, RESIZING)) {
                    while (e != null) {
                        this.unconditionalCopy(dest, e);
                        e = e.getNext();
                    }
                    src.set(j, RESIZED);
                    j++;
                }
            }
        }
        resizeContainer.decrementResizerAndNotify();
        resizeContainer.waitForAllResizers();
    }

    private void reverseTransfer(AtomicReferenceArray src, ResizeContainer resizeContainer) {
        AtomicReferenceArray dest = resizeContainer.nextArray;
        while (resizeContainer.getQueuePosition() > 0) {
            int start = resizeContainer.subtractAndGetQueuePosition();
            int end = start + ResizeContainer.QUEUE_INCREMENT;
            if (end > 0) {
                if (start < 0) {
                    start = 0;
                }
                for (int j = end - 1; j >= start; ) {
                    Object o = src.get(j);
                    if (o == null) {
                        if (src.compareAndSet(j, null, RESIZED)) {
                            j--;
                        }
                    } else if (o == RESIZED || o == RESIZING) {
                        resizeContainer.zeroOutQueuePosition();
                        return;
                    } else {
                        Node<E> e = (Node<E>) o;
                        if (src.compareAndSet(j, o, RESIZING)) {
                            while (e != null) {
                                this.unconditionalCopy(dest, e);
                                e = e.getNext();
                            }
                            src.set(j, RESIZED);
                            j--;
                        }
                    }
                }
            }
        }
    }

    private void unconditionalCopy(AtomicReferenceArray dest, Node<E> toCopyNode) {
        int hash = this.hash(toCopyNode.value);
        AtomicReferenceArray currentArray = dest;
        while (true) {
            int length = currentArray.length();
            int index = HeapTrackingConcurrentHashMap.indexFor(hash, length);
            Object o = currentArray.get(index);
            if (o == RESIZED || o == RESIZING) {
                currentArray = ((ResizeContainer) currentArray.get(length - 1)).nextArray;
            } else {
                Node<E> newNode;
                if (o == null) {
                    if (toCopyNode.getNext() == null) {
                        newNode = toCopyNode; // no need to duplicate
                    } else {
                        newNode = new Node<>(toCopyNode.value);
                    }
                } else {
                    newNode = new Node<>(toCopyNode.value, (Node<E>) o);
                }
                if (currentArray.compareAndSet(index, o, newNode)) {
                    return;
                }
            }
        }
    }

    private void addToSize(int value) {
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

    @Override
    public int size() {
        int localSize = this.size;
        if (this.partitionedSize != null) {
            for (int i = 0; i < SIZE_BUCKETS; i++) {
                localSize += this.partitionedSize.get(i << 4);
            }
        }
        return localSize;
    }

    @Override
    public boolean isEmpty() {
        return this.size == 0;
    }

    @Override
    public boolean contains(Object value) {
        int hash = this.hash(value);
        AtomicReferenceArray currentArray = this.table;
        while (true) {
            int length = currentArray.length();
            int index = HeapTrackingConcurrentHashMap.indexFor(hash, length);
            Object o = currentArray.get(index);
            if (o == RESIZED || o == RESIZING) {
                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
            } else {
                Node<E> e = (Node<E>) o;
                while (e != null) {
                    Object candidate = e.value;
                    if (candidate.equals(value)) {
                        return true;
                    }
                    e = e.getNext();
                }
                return false;
            }
        }
    }

    @Override
    public void clear() {
        AtomicReferenceArray currentArray = this.table;
        ResizeContainer resizeContainer;
        do {
            resizeContainer = null;
            for (int i = 0; i < currentArray.length() - 1; i++) {
                Object o = currentArray.get(i);
                if (o == RESIZED || o == RESIZING) {
                    resizeContainer = (ResizeContainer) currentArray.get(currentArray.length() - 1);
                } else if (o != null) {
                    Node<E> e = (Node<E>) o;
                    if (currentArray.compareAndSet(i, o, null)) {
                        int removedEntries = 0;
                        while (e != null) {
                            removedEntries++;
                            e = e.getNext();
                        }
                        this.addToSize(-removedEntries);
                    }
                }
            }
            if (resizeContainer != null) {
                if (resizeContainer.isNotDone()) {
                    this.helpWithResize(currentArray);
                    resizeContainer.waitForAllResizers();
                }
                currentArray = resizeContainer.nextArray;
            }
        } while (resizeContainer != null);
    }

    @Override
    public boolean remove(Object value) {
        int hash = this.hash(value);
        AtomicReferenceArray currentArray = this.table;
        int length = currentArray.length();
        int index = HeapTrackingConcurrentHashMap.indexFor(hash, length);
        Object o = currentArray.get(index);
        if (o == RESIZED || o == RESIZING) {
            return this.slowRemove(value, hash, currentArray);
        }
        Node<E> e = (Node<E>) o;
        while (e != null) {
            Object candidate = e.value;
            if (candidate.equals(value)) {
                Node<E> replacement = this.createReplacementChainForRemoval((Node<E>) o, e);
                if (currentArray.compareAndSet(index, o, replacement)) {
                    this.addToSize(-1);
                    return true;
                }
                return this.slowRemove(value, hash, currentArray);
            }
            e = e.getNext();
        }
        return false;
    }

    private boolean slowRemove(Object value, int hash, AtomicReferenceArray currentArray) {
        //noinspection LabeledStatement
        outer:
        while (true) {
            int length = currentArray.length();
            int index = HeapTrackingConcurrentHashMap.indexFor(hash, length);
            Object o = currentArray.get(index);
            if (o == RESIZED || o == RESIZING) {
                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
            } else {
                Node<E> e = (Node<E>) o;
                while (e != null) {
                    Object candidate = e.value;
                    if (candidate.equals(value)) {
                        Node<E> replacement = this.createReplacementChainForRemoval((Node<E>) o, e);
                        if (currentArray.compareAndSet(index, o, replacement)) {
                            this.addToSize(-1);
                            return true;
                        }
                        //noinspection ContinueStatementWithLabel
                        continue outer;
                    }
                    e = e.getNext();
                }
                return false;
            }
        }
    }

    private Node<E> createReplacementChainForRemoval(Node<E> original, Node<E> toRemove) {
        if (original == toRemove) {
            return original.getNext();
        }
        Node<E> replacement = null;
        Node<E> e = original;
        while (e != null) {
            if (e != toRemove) {
                replacement = new Node<>(e.value, replacement);
            }
            e = e.getNext();
        }
        return replacement;
    }

    @Override
    public int hashCode() {
        int h = 0;
        AtomicReferenceArray currentArray = this.table;
        for (int i = 0; i < currentArray.length() - 1; i++) {
            Object o = currentArray.get(i);
            if (o == RESIZED || o == RESIZING) {
                throw new ConcurrentModificationException("can't compute hashcode while resizing!");
            }
            Node<E> e = (Node<E>) o;
            while (e != null) {
                Object value = e.value;
                h += (value == null ? 0 : value.hashCode());
                e = e.getNext();
            }
        }
        return h;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }

        if (!(o instanceof Set)) {
            return false;
        }
        Set<E> s = (Set<E>) o;
        if (s.size() != this.size()) {
            return false;
        }
        for (E e : this) {
            if (!s.contains(e)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void close() {
        memoryTracker.releaseHeap(SHALLOW_SIZE_THIS + shallowSizeOfAtomicReferenceArray(trackedCapacity));
        if (partitionedSize != null) {
            memoryTracker.releaseHeap(SIZE_INTEGER_REFERENCE_ARRAY);
        }
    }

    private class HashIterator<E> implements Iterator<E> {
        private List<IteratorState> todo;
        private IteratorState currentState;
        private Node<E> next;
        private int index;

        protected HashIterator() {
            this.currentState = new IteratorState(HeapTrackingConcurrentHashSet.this.table);
            this.findNext();
        }

        private void findNext() {
            while (this.index < this.currentState.end) {
                Object o = this.currentState.currentTable.get(this.index);
                if (o == RESIZED || o == RESIZING) {
                    AtomicReferenceArray nextArray = HeapTrackingConcurrentHashSet.this.helpWithResizeWhileCurrentIndex(
                            this.currentState.currentTable, this.index);
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
                    this.next = (Node<E>) o;
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

        @Override
        public final boolean hasNext() {
            return this.next != null;
        }

        @Override
        public final E next() {
            Node<E> e = this.next;
            if (e == null) {
                throw new NoSuchElementException();
            }

            if ((this.next = e.getNext()) == null) {
                this.findNext();
            }
            return e.value;
        }
    }

    public static final class Node<E> {
        private final E value;
        private final Node<E> next;

        private Node(E value) {
            this.value = value;
            this.next = null;
        }

        private Node(E value, Node<E> next) {
            this.value = value;
            this.next = next;
        }

        public Node<E> getNext() {
            return this.next;
        }
    }
}
