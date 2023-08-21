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

import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

import java.util.ConcurrentModificationException;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.eclipse.collections.api.iterator.LongIterator;
import org.neo4j.memory.MemoryTracker;

public final class HeapTrackingConcurrentLongHashSet extends AbstractHeapTrackingConcurrentHash
        implements AutoCloseable {
    private static final long SHALLOW_SIZE_THIS = shallowSizeOfInstance(HeapTrackingConcurrentLongHashSet.class);
    private static final long SHALLOW_SIZE_WRAPPER = shallowSizeOfInstance(Node.class);

    @Override
    public long sizeOfWrapperObject() {
        return SHALLOW_SIZE_WRAPPER;
    }

    public static HeapTrackingConcurrentLongHashSet newSet(MemoryTracker memoryTracker) {
        return newSet(memoryTracker, DEFAULT_INITIAL_CAPACITY);
    }

    public static HeapTrackingConcurrentLongHashSet newSet(MemoryTracker memoryTracker, int size) {
        memoryTracker.allocateHeap(SHALLOW_SIZE_THIS);
        return new HeapTrackingConcurrentLongHashSet(memoryTracker, size);
    }

    private HeapTrackingConcurrentLongHashSet(MemoryTracker memoryTracker, int initialCapacity) {
        super(memoryTracker, initialCapacity);
    }

    public boolean add(long value) {
        int hash = this.hash(Long.hashCode(value));
        AtomicReferenceArray<Object> currentArray = this.table;
        int length = currentArray.length();
        int index = indexFor(hash, length);
        Object o = currentArray.get(index);
        if (o == null) {
            Node newNode = new Node(value, null);
            this.addToSize(1);
            if (currentArray.compareAndSet(index, null, newNode)) {
                return true;
            }
            addToSize(-1);
        }
        return slowAdd(value, hash, currentArray);
    }

    private boolean slowAdd(long value, int hash, AtomicReferenceArray<Object> currentArray) {
        while (true) {
            int length = currentArray.length();
            int index = indexFor(hash, length);
            Object o = currentArray.get(index);
            if (o == RESIZED || o == RESIZING) {
                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
            } else {
                Node e = (Node) o;
                while (e != null) {
                    long candidate = e.value;
                    if (candidate == value) {
                        return false;
                    }
                    e = e.getNext();
                }
                Node newNode = new Node(value, (Node) o);
                if (currentArray.compareAndSet(index, o, newNode)) {
                    this.incrementSizeAndPossiblyResize(currentArray, length, o);
                    return true;
                }
            }
        }
    }

    @Override
    void transfer(AtomicReferenceArray<Object> src, ResizeContainer resizeContainer) {
        AtomicReferenceArray<Object> dest = resizeContainer.nextArray;

        for (int j = 0; j < src.length() - 1; ) {
            Object o = src.get(j);
            if (o == null) {
                if (src.compareAndSet(j, null, RESIZED)) {
                    j++;
                }
            } else if (o == RESIZED || o == RESIZING) {
                j = (j & -ResizeContainer.QUEUE_INCREMENT) + ResizeContainer.QUEUE_INCREMENT;
                if (resizeContainer.resizers.get() == 1) {
                    break;
                }
            } else {
                Node e = (Node) o;
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

    @Override
    void reverseTransfer(AtomicReferenceArray<Object> src, ResizeContainer resizeContainer) {
        AtomicReferenceArray<Object> dest = resizeContainer.nextArray;
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
                        Node e = (Node) o;
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

    private void unconditionalCopy(AtomicReferenceArray<Object> dest, Node toCopyNode) {
        int hash = this.hash(Long.hashCode(toCopyNode.value));
        AtomicReferenceArray<Object> currentArray = dest;
        while (true) {
            int length = currentArray.length();
            int index = indexFor(hash, length);
            Object o = currentArray.get(index);
            if (o == RESIZED || o == RESIZING) {
                currentArray = ((ResizeContainer) currentArray.get(length - 1)).nextArray;
            } else {
                Node newNode;
                if (o == null) {
                    if (toCopyNode.getNext() == null) {
                        newNode = toCopyNode; // no need to duplicate
                    } else {
                        newNode = new Node(toCopyNode.value);
                    }
                } else {
                    newNode = new Node(toCopyNode.value, (Node) o);
                }
                if (currentArray.compareAndSet(index, o, newNode)) {
                    return;
                }
            }
        }
    }

    public boolean contains(long value) {
        int hash = this.hash(value);
        AtomicReferenceArray<Object> currentArray = this.table;
        while (true) {
            int length = currentArray.length();
            int index = indexFor(hash, length);
            Object o = currentArray.get(index);
            if (o == RESIZED || o == RESIZING) {
                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
            } else {
                Node e = (Node) o;
                while (e != null) {
                    long candidate = e.value;
                    if (candidate == value) {
                        return true;
                    }
                    e = e.getNext();
                }
                return false;
            }
        }
    }

    public void clear() {
        AtomicReferenceArray<Object> currentArray = this.table;
        ResizeContainer resizeContainer;
        do {
            resizeContainer = null;
            for (int i = 0; i < currentArray.length() - 1; i++) {
                Object o = currentArray.get(i);
                if (o == RESIZED || o == RESIZING) {
                    resizeContainer = (ResizeContainer) currentArray.get(currentArray.length() - 1);
                } else if (o != null) {
                    Node e = (Node) o;
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

    public boolean remove(long value) {
        int hash = this.hash(Long.hashCode(value));
        AtomicReferenceArray<Object> currentArray = this.table;
        int length = currentArray.length();
        int index = indexFor(hash, length);
        Object o = currentArray.get(index);
        if (o == RESIZED || o == RESIZING) {
            return this.slowRemove(value, hash, currentArray);
        }
        Node e = (Node) o;
        while (e != null) {
            long candidate = e.value;
            if (candidate == value) {
                Node replacement = this.createReplacementChainForRemoval((Node) o, e);
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

    private boolean slowRemove(long value, int hash, AtomicReferenceArray<Object> currentArray) {
        //noinspection LabeledStatement
        outer:
        while (true) {
            int length = currentArray.length();
            int index = indexFor(hash, length);
            Object o = currentArray.get(index);
            if (o == RESIZED || o == RESIZING) {
                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
            } else {
                Node e = (Node) o;
                while (e != null) {
                    long candidate = e.value;
                    if (candidate == value) {
                        Node replacement = this.createReplacementChainForRemoval((Node) o, e);
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

    private Node createReplacementChainForRemoval(Node original, Node toRemove) {
        if (original == toRemove) {
            return original.getNext();
        }
        Node replacement = null;
        Node e = original;
        while (e != null) {
            if (e != toRemove) {
                replacement = new Node(e.value, replacement);
            }
            e = e.getNext();
        }
        return replacement;
    }

    @Override
    public int hashCode() {
        int h = 0;
        AtomicReferenceArray<Object> currentArray = this.table;
        for (int i = 0; i < currentArray.length() - 1; i++) {
            Object o = currentArray.get(i);
            if (o == RESIZED || o == RESIZING) {
                throw new ConcurrentModificationException("can't compute hashcode while resizing!");
            }
            Node e = (Node) o;
            while (e != null) {
                long value = e.value;
                h += Long.hashCode(value);
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

        if (!(o instanceof HeapTrackingConcurrentLongHashSet s)) {
            return false;
        }
        if (s.size() != this.size()) {
            return false;
        }
        var iterator = this.iterator();
        while (iterator.hasNext()) {
            var e = iterator.next();
            if (!s.contains(e)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void close() {
        memoryTracker.releaseHeap(SHALLOW_SIZE_THIS);
        releaseHeap();
    }

    public LongIterator iterator() {
        return new LongHashSetIterator();
    }

    private class LongHashSetIterator extends HashIterator<Node> implements LongIterator {
        @Override
        public long next() {
            Node e = this.next;
            if (e == null) {
                throw new NoSuchElementException();
            }

            if ((this.next = e.getNext()) == null) {
                this.findNext();
            }
            return e.value;
        }
    }

    private static final class Node implements Wrapper<Node> {
        private final long value;
        private final Node next;

        private Node(long value) {
            this.value = value;
            this.next = null;
        }

        private Node(long value, Node next) {
            this.value = value;
            this.next = next;
        }

        @Override
        public Node getNext() {
            return this.next;
        }
    }
}
