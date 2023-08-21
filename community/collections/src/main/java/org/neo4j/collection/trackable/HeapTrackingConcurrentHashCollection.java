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

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.neo4j.memory.MemoryTracker;

@SuppressWarnings({"unchecked"})
abstract class HeapTrackingConcurrentHashCollection<E> extends AbstractHeapTrackingConcurrentHash
        implements AutoCloseable {
    private static final long SHALLOW_SIZE_THIS = shallowSizeOfInstance(HeapTrackingConcurrentHashCollection.class);
    static final long SHALLOW_SIZE_WRAPPER = shallowSizeOfInstance(Node.class);

    HeapTrackingConcurrentHashCollection(MemoryTracker memoryTracker, int initialCapacity) {
        super(memoryTracker, initialCapacity);
    }

    @Override
    public final long sizeOfWrapperObject() {
        return SHALLOW_SIZE_WRAPPER;
    }

    @Override
    final void transfer(AtomicReferenceArray<Object> src, ResizeContainer resizeContainer) {
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

    @Override
    final void reverseTransfer(AtomicReferenceArray<Object> src, ResizeContainer resizeContainer) {
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

    private void unconditionalCopy(AtomicReferenceArray<Object> dest, Node<E> toCopyNode) {
        int hash = this.hash(toCopyNode.value);
        AtomicReferenceArray<Object> currentArray = dest;
        while (true) {
            int length = currentArray.length();
            int index = indexFor(hash, length);
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

    @Override
    public final void close() {
        memoryTracker.releaseHeap(SHALLOW_SIZE_THIS);
        releaseHeap();
    }

    public final Iterator<E> iterator() {
        return new HashSetIterator<>();
    }

    private final class HashSetIterator<T> extends HashIterator<Node<T>> implements Iterator<T> {
        @Override
        public T next() {
            Node<T> e = this.next;
            if (e == null) {
                throw new NoSuchElementException();
            }

            if ((this.next = e.getNext()) == null) {
                this.findNext();
            }
            return e.value;
        }
    }

    static final class Node<T> implements Wrapper<Node<T>> {
        final T value;
        final Node<T> next;

        Node(T value) {
            this.value = value;
            this.next = null;
        }

        Node(T value, Node<T> next) {
            this.value = value;
            this.next = next;
        }

        @Override
        public Node<T> getNext() {
            return this.next;
        }
    }
}
