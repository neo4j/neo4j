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
package org.neo4j.kernel.impl.util.collection;

import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.neo4j.memory.MemoryTracker;

/**
 * A specialized concurrent collection used in parallel runtime.
 * <p>
 * The collection exposes only two methods, {@link #add(Object)} and {@link #iterator()}, it doesn't keep track of its size nor does it automatically track memory.
 * However, memory tracking can easily be added by the client using {@link #SIZE_OF_NODE} to compute the size of each added item. The imagined use-case for
 * this collection, and what it has been optimised for, is when you have multiple threads adding to the collection and then a single reader reading the accumulated
 * result. The implementation is based on the LockFreeQueue in "The ART of MULTIPROCESSOR PROGRAMMING" (Herlihy, Luchangco, Shavi & Spear) and it is based
 * around having quicker threads help slower threads
 * <p>
 * Other approaches have been tested but all with inferior performance, including
 * <list>
 *     <li>java.util.concurrent.ConcurrentLinkedQueue</li>
 *     <li>A bag implementation based on a HeapTrackingConcurrentHashSet with referential equality</li>
 *     <li>A bag implementation using ThreadLocal<HeapTrackingArrayList> so that each thread adds to a local list.</li>
 * </list>
 * @param <T> the type of the values to add.
 */
public final class ConcurrentBag<T> {

    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<ConcurrentBag, Node> UPDATE_TAIL =
            AtomicReferenceFieldUpdater.newUpdater(ConcurrentBag.class, Node.class, "tail");

    private static final AtomicReferenceFieldUpdater<Node, Node> UPDATE_NODE =
            AtomicReferenceFieldUpdater.newUpdater(Node.class, Node.class, "next");
    public static final long SIZE_OF_NODE = shallowSizeOfInstance(Node.class);
    static final long SIZE_OF_BAG = shallowSizeOfInstance(ConcurrentBag.class) + SIZE_OF_NODE;

    private final Node head;
    private volatile Node tail;

    /**
     * Allocate a new ConcurrentBag
     */
    public static <T> ConcurrentBag<T> newBag(MemoryTracker memoryTracker) {
        memoryTracker.allocateHeap(SIZE_OF_BAG);
        return new ConcurrentBag<>();
    }

    public ConcurrentBag() {
        Node node = new Node(null);
        this.head = node;
        this.tail = node;
    }

    public void add(T value) {
        Node node = new Node(value);
        while (true) {
            Node last = tail;
            Node next = last.next;
            if (last == tail) {
                if (next == null) {
                    if (UPDATE_NODE.compareAndSet(last, null, node)) {
                        // NOTE: if this CAS operation fails we can still return successfully since it means
                        // another thread "helped out" (see NOTE below).
                        UPDATE_TAIL.compareAndSet(this, last, node);
                        return;
                    }
                } else {
                    // NOTE: some other thread appended its node but has not yet updated tail,
                    // let's be helpful and set it for them.
                    UPDATE_TAIL.compareAndSet(this, last, next);
                }
            }
        }
    }

    public Iterator<T> iterator() {
        return new Iterator<>() {
            private Node current = head;

            @Override
            public boolean hasNext() {
                return current.next != null;
            }

            @SuppressWarnings("unchecked")
            @Override
            public T next() {
                current = current.next;
                return (T) current.value;
            }
        };
    }

    static class Node {
        private final Object value;
        volatile Node next;

        Node(Object value) {
            this.value = value;
            this.next = null;
        }

        boolean compareAndSetNext(Node newNode) {
            return UPDATE_NODE.compareAndSet(this, null, newNode);
        }
    }
}
