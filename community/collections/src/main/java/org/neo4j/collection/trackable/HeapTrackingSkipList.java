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

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Random;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.memory.Measurable;
import org.neo4j.memory.MemoryTracker;

/**
 * A sorted set with average O(log n) insertion time and no copying/re-balancing overhead.
 *
 * <a href="https://en.wikipedia.org/wiki/Skip_list">Wikipedia: Skip List</a>
 */
public class HeapTrackingSkipList<T> implements Iterable<T>, AutoCloseable {

    private static class Node<T> implements Measurable {
        public final T value;
        public final Node<T>[] next;

        Node(T value, int size) {
            this(value, Node.array(size));
        }

        Node(T value, Node<T>[] next) {
            this.value = value;
            this.next = next;
        }

        @Override
        public String toString() {
            var s = new StringBuilder();
            if (value != null) {
                s.append(value);
            }

            s.append('[');
            for (int i = 0; i < next.length; i++) {
                s.append(i).append(':');
                if (next[i] != null) {
                    s.append(next[i].value);
                } else {
                    s.append("null");
                }
                s.append(' ');
            }
            s.append(']');
            return s.toString();
        }

        public static long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(Node.class);

        @Override
        public long estimatedHeapUsage() {
            return SHALLOW_SIZE + HeapEstimator.shallowSizeOf(next);
        }

        // yay java
        public static <T> Node<T>[] array(int size) {
            //noinspection unchecked
            return (Node<T>[]) Array.newInstance(Node.class, size);
        }
    }

    private Node<T>[] nodeArray(int size) {
        return Node.array(size);
    }

    private static final int MAX_LEVEL = 32;

    private static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(HeapTrackingSkipList.class)
            + HeapEstimator.shallowSizeOfInstance(Random.class);

    private final MemoryTracker memoryTracker;
    private final Comparator<T> comparator;
    private final Node<T> head;
    private final Random random = new Random();
    private int levels = 1;

    public HeapTrackingSkipList(MemoryTracker memoryTracker, Comparator<T> comparator) {
        this.memoryTracker = memoryTracker.getScopedMemoryTracker();
        this.comparator = comparator;
        this.head = new Node<>(null, MAX_LEVEL + 1);
        this.memoryTracker.allocateHeap(SHALLOW_SIZE + head.estimatedHeapUsage());
    }

    /**
     * Chooses the level of a new element probabilistically. By counting trailing zeroes we get a geometric
     * distribution Geo(0.5).
     *
     * The value is passed so that overriding subclasses can use it, eg for deterministic testing
     */
    protected int getLevel(T value) {
        int level = 0;
        for (int r = random.nextInt(); (r & 1) == 1; r >>= 1) {
            if (++level == levels) {
                levels++;
                break;
            }
        }
        return level;
    }

    /** Inserts a value into the list; duplicates are ignored. Returns true if the value was added. */
    public boolean insert(T value) {
        int level = getLevel(value);

        var current = head;

        var prevStack = nodeArray(level + 1);
        for (int i = levels - 1; i >= 0; i--) {
            for (; current.next[i] != null; current = current.next[i]) {
                int cmp = comparator.compare(current.next[i].value, value);

                if (cmp > 0) {
                    break;
                }

                if (cmp == 0) {
                    return false;
                }
            }

            if (i <= level) {
                prevStack[i] = current;
            }
        }

        var newNode = new Node<>(value, level + 1);
        memoryTracker.allocateHeap(newNode.estimatedHeapUsage());

        for (int i = 0; i <= level; i++) {
            newNode.next[i] = prevStack[i].next[i];
            prevStack[i].next[i] = newNode;
        }

        return true;
    }

    /** Removes and returns the smallest element from the collection */
    public T pop() {
        var popped = head.next[0];
        if (popped == null) {
            return null;
        }

        for (int i = levels - 1; i >= 0; i--) {
            if (head.next[i] == popped) {
                head.next[i] = popped.next[i];
                assert head.next[i] != null || head.next[i + 1] == null;
            }
        }

        memoryTracker.releaseHeap(popped.estimatedHeapUsage());

        return popped.value;
    }

    /** Returns the smallest element from the collection */
    public T peek() {
        var node = head.next[0];
        if (node == null) {
            return null;
        }

        return node.value;
    }

    /** Iterates the collection in ascending order */
    @Override
    public Iterator<T> iterator() {
        return new Iterator<>() {
            private Node<T> current = head;

            @Override
            public boolean hasNext() {
                return current.next[0] != null;
            }

            @Override
            public T next() {
                current = current.next[0];
                return current.value;
            }
        };
    }

    @Override
    public String toString() {
        var sb = new StringBuilder("{");
        for (T n : this) {
            sb.append(n).append(',');
        }
        sb.append("}");
        return sb.toString();
    }

    public boolean isEmpty() {
        return head.next[0] == null;
    }

    @Override
    public void close() {
        Arrays.fill(this.head.next, null);
        this.memoryTracker.close();
    }
}
