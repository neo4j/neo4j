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

import static org.neo4j.internal.helpers.ArrayUtil.MAX_ARRAY_SIZE;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfObjectArray;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.neo4j.memory.MemoryTracker;

/**
 * A heap tracking array deque. It only tracks the internal structure, not the elements within.
 * <p>
 * This is mostly a copy of {@link ArrayDeque} to expose the {@link #grow(int)} method.
 *
 * @param <E> element type
 */
public class HeapTrackingArrayDeque<E> implements Deque<E>, AutoCloseable {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(HeapTrackingArrayDeque.class);
    private static final int DEFAULT_SIZE = 16;

    private final MemoryTracker memoryTracker;

    private long trackedSize;
    private Object[] elements;
    private int head;
    private int tail;

    public static <T> HeapTrackingArrayDeque<T> newArrayDeque(MemoryTracker memoryTracker) {
        return newArrayDeque(DEFAULT_SIZE, memoryTracker);
    }

    public static <T> HeapTrackingArrayDeque<T> newArrayDeque(int numElements, MemoryTracker memoryTracker) {
        int actualNumElements =
                (numElements < 1) ? 1 : (numElements == Integer.MAX_VALUE) ? Integer.MAX_VALUE : numElements + 1;
        long trackedSize = shallowSizeOfObjectArray(actualNumElements);
        memoryTracker.allocateHeap(SHALLOW_SIZE + trackedSize);
        return new HeapTrackingArrayDeque<>(actualNumElements, memoryTracker, trackedSize);
    }

    private HeapTrackingArrayDeque(int numElements, MemoryTracker memoryTracker, long trackedSize) {
        elements = new Object[numElements];
        this.memoryTracker = memoryTracker;
        this.trackedSize = trackedSize;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for (Object e : c) {
            if (!contains(e)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void addFirst(E e) {
        if (e == null) {
            throw new NullPointerException();
        }
        final Object[] es = elements;
        es[head = dec(head, es.length)] = e;
        if (head == tail) {
            grow(1);
        }
    }

    @Override
    public void addLast(E e) {
        if (e == null) {
            throw new NullPointerException();
        }
        final Object[] es = elements;
        es[tail] = e;
        if (head == (tail = inc(tail, es.length))) {
            grow(1);
        }
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        final int s, needed;
        if ((needed = (s = size()) + c.size() + 1 - elements.length) > 0) {
            grow(needed);
        }
        copyElements(c);
        return size() > s;
    }

    @Override
    public boolean offerFirst(E e) {
        addFirst(e);
        return true;
    }

    @Override
    public boolean offerLast(E e) {
        addLast(e);
        return true;
    }

    @Override
    public E removeFirst() {
        E e = pollFirst();
        if (e == null) {
            throw new NoSuchElementException();
        }
        return e;
    }

    @Override
    public E removeLast() {
        E e = pollLast();
        if (e == null) {
            throw new NoSuchElementException();
        }
        return e;
    }

    @Override
    public E pollFirst() {
        final Object[] es;
        final int h;
        E e = elementAt(es = elements, h = head);
        if (e != null) {
            es[h] = null;
            head = inc(h, es.length);
        }
        return e;
    }

    @Override
    public E pollLast() {
        final Object[] es;
        final int t;
        E e = elementAt(es = elements, t = dec(tail, es.length));
        if (e != null) {
            es[tail = t] = null;
        }
        return e;
    }

    @Override
    public E getFirst() {
        E e = elementAt(elements, head);
        if (e == null) {
            throw new NoSuchElementException();
        }
        return e;
    }

    @Override
    public E getLast() {
        final Object[] es = elements;
        E e = elementAt(es, dec(tail, es.length));
        if (e == null) {
            throw new NoSuchElementException();
        }
        return e;
    }

    @Override
    public E peekFirst() {
        return elementAt(elements, head);
    }

    @Override
    public E peekLast() {
        final Object[] es;
        return elementAt(es = elements, dec(tail, es.length));
    }

    @Override
    public boolean removeFirstOccurrence(Object o) {
        if (o != null) {
            final Object[] es = elements;
            for (int i = head, end = tail, to = (i <= end) ? end : es.length; ; i = 0, to = end) {
                for (; i < to; i++) {
                    if (o.equals(es[i])) {
                        delete(i);
                        return true;
                    }
                }
                if (to == end) {
                    break;
                }
            }
        }
        return false;
    }

    @Override
    public boolean removeLastOccurrence(Object o) {
        if (o != null) {
            final Object[] es = elements;
            for (int i = tail, end = head, to = (i >= end) ? end : 0; ; i = es.length, to = end) {
                for (i--; i > to - 1; i--) {
                    if (o.equals(es[i])) {
                        delete(i);
                        return true;
                    }
                }
                if (to == end) {
                    break;
                }
            }
        }
        return false;
    }

    @Override
    public boolean add(E e) {
        addLast(e);
        return true;
    }

    @Override
    public boolean offer(E e) {
        return offerLast(e);
    }

    @Override
    public E remove() {
        return removeFirst();
    }

    @Override
    public E poll() {
        return pollFirst();
    }

    @Override
    public E element() {
        return getFirst();
    }

    @Override
    public E peek() {
        return peekFirst();
    }

    @Override
    public void push(E e) {
        addFirst(e);
    }

    @Override
    public E pop() {
        return removeFirst();
    }

    @Override
    public int size() {
        return sub(tail, head, elements.length);
    }

    @Override
    public boolean isEmpty() {
        return head == tail;
    }

    @Override
    public Iterator<E> iterator() {
        return new DeqIterator();
    }

    @Override
    public Iterator<E> descendingIterator() {
        return new DescendingIterator();
    }

    @Override
    public void forEach(Consumer<? super E> action) {
        Objects.requireNonNull(action);
        final Object[] es = elements;
        for (int i = head, end = tail, to = (i <= end) ? end : es.length; ; i = 0, to = end) {
            for (; i < to; i++) {
                action.accept(elementAt(es, i));
            }
            if (to == end) {
                if (end != tail) {
                    throw new ConcurrentModificationException();
                }
                break;
            }
        }
    }

    @Override
    public boolean removeIf(Predicate<? super E> filter) {
        Objects.requireNonNull(filter);
        return bulkRemove(filter);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        Objects.requireNonNull(c);
        return bulkRemove(c::contains);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        Objects.requireNonNull(c);
        return bulkRemove(e -> !c.contains(e));
    }

    static int inc(int i, int modulus) {
        if (++i >= modulus) {
            i = 0;
        }
        return i;
    }

    static int dec(int i, int modulus) {
        if (--i < 0) {
            i = modulus - 1;
        }
        return i;
    }

    static int sub(int i, int j, int modulus) {
        if ((i -= j) < 0) {
            i += modulus;
        }
        return i;
    }

    @SuppressWarnings("unchecked")
    static <E> E elementAt(Object[] es, int i) {
        return (E) es[i];
    }

    static <E> E nonNullElementAt(Object[] es, int i) {
        @SuppressWarnings("unchecked")
        E e = (E) es[i];
        if (e == null) {
            throw new ConcurrentModificationException();
        }
        return e;
    }

    boolean delete(int i) {
        final Object[] es = elements;
        final int capacity = es.length;
        final int h, t;
        // number of elements before to-be-deleted elt
        final int front = sub(i, h = head, capacity);
        // number of elements after to-be-deleted elt
        final int back = sub(t = tail, i, capacity) - 1;
        if (front < back) {
            // move front elements forwards
            if (h <= i) {
                System.arraycopy(es, h, es, h + 1, front);
            } else { // Wrap around
                System.arraycopy(es, 0, es, 1, i);
                es[0] = es[capacity - 1];
                System.arraycopy(es, h, es, h + 1, front - (i + 1));
            }
            es[h] = null;
            head = inc(h, capacity);
            return false;
        } else {
            // move back elements backwards
            tail = dec(t, capacity);
            if (i <= tail) {
                System.arraycopy(es, i + 1, es, i, back);
            } else { // Wrap around
                System.arraycopy(es, i + 1, es, i, capacity - (i + 1));
                es[capacity - 1] = es[0];
                System.arraycopy(es, 1, es, 0, t - 1);
            }
            es[tail] = null;
            return true;
        }
    }

    private void copyElements(Collection<? extends E> c) {
        c.forEach(this::addLast);
    }

    private boolean bulkRemove(Predicate<? super E> filter) {
        final Object[] es = elements;
        // Optimize for initial run of survivors
        for (int i = head, end = tail, to = (i <= end) ? end : es.length; ; i = 0, to = end) {
            for (; i < to; i++) {
                if (filter.test(elementAt(es, i))) {
                    return bulkRemoveModified(filter, i);
                }
            }
            if (to == end) {
                if (end != tail) {
                    throw new ConcurrentModificationException();
                }
                break;
            }
        }
        return false;
    }

    @Override
    public boolean contains(Object o) {
        if (o != null) {
            final Object[] es = elements;
            for (int i = head, end = tail, to = (i <= end) ? end : es.length; ; i = 0, to = end) {
                for (; i < to; i++) {
                    if (o.equals(es[i])) {
                        return true;
                    }
                }
                if (to == end) {
                    break;
                }
            }
        }
        return false;
    }

    @Override
    public boolean remove(Object o) {
        return removeFirstOccurrence(o);
    }

    @Override
    public void clear() {
        circularClear(elements, head, tail);
        head = tail = 0;
    }

    @Override
    public Object[] toArray() {
        return toArray(Object[].class);
    }

    private <T> T[] toArray(Class<T[]> klazz) {
        final Object[] es = elements;
        final T[] a;
        final int head = this.head, tail = this.tail, end;
        if ((end = tail + ((head <= tail) ? 0 : es.length)) >= 0) {
            // Uses null extension feature of copyOfRange
            a = Arrays.copyOfRange(es, head, end, klazz);
        } else {
            // integer overflow!
            a = Arrays.copyOfRange(es, 0, end - head, klazz);
            System.arraycopy(es, head, a, 0, es.length - head);
        }
        if (end != tail) {
            System.arraycopy(es, 0, a, es.length - head, tail);
        }
        return a;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T[] toArray(T[] a) {
        final int size;
        if ((size = size()) > a.length) {
            return toArray((Class<T[]>) a.getClass());
        }
        final Object[] es = elements;
        for (int i = head, j = 0, len = Math.min(size, es.length - i); ; i = 0, len = tail) {
            System.arraycopy(es, i, a, j, len);
            if ((j += len) == size) {
                break;
            }
        }
        if (size < a.length) {
            a[size] = null;
        }
        return a;
    }

    @Override
    public void close() {
        if (elements != null) {
            memoryTracker.releaseHeap(trackedSize + SHALLOW_SIZE);
            elements = null;
        }
    }

    public Iterator<E> autoClosingIterator() {
        return new DeqIterator() {
            @Override
            void done() {
                close();
            }
        };
    }

    /**
     * Grow and report size change to tracker
     */
    private void grow(int needed) {
        // overflow-conscious code
        final int oldCapacity = elements.length;
        int newCapacity;
        // Double capacity if small; else grow by 50%
        int jump = (oldCapacity < 64) ? (oldCapacity + 2) : (oldCapacity >> 1);
        if (jump < needed || (newCapacity = oldCapacity + jump) - MAX_ARRAY_SIZE > 0) {
            newCapacity = newCapacity(needed, jump);
        }
        long oldHeapUsage = trackedSize;
        trackedSize = shallowSizeOfObjectArray(newCapacity);
        memoryTracker.allocateHeap(trackedSize);
        final Object[] es = elements = Arrays.copyOf(elements, newCapacity);
        memoryTracker.releaseHeap(oldHeapUsage);
        // Exceptionally, here tail == head needs to be disambiguated
        if (tail < head || (tail == head && es[head] != null)) {
            // wrap around; slide first leg forward to end of array
            int newSpace = newCapacity - oldCapacity;
            System.arraycopy(es, head, es, head + newSpace, oldCapacity - head);
            for (int i = head, to = head += newSpace; i < to; i++) {
                es[i] = null;
            }
        }
    }

    private int newCapacity(int needed, int jump) {
        final int oldCapacity = elements.length, minCapacity;
        if ((minCapacity = oldCapacity + needed) - MAX_ARRAY_SIZE > 0) {
            if (minCapacity < 0) {
                throw new IllegalStateException("Sorry, deque too big");
            }
            return Integer.MAX_VALUE;
        }
        if (needed > jump) {
            return minCapacity;
        }
        return (oldCapacity + jump - MAX_ARRAY_SIZE < 0) ? oldCapacity + jump : MAX_ARRAY_SIZE;
    }

    private class DeqIterator implements Iterator<E> {
        /**
         * Index of element to be returned by subsequent call to next.
         */
        int cursor;

        /**
         * Number of elements yet to be returned.
         */
        int remaining = size();

        /**
         * Index of element returned by most recent call to next. Reset to -1 if element is deleted by a call to remove.
         */
        int lastRet = -1;

        DeqIterator() {
            cursor = head;
        }

        void done() {
            // extension point
        }

        @Override
        public final boolean hasNext() {
            boolean hasNext = remaining > 0;
            if (!hasNext) {
                done();
            }
            return hasNext;
        }

        @Override
        public E next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            final Object[] es = elements;
            E e = nonNullElementAt(es, cursor);
            cursor = inc(lastRet = cursor, es.length);
            remaining--;
            return e;
        }

        void postDelete(boolean leftShifted) {
            if (leftShifted) {
                cursor = dec(cursor, elements.length);
            }
        }

        @Override
        public final void remove() {
            if (lastRet < 0) {
                throw new IllegalStateException();
            }
            postDelete(delete(lastRet));
            lastRet = -1;
        }

        @Override
        public void forEachRemaining(Consumer<? super E> action) {
            Objects.requireNonNull(action);
            int r;
            if ((r = remaining) <= 0) {
                return;
            }
            remaining = 0;
            final Object[] es = elements;
            if (es[cursor] == null || sub(tail, cursor, es.length) != r) {
                throw new ConcurrentModificationException();
            }
            for (int i = cursor, end = tail, to = (i <= end) ? end : es.length; ; i = 0, to = end) {
                for (; i < to; i++) {
                    action.accept(elementAt(es, i));
                }
                if (to == end) {
                    if (end != tail) {
                        throw new ConcurrentModificationException();
                    }
                    lastRet = dec(end, es.length);
                    break;
                }
            }
        }
    }

    private class DescendingIterator extends DeqIterator {
        DescendingIterator() {
            cursor = dec(tail, elements.length);
        }

        @Override
        public final E next() {
            if (remaining <= 0) {
                throw new NoSuchElementException();
            }
            final Object[] es = elements;
            E e = nonNullElementAt(es, cursor);
            cursor = dec(lastRet = cursor, es.length);
            remaining--;
            return e;
        }

        @Override
        void postDelete(boolean leftShifted) {
            if (!leftShifted) {
                cursor = inc(cursor, elements.length);
            }
        }

        @Override
        public final void forEachRemaining(Consumer<? super E> action) {
            Objects.requireNonNull(action);
            int r;
            if ((r = remaining) <= 0) {
                return;
            }
            remaining = 0;
            final Object[] es = elements;
            if (es[cursor] == null || sub(cursor, head, es.length) + 1 != r) {
                throw new ConcurrentModificationException();
            }
            for (int i = cursor, end = head, to = (i >= end) ? end : 0; ; i = es.length - 1, to = end) {
                // hotspot generates faster code than for: i >= to !
                for (; i > to - 1; i--) {
                    action.accept(elementAt(es, i));
                }
                if (to == end) {
                    if (end != head) {
                        throw new ConcurrentModificationException();
                    }
                    lastRet = end;
                    break;
                }
            }
        }
    }

    private static void circularClear(Object[] es, int i, int end) {
        for (int to = (i <= end) ? end : es.length; ; i = 0, to = end) {
            for (; i < to; i++) {
                es[i] = null;
            }
            if (to == end) {
                break;
            }
        }
    }

    // A tiny bit set implementation

    private static long[] nBits(int n) {
        return new long[((n - 1) >> 6) + 1];
    }

    private static void setBit(long[] bits, int i) {
        bits[i >> 6] |= 1L << i;
    }

    private static boolean isClear(long[] bits, int i) {
        return (bits[i >> 6] & (1L << i)) == 0;
    }

    private boolean bulkRemoveModified(Predicate<? super E> filter, final int beg) {
        final Object[] es = elements;
        final int capacity = es.length;
        final int end = tail;
        final long[] deathRow = nBits(sub(end, beg, capacity));
        deathRow[0] = 1L; // set bit 0
        for (int i = beg + 1, to = (i <= end) ? end : es.length, k = beg; ; i = 0, to = end, k -= capacity) {
            for (; i < to; i++) {
                if (filter.test(elementAt(es, i))) {
                    setBit(deathRow, i - k);
                }
            }
            if (to == end) {
                break;
            }
        }
        // a two-finger traversal, with hare i reading, tortoise w writing
        int w = beg;
        for (int i = beg + 1, to = (i <= end) ? end : es.length, k = beg; ; w = 0) { // w rejoins i on second leg
            // In this loop, i and w are on the same leg, with i > w
            for (; i < to; i++) {
                if (isClear(deathRow, i - k)) {
                    es[w++] = es[i];
                }
            }
            if (to == end) {
                break;
            }
            // In this loop, w is on the first leg, i on the second
            for (i = 0, to = end, k -= capacity; i < to && w < capacity; i++) {
                if (isClear(deathRow, i - k)) {
                    es[w++] = es[i];
                }
            }
            if (i >= to) {
                if (w == capacity) {
                    w = 0; // "corner" case
                }
                break;
            }
        }
        if (end != tail) {
            throw new ConcurrentModificationException();
        }
        circularClear(es, tail = w, end);
        return true;
    }
}
