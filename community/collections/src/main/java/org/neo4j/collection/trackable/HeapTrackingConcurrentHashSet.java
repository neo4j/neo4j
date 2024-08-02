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

import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.neo4j.memory.MemoryTracker;

@SuppressWarnings({"unchecked", "NullableProblems"})
public final class HeapTrackingConcurrentHashSet<E> extends HeapTrackingConcurrentHashCollection<E>
        implements Set<E>, AutoCloseable {
    private static final long SHALLOW_SIZE_THIS = shallowSizeOfInstance(HeapTrackingConcurrentHashSet.class);

    public static <E> HeapTrackingConcurrentHashSet<E> newSet(MemoryTracker memoryTracker) {
        return newSet(memoryTracker, DEFAULT_INITIAL_CAPACITY);
    }

    public static <E> HeapTrackingConcurrentHashSet<E> newSet(MemoryTracker memoryTracker, int size) {
        memoryTracker.allocateHeap(SHALLOW_SIZE_THIS);
        return new HeapTrackingConcurrentHashSet<>(memoryTracker, size);
    }

    private HeapTrackingConcurrentHashSet(MemoryTracker memoryTracker, int initialCapacity) {
        super(memoryTracker, initialCapacity);
    }

    @Override
    public boolean add(E value) {
        int hash = this.hash(value);
        AtomicReferenceArray<Object> currentArray = this.table;
        int length = currentArray.length();
        int index = indexFor(hash, length);
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

    private boolean slowAdd(E value, int hash, AtomicReferenceArray<Object> currentArray) {
        while (true) {
            int length = currentArray.length();
            int index = indexFor(hash, length);
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
    public Object[] toArray() {
        final var arrayList = new ArrayList<>(size());
        for (final var e : this) arrayList.add(e);
        return arrayList.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        ArrayList<T> arrayList = new ArrayList<>(size());
        for (E e : this) {
            arrayList.add((T) e);
        }
        return arrayList.toArray(a);
    }

    @Override
    public boolean contains(Object value) {
        int hash = this.hash(value);
        AtomicReferenceArray<Object> currentArray = this.table;
        while (true) {
            int length = currentArray.length();
            int index = indexFor(hash, length);
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
        AtomicReferenceArray<Object> currentArray = this.table;
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
        AtomicReferenceArray<Object> currentArray = this.table;
        int length = currentArray.length();
        int index = indexFor(hash, length);
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

    @Override
    public boolean containsAll(Collection<?> c) {
        for (Object e : c) if (!contains(e)) return false;
        return true;
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        boolean modified = false;
        for (E e : c) if (add(e)) modified = true;
        return modified;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        Objects.requireNonNull(c);
        var modified = false;
        for (final var e : this) if (!c.contains(e) && remove(e)) modified = true;
        return modified;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        var modified = false;
        for (final var e : c) if (remove(e)) modified = true;
        return modified;
    }

    private boolean slowRemove(Object value, int hash, AtomicReferenceArray<Object> currentArray) {
        //noinspection LabeledStatement
        outer:
        while (true) {
            int length = currentArray.length();
            int index = indexFor(hash, length);
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
        AtomicReferenceArray<Object> currentArray = this.table;
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
}
