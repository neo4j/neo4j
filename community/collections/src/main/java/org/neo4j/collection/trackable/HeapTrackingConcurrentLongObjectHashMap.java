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
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongFunction;
import org.eclipse.collections.api.block.procedure.primitive.LongObjectProcedure;
import org.eclipse.collections.api.iterator.LongIterator;
import org.neo4j.memory.MemoryTracker;

/**
 * Specialization of HeapTrackingConcurrentHashMap that use long keys.
 * <p>
 * Implementation is based on org.eclipse.collections.impl.map.mutable.ConcurrentHashMap but adapted for using long keys.
 * @param <V> the value type.
 */
@SuppressWarnings({"unchecked"})
public final class HeapTrackingConcurrentLongObjectHashMap<V> extends AbstractHeapTrackingConcurrentHash
        implements AutoCloseable {

    private static final long SHALLOW_SIZE_THIS = shallowSizeOfInstance(HeapTrackingConcurrentLongObjectHashMap.class);
    private static final long SHALLOW_SIZE_WRAPPER = shallowSizeOfInstance(Entry.class);

    public static <V> HeapTrackingConcurrentLongObjectHashMap<V> newMap(MemoryTracker memoryTracker) {
        return newMap(memoryTracker, DEFAULT_INITIAL_CAPACITY);
    }

    public static <V> HeapTrackingConcurrentLongObjectHashMap<V> newMap(MemoryTracker memoryTracker, int size) {
        memoryTracker.allocateHeap(SHALLOW_SIZE_THIS);
        return new HeapTrackingConcurrentLongObjectHashMap<>(memoryTracker, size);
    }

    @Override
    public long sizeOfWrapperObject() {
        return SHALLOW_SIZE_WRAPPER;
    }

    private HeapTrackingConcurrentLongObjectHashMap(MemoryTracker memoryTracker, int initialCapacity) {
        super(memoryTracker, initialCapacity);
    }

    public V put(long key, V value) {
        int hash = this.hash(Long.hashCode(key));
        var currentArray = this.table;
        int length = currentArray.length();
        int index = indexFor(hash, length);
        Object o = currentArray.get(index);
        if (o == null) {
            Entry<V> newEntry = new Entry<>(key, value, null);
            addToSize(1);
            if (currentArray.compareAndSet(index, null, newEntry)) {
                return null;
            }
            addToSize(-1);
        }
        return this.slowPut(key, value, hash, currentArray);
    }

    private V slowPut(long key, V value, int hash, AtomicReferenceArray<Object> currentArray) {
        //noinspection LabeledStatement
        outer:
        while (true) {
            int length = currentArray.length();
            int index = indexFor(hash, length);
            Object o = currentArray.get(index);
            while (o == RESERVED) {
                Thread.onSpinWait();
                o = currentArray.get(index);
            }
            if (o == RESIZED || o == RESIZING) {
                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
            } else {
                Entry<V> e = (Entry<V>) o;
                while (e != null) {
                    if (e.key == key) {
                        V oldValue = e.getValue();
                        Entry<V> newEntry =
                                new Entry<>(e.key, value, this.createReplacementChainForRemoval((Entry<V>) o, e));
                        if (!currentArray.compareAndSet(index, o, newEntry)) {
                            //noinspection ContinueStatementWithLabel
                            continue outer;
                        }
                        return oldValue;
                    }
                    e = e.getNext();
                }
                Entry<V> newEntry = new Entry<>(key, value, (Entry<V>) o);
                if (currentArray.compareAndSet(index, o, newEntry)) {
                    this.incrementSizeAndPossiblyResize(currentArray, length, o);
                    return null;
                }
            }
        }
    }

    public V putIfAbsent(long key, V value) {
        int hash = this.hash(Long.hashCode(key));
        AtomicReferenceArray<Object> currentArray = this.table;
        while (true) {
            int length = currentArray.length();
            int index = indexFor(hash, length);
            Object o = currentArray.get(index);
            while (o == RESERVED) {
                Thread.onSpinWait();
                o = currentArray.get(index);
            }
            if (o == RESIZED || o == RESIZING) {
                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
            } else {
                Entry<V> e = (Entry<V>) o;
                while (e != null) {
                    if (e.key == key) {
                        return e.getValue();
                    }
                    e = e.getNext();
                }
                Entry<V> newEntry = new Entry<>(key, value, (Entry<V>) o);
                if (currentArray.compareAndSet(index, o, newEntry)) {
                    this.incrementSizeAndPossiblyResize(currentArray, length, o);
                    return null; // per the contract of putIfAbsent, we return null when the map didn't have this key
                    // before
                }
            }
        }
    }

    public V computeIfAbsent(long key, LongFunction<V> mappingFunction) {
        int hash = this.hash(Long.hashCode(key));
        AtomicReferenceArray<Object> currentArray = this.table;
        while (true) {
            int length = currentArray.length();
            int index = indexFor(hash, length);
            Object o = currentArray.get(index);
            while (o == RESERVED) {
                Thread.onSpinWait();
                o = currentArray.get(index);
            }
            if (o == RESIZED || o == RESIZING) {
                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
            } else {
                Entry<V> e = (Entry<V>) o;
                while (e != null) {
                    if (e.key == key) {
                        return e.getValue();
                    }
                    e = e.getNext();
                }
                if (currentArray.compareAndSet(index, o, RESERVED)) {
                    V newValue = mappingFunction.apply(key);
                    Entry<V> newEntry = new Entry<>(key, newValue, (Entry<V>) o);
                    currentArray.set(index, newEntry);
                    this.incrementSizeAndPossiblyResize(currentArray, length, o);
                    // per the contract of computeIfAbsent, we return the newvalue when the map
                    // didn't have this key before
                    return newValue;
                }
            }
        }
    }

    @Override
    void transfer(AtomicReferenceArray<Object> src, ResizeContainer resizeContainer) {
        AtomicReferenceArray<Object> dest = resizeContainer.nextArray;

        for (int j = 0; j < src.length() - 1; ) {
            Object o = src.get(j);
            while (o == RESERVED) {
                Thread.onSpinWait();
                o = src.get(j);
            }
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
                Entry<V> e = (Entry<V>) o;
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
        var dest = resizeContainer.nextArray;
        while (resizeContainer.getQueuePosition() > 0) {
            int start = resizeContainer.subtractAndGetQueuePosition();
            int end = start + ResizeContainer.QUEUE_INCREMENT;
            if (end > 0) {
                if (start < 0) {
                    start = 0;
                }
                for (int j = end - 1; j >= start; ) {
                    Object o = src.get(j);
                    while (o == RESERVED) {
                        Thread.onSpinWait();
                        o = src.get(j);
                    }
                    if (o == null) {
                        if (src.compareAndSet(j, null, RESIZED)) {
                            j--;
                        }
                    } else if (o == RESIZED || o == RESIZING) {
                        resizeContainer.zeroOutQueuePosition();
                        return;
                    } else {
                        Entry<V> e = (Entry<V>) o;
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

    private void unconditionalCopy(AtomicReferenceArray<Object> dest, Entry<V> toCopyEntry) {
        int hash = hash(toCopyEntry.key);
        AtomicReferenceArray<Object> currentArray = dest;
        while (true) {
            int length = currentArray.length();
            int index = indexFor(hash, length);
            Object o = currentArray.get(index);
            while (o == RESERVED) {
                Thread.onSpinWait();
                o = currentArray.get(index);
            }
            if (o == RESIZED || o == RESIZING) {
                currentArray = ((ResizeContainer) currentArray.get(length - 1)).nextArray;
            } else {
                Entry<V> newEntry;
                if (o == null) {
                    if (toCopyEntry.getNext() == null) {
                        newEntry = toCopyEntry; // no need to duplicate
                    } else {
                        newEntry = new Entry<>(toCopyEntry.key, toCopyEntry.getValue());
                    }
                } else {
                    newEntry = new Entry<>(toCopyEntry.key, toCopyEntry.getValue(), (Entry<V>) o);
                }
                if (currentArray.compareAndSet(index, o, newEntry)) {
                    return;
                }
            }
        }
    }

    public boolean remove(long key, Object value) {
        int hash = this.hash(Long.hashCode(key));
        AtomicReferenceArray<Object> currentArray = this.table;
        //noinspection LabeledStatement
        outer:
        while (true) {
            int length = currentArray.length();
            int index = indexFor(hash, length);
            Object o = currentArray.get(index);
            while (o == RESERVED) {
                Thread.onSpinWait();
                o = currentArray.get(index);
            }
            if (o == RESIZED || o == RESIZING) {
                currentArray = helpWithResizeWhileCurrentIndex(currentArray, index);
            } else {
                Entry<V> e = (Entry<V>) o;
                while (e != null) {
                    if (e.key == key && Objects.equals(e.getValue(), value)) {
                        Entry<V> replacement = this.createReplacementChainForRemoval((Entry<V>) o, e);
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

    public boolean containsKey(long key) {
        return this.getEntry(key) != null;
    }

    public boolean containsValue(Object value) {
        AtomicReferenceArray<Object> currentArray = this.table;
        ResizeContainer resizeContainer;
        do {
            resizeContainer = null;
            for (int i = 0; i < currentArray.length() - 1; i++) {
                Object o = currentArray.get(i);
                while (o == RESERVED) {
                    Thread.onSpinWait();
                    o = currentArray.get(i);
                }
                if (o == RESIZED || o == RESIZING) {
                    resizeContainer = (ResizeContainer) currentArray.get(currentArray.length() - 1);
                } else if (o != null) {
                    Entry<V> e = (Entry<V>) o;
                    while (e != null) {
                        Object v = e.getValue();
                        if (Objects.equals(v, value)) {
                            return true;
                        }
                        e = e.getNext();
                    }
                }
            }
            if (resizeContainer != null) {
                if (resizeContainer.isNotDone()) {
                    helpWithResize(currentArray);
                    resizeContainer.waitForAllResizers();
                }
                currentArray = resizeContainer.nextArray;
            }
        } while (resizeContainer != null);
        return false;
    }

    public V get(long key) {
        int hash = this.hash(Long.hashCode(key));
        AtomicReferenceArray<Object> currentArray = this.table;
        int index = indexFor(hash, currentArray.length());
        Object o = currentArray.get(index);
        while (o == RESERVED) {
            Thread.onSpinWait();
            o = currentArray.get(index);
        }
        if (o == RESIZED || o == RESIZING) {
            return this.slowGet(key, hash, currentArray);
        }
        for (Entry<V> e = (Entry<V>) o; e != null; e = e.getNext()) {
            if (e.key == key) {
                return e.value;
            }
        }
        return null;
    }

    private V slowGet(long key, int hash, AtomicReferenceArray<Object> currentArray) {
        while (true) {
            int length = currentArray.length();
            int index = indexFor(hash, length);
            Object o = currentArray.get(index);
            while (o == RESERVED) {
                Thread.onSpinWait();
                o = currentArray.get(index);
            }
            if (o == RESIZED || o == RESIZING) {
                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
            } else {
                Entry<V> e = (Entry<V>) o;
                while (e != null) {
                    if (e.key == key) {
                        return e.getValue();
                    }
                    e = e.getNext();
                }
                return null;
            }
        }
    }

    public LongIterator keys() {
        return new KeyIterator();
    }

    public Iterator<V> values() {
        return new ValueIterator();
    }

    public void forEach(LongObjectProcedure<? super V> action) {
        if (action == null) throw new NullPointerException();
        EntryIterator iterator = new EntryIterator();
        while (iterator.hasNext()) {
            Entry<V> next = iterator.next();
            action.value(next.key, next.value);
        }
    }

    public void forEachValue(Consumer<? super V> action) {
        if (action == null) throw new NullPointerException();
        var values = values();
        while (values.hasNext()) {
            action.accept(values.next());
        }
    }

    /**
     * WARNING: The map function could be called multiple times if the map is concurrently modified.
     * This is different from java.util.concurrent.ConcurrentHashMap where it is guaranteed to be called exactly once.
     */
    public V compute(long key, Function<V, ? extends V> mapFunction) {
        int hash = this.hash(Long.hashCode(key));
        var currentArray = this.table;
        int length = currentArray.length();
        int index = indexFor(hash, length);
        Object o = currentArray.get(index);
        if (o == null) {
            V mappedValue = mapFunction.apply(null);
            Entry<V> newEntry = new Entry<>(key, mappedValue, null);
            addToSize(1);
            if (currentArray.compareAndSet(index, null, newEntry)) {
                return mappedValue;
            }
            addToSize(-1);
        }
        return this.slowCompute(key, mapFunction, hash, currentArray);
    }

    private V slowCompute(
            long key, Function<V, ? extends V> mapFunction, int hash, AtomicReferenceArray<Object> currentArray) {
        //noinspection LabeledStatement
        outer:
        while (true) {
            int length = currentArray.length();
            int index = indexFor(hash, length);
            Object o = currentArray.get(index);
            while (o == RESERVED) {
                Thread.onSpinWait();
                o = currentArray.get(index);
            }
            if (o == RESIZED || o == RESIZING) {
                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
            } else {
                Entry<V> e = (Entry<V>) o;
                while (e != null) {
                    if (e.key == key) {
                        V mappedValue = mapFunction.apply(e.getValue());
                        if (mappedValue == e.getValue()) {
                            return mappedValue;
                        }
                        Entry<V> newEntry =
                                new Entry<>(e.key, mappedValue, this.createReplacementChainForRemoval((Entry<V>) o, e));
                        if (!currentArray.compareAndSet(index, o, newEntry)) {
                            //noinspection ContinueStatementWithLabel
                            continue outer;
                        }
                        return mappedValue;
                    }
                    e = e.getNext();
                }

                V mappedValue = mapFunction.apply(null);
                Entry<V> newEntry = new Entry<>(key, mappedValue, (Entry<V>) o);
                if (currentArray.compareAndSet(index, o, newEntry)) {
                    this.incrementSizeAndPossiblyResize(currentArray, length, o);
                    return mappedValue;
                }
            }
        }
    }

    private Entry<V> getEntry(long key) {
        int hash = this.hash(Long.hashCode(key));
        AtomicReferenceArray<Object> currentArray = this.table;
        while (true) {
            int length = currentArray.length();
            int index = indexFor(hash, length);
            Object o = currentArray.get(index);
            while (o == RESERVED) {
                Thread.onSpinWait();
                o = currentArray.get(index);
            }
            if (o == RESIZED || o == RESIZING) {
                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
            } else {
                Entry<V> e = (Entry<V>) o;
                while (e != null) {
                    if (e.key == key) {
                        return e;
                    }
                    e = e.getNext();
                }
                return null;
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
                while (o == RESERVED) {
                    Thread.onSpinWait();
                    o = currentArray.get(i);
                }
                if (o == RESIZED || o == RESIZING) {
                    resizeContainer = (ResizeContainer) currentArray.get(currentArray.length() - 1);
                } else if (o != null) {
                    Entry<V> e = (Entry<V>) o;
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

    public boolean replace(long key, V oldValue, V newValue) {
        int hash = this.hash(Long.hashCode(key));
        AtomicReferenceArray<Object> currentArray = this.table;
        int length = currentArray.length();
        int index = indexFor(hash, length);
        Object o = currentArray.get(index);
        while (o == RESERVED) {
            Thread.onSpinWait();
            o = currentArray.get(index);
        }
        if (o == RESIZED || o == RESIZING) {
            return this.slowReplace(key, oldValue, newValue, hash, currentArray);
        }
        Entry<V> e = (Entry<V>) o;
        while (e != null) {
            if (e.key == key) {
                if (oldValue == e.getValue() || (oldValue != null && oldValue.equals(e.getValue()))) {
                    Entry<V> replacement = this.createReplacementChainForRemoval((Entry<V>) o, e);
                    Entry<V> newEntry = new Entry<>(key, newValue, replacement);
                    return currentArray.compareAndSet(index, o, newEntry)
                            || this.slowReplace(key, oldValue, newValue, hash, currentArray);
                }
                return false;
            }
            e = e.getNext();
        }
        return false;
    }

    private boolean slowReplace(long key, V oldValue, V newValue, int hash, AtomicReferenceArray<Object> currentArray) {
        //noinspection LabeledStatement
        outer:
        while (true) {
            int length = currentArray.length();
            int index = indexFor(hash, length);
            Object o = currentArray.get(index);
            while (o == RESERVED) {
                Thread.onSpinWait();
                o = currentArray.get(index);
            }
            if (o == RESIZED || o == RESIZING) {
                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
            } else {
                Entry<V> e = (Entry<V>) o;
                while (e != null) {
                    if (e.key == key) {
                        if (oldValue == e.getValue() || (oldValue != null && oldValue.equals(e.getValue()))) {
                            Entry<V> replacement = this.createReplacementChainForRemoval((Entry<V>) o, e);
                            Entry<V> newEntry = new Entry<>(key, newValue, replacement);
                            if (currentArray.compareAndSet(index, o, newEntry)) {
                                return true;
                            }
                            //noinspection ContinueStatementWithLabel
                            continue outer;
                        }
                        return false;
                    }
                    e = e.getNext();
                }
                return false;
            }
        }
    }

    public V replace(long key, V value) {
        int hash = this.hash(Long.hashCode(key));
        AtomicReferenceArray<Object> currentArray = this.table;
        int length = currentArray.length();
        int index = indexFor(hash, length);
        Object o = currentArray.get(index);
        if (o == null) {
            return null;
        }
        return this.slowReplace(key, value, hash, currentArray);
    }

    private V slowReplace(long key, V value, int hash, AtomicReferenceArray<Object> currentArray) {
        //noinspection LabeledStatement
        outer:
        while (true) {
            int length = currentArray.length();
            int index = indexFor(hash, length);
            Object o = currentArray.get(index);
            while (o == RESERVED) {
                Thread.onSpinWait();
                o = currentArray.get(index);
            }
            if (o == RESIZED || o == RESIZING) {
                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
            } else {
                Entry<V> e = (Entry<V>) o;
                while (e != null) {
                    if (e.key == key) {
                        V oldValue = e.getValue();
                        Entry<V> newEntry =
                                new Entry<>(e.key, value, this.createReplacementChainForRemoval((Entry<V>) o, e));
                        if (!currentArray.compareAndSet(index, o, newEntry)) {
                            //noinspection ContinueStatementWithLabel
                            continue outer;
                        }
                        return oldValue;
                    }
                    e = e.getNext();
                }
                return null;
            }
        }
    }

    public V remove(long key) {
        int hash = this.hash(Long.hashCode(key));
        AtomicReferenceArray<Object> currentArray = this.table;
        int length = currentArray.length();
        int index = indexFor(hash, length);
        Object o = currentArray.get(index);
        while (o == RESERVED) {
            Thread.onSpinWait();
            o = currentArray.get(index);
        }
        if (o == RESIZED || o == RESIZING) {
            return this.slowRemove(key, hash, currentArray);
        }
        Entry<V> e = (Entry<V>) o;
        while (e != null) {
            if (e.key == key) {
                Entry<V> replacement = this.createReplacementChainForRemoval((Entry<V>) o, e);
                if (currentArray.compareAndSet(index, o, replacement)) {
                    this.addToSize(-1);
                    return e.getValue();
                }
                return this.slowRemove(key, hash, currentArray);
            }
            e = e.getNext();
        }
        return null;
    }

    private V slowRemove(long key, int hash, AtomicReferenceArray<Object> currentArray) {
        //noinspection LabeledStatement
        outer:
        while (true) {
            int length = currentArray.length();
            int index = indexFor(hash, length);
            Object o = currentArray.get(index);
            while (o == RESERVED) {
                Thread.onSpinWait();
                o = currentArray.get(index);
            }
            if (o == RESIZED || o == RESIZING) {
                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
            } else {
                Entry<V> e = (Entry<V>) o;
                while (e != null) {
                    if (e.key == key) {
                        Entry<V> replacement = this.createReplacementChainForRemoval((Entry<V>) o, e);
                        if (currentArray.compareAndSet(index, o, replacement)) {
                            this.addToSize(-1);
                            return e.getValue();
                        }
                        //noinspection ContinueStatementWithLabel
                        continue outer;
                    }
                    e = e.getNext();
                }
                return null;
            }
        }
    }

    private Entry<V> createReplacementChainForRemoval(Entry<V> original, Entry<V> toRemove) {
        if (original == toRemove) {
            return original.getNext();
        }
        Entry<V> replacement = null;
        Entry<V> e = original;
        while (e != null) {
            if (e != toRemove) {
                replacement = new Entry<>(e.key, e.getValue(), replacement);
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
            while (o == RESERVED) {
                Thread.onSpinWait();
                o = currentArray.get(i);
            }
            if (o == RESIZED || o == RESIZING) {
                throw new ConcurrentModificationException("can't compute hashcode while resizing!");
            }
            Entry<V> e = (Entry<V>) o;
            while (e != null) {
                long key = e.key;
                Object value = e.getValue();
                h += Long.hashCode(key) ^ (value == null ? 0 : value.hashCode());
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

        if (!(o instanceof HeapTrackingConcurrentLongObjectHashMap<?>)) {
            return false;
        }
        HeapTrackingConcurrentLongObjectHashMap<V> m = (HeapTrackingConcurrentLongObjectHashMap<V>) o;
        if (m.size() != this.size()) {
            return false;
        }
        EntryIterator iterator = new EntryIterator();
        while (iterator.hasNext()) {
            var e = iterator.next();
            long key = e.key;
            V value = e.getValue();
            if (value == null) {
                if (!(m.get(key) == null && m.containsKey(key))) {
                    return false;
                }
            } else {
                if (!value.equals(m.get(key))) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public void close() {
        memoryTracker.releaseHeap(SHALLOW_SIZE_THIS);
        releaseHeap();
    }

    private abstract class LongHashIterator<E> extends HashIterator<Entry<V>> {
        final Entry<V> nextEntry() {
            Entry<V> e = this.next;
            if (e == null) {
                throw new NoSuchElementException();
            }

            if ((this.next = e.getNext()) == null) {
                this.findNext();
            }
            this.current = e;
            return e;
        }

        protected boolean removeByKeyValue() {
            if (this.current == null) {
                throw new IllegalStateException();
            }
            long key = this.current.key;
            V val = this.current.value;
            this.current = null;
            return HeapTrackingConcurrentLongObjectHashMap.this.remove(key, val);
        }
    }

    private class KeyIterator extends LongHashIterator<Object> implements LongIterator {
        @Override
        public long next() {
            return this.nextEntry().key;
        }
    }

    private class ValueIterator extends LongHashIterator<V> implements Iterator<V> {

        @Override
        public void remove() {
            this.removeByKeyValue();
        }

        @Override
        public V next() {
            return this.nextEntry().value;
        }
    }

    private class EntryIterator extends LongHashIterator<V> implements Iterator<Entry<V>> {

        @Override
        public void remove() {
            this.removeByKeyValue();
        }

        @Override
        public Entry<V> next() {
            return this.nextEntry();
        }
    }

    private static final class Entry<V> implements Map.Entry<Long, V>, Wrapper<Entry<V>> {
        private final long key;
        private final V value;
        private final Entry<V> next;

        private Entry(long key, V value) {
            this.key = key;
            this.value = value;
            this.next = null;
        }

        private Entry(long key, V value, Entry<V> next) {
            this.key = key;
            this.value = value;
            this.next = next;
        }

        @Override
        public Long getKey() {
            return this.key;
        }

        @Override
        public V getValue() {
            return this.value;
        }

        @Override
        public V setValue(V value) {
            throw new RuntimeException("not implemented");
        }

        @Override
        public Entry<V> getNext() {
            return this.next;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Map.Entry<?, ?>)) {
                return false;
            }
            Map.Entry<?, V> e = (Map.Entry<?, V>) o;
            Object k2 = e.getKey();
            if (!Objects.equals(this.key, k2)) {
                return false;
            }
            Object v2 = e.getValue();
            return Objects.equals(this.value, v2);
        }

        @Override
        public int hashCode() {
            return Long.hashCode(key) ^ (this.value == null ? 0 : this.value.hashCode());
        }

        @Override
        public String toString() {
            return this.key + "=" + this.value;
        }
    }
}
