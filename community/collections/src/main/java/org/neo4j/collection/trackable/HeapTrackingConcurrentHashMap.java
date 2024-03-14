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

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.eclipse.collections.impl.utility.MapIterate;
import org.neo4j.memory.MemoryTracker;

@SuppressWarnings({"NullableProblems", "unchecked"})
public final class HeapTrackingConcurrentHashMap<K, V> extends AbstractHeapTrackingConcurrentHash
        implements ConcurrentMap<K, V>, AutoCloseable {

    private static final long SHALLOW_SIZE_THIS = shallowSizeOfInstance(HeapTrackingConcurrentHashMap.class);
    private static final long SHALLOW_SIZE_WRAPPER = shallowSizeOfInstance(Entry.class);

    public static <K, V> HeapTrackingConcurrentHashMap<K, V> newMap(MemoryTracker memoryTracker) {
        return newMap(memoryTracker, DEFAULT_INITIAL_CAPACITY);
    }

    public static <K, V> HeapTrackingConcurrentHashMap<K, V> newMap(MemoryTracker memoryTracker, int size) {
        memoryTracker.allocateHeap(SHALLOW_SIZE_THIS);
        return new HeapTrackingConcurrentHashMap<>(memoryTracker, size);
    }

    @Override
    public long sizeOfWrapperObject() {
        return SHALLOW_SIZE_WRAPPER;
    }

    private HeapTrackingConcurrentHashMap(MemoryTracker memoryTracker, int initialCapacity) {
        super(memoryTracker, initialCapacity);
    }

    @Override
    public V put(K key, V value) {
        int hash = this.hash(key);
        var currentArray = this.table;
        int length = currentArray.length();
        int index = indexFor(hash, length);
        Object o = currentArray.get(index);
        if (o == null) {
            Entry<K, V> newEntry = new Entry<>(key, value, null);
            addToSize(1);
            if (currentArray.compareAndSet(index, null, newEntry)) {
                return null;
            }
            addToSize(-1);
        }
        return this.slowPut(key, value, hash, currentArray);
    }

    private V slowPut(K key, V value, int hash, AtomicReferenceArray<Object> currentArray) {
        //noinspection LabeledStatement
        outer:
        while (true) {
            int length = currentArray.length();
            int index = indexFor(hash, length);
            Object o = getAtIndex(currentArray, index);
            if (o == RESIZED || o == RESIZING) {
                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
            } else {
                Entry<K, V> e = (Entry<K, V>) o;
                while (e != null) {
                    Object candidate = e.getKey();
                    if (candidate.equals(key)) {
                        V oldValue = e.getValue();
                        Entry<K, V> newEntry = new Entry<>(
                                e.getKey(), value, this.createReplacementChainForRemoval((Entry<K, V>) o, e));
                        if (!currentArray.compareAndSet(index, o, newEntry)) {
                            //noinspection ContinueStatementWithLabel
                            continue outer;
                        }
                        return oldValue;
                    }
                    e = e.getNext();
                }
                Entry<K, V> newEntry = new Entry<>(key, value, (Entry<K, V>) o);
                if (currentArray.compareAndSet(index, o, newEntry)) {
                    this.incrementSizeAndPossiblyResize(currentArray, length, o);
                    return null;
                }
            }
        }
    }

    @Override
    public V putIfAbsent(K key, V value) {
        int hash = this.hash(key);
        AtomicReferenceArray<Object> currentArray = this.table;
        while (true) {
            int length = currentArray.length();
            int index = indexFor(hash, length);
            Object o = getAtIndex(currentArray, index);
            if (o == RESIZED || o == RESIZING) {
                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
            } else {
                Entry<K, V> e = (Entry<K, V>) o;
                while (e != null) {
                    K candidate = e.getKey();
                    if (candidate.equals(key)) {
                        return e.getValue();
                    }
                    e = e.getNext();
                }
                Entry<K, V> newEntry = new Entry<>(key, value, (Entry<K, V>) o);
                if (currentArray.compareAndSet(index, o, newEntry)) {
                    this.incrementSizeAndPossiblyResize(currentArray, length, o);
                    return null; // per the contract of putIfAbsent, we return null when the map didn't have this key
                    // before
                }
            }
        }
    }

    @Override
    public V computeIfAbsent(K key, java.util.function.Function<? super K, ? extends V> mappingFunction) {
        int hash = this.hash(key);
        AtomicReferenceArray<Object> currentArray = this.table;
        while (true) {
            int length = currentArray.length();
            int index = indexFor(hash, length);
            Object o = getAtIndex(currentArray, index);
            if (o == RESIZED || o == RESIZING) {
                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
            } else {
                Entry<K, V> e = (Entry<K, V>) o;
                while (e != null) {
                    K candidate = e.getKey();
                    if (candidate.equals(key)) {
                        return e.getValue();
                    }
                    e = e.getNext();
                }
                if (currentArray.compareAndSet(index, o, RESERVED)) {
                    V newValue = mappingFunction.apply(key);
                    Entry<K, V> newEntry = new Entry<>(key, newValue, (Entry<K, V>) o);
                    currentArray.set(index, newEntry);
                    this.incrementSizeAndPossiblyResize(currentArray, length, o);
                    // per the contract of computeIfAbsent, we return the newvalue when the map
                    // didn't have this key before
                    return newValue;
                }
            }
        }
    }

    public Iterator<V> iterator() {
        return this.values().iterator();
    }

    @Override
    public void forEach(BiConsumer<? super K, ? super V> action) {
        if (action == null) throw new NullPointerException();
        var entries = entrySet();
        for (Map.Entry<K, V> entry : entries) {
            action.accept(entry.getKey(), entry.getValue());
        }
    }

    public void forEachValue(Consumer<? super V> action) {
        if (action == null) throw new NullPointerException();
        var entries = values();
        for (V value : entries) {
            action.accept(value);
        }
    }

    @Override
    void transfer(AtomicReferenceArray<Object> src, ResizeContainer resizeContainer) {
        AtomicReferenceArray<Object> dest = resizeContainer.nextArray;

        for (int j = 0; j < src.length() - 1; ) {
            Object o = getAtIndex(src, j);
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
                Entry<K, V> e = (Entry<K, V>) o;
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
                    Object o = getAtIndex(src, j);
                    if (o == null) {
                        if (src.compareAndSet(j, null, RESIZED)) {
                            j--;
                        }
                    } else if (o == RESIZED || o == RESIZING) {
                        resizeContainer.zeroOutQueuePosition();
                        return;
                    } else {
                        Entry<K, V> e = (Entry<K, V>) o;
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

    private void unconditionalCopy(AtomicReferenceArray<Object> dest, Entry<K, V> toCopyEntry) {
        int hash = hash(toCopyEntry.getKey());
        AtomicReferenceArray<Object> currentArray = dest;
        while (true) {
            int length = currentArray.length();
            int index = indexFor(hash, length);
            Object o = getAtIndex(currentArray, index);
            if (o == RESIZED || o == RESIZING) {
                currentArray = ((ResizeContainer) currentArray.get(length - 1)).nextArray;
            } else {
                Entry<K, V> newEntry;
                if (o == null) {
                    if (toCopyEntry.getNext() == null) {
                        newEntry = toCopyEntry; // no need to duplicate
                    } else {
                        newEntry = new Entry<>(toCopyEntry.getKey(), toCopyEntry.getValue());
                    }
                } else {
                    newEntry = new Entry<>(toCopyEntry.getKey(), toCopyEntry.getValue(), (Entry<K, V>) o);
                }
                if (currentArray.compareAndSet(index, o, newEntry)) {
                    return;
                }
            }
        }
    }

    @Override
    public boolean remove(Object key, Object value) {
        int hash = this.hash(key);
        AtomicReferenceArray<Object> currentArray = this.table;
        //noinspection LabeledStatement
        outer:
        while (true) {
            int length = currentArray.length();
            int index = indexFor(hash, length);
            Object o = getAtIndex(currentArray, index);
            if (o == RESIZED || o == RESIZING) {
                currentArray = helpWithResizeWhileCurrentIndex(currentArray, index);
            } else {
                Entry<K, V> e = (Entry<K, V>) o;
                while (e != null) {
                    Object candidate = e.getKey();
                    if (candidate.equals(key) && Objects.equals(e.getValue(), value)) {
                        Entry<K, V> replacement = this.createReplacementChainForRemoval((Entry<K, V>) o, e);
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

    @Override
    public boolean containsKey(Object key) {
        return this.getEntry(key) != null;
    }

    @Override
    public boolean containsValue(Object value) {
        AtomicReferenceArray<Object> currentArray = this.table;
        ResizeContainer resizeContainer;
        do {
            resizeContainer = null;
            for (int i = 0; i < currentArray.length() - 1; i++) {
                Object o = getAtIndex(currentArray, i);
                if (o == RESIZED || o == RESIZING) {
                    resizeContainer = (ResizeContainer) currentArray.get(currentArray.length() - 1);
                } else if (o != null) {
                    Entry<K, V> e = (Entry<K, V>) o;
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

    @Override
    public V get(Object key) {
        int hash = this.hash(key);
        AtomicReferenceArray<Object> currentArray = this.table;
        int index = indexFor(hash, currentArray.length());
        Object o = getAtIndex(currentArray, index);
        if (o == RESIZED || o == RESIZING) {
            return this.slowGet(key, hash, currentArray);
        }
        for (Entry<K, V> e = (Entry<K, V>) o; e != null; e = e.getNext()) {
            Object k;
            if ((k = e.key) == key || key.equals(k)) {
                return e.value;
            }
        }
        return null;
    }

    private V slowGet(Object key, int hash, AtomicReferenceArray<Object> currentArray) {
        while (true) {
            int length = currentArray.length();
            int index = indexFor(hash, length);
            Object o = getAtIndex(currentArray, index);
            if (o == RESIZED || o == RESIZING) {
                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
            } else {
                Entry<K, V> e = (Entry<K, V>) o;
                while (e != null) {
                    Object candidate = e.getKey();
                    if (candidate.equals(key)) {
                        return e.getValue();
                    }
                    e = e.getNext();
                }
                return null;
            }
        }
    }

    private Entry<K, V> getEntry(Object key) {
        int hash = this.hash(key);
        AtomicReferenceArray<Object> currentArray = this.table;
        while (true) {
            int length = currentArray.length();
            int index = indexFor(hash, length);
            Object o = getAtIndex(currentArray, index);
            if (o == RESIZED || o == RESIZING) {
                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
            } else {
                Entry<K, V> e = (Entry<K, V>) o;
                while (e != null) {
                    Object candidate = e.getKey();
                    if (candidate.equals(key)) {
                        return e;
                    }
                    e = e.getNext();
                }
                return null;
            }
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        MapIterate.forEachKeyValue(map, this::put);
    }

    @Override
    public void clear() {
        AtomicReferenceArray<Object> currentArray = this.table;
        ResizeContainer resizeContainer;
        do {
            resizeContainer = null;
            for (int i = 0; i < currentArray.length() - 1; i++) {
                Object o = getAtIndex(currentArray, i);
                if (o == RESIZED || o == RESIZING) {
                    resizeContainer = (ResizeContainer) currentArray.get(currentArray.length() - 1);
                } else if (o != null) {
                    Entry<K, V> e = (Entry<K, V>) o;
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
    public Set<K> keySet() {
        return new KeySet();
    }

    @Override
    public Collection<V> values() {
        return new Values();
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        return new EntrySet();
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        int hash = this.hash(key);
        AtomicReferenceArray<Object> currentArray = this.table;
        int length = currentArray.length();
        int index = indexFor(hash, length);
        Object o = getAtIndex(currentArray, index);
        if (o == RESIZED || o == RESIZING) {
            return this.slowReplace(key, oldValue, newValue, hash, currentArray);
        }
        Entry<K, V> e = (Entry<K, V>) o;
        while (e != null) {
            Object candidate = e.getKey();
            if (candidate == key || candidate.equals(key)) {
                if (oldValue == e.getValue() || (oldValue != null && oldValue.equals(e.getValue()))) {
                    Entry<K, V> replacement = this.createReplacementChainForRemoval((Entry<K, V>) o, e);
                    Entry<K, V> newEntry = new Entry<>(key, newValue, replacement);
                    return currentArray.compareAndSet(index, o, newEntry)
                            || this.slowReplace(key, oldValue, newValue, hash, currentArray);
                }
                return false;
            }
            e = e.getNext();
        }
        return false;
    }

    private boolean slowReplace(K key, V oldValue, V newValue, int hash, AtomicReferenceArray<Object> currentArray) {
        //noinspection LabeledStatement
        outer:
        while (true) {
            int length = currentArray.length();
            int index = indexFor(hash, length);
            Object o = getAtIndex(currentArray, index);
            if (o == RESIZED || o == RESIZING) {
                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
            } else {
                Entry<K, V> e = (Entry<K, V>) o;
                while (e != null) {
                    Object candidate = e.getKey();
                    if (candidate == key || candidate.equals(key)) {
                        if (oldValue == e.getValue() || (oldValue != null && oldValue.equals(e.getValue()))) {
                            Entry<K, V> replacement = this.createReplacementChainForRemoval((Entry<K, V>) o, e);
                            Entry<K, V> newEntry = new Entry<>(key, newValue, replacement);
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

    @Override
    public V replace(K key, V value) {
        int hash = this.hash(key);
        AtomicReferenceArray<Object> currentArray = this.table;
        int length = currentArray.length();
        int index = indexFor(hash, length);
        Object o = currentArray.get(index);
        if (o == null) {
            return null;
        }
        return this.slowReplace(key, value, hash, currentArray);
    }

    private V slowReplace(K key, V value, int hash, AtomicReferenceArray<Object> currentArray) {
        //noinspection LabeledStatement
        outer:
        while (true) {
            int length = currentArray.length();
            int index = indexFor(hash, length);
            Object o = getAtIndex(currentArray, index);
            if (o == RESIZED || o == RESIZING) {
                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
            } else {
                Entry<K, V> e = (Entry<K, V>) o;
                while (e != null) {
                    Object candidate = e.getKey();
                    if (candidate.equals(key)) {
                        V oldValue = e.getValue();
                        Entry<K, V> newEntry = new Entry<>(
                                e.getKey(), value, this.createReplacementChainForRemoval((Entry<K, V>) o, e));
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

    @Override
    public V remove(Object key) {
        int hash = this.hash(key);
        AtomicReferenceArray<Object> currentArray = this.table;
        int length = currentArray.length();
        int index = indexFor(hash, length);
        Object o = getAtIndex(currentArray, index);
        if (o == RESIZED || o == RESIZING) {
            return this.slowRemove(key, hash, currentArray);
        }
        Entry<K, V> e = (Entry<K, V>) o;
        while (e != null) {
            Object candidate = e.getKey();
            if (candidate.equals(key)) {
                Entry<K, V> replacement = this.createReplacementChainForRemoval((Entry<K, V>) o, e);
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

    private V slowRemove(Object key, int hash, AtomicReferenceArray<Object> currentArray) {
        //noinspection LabeledStatement
        outer:
        while (true) {
            int length = currentArray.length();
            int index = indexFor(hash, length);
            Object o = getAtIndex(currentArray, index);
            if (o == RESIZED || o == RESIZING) {
                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
            } else {
                Entry<K, V> e = (Entry<K, V>) o;
                while (e != null) {
                    Object candidate = e.getKey();
                    if (candidate.equals(key)) {
                        Entry<K, V> replacement = this.createReplacementChainForRemoval((Entry<K, V>) o, e);
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

    private Entry<K, V> createReplacementChainForRemoval(Entry<K, V> original, Entry<K, V> toRemove) {
        if (original == toRemove) {
            return original.getNext();
        }
        Entry<K, V> replacement = null;
        Entry<K, V> e = original;
        while (e != null) {
            if (e != toRemove) {
                replacement = new Entry<>(e.getKey(), e.getValue(), replacement);
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
            Object o = getAtIndex(currentArray, i);
            if (o == RESIZED || o == RESIZING) {
                throw new ConcurrentModificationException("can't compute hashcode while resizing!");
            }
            Entry<K, V> e = (Entry<K, V>) o;
            while (e != null) {
                Object key = e.getKey();
                Object value = e.getValue();
                h += (key == null ? 0 : key.hashCode()) ^ (value == null ? 0 : value.hashCode());
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

        if (!(o instanceof Map)) {
            return false;
        }
        Map<K, V> m = (Map<K, V>) o;
        if (m.size() != this.size()) {
            return false;
        }

        for (Map.Entry<K, V> e : this.entrySet()) {
            K key = e.getKey();
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
    public String toString() {
        if (this.isEmpty()) {
            return "{}";
        }
        Iterator<Map.Entry<K, V>> iterator = this.entrySet().iterator();

        StringBuilder sb = new StringBuilder();
        sb.append('{');
        while (true) {
            Map.Entry<K, V> e = iterator.next();
            K key = e.getKey();
            V value = e.getValue();
            sb.append(key == this ? "(this Map)" : key);
            sb.append('=');
            sb.append(value == this ? "(this Map)" : value);
            if (!iterator.hasNext()) {
                return sb.append('}').toString();
            }
            sb.append(", ");
        }
    }

    @Override
    public void close() {
        memoryTracker.releaseHeap(SHALLOW_SIZE_THIS);
        releaseHeap();
    }

    private abstract class HashMapIterator<E> extends HashIterator<Entry<K, V>> {
        final Entry<K, V> nextEntry() {
            Entry<K, V> e = this.next;
            if (e == null) {
                throw new NoSuchElementException();
            }

            if ((this.next = e.getNext()) == null) {
                this.findNext();
            }
            this.current = e;
            return e;
        }

        protected void removeByKey() {
            if (this.current == null) {
                throw new IllegalStateException();
            }
            K key = this.current.key;
            this.current = null;
            HeapTrackingConcurrentHashMap.this.remove(key);
        }

        protected boolean removeByKeyValue() {
            if (this.current == null) {
                throw new IllegalStateException();
            }
            K key = this.current.key;
            V val = this.current.value;
            this.current = null;
            return HeapTrackingConcurrentHashMap.this.remove(key, val);
        }
    }

    private final class ValueIterator extends HashMapIterator<V> implements Iterator<V> {
        @Override
        public void remove() {
            this.removeByKeyValue();
        }

        @Override
        public V next() {
            return this.nextEntry().value;
        }
    }

    private final class KeyIterator extends HashMapIterator<K> implements Iterator<K> {
        @Override
        public K next() {
            return this.nextEntry().getKey();
        }

        @Override
        public void remove() {
            this.removeByKey();
        }
    }

    private final class EntryIterator extends HashMapIterator<Map.Entry<K, V>> implements Iterator<Map.Entry<K, V>> {
        @Override
        public Map.Entry<K, V> next() {
            return this.nextEntry();
        }

        @Override
        public void remove() {
            this.removeByKeyValue();
        }
    }

    private final class KeySet extends AbstractSet<K> {
        @Override
        public Iterator<K> iterator() {
            return new KeyIterator();
        }

        @Override
        public int size() {
            return HeapTrackingConcurrentHashMap.this.size();
        }

        @Override
        public boolean contains(Object o) {
            return HeapTrackingConcurrentHashMap.this.containsKey(o);
        }

        @Override
        public boolean remove(Object o) {
            return HeapTrackingConcurrentHashMap.this.remove(o) != null;
        }

        @Override
        public void clear() {
            HeapTrackingConcurrentHashMap.this.clear();
        }
    }

    private final class Values extends AbstractCollection<V> {
        @Override
        public Iterator<V> iterator() {
            return new ValueIterator();
        }

        @Override
        public boolean removeAll(Collection<?> col) {
            Objects.requireNonNull(col);
            boolean removed = false;
            ValueIterator itr = new ValueIterator();
            while (itr.hasNext()) {
                if (col.contains(itr.next())) {
                    removed |= itr.removeByKeyValue();
                }
            }
            return removed;
        }

        @Override
        public boolean removeIf(Predicate<? super V> filter) {
            Objects.requireNonNull(filter);
            boolean removed = false;
            ValueIterator itr = new ValueIterator();
            while (itr.hasNext()) {
                if (filter.test(itr.next())) {
                    removed |= itr.removeByKeyValue();
                }
            }
            return removed;
        }

        @Override
        public int size() {
            return HeapTrackingConcurrentHashMap.this.size();
        }

        @Override
        public boolean contains(Object o) {
            return HeapTrackingConcurrentHashMap.this.containsValue(o);
        }

        @Override
        public void clear() {
            HeapTrackingConcurrentHashMap.this.clear();
        }
    }

    private final class EntrySet extends AbstractSet<Map.Entry<K, V>> {
        @Override
        public Iterator<Map.Entry<K, V>> iterator() {
            return new EntryIterator();
        }

        @Override
        public boolean removeAll(Collection<?> col) {
            Objects.requireNonNull(col);
            boolean removed = false;

            if (this.size() > col.size()) {
                for (Object o : col) {
                    removed |= this.remove(o);
                }
            } else {
                for (EntryIterator itr = new EntryIterator(); itr.hasNext(); ) {
                    if (col.contains(itr.next())) {
                        removed |= itr.removeByKeyValue();
                    }
                }
            }
            return removed;
        }

        @Override
        public boolean removeIf(Predicate<? super Map.Entry<K, V>> filter) {
            Objects.requireNonNull(filter);
            boolean removed = false;
            EntryIterator itr = new EntryIterator();
            while (itr.hasNext()) {
                if (filter.test(itr.next())) {
                    removed |= itr.removeByKeyValue();
                }
            }
            return removed;
        }

        @Override
        public boolean contains(Object o) {
            if (!(o instanceof Map.Entry<?, ?>)) {
                return false;
            }
            Map.Entry<K, V> e = (Map.Entry<K, V>) o;
            Entry<K, V> candidate = HeapTrackingConcurrentHashMap.this.getEntry(e.getKey());
            return e.equals(candidate);
        }

        @Override
        public boolean remove(Object o) {
            if (!(o instanceof Map.Entry<?, ?>)) {
                return false;
            }
            Map.Entry<K, V> e = (Map.Entry<K, V>) o;
            return HeapTrackingConcurrentHashMap.this.remove(e.getKey(), e.getValue());
        }

        @Override
        public int size() {
            return HeapTrackingConcurrentHashMap.this.size();
        }

        @Override
        public void clear() {
            HeapTrackingConcurrentHashMap.this.clear();
        }
    }

    private static final class Entry<K, V> implements Map.Entry<K, V>, Wrapper<Entry<K, V>> {
        private final K key;
        private final V value;
        private final Entry<K, V> next;

        private Entry(K key, V value) {
            this.key = key;
            this.value = value;
            this.next = null;
        }

        private Entry(K key, V value, Entry<K, V> next) {
            this.key = key;
            this.value = value;
            this.next = next;
        }

        @Override
        public K getKey() {
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
        public Entry<K, V> getNext() {
            return this.next;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Map.Entry<?, ?>)) {
                return false;
            }
            Map.Entry<K, V> e = (Map.Entry<K, V>) o;
            Object k2 = e.getKey();
            if (!Objects.equals(this.key, k2)) {
                return false;
            }
            Object v2 = e.getValue();
            return Objects.equals(this.value, v2);
        }

        @Override
        public int hashCode() {
            return (this.key == null ? 0 : this.key.hashCode()) ^ (this.value == null ? 0 : this.value.hashCode());
        }

        @Override
        public String toString() {
            return this.key + "=" + this.value;
        }
    }
}
