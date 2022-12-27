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

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.utility.MapIterate;
import org.neo4j.memory.MemoryTracker;

@SuppressWarnings({"rawtypes", "ObjectEquality"})
public final class HeapTrackingConcurrentHashMap<K, V> implements ConcurrentMap<K, V>, AutoCloseable {

    static final Object RESIZE_SENTINEL = new Object();
    static final int DEFAULT_INITIAL_CAPACITY = 16;

    /**
     * The maximum capacity, used if a higher value is implicitly specified
     * by either of the constructors with arguments.
     * MUST be a power of two <= 1<<30.
     */
    static final int MAXIMUM_CAPACITY = 1 << 30;

    private static final AtomicReferenceFieldUpdater<HeapTrackingConcurrentHashMap, AtomicReferenceArray>
            TABLE_UPDATER = AtomicReferenceFieldUpdater.newUpdater(
                    HeapTrackingConcurrentHashMap.class, AtomicReferenceArray.class, "table");
    private static final AtomicIntegerFieldUpdater<HeapTrackingConcurrentHashMap> SIZE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(HeapTrackingConcurrentHashMap.class, "size");
    static final Object RESIZED = new Object();
    static final Object RESIZING = new Object();
    static final int PARTITIONED_SIZE_THRESHOLD = 4096; // chosen to keep size below 1% of the total size of the map
    static final int SIZE_BUCKETS = 7;
    static final int PARTITIONED_SIZE = SIZE_BUCKETS * 16;
    static final long SHALLOW_SIZE_ATOMIC_REFERENCE_ARRAY = shallowSizeOfInstance(AtomicReferenceArray.class);
    static final long SIZE_INTEGER_REFERENCE_ARRAY =
            shallowSizeOfInstance(AtomicIntegerArray.class) + sizeOfIntArray(PARTITIONED_SIZE);
    private static final long SHALLOW_SIZE_THIS = shallowSizeOfInstance(HeapTrackingConcurrentHashMap.class);

    /**
     * The table, resized as necessary. Length MUST Always be a power of two.
     */
    private volatile AtomicReferenceArray table;

    private AtomicIntegerArray partitionedSize;

    @SuppressWarnings("UnusedDeclaration")
    private volatile int size; // updated via atomic field updater

    private final MemoryTracker memoryTracker;
    private volatile int trackedCapacity;

    private HeapTrackingConcurrentHashMap(MemoryTracker memoryTracker) {
        this(memoryTracker, DEFAULT_INITIAL_CAPACITY);
    }

    public HeapTrackingConcurrentHashMap(MemoryTracker memoryTracker, int initialCapacity) {
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

    public static <K, V> HeapTrackingConcurrentHashMap<K, V> newMap(MemoryTracker memoryTracker) {
        memoryTracker.allocateHeap(SHALLOW_SIZE_THIS);
        return new HeapTrackingConcurrentHashMap<>(memoryTracker);
    }

    public static <K, V> HeapTrackingConcurrentHashMap<K, V> newMap(MemoryTracker memoryTracker, int size) {
        memoryTracker.allocateHeap(SHALLOW_SIZE_THIS);
        return new HeapTrackingConcurrentHashMap<>(memoryTracker, size);
    }

    static int indexFor(int h, int length) {
        return h & length - 2;
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
    public V put(K key, V value) {
        int hash = this.hash(key);
        AtomicReferenceArray currentArray = this.table;
        int length = currentArray.length();
        int index = HeapTrackingConcurrentHashMap.indexFor(hash, length);
        Object o = currentArray.get(index);
        if (o == null) {
            Entry<K, V> newEntry = new Entry<>(key, value, null);
            this.addToSize(1);
            if (currentArray.compareAndSet(index, null, newEntry)) {
                return null;
            }
            this.addToSize(-1);
        }
        return this.slowPut(key, value, hash, currentArray);
    }

    private V slowPut(K key, V value, int hash, AtomicReferenceArray currentArray) {
        //noinspection LabeledStatement
        outer:
        while (true) {
            int length = currentArray.length();
            int index = HeapTrackingConcurrentHashMap.indexFor(hash, length);
            Object o = currentArray.get(index);
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
        AtomicReferenceArray currentArray = this.table;
        while (true) {
            int length = currentArray.length();
            int index = HeapTrackingConcurrentHashMap.indexFor(hash, length);
            Object o = currentArray.get(index);
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
        AtomicReferenceArray currentArray = this.table;
        while (true) {
            int length = currentArray.length();
            int index = HeapTrackingConcurrentHashMap.indexFor(hash, length);
            Object o = currentArray.get(index);
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
                V newValue = mappingFunction.apply(key);
                Entry<K, V> newEntry = new Entry<>(key, newValue, (Entry<K, V>) o);
                if (currentArray.compareAndSet(index, o, newEntry)) {
                    this.incrementSizeAndPossiblyResize(currentArray, length, o);
                    return newValue; // per the contract of computeIfAbsent, we return the newvalue when the map didn't
                    // have this key
                    // before
                }
            }
        }
    }

    public boolean notEmpty() {
        return !this.isEmpty();
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

    private void unconditionalCopy(AtomicReferenceArray dest, Entry<K, V> toCopyEntry) {
        int hash = this.hash(toCopyEntry.getKey());
        AtomicReferenceArray currentArray = dest;
        while (true) {
            int length = currentArray.length();
            int index = HeapTrackingConcurrentHashMap.indexFor(hash, length);
            Object o = currentArray.get(index);
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
        AtomicReferenceArray currentArray = this.table;
        //noinspection LabeledStatement
        outer:
        while (true) {
            int length = currentArray.length();
            int index = HeapTrackingConcurrentHashMap.indexFor(hash, length);
            Object o = currentArray.get(index);
            if (o == RESIZED || o == RESIZING) {
                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
            } else {
                Entry<K, V> e = (Entry<K, V>) o;
                while (e != null) {
                    Object candidate = e.getKey();
                    if (candidate.equals(key) && this.nullSafeEquals(e.getValue(), value)) {
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
    public boolean containsKey(Object key) {
        return this.getEntry(key) != null;
    }

    @Override
    public boolean containsValue(Object value) {
        AtomicReferenceArray currentArray = this.table;
        ResizeContainer resizeContainer;
        do {
            resizeContainer = null;
            for (int i = 0; i < currentArray.length() - 1; i++) {
                Object o = currentArray.get(i);
                if (o == RESIZED || o == RESIZING) {
                    resizeContainer = (ResizeContainer) currentArray.get(currentArray.length() - 1);
                } else if (o != null) {
                    Entry<K, V> e = (Entry<K, V>) o;
                    while (e != null) {
                        Object v = e.getValue();
                        if (this.nullSafeEquals(v, value)) {
                            return true;
                        }
                        e = e.getNext();
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
        return false;
    }

    private boolean nullSafeEquals(Object v, Object value) {
        return v == value || v != null && v.equals(value);
    }

    @Override
    public V get(Object key) {
        int hash = this.hash(key);
        AtomicReferenceArray currentArray = this.table;
        int index = HeapTrackingConcurrentHashMap.indexFor(hash, currentArray.length());
        Object o = currentArray.get(index);
        if (o == RESIZED || o == RESIZING) {
            return this.slowGet(key, hash, index, currentArray);
        }
        for (Entry<K, V> e = (Entry<K, V>) o; e != null; e = e.getNext()) {
            Object k;
            if ((k = e.key) == key || key.equals(k)) {
                return e.value;
            }
        }
        return null;
    }

    private V slowGet(Object key, int hash, int index, AtomicReferenceArray currentArray) {
        while (true) {
            int length = currentArray.length();
            index = HeapTrackingConcurrentHashMap.indexFor(hash, length);
            Object o = currentArray.get(index);
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
        AtomicReferenceArray currentArray = this.table;
        while (true) {
            int length = currentArray.length();
            int index = HeapTrackingConcurrentHashMap.indexFor(hash, length);
            Object o = currentArray.get(index);
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
        AtomicReferenceArray currentArray = this.table;
        ResizeContainer resizeContainer;
        do {
            resizeContainer = null;
            for (int i = 0; i < currentArray.length() - 1; i++) {
                Object o = currentArray.get(i);
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
        AtomicReferenceArray currentArray = this.table;
        int length = currentArray.length();
        int index = HeapTrackingConcurrentHashMap.indexFor(hash, length);
        Object o = currentArray.get(index);
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

    private boolean slowReplace(K key, V oldValue, V newValue, int hash, AtomicReferenceArray currentArray) {
        //noinspection LabeledStatement
        outer:
        while (true) {
            int length = currentArray.length();
            int index = HeapTrackingConcurrentHashMap.indexFor(hash, length);
            Object o = currentArray.get(index);
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
        AtomicReferenceArray currentArray = this.table;
        int length = currentArray.length();
        int index = HeapTrackingConcurrentHashMap.indexFor(hash, length);
        Object o = currentArray.get(index);
        if (o == null) {
            return null;
        }
        return this.slowReplace(key, value, hash, currentArray);
    }

    private V slowReplace(K key, V value, int hash, AtomicReferenceArray currentArray) {
        //noinspection LabeledStatement
        outer:
        while (true) {
            int length = currentArray.length();
            int index = HeapTrackingConcurrentHashMap.indexFor(hash, length);
            Object o = currentArray.get(index);
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
        AtomicReferenceArray currentArray = this.table;
        int length = currentArray.length();
        int index = HeapTrackingConcurrentHashMap.indexFor(hash, length);
        Object o = currentArray.get(index);
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

    private V slowRemove(Object key, int hash, AtomicReferenceArray currentArray) {
        //noinspection LabeledStatement
        outer:
        while (true) {
            int length = currentArray.length();
            int index = HeapTrackingConcurrentHashMap.indexFor(hash, length);
            Object o = currentArray.get(index);
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
        AtomicReferenceArray currentArray = this.table;
        for (int i = 0; i < currentArray.length() - 1; i++) {
            Object o = currentArray.get(i);
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
        memoryTracker.releaseHeap(SHALLOW_SIZE_THIS + shallowSizeOfAtomicReferenceArray(trackedCapacity));
        if (partitionedSize != null) {
            memoryTracker.releaseHeap(SIZE_INTEGER_REFERENCE_ARRAY);
        }
    }

    static final class IteratorState {
        AtomicReferenceArray currentTable;
        int start;
        int end;

        IteratorState(AtomicReferenceArray currentTable) {
            this.currentTable = currentTable;
            this.end = this.currentTable.length() - 1;
        }

        IteratorState(AtomicReferenceArray currentTable, int start, int end) {
            this.currentTable = currentTable;
            this.start = start;
            this.end = end;
        }
    }

    private abstract class HashIterator<E> implements Iterator<E> {
        private List<IteratorState> todo;
        private IteratorState currentState;
        private Entry<K, V> next;
        private int index;
        private Entry<K, V> current;

        protected HashIterator() {
            this.currentState = new IteratorState(HeapTrackingConcurrentHashMap.this.table);
            this.findNext();
        }

        private void findNext() {
            while (this.index < this.currentState.end) {
                Object o = this.currentState.currentTable.get(this.index);
                if (o == RESIZED || o == RESIZING) {
                    AtomicReferenceArray nextArray = HeapTrackingConcurrentHashMap.this.helpWithResizeWhileCurrentIndex(
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
                    this.next = (Entry<K, V>) o;
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
        public boolean hasNext() {
            return this.next != null;
        }

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

    private final class ValueIterator extends HashIterator<V> {
        @Override
        public void remove() {
            this.removeByKeyValue();
        }

        @Override
        public V next() {
            return this.nextEntry().value;
        }
    }

    private final class KeyIterator extends HashIterator<K> {
        @Override
        public K next() {
            return this.nextEntry().getKey();
        }

        @Override
        public void remove() {
            this.removeByKey();
        }
    }

    private final class EntryIterator extends HashIterator<Map.Entry<K, V>> {
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

    public static final class Entry<K, V> implements Map.Entry<K, V> {
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

        public Entry<K, V> getNext() {
            return this.next;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Map.Entry<?, ?>)) {
                return false;
            }
            Map.Entry<K, V> e = (Map.Entry<K, V>) o;
            K k1 = this.key;
            Object k2 = e.getKey();
            if (k1 == k2 || k1 != null && k1.equals(k2)) {
                V v1 = this.value;
                Object v2 = e.getValue();
                if (v1 == v2 || v1 != null && v1.equals(v2)) {
                    return true;
                }
            }
            return false;
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

    static final class ResizeContainer {
        static final int QUEUE_INCREMENT =
                Math.min(1 << 10, Integer.highestOneBit(Runtime.getRuntime().availableProcessors()) << 4);
        final AtomicInteger resizers = new AtomicInteger(1);
        final AtomicReferenceArray nextArray;
        final AtomicInteger queuePosition;

        ResizeContainer(AtomicReferenceArray nextArray, int oldSize) {
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
}
