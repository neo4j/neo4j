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
package org.neo4j.values.virtual;

import static org.neo4j.memory.HeapEstimator.SCOPED_MEMORY_TRACKER_SHALLOW_SIZE;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.values.SequenceValue.IterationPreference.ITERATION;
import static org.neo4j.values.storable.Values.NO_VALUE;

import java.util.Iterator;
import java.util.Objects;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.iterator.MutableLongIterator;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.neo4j.collection.trackable.HeapTrackingCollections;
import org.neo4j.collection.trackable.HeapTrackingLongHashSet;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.NoValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueRepresentation;
import org.neo4j.values.storable.Values;

/// UnorderedLongSetListValue is a specialization that should only be used if
/// - Insertion order doesn't matter
/// - All items are guaranteed to be longs
public final class UnorderedLongSetListValue extends ListValue {
    private static final long SET_LIST_VALUE_SHALLOW_SIZE = shallowSizeOfInstance(UnorderedLongSetListValue.class);
    private final HeapTrackingLongHashSet set;
    private final long payload;

    public static final class HeapTrackingBuilder implements AutoCloseable {
        private static final long SHALLOW_SIZE = shallowSizeOfInstance(HeapTrackingBuilder.class);

        /*
         * Estimated heap usage in bytes of items that has been added to the
         * builder but not yet accounted for in the memory tracker.
         *
         * We have seen queries that spend a lot of time to allocate heap in the
         * memory tracker when adding lots of small items (RollupApply micro
         * benchmark). This is an optimisation for such cases.
         */
        private long unAllocatedHeapSize;

        private final HeapTrackingLongHashSet set;
        private final MemoryTracker scopedMemoryTracker;

        private HeapTrackingBuilder(MemoryTracker memoryTracker, int initialCapacity) {
            // To be in control of the heap usage of both the added values and the internal array list holding them,
            // we use a scoped memory tracker
            scopedMemoryTracker = memoryTracker.getScopedMemoryTracker();
            scopedMemoryTracker.allocateHeap(SHALLOW_SIZE + SCOPED_MEMORY_TRACKER_SHALLOW_SIZE);
            set = HeapTrackingCollections.newLongSet(memoryTracker, initialCapacity);
        }

        public void add(long value) {
            set.add(value);
        }

        public void addAll(UnorderedLongSetListValue other) {
            set.addAll(other.set);
        }

        public UnorderedLongSetListValue build() {
            scopedMemoryTracker.allocateHeap(unAllocatedHeapSize);
            unAllocatedHeapSize = 0;
            return new UnorderedLongSetListValue(set, payloadSize());
        }

        public UnorderedLongSetListValue buildAndClose() {
            var value = build();
            close();
            return value;
        }

        @Override
        public void close() {
            scopedMemoryTracker.close();
            set.close();
        }

        private long payloadSize() {
            // The shallow size should not be transferred to the ListValue (but the ScopedMemoryTracker is)
            // Note if the scopedMemoryTracker is an EmptyMemoryTracker then we might get a negative value here
            return Math.max(unAllocatedHeapSize + scopedMemoryTracker.estimatedHeapMemory() - SHALLOW_SIZE, 0L);
        }
    }

    public static HeapTrackingBuilder heapTrackingBuilder(MemoryTracker memoryTracker, int initialCapacity) {
        return new HeapTrackingBuilder(memoryTracker, initialCapacity);
    }

    UnorderedLongSetListValue(HeapTrackingLongHashSet set, long payload) {
        this.set = set;
        this.payload = payload;
    }

    @Override
    public ValueRepresentation itemValueRepresentation() {
        return ValueRepresentation.INT64;
    }

    @Override
    public long estimatedHeapUsage() {
        return SET_LIST_VALUE_SHALLOW_SIZE + payload;
    }

    @Override
    public long actualSize() {
        return set.size();
    }

    @Override
    public AnyValue value(long offset) {
        int size = set.size();
        Objects.checkIndex(offset, size);
        MutableLongIterator iterator = set.longIterator();
        var i = 0;
        while (iterator.hasNext()) {
            long next = iterator.next();
            if (i++ == offset) {
                return Values.longValue(next);
            }
        }
        throw new IndexOutOfBoundsException(offset + " is outside range " + size);
    }

    @Override
    public boolean isEmpty() {
        return set.isEmpty();
    }

    @Override
    public AnyValue head() {
        if (set.isEmpty()) {
            return NO_VALUE;
        } else {
            return Values.longValue(set.longIterator().next());
        }
    }

    @Override
    public ListValue reverse() {
        return new ReversedList(VirtualValues.fromArray(Values.longArray(set.toArray())));
    }

    @Override
    public Value ternaryContains(AnyValue value) {
        return switch (value) {
            case NoValue ignore -> NO_VALUE;
            case NumberValue numberValue -> Values.booleanValue(set.contains(numberValue.longValue()));
            default -> Values.FALSE;
        };
    }

    public LongSet primitiveLongSet() {
        return set.freeze();
    }

    public boolean contains(long value) {
        return set.contains(value);
    }

    @Override
    public ListValue distinct() {
        return this;
    }

    @Override
    public Iterator<AnyValue> iterator() {
        return new Iterator<>() {
            private final LongIterator iterator = set.longIterator();

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public AnyValue next() {
                return Values.longValue(iterator.next());
            }
        };
    }

    @Override
    public IterationPreference iterationPreference() {
        return ITERATION;
    }
}
