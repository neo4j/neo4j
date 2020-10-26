/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.values.virtual;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import org.neo4j.collection.trackable.HeapTrackingArrayList;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.values.AnyValue;

import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

public abstract class ListValueBuilder implements AutoCloseable
{
    /**
     * @return a collector for {@link ListValue}s
     */
    public static Collector<AnyValue,?,ListValue> collector()
    {
        return LIST_VALUE_COLLECTOR;
    }

    /**
     * Start building a list of known size
     * @param size the final size of the list
     * @return a new builder
     */
    public static ListValueBuilder newListBuilder( int size )
    {
        return new FixedSizeListValueBuilder( size );
    }

    /**
     * Start building a list of unknown size
     * @return a new builder
     */
    public static ListValueBuilder newListBuilder()
    {
        return new UnknownSizeListValueBuilder();
    }

    /**
     * Start building a list of unknown size with heap tracking
     * Values added to the list will have their heap usage estimated and tracked in the give memory tracker.
     *
     * Caveat: When calling build() the ownership of the internal heap-tracking list will be transferred
     * to the returned ListValue, and it will carry the heap usage accumulated by the builder as its payload size.
     * But to be accounted for, this ListValue will need to be measured and allocated in a memory tracker.
     * (This is in alignment with other AnyValues)
     * Beware that in the time window between closing the builder and allocating the returned ListValue,
     * the total memory usage may either be underestimated (un-accounted) or overestimated (double counted) depending
     * on the order of events.
     *
     * @return a new heap tracking builder
     */
    public static ListValueBuilder newHeapTrackingListBuilder( MemoryTracker memoryTracker )
    {
        return new HeapTrackingListValueBuilder( memoryTracker );
    }

    public abstract void add( AnyValue value );

    public abstract ListValue build();

    private static class FixedSizeListValueBuilder extends ListValueBuilder
    {
        long estimatedHeapSize;
        private final AnyValue[] values;
        private int index;

        private FixedSizeListValueBuilder( int size )
        {
            this.values = new AnyValue[size];
        }

        @Override
        public void add( AnyValue value )
        {
            estimatedHeapSize += value.estimatedHeapUsage();
            values[index++] = value;
        }

        @Override
        public ListValue build()
        {
            return new ListValue.ArrayListValue( values, estimatedHeapSize );
        }

        @Override
        public void close() throws Exception
        {
        }
    }

    private static final long ARRAY_LIST_SHALLOW_SIZE = shallowSizeOfInstance( ArrayList.class );
    public static final long UNKNOWN_LIST_VALUE_BUILDER_SHALLOW_SIZE = shallowSizeOfInstance( UnknownSizeListValueBuilder.class ) + ARRAY_LIST_SHALLOW_SIZE;
    private static class UnknownSizeListValueBuilder extends ListValueBuilder
    {
        long estimatedHeapSize;
        private final List<AnyValue> values = new ArrayList<>();

        public UnknownSizeListValueBuilder()
        {
            super();
            estimatedHeapSize += ARRAY_LIST_SHALLOW_SIZE;
        }

        @Override
        public void add( AnyValue value )
        {
            estimatedHeapSize += value.estimatedHeapUsage();
            values.add( value );
        }

        public UnknownSizeListValueBuilder combine( UnknownSizeListValueBuilder rhs )
        {
            values.addAll( rhs.values );
            estimatedHeapSize += rhs.estimatedHeapSize;
            return this;
        }

        @Override
        public ListValue build()
        {
            return new ListValue.JavaListListValue( values, estimatedHeapSize );
        }

        @Override
        public void close() throws Exception
        {
        }
    }

    // TODO: Avoid megamorphic calls?
    private static class HeapTrackingListValueBuilder extends ListValueBuilder
    {
        private static final long SHALLOW_SIZE = shallowSizeOfInstance( HeapTrackingListValueBuilder.class );

        private final HeapTrackingArrayList<AnyValue> values;
        private final MemoryTracker scopedMemoryTracker;

        public HeapTrackingListValueBuilder( MemoryTracker memoryTracker )
        {
            super();
            // To be in control of the heap usage of both the added values and the internal array list holding them,
            // we use a scoped memory tracker
            scopedMemoryTracker = memoryTracker.getScopedMemoryTracker();
            scopedMemoryTracker.allocateHeap( SHALLOW_SIZE );
            values = HeapTrackingArrayList.newArrayList( 16, scopedMemoryTracker );
        }

        @Override
        public void add( AnyValue value )
        {
            scopedMemoryTracker.allocateHeap( value.estimatedHeapUsage() );
            values.add( value );
        }

        @Override
        public ListValue build()
        {
            long payloadSize = scopedMemoryTracker.estimatedHeapMemory() - SHALLOW_SIZE; // The shallow size will not be transferred to the ListValue
            return new ListValue.JavaListListValue( values, payloadSize );
        }

        @Override
        public void close() throws Exception
        {
            scopedMemoryTracker.close();
        }
    }

    private static final Collector<AnyValue,UnknownSizeListValueBuilder,ListValue> LIST_VALUE_COLLECTOR = new Collector<>()
    {
        @Override
        public Supplier<UnknownSizeListValueBuilder> supplier()
        {
            return UnknownSizeListValueBuilder::new;
        }

        @Override
        public BiConsumer<UnknownSizeListValueBuilder,AnyValue> accumulator()
        {
            return ListValueBuilder::add;
        }

        @Override
        public BinaryOperator<UnknownSizeListValueBuilder> combiner()
        {
            return UnknownSizeListValueBuilder::combine;
        }

        @Override
        public Function<UnknownSizeListValueBuilder,ListValue> finisher()
        {
            return UnknownSizeListValueBuilder::build;
        }

        @Override
        public Set<Characteristics> characteristics()
        {
            return Collections.emptySet();
        }
    };
}

