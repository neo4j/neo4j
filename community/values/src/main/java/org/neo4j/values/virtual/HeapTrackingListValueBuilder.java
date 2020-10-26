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

import java.util.Collections;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import org.neo4j.collection.trackable.HeapTrackingArrayList;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.values.AnyValue;

import static org.neo4j.memory.HeapEstimator.SCOPED_MEMORY_TRACKER_SHALLOW_SIZE;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

public class HeapTrackingListValueBuilder implements AutoCloseable
{
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
    public static HeapTrackingListValueBuilder newHeapTrackingListBuilder( MemoryTracker memoryTracker )
    {
        return new HeapTrackingListValueBuilder( memoryTracker );
    }

    private static final long SHALLOW_SIZE = shallowSizeOfInstance( HeapTrackingListValueBuilder.class );

    private final HeapTrackingArrayList<AnyValue> values;
    private final MemoryTracker scopedMemoryTracker;

    public HeapTrackingListValueBuilder( MemoryTracker memoryTracker )
    {
        // To be in control of the heap usage of both the added values and the internal array list holding them,
        // we use a scoped memory tracker
        scopedMemoryTracker = memoryTracker.getScopedMemoryTracker();
        scopedMemoryTracker.allocateHeap( SHALLOW_SIZE + SCOPED_MEMORY_TRACKER_SHALLOW_SIZE );
        values = HeapTrackingArrayList.newArrayList( 16, scopedMemoryTracker );
    }

    public void add( AnyValue value )
    {
        scopedMemoryTracker.allocateHeap( value.estimatedHeapUsage() );
        values.add( value );
    }

    public ListValue build()
    {
        return new ListValue.JavaListListValue( values, payloadSize() );
    }

    public ListValue buildAndClose()
    {
        ListValue value = build();
        close();
        return value;
    }

    public HeapTrackingListValueBuilder combine( HeapTrackingListValueBuilder rhs )
    {
        values.addAll( rhs.values );
        scopedMemoryTracker.allocateHeap( rhs.payloadSize() );
        return this;
    }

    private long payloadSize()
    {
        // The shallow size should not be transferred to the ListValue (but the ScopedMemoryTracker is)
        return scopedMemoryTracker.estimatedHeapMemory() - SHALLOW_SIZE;
    }

    @Override
    public void close()
    {
        scopedMemoryTracker.close();
    }

    /**
     * @return a collector for {@link ListValue}s
     */
    public static Collector<AnyValue,HeapTrackingListValueBuilder,ListValue> collector( MemoryTracker memoryTracker )
    {
        return new Collector<>()
        {
            @Override
            public Supplier<HeapTrackingListValueBuilder> supplier()
            {
                return () -> newHeapTrackingListBuilder( memoryTracker );
            }

            @Override
            public BiConsumer<HeapTrackingListValueBuilder,AnyValue> accumulator()
            {
                return org.neo4j.values.virtual.HeapTrackingListValueBuilder::add;
            }

            @Override
            public BinaryOperator<HeapTrackingListValueBuilder> combiner()
            {
                return HeapTrackingListValueBuilder::combine;
            }

            @Override
            public Function<HeapTrackingListValueBuilder,ListValue> finisher()
            {
                return HeapTrackingListValueBuilder::buildAndClose;
            }

            @Override
            public Set<Characteristics> characteristics()
            {
                return Collections.emptySet();
            }
        };
    }
}

