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
package org.neo4j.kernel.impl.util.collection;

import org.github.jamm.MemoryMeter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Iterator;

import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.RandomSupport;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.HeapTrackingListValueBuilder;
import org.neo4j.values.virtual.ListValue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

// For dependency reasons (jamm) this test is located here while HeapTrackingListValueBuilder is in the values module

@ExtendWith( RandomExtension.class )
class HeapTrackingListValueBuilderTest
{
    @Inject
    private RandomSupport rnd;

    private final MemoryMeter meter = new MemoryMeter();
    private final MemoryTracker memoryTracker = new LocalMemoryTracker();
    private final HeapTrackingListValueBuilder listValueBuilder = HeapTrackingListValueBuilder.newHeapTrackingListBuilder( memoryTracker );

    @AfterEach
    void tearDown()
    {
        listValueBuilder.close();
        assertEquals( 0, memoryTracker.estimatedHeapMemory(), "Leaking memory" );
    }

    @Test
    void addAndIterateElements()
    {
        int iterations = rnd.nextInt( 10, 1000 );
        for ( int i = 0; i < iterations; i++ )
        {
            listValueBuilder.add( Values.longValue( i ) );
        }

        // Validate builder size
        long memoryTrackerActualSize = meter.measureDeep( memoryTracker );
        long actualBuilderSize = meter.measureDeep( listValueBuilder ) - memoryTrackerActualSize;
        long estimatedBuilderSize = memoryTracker.estimatedHeapMemory();
        assertEquals( actualBuilderSize, estimatedBuilderSize );

        // Validate value size
        ListValue listValue = listValueBuilder.build();
        long actualValueSize = meter.measureDeep( listValue ) - memoryTrackerActualSize;
        long estimatedValueSize = listValue.estimatedHeapUsage();
        assertEquals( actualValueSize, estimatedValueSize );

        // Validate items
        Iterator<AnyValue> iterator = listValue.iterator();
        for ( int i = 0; i < iterations; i++ )
        {
            assertTrue( iterator.hasNext() );
            assertEquals( i, ((LongValue) iterator.next()).longValue() );
        }
        assertFalse( iterator.hasNext() );
    }

    @Test
    void streamAndCollectElements()
    {
        int iterations = rnd.nextInt( 10, 1000 );
        ArrayList<LongValue> list = new ArrayList<>( iterations );
        for ( int i = 0; i < iterations; i++ )
        {
            list.add( Values.longValue( i ) );
        }

        var collector = HeapTrackingListValueBuilder.collector( memoryTracker );
        ListValue listValue = list.stream().collect( collector );

        // Validate value size
        long memoryTrackerActualSize = meter.measureDeep( memoryTracker );
        long actualValueSize = meter.measureDeep( listValue ) - memoryTrackerActualSize;
        long estimatedValueSize = listValue.estimatedHeapUsage();
        assertEquals( actualValueSize, estimatedValueSize );

        // Validate items
        Iterator<AnyValue> iterator = listValue.iterator();
        for ( int i = 0; i < iterations; i++ )
        {
            assertTrue( iterator.hasNext() );
            assertEquals( i, ((LongValue) iterator.next()).longValue() );
        }
        assertFalse( iterator.hasNext() );
    }

}
