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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.memory.MemoryPools.NO_TRACKING;
import static org.neo4j.values.virtual.HeapTrackingMapValueBuilder.newHeapTrackingMapValueBuilder;

import org.github.jamm.MemoryMeter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.values.storable.IntValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.HeapTrackingMapValueBuilder;

// For dependency reasons (jamm) this test is located here while HeapTrackingListValueBuilder is in the values module

class HeapTrackingMapValueBuilderTest
{
    private final MemoryMeter meter = new MemoryMeter();
    private MemoryTracker memoryTracker;
    private HeapTrackingMapValueBuilder builder;

    @BeforeEach
    void setup()
    {
        this.memoryTracker = new LocalMemoryTracker( NO_TRACKING );
        this.builder = newHeapTrackingMapValueBuilder( memoryTracker );
    }

    @AfterEach
    void tearDown()
    {
        builder.close();
        assertEquals( 0, memoryTracker.estimatedHeapMemory(), "Leaking memory" );
    }

    @Test
    void calculateEmptySize()
    {
        assertExactEstimation( 0 );
    }

    @Test
    void calculateSingleItem()
    {
        final var key = "a";
        builder.put( key, Values.stringValue( "a".repeat( 128 ) ) );
        assertExactEstimation( HeapEstimator.sizeOf( key ) );
    }

    @Test
    void addAndIterateElements()
    {
        int iterations = 10;
        long unaccounted = 0;

        for ( int i = 0; i < iterations; i++ )
        {
            final var key = String.valueOf( i );
            builder.put( key, Values.of( i ) );
            unaccounted += HeapEstimator.sizeOf( key );
        }

        // Validate builder size
        assertExactEstimation( unaccounted );

        // Validate items
        final var mapValue = builder.build();
        assertEquals( iterations, mapValue.size() );
        for ( int i = 0; i < iterations; i++ )
        {
            assertTrue( mapValue.containsKey( String.valueOf( i ) ) );
            assertEquals( i, ((IntValue) mapValue.get( String.valueOf( i ) ) ).intValue() );
        }
    }

    void assertExactEstimation( long unaccounted )
    {
        assertEquals(
                meter.measureDeep(builder) - meter.measureDeep(memoryTracker),
                memoryTracker.estimatedHeapMemory() + builder.getUnAllocatedHeapSize() + unaccounted );
    }
}
