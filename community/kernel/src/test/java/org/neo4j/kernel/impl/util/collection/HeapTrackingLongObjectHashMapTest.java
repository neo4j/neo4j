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
package org.neo4j.kernel.impl.util.collection;

import org.github.jamm.MemoryMeter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryTracker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.kernel.impl.util.collection.HeapTrackingLongObjectHashMap.createLongObjectHashMap;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

class HeapTrackingLongObjectHashMapTest
{
    private static final long INTEGER_SIZE = shallowSizeOfInstance( Integer.class );
    private MemoryMeter meter = new MemoryMeter();
    private MemoryTracker globalTracker;
    private MemoryTracker memoryTracker;

    @BeforeEach
    void setUp()
    {
        globalTracker = new LocalMemoryTracker();
        memoryTracker = new LocalMemoryTracker( globalTracker );
    }

    @Test
    void calculateEmptySize()
    {
        HeapTrackingLongObjectHashMap<Object> longObjectHashMap = createLongObjectHashMap( memoryTracker );
        assertExactEstimation( longObjectHashMap );
        longObjectHashMap.close();
        assertEquals( 0, memoryTracker.estimatedHeapMemory() );
    }

    @Test
    void reactToGrowth()
    {
        long totalBytesIntegers = 0;
        HeapTrackingLongObjectHashMap<Integer> longObjectHashMap = createLongObjectHashMap( memoryTracker );
        assertExactEstimation( longObjectHashMap );
        long emptySize = memoryTracker.estimatedHeapMemory();
        // We avoid 0 and 1 since they are sentinel values and we don't track them
        for ( int i = 2; i <= 10; i++ )
        {
            totalBytesIntegers += INTEGER_SIZE;
            memoryTracker.allocateHeap( INTEGER_SIZE );
            longObjectHashMap.put( i, i );
        }

        assertExactEstimation( longObjectHashMap );
        assertThat( memoryTracker.estimatedHeapMemory() ).isGreaterThan( emptySize );
        assertThat( globalTracker.estimatedHeapMemory() ).isGreaterThanOrEqualTo( memoryTracker.estimatedHeapMemory() );

        longObjectHashMap.close();
        memoryTracker.releaseHeap( totalBytesIntegers );
        assertEquals( 0, memoryTracker.estimatedHeapMemory() );

        memoryTracker.reset();
        assertEquals( 0, globalTracker.estimatedHeapMemory() );
    }

    private void assertExactEstimation( HeapTrackingLongObjectHashMap<?> longObjectHashMap )
    {
        assertEquals( meter.measureDeep( longObjectHashMap ) - meter.measureDeep( memoryTracker ), memoryTracker.estimatedHeapMemory() );
    }
}
