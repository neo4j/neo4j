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
package org.neo4j.collection.trackable;

import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.github.jamm.MemoryMeter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryGroup;
import org.neo4j.memory.MemoryPool;
import org.neo4j.memory.MemoryPools;
import org.neo4j.memory.MemoryTracker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.collection.trackable.HeapTrackingUnifiedSet.arrayHeapSize;
import static org.neo4j.collection.trackable.HeapTrackingUnifiedSet.createUnifiedSet;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

class HeapTrackingUnifiedSetTest
{
    private static final long INTEGER_SIZE = shallowSizeOfInstance( Integer.class );
    private final MemoryMeter meter = new MemoryMeter();
    private MemoryPool memoryPool;
    private MemoryTracker memoryTracker;

    @BeforeEach
    void setUp()
    {
        memoryPool = new MemoryPools().pool( MemoryGroup.TRANSACTION, 0L, null );
        memoryTracker = new LocalMemoryTracker( memoryPool );
    }

    @Test
    void calculateEmptySize()
    {
        HeapTrackingUnifiedSet<Integer> unifiedSet = createUnifiedSet( memoryTracker );
        assertExactEstimation( unifiedSet );
        unifiedSet.close();
        assertEquals( 0, memoryTracker.estimatedHeapMemory() );
    }

    @Test
    void reactToGrowth()
    {
        long totalBytesIntegers = 0;
        HeapTrackingUnifiedSet<Integer> unifiedSet = createUnifiedSet( memoryTracker );
        assertExactEstimation( unifiedSet );
        for ( int i = 0; i < 10; i++ )
        {
            totalBytesIntegers += INTEGER_SIZE;
            memoryTracker.allocateHeap( INTEGER_SIZE );
            unifiedSet.add( i );
        }

        assertExactEstimation( unifiedSet );
        assertThat( memoryPool.usedHeap() ).isGreaterThanOrEqualTo( memoryTracker.estimatedHeapMemory() );

        unifiedSet.close();
        memoryTracker.releaseHeap( totalBytesIntegers );
        assertEquals( 0, memoryTracker.estimatedHeapMemory() );

        memoryTracker.reset();
        assertEquals( 0, memoryPool.usedHeap() );
    }

    @Test
    void handleLargeArraysWithoutOverflowing()
    {
        assertThat( arrayHeapSize( 536870912 ) ).isGreaterThan( 0L );
    }

    private void assertExactEstimation( UnifiedSet<?> unifiedSet )
    {
        assertEquals( meter.measureDeep( unifiedSet ) - meter.measureDeep( memoryTracker ), memoryTracker.estimatedHeapMemory() );
    }
}
