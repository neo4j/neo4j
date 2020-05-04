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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.Values;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DistinctSetTest
{
    private final MemoryMeter meter = new MemoryMeter();
    private final MemoryTracker memoryTracker = new LocalMemoryTracker();

    private final DistinctSet<LongValue> distinctSet = DistinctSet.createDistinctSet( memoryTracker );

    @AfterEach
    void tearDown()
    {
        distinctSet.close();
        assertEquals( 0, memoryTracker.estimatedHeapMemory(), "Leaking memory" );
    }

    @Test
    void emptySize()
    {
        long actual = meter.measureDeep( distinctSet ) - meter.measureDeep( memoryTracker );
        assertEquals( actual, memoryTracker.estimatedHeapMemory() );
    }

    @Test
    void countInternalStructure()
    {
        boolean added1 = distinctSet.add( Values.longValue( 0L ) );
        boolean added2 = distinctSet.add( Values.longValue( 0L ) );
        boolean added3 = distinctSet.add( Values.longValue( 1L ) );

        // Validate size
        long actualSize = meter.measureDeep( distinctSet ) - meter.measureDeep( memoryTracker );
        assertEquals( actualSize, memoryTracker.estimatedHeapMemory() );

        // Validate content
        assertTrue( added1 );
        assertFalse( added2 );
        assertTrue( added3 );
    }

    @Test
    void closeShouldReleaseEverything()
    {
        // Allocate outside of set
        long externalAllocation = 113L;
        memoryTracker.allocateHeap( externalAllocation );

        distinctSet.add( Values.longValue( 0L ) );
        distinctSet.add( Values.longValue( 0L ) );
        distinctSet.add( Values.longValue( 1L ) );

        // Validate size
        long actualSize = meter.measureDeep( distinctSet ) - meter.measureDeep( memoryTracker );
        assertEquals( actualSize + externalAllocation, memoryTracker.estimatedHeapMemory() );

        // Close should release everything related to the table
        distinctSet.close();
        assertEquals( externalAllocation, memoryTracker.estimatedHeapMemory() );

        memoryTracker.releaseHeap( externalAllocation );
    }
}
