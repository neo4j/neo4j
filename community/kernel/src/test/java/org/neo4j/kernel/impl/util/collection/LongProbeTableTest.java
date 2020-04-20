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

import java.util.Iterator;

import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.Values;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LongProbeTableTest
{
    private final MemoryMeter meter = new MemoryMeter();
    private final MemoryTracker memoryTracker = new LocalMemoryTracker();

    private final LongProbeTable<LongValue> table = LongProbeTable.createLongProbeTable( memoryTracker );

    @AfterEach
    void tearDown()
    {
        table.close();
        assertEquals( 0, memoryTracker.estimatedHeapMemory(), "Leaking memory" );
    }

    @Test
    void emptySize()
    {
        long actual = meter.measureDeep( table ) - meter.measureDeep( memoryTracker );
        assertEquals( actual, memoryTracker.estimatedHeapMemory() );
    }

    @Test
    void countInternalStructure()
    {
        // We avoid key 0 and 1 since they are sentinel values and we don't track them
        table.put( 2, Values.longValue( 1L ) );
        table.put( 2, Values.longValue( 2L ) );
        table.put( 3, Values.longValue( 3L ) );

        // Validate size
        long itemSize = meter.measure( 1L ) * 3;
        long actualSize = meter.measureDeep( table ) - meter.measureDeep( memoryTracker );
        assertEquals( actualSize, memoryTracker.estimatedHeapMemory() );

        // Validate content
        Iterator<LongValue> iterator2 = table.get( 2 );
        assertTrue( iterator2.hasNext() );
        assertEquals( 1, iterator2.next().longValue() );
        assertTrue( iterator2.hasNext() );
        assertEquals( 2, iterator2.next().longValue() );
        assertFalse( iterator2.hasNext() );

        Iterator<LongValue> iterator3 = table.get( 3 );
        assertTrue( iterator3.hasNext() );
        assertEquals( 3, iterator3.next().longValue() );
        assertFalse( iterator3.hasNext() );
    }

    @Test
    void closeShouldReleaseEverything()
    {
        // Allocate outside of table
        long externalAllocation = 113L;
        memoryTracker.allocateHeap( externalAllocation );

        // We avoid key 0 and 1 since they are sentinel values and we don't track them
        table.put( 2, Values.longValue( 1L ) );
        table.put( 2, Values.longValue( 2L ) );
        table.put( 3, Values.longValue( 3L ) );

        // Validate size
        long itemSize = meter.measure( 1L ) * 3;
        long actualSize = meter.measureDeep( table ) - meter.measureDeep( memoryTracker );
        assertEquals( actualSize + externalAllocation, memoryTracker.estimatedHeapMemory() );

        // Close should release everything related to the table
        table.close();
        assertEquals( externalAllocation, memoryTracker.estimatedHeapMemory() );

        memoryTracker.releaseHeap( externalAllocation );
    }
}
