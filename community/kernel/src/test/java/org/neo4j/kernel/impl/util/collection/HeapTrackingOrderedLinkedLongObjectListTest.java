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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class HeapTrackingOrderedLinkedLongObjectListTest
{
    private final MemoryMeter meter = new MemoryMeter();
    private final MemoryTracker memoryTracker = new LocalMemoryTracker();

    private final HeapTrackingOrderedChunkedList<Long> table = HeapTrackingOrderedChunkedList.createOrderedMap( memoryTracker );

    @AfterEach
    void tearDown()
    {
        table.close();
        assertEquals( 0, memoryTracker.estimatedHeapMemory(), "Leaking memory" );
    }

    @Test
    void add()
    {
        for ( int i = 0; i < 10000; i++ )
        {
            table.add( Long.valueOf( i + 1 ) );
        }

        for ( int i = 10000 - 1; i >= 0; i-- )
        {
            assertEquals( Long.valueOf( i + 1 ), table.get( i ) );
        }
    }

    @Test
    void remove()
    {
        int size = 10000;

        // add 0, .., 4999
        for ( int i = 0; i < size / 2; i++ )
        {
            table.add( Long.valueOf( i + 1 ) );
        }

        // remove 0, .., 2499
        for ( int i = 0; i < size / 4; i++ )
        {
            table.remove( Long.valueOf( i ) );
        }

        // add 5000, .., 9999
        for ( int i = size / 2; i < size; i++ )
        {
            table.add( Long.valueOf( i + 1 ) );
        }

        // remove 9901
        table.remove( 9901 );

        for ( int i = 0; i < size; i++ )
        {
            if ( i < size / 4 || i == 9901 )
            {
                assertEquals( table.get( i ), null );
            }
            else
            {
                assertEquals( table.get( i ), Long.valueOf( i + 1 ) );
            }
        }
    }

    @Test
    void valuesIterator()
    {
        int size = 10000;

        // add 0, .., 4999
        for ( int i = 0; i < size / 2; i++ )
        {
            table.add( Long.valueOf( i + 1 ) );
        }

        // remove 0, .., 2499
        for ( int i = 0; i < size / 4; i++ )
        {
            table.remove( Long.valueOf( i ) );
        }

        // add 5000, .., 9999
        for ( int i = size / 2; i < size; i++ )
        {
            table.add( Long.valueOf( i + 1 ) );
        }

        // remove 9901
        table.remove( 9901 );

        Iterator<Long> it = table.iterator();
        for ( int i = size / 4; i < size; i++ )
        {
            if ( i != 9901 ) // 9901 has been removed
            {
                assert (it.hasNext());
                Long entry = it.next();
                Long key = Long.valueOf( i );
                assertEquals( key + 1, entry );
            }
        }
        assertFalse( it.hasNext() );
    }

    @Test
    void emptySize()
    {
        long actual = meter.measureDeep( table ) - meter.measureDeep( memoryTracker );
        assertEquals( actual, memoryTracker.estimatedHeapMemory() );
    }

    @Test
    void closeShouldReleaseEverything()
    {
        var value1 = 11L;
        // Allocate outside of table
        long externalAllocation = meter.measure(value1);
        memoryTracker.allocateHeap( externalAllocation );

        table.add(value1);

        // Validate size
        long actualSize = meter.measureDeep( table ) - meter.measureDeep( memoryTracker );
        assertEquals( actualSize, memoryTracker.estimatedHeapMemory() );

        // Close should release everything related to the table
        table.close();
        assertEquals( externalAllocation, memoryTracker.estimatedHeapMemory() );

        memoryTracker.releaseHeap( externalAllocation );
    }
}
