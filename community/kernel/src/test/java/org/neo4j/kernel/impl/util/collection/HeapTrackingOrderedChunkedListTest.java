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
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryTracker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.impl.util.collection.HeapTrackingOrderedChunkedList.DEFAULT_CHUNK_SIZE;

class HeapTrackingOrderedChunkedListTest
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
    void shouldThrowIfNotPowerOfTwo()
    {
        assertThrows( IllegalArgumentException.class, () -> HeapTrackingOrderedChunkedList.createOrderedMap( memoryTracker, 3 ) );
    }

    @Test
    void addNullShouldBeTheSameAsAddingAndRemoving()
    {
        table.add( null );
        table.add( null );
        table.add( null );

        assertNull( table.get( 0 ) );
        assertNull( table.get( 1 ) );
        assertNull( table.get( 2 ) );
        assertNull( table.getFirst() );

        table.add( 42L );

        assertNull( table.get( 0 ) );
        assertNull( table.get( 1 ) );
        assertNull( table.get( 2 ) );
        assertEquals( 42L, table.get( 3 ) );
        assertEquals( 42L, table.getFirst() );

        table.add( null );
        table.add( null );
        table.add( null );

        assertNull( table.get( 4 ) );
        assertNull( table.get( 5 ) );
        assertNull( table.get( 6 ) );
        assertEquals( 42L, table.getFirst() );
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
                assertEquals( null, table.get( i ) );
            }
            else
            {
                assertEquals( Long.valueOf( i + 1 ), table.get( i ) );
            }
        }
    }

    @Test
    void foreach()
    {
        int size = 10000;

        // add 0, .., 4999
        for ( int i = 0; i < 5000; i++ )
        {
            table.add( (long) i );
        }

        // remove 0, .., 2499
        for ( int i = 0; i < 2500; i++ )
        {
            table.remove( i );
        }

        // add 5000, .., 9999
        for ( int i = 5000; i < size; i++ )
        {
            table.add( (long) i );
        }

        // remove 9901
        table.remove( 9901 );

        final AtomicInteger i = new AtomicInteger( 2500 );
        table.foreach( ( k, v ) ->
        {
            var expected = i.get();
            if ( expected >= 9901 ) // Has been removed
            {
                expected += 1;
            }
            assertEquals( expected, k );
            assertEquals( expected, v );
            i.getAndIncrement();
        } );
        assertEquals( i.get(), 9999 ); // One removed
    }

    @Test
    void removeAtChunkBoundaries()
    {
        int size = DEFAULT_CHUNK_SIZE;

        for ( int i = 0; i < size * 3 + 1; i++ )
        {
            table.add( (long) i );
        }

        // remove the entire second chunk
        for ( int i = size; i < size * 2; i++ )
        {
            table.remove( i );
        }

        // Remove the first element
        table.remove( 0 );

        // Remove the last element of chunk 1
        table.remove( size - 1 );

        // Remove the first element of chunk 3
        table.remove( size * 2 );

        // Test getFirst
        assertEquals( 1, table.getFirst() );

        // Test get
        assertNull( table.get( 0 ) );
        assertNull( table.get( size - 1 ) );
        assertNull( table.get( size * 2 ) );
        assertNull( table.get( size * 3 + 1 ) );
        assertEquals( 1, table.get( 1 ) );
        assertEquals( size * 2 + 1, table.get( size * 2 + 1 ) );
        assertEquals( size * 3, table.get( size * 3 ) );
        for ( int i = size; i < size * 2; i++ )
        {
            assertNull( table.get( i ) );
        }

        // Test foreach
        final AtomicInteger i = new AtomicInteger( 0 );
        table.foreach( ( k, v ) ->
        {
            assertEquals( k, v );
            assertNotEquals( 0, k );
            assertNotEquals( size - 1, k );
            assertNotEquals( size * 2, k );
            assertFalse( (k >= size && k < size * 2) || k > size * 3 + 1 );
            i.getAndIncrement();
        } );
        assertEquals( i.get(), size * 2 - 2 );

        // Test iterator
        Iterator<Long> it = table.iterator();
        long expected = 1; // First element was removed
        while ( it.hasNext() )
        {
            Long value = it.next();
            if ( expected == size - 1 )
            {
                expected += size + 2; // Removed one chunk plus two adjacent elements
            }
            assertEquals( expected, value );
            expected++;
        }
    }

    @Test
    void getFirstAddRemoveAllCycle()
    {
        long key = 0;

        assertNull( table.getFirst() );
        for ( int i = 0; i < 5000; i++ )
        {
            table.add( key++ );
        }
        for ( int i = 0; i < 5000; i++ )
        {
            table.remove( i );
        }
        assertNull( table.getFirst() );
        table.add( key );
        assertEquals( key, table.getFirst() );
        key++;
        for ( int i = 1; i < 5000; i++ )
        {
            table.add( key++ );
        }
        for ( int i = 0; i < 5000; i++ )
        {
            table.remove( i + 5000 );
        }
        assertNull( table.getFirst() );
        table.add( key );
        assertEquals( key, table.getFirst() );
    }

    @Test
    void getFirstAddRemoveChunkCycle()
    {
        int size = DEFAULT_CHUNK_SIZE;
        long key = 0;

        assertNull( table.getFirst() );
        for ( int i = 0; i < size; i++ )
        {
            table.add( key++ );
        }
        for ( int i = 0; i < size; i++ )
        {
            table.remove( i );
        }
        assertNull( table.getFirst() );
        table.add( key );
        assertEquals( key, table.getFirst() );
        key++;
        for ( int i = 1; i < size; i++ )
        {
            table.add( key++ );
        }
        for ( int i = 0; i < size; i++ )
        {
            table.remove( i + size );
        }
        assertNull( table.getFirst() );
        table.add( key );
        assertEquals( key, table.getFirst() );
    }

    @Test
    void valuesIterator()
    {
        int size = 10000;

        // add 0, .., 4999
        for ( int i = 0; i < size / 2; i++ )
        {
            table.add( (long) i );
        }

        // remove 0, .., 2499
        for ( int i = 0; i < size / 4; i++ )
        {
            table.remove( i );
        }

        // add 5000, .., 9999
        for ( int i = size / 2; i < size; i++ )
        {
            table.add( (long) i );
        }

        // remove 9901
        table.remove( 9901 );

        Iterator<Long> it = table.iterator();
        for ( int i = size / 4; i < size - 1; i++ )
        {
            assertTrue( it.hasNext() );
            Long entry = it.next();
            if ( i < 9901 ) // 9901 has been removed
            {
                assertEquals( i, entry );
            }
            else
            {
                assertEquals( i + 1, entry );
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
        long externalAllocation = meter.measure( value1 );
        memoryTracker.allocateHeap( externalAllocation );

        table.add( value1 );

        // Validate size
        long actualSize = meter.measureDeep( table ) - meter.measureDeep( memoryTracker );
        assertEquals( actualSize, memoryTracker.estimatedHeapMemory() );

        // Close should release everything related to the table
        table.close();
        assertEquals( externalAllocation, memoryTracker.estimatedHeapMemory() );

        memoryTracker.releaseHeap( externalAllocation );
    }
}
