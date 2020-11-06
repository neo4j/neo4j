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

import org.neo4j.memory.HeapEstimator;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryTracker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.kernel.impl.util.collection.HeapTrackingLongEnumerationList.DEFAULT_CHUNK_SIZE;

class HeapTrackingLongEnumerationListTest
{
    private final MemoryMeter meter = new MemoryMeter();
    private final MemoryTracker memoryTracker = new LocalMemoryTracker();
    private final long measuredMemoryTracker = meter.measureDeep( memoryTracker );

    private final HeapTrackingLongEnumerationList<Long> table = HeapTrackingLongEnumerationList.create( memoryTracker );

    @AfterEach
    void tearDown()
    {
        table.close();
        assertEquals( 0, memoryTracker.estimatedHeapMemory(), "Leaking memory" );
    }

    @Test
    void shouldThrowIfChunkSizeNotPowerOfTwo()
    {
        assertThrows( IllegalArgumentException.class, () -> HeapTrackingLongEnumerationList.create( memoryTracker, 3 ) );
    }

    @Test
    void add()
    {
        // When
        for ( long i = 0; i < 10000; i++ )
        {
            table.add( i + 1 );
        }

        // Then
        for ( long i = 10000 - 1; i >= 0; i-- )
        {
            assertEquals( i + 1, table.get( i ) );
        }

        assertHeapUsageWithNumberOfLongs( 10000 );
    }

    @Test
    void addNullShouldBeTheSameAsAddingAndRemoving()
    {
        // When
        table.add( null );
        table.add( null );
        table.add( null );

        // Then
        assertNull( table.get( 0 ) );
        assertNull( table.get( 1 ) );
        assertNull( table.get( 2 ) );
        assertNull( table.getFirst() );
        table.foreach( ( k, v ) -> fail() );
        assertFalse( table.valuesIterator().hasNext() );
        assertHeapUsageWithNumberOfLongs( 0 );

        // When
        table.add( 42L );

        // Then
        assertNull( table.get( 0 ) );
        assertNull( table.get( 1 ) );
        assertNull( table.get( 2 ) );
        assertEquals( 42L, table.get( 3 ) );
        assertEquals( 42L, table.getFirst() );
        table.foreach( ( k, v ) -> assertEquals( 42L, v ) );
        Iterator<Long> it = table.valuesIterator();
        assertTrue( it.hasNext() );
        assertEquals( 42L, it.next() );
        assertFalse( it.hasNext() );
        assertHeapUsageWithNumberOfLongs( 1 );

        // When
        table.add( null );
        table.add( null );
        table.add( null );

        // Then
        assertNull( table.get( 4 ) );
        assertNull( table.get( 5 ) );
        assertNull( table.get( 6 ) );
        assertEquals( 42L, table.getFirst() );
        table.foreach( ( k, v ) -> assertEquals( 42L, v ) );
        Iterator<Long> it2 = table.valuesIterator();
        assertTrue( it2.hasNext() );
        assertEquals( 42L, it2.next() );
        assertFalse( it2.hasNext() );
        assertHeapUsageWithNumberOfLongs( 1 );

        // When
        table.remove( 3 );

        // Then
        assertNull( table.get( 0 ) );
        assertNull( table.get( 1 ) );
        assertNull( table.get( 2 ) );
        assertNull( table.getFirst() );
        table.foreach( ( k, v ) -> fail() );
        assertFalse( table.valuesIterator().hasNext() );
        assertHeapUsageWithNumberOfLongs( 0 );
    }

    @Test
    void remove()
    {
        long size = 10000;

        // add 0, .., 4999
        for ( long i = 0; i < size / 2; i++ )
        {
            table.add( i + 1 );
        }
        assertHeapUsageWithNumberOfLongs( 5000 );

        // remove 0, .., 2499
        for ( long i = 0; i < size / 4; i++ )
        {
            table.remove( i );
        }
        assertHeapUsageWithNumberOfLongs( 2500 );

        // add 5000, .., 9999
        for ( long i = size / 2; i < size; i++ )
        {
            table.add( i + 1 );
        }
        assertHeapUsageWithNumberOfLongs( 7500 );

        // remove 9901
        table.remove( 9901 );

        for ( long i = 0; i < size; i++ )
        {
            if ( i < size / 4 || i == 9901 )
            {
                assertNull( table.get( i ) );
            }
            else
            {
                assertEquals( i + 1, table.get( i ) );
            }
        }
        assertHeapUsageWithNumberOfLongs( 7499 );
    }

    @Test
    void removeAtChunkBoundaries()
    {
        int size = Math.max( DEFAULT_CHUNK_SIZE, 4 ); // This test will not work for chunk sizes 1 and 2

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
        Iterator<Long> it = table.valuesIterator();
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

        // Test heap usage estimation
        assertHeapUsageWithNumberOfLongs( size * 2 - 2 );
    }

    @Test
    void foreach()
    {
        long size = 10000;

        // add 0, .., 4999
        for ( long i = 0; i < 5000; i++ )
        {
            table.add( i );
        }

        // remove 0, .., 2499
        for ( long i = 0; i < 2500; i++ )
        {
            table.remove( i );
        }

        // add 5000, .., 9999
        for ( long i = 5000; i < size; i++ )
        {
            table.add( i );
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
    void getFirstAddRemoveAllCycle()
    {
        long key = 0;

        assertNull( table.getFirst() );
        for ( int i = 0; i < 5000; i++ )
        {
            table.add( key++ );
        }
        long measuredAfterAdd1 = meter.measureDeep( table ) - measuredMemoryTracker;
        long estimatedAfterAdd1 = memoryTracker.estimatedHeapMemory() + 5000 * HeapEstimator.LONG_SIZE;

        for ( int i = 0; i < 5000; i++ )
        {
            table.remove( i );
        }
        long measuredAfterRemove1 = meter.measureDeep( table ) - measuredMemoryTracker;
        long estimatedAfterRemove1 = memoryTracker.estimatedHeapMemory();

        assertNull( table.getFirst() );
        table.add( key );
        assertEquals( key, table.getFirst() );
        key++;
        for ( int i = 1; i < 5000; i++ )
        {
            table.add( key++ );
        }
        long measuredAfterAdd2 = meter.measureDeep( table ) - measuredMemoryTracker;
        long estimatedAfterAdd2 = memoryTracker.estimatedHeapMemory() + 5000 * HeapEstimator.LONG_SIZE;

        for ( int i = 0; i < 5000; i++ )
        {
            table.remove( i + 5000 );
        }
        long measuredAfterRemove2 = meter.measureDeep( table ) - measuredMemoryTracker;
        long estimatedAfterRemove2 = memoryTracker.estimatedHeapMemory();

        assertNull( table.getFirst() );
        table.add( key );
        assertEquals( key, table.getFirst() );
        long measured = meter.measureDeep( table ) - measuredMemoryTracker;

        // Memory estimation should be accurate
        assertEquals( measuredAfterAdd1, estimatedAfterAdd1 );
        assertEquals( measuredAfterAdd2, estimatedAfterAdd2 );
        assertEquals( measured, memoryTracker.estimatedHeapMemory() + HeapEstimator.LONG_SIZE );

        // Memory usage should not increase
        assertEquals( measuredAfterAdd1, measuredAfterAdd2 );
        assertEquals( measuredAfterRemove1, measuredAfterRemove2 );
        assertEquals( estimatedAfterAdd1, estimatedAfterAdd2 );
        assertEquals( estimatedAfterRemove1, estimatedAfterRemove2 );
    }

    @Test
    void getFirstAddRemoveChunkCycle()
    {
        int size = DEFAULT_CHUNK_SIZE;
        long key = 0;

        long measuredEmpty = meter.measureDeep( table ) - measuredMemoryTracker;
        long estimatedEmpty = memoryTracker.estimatedHeapMemory();

        assertNull( table.getFirst() );
        for ( int i = 0; i < size; i++ )
        {
            table.add( key++ );
        }
        long measuredAfterAdd1 = meter.measureDeep( table ) - measuredMemoryTracker;
        long estimatedAfterAdd1 = memoryTracker.estimatedHeapMemory() + size * HeapEstimator.LONG_SIZE;

        for ( int i = 0; i < size; i++ )
        {
            table.remove( i );
        }
        long measuredAfterRemove1 = meter.measureDeep( table ) - measuredMemoryTracker;
        long estimatedAfterRemove1 = memoryTracker.estimatedHeapMemory();

        assertNull( table.getFirst() );
        table.add( key );
        assertEquals( key, table.getFirst() );
        key++;
        for ( int i = 1; i < size; i++ )
        {
            table.add( key++ );
        }
        long measuredAfterAdd2 = meter.measureDeep( table ) - measuredMemoryTracker;
        long estimatedAfterAdd2 = memoryTracker.estimatedHeapMemory() + size * HeapEstimator.LONG_SIZE;

        for ( int i = 0; i < size; i++ )
        {
            table.remove( i + size );
        }
        long measuredAfterRemove2 = meter.measureDeep( table ) - measuredMemoryTracker;
        long estimatedAfterRemove2 = memoryTracker.estimatedHeapMemory();

        assertNull( table.getFirst() );
        table.add( key );
        assertEquals( key, table.getFirst() );
        long measuredLast = meter.measureDeep( table ) - measuredMemoryTracker;
        long estimatedLast = memoryTracker.estimatedHeapMemory() + HeapEstimator.LONG_SIZE;

        // Memory estimation should be accurate
        assertEquals( measuredAfterAdd1, estimatedAfterAdd1 );
        assertEquals( measuredAfterRemove1, estimatedAfterRemove1 );
        assertEquals( measuredAfterAdd2, estimatedAfterAdd2 );
        assertEquals( measuredAfterRemove2, estimatedAfterRemove2 );
        assertEquals( measuredLast, estimatedLast );

        // Memory usage should not increase
        assertEquals( measuredAfterAdd1, measuredAfterAdd2 );
        assertEquals( measuredAfterRemove1, measuredAfterRemove2 );
        assertEquals( estimatedAfterAdd1, estimatedAfterAdd2 );
        assertEquals( estimatedAfterRemove1, estimatedAfterRemove2 );

        // Memory usage of internal structure should also not increase when adding unless a new chunk is needed
        assertEquals( measuredEmpty, measuredAfterAdd1 - size * HeapEstimator.LONG_SIZE );
        assertEquals( measuredEmpty, measuredAfterRemove1 );
        assertEquals( measuredEmpty, measuredAfterAdd2 - size * HeapEstimator.LONG_SIZE );
        assertEquals( measuredEmpty, measuredAfterRemove2 );

        assertEquals( estimatedEmpty, estimatedAfterAdd1 - size * HeapEstimator.LONG_SIZE );
        assertEquals( estimatedEmpty, estimatedAfterRemove1 );
        assertEquals( estimatedEmpty, estimatedAfterAdd2 - size * HeapEstimator.LONG_SIZE );
        assertEquals( estimatedEmpty, estimatedAfterRemove2 );
    }

    @Test
    void getWhenEmpty()
    {
        assertNull( table.get( -1 ) );
        assertNull( table.get( 0 ) );
        assertNull( table.get( DEFAULT_CHUNK_SIZE - 1 ) );
        assertNull( table.get( DEFAULT_CHUNK_SIZE ) );
    }

    @Test
    void valuesIterator()
    {
        long size = 10000;

        // add 0, .., 4999
        for ( long i = 0; i < size / 2; i++ )
        {
            table.add( i );
        }

        // remove 0, .., 2499
        for ( long i = 0; i < size / 4; i++ )
        {
            table.remove( i );
        }

        // add 5000, .., 9999
        for ( long i = size / 2; i < size; i++ )
        {
            table.add( i );
        }

        // remove 9901
        table.remove( 9901 );

        Iterator<Long> it = table.valuesIterator();
        for ( long i = size / 4; i < size - 1; i++ )
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
        assertHeapUsageWithNumberOfLongs( 0 );
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
        long actualSize = meter.measureDeep( table ) - measuredMemoryTracker;
        assertEquals( actualSize, memoryTracker.estimatedHeapMemory() );

        // Close should release everything related to the table
        table.close();
        assertEquals( externalAllocation, memoryTracker.estimatedHeapMemory() );

        memoryTracker.releaseHeap( externalAllocation );
    }

    private void assertHeapUsageWithNumberOfLongs( long numberOfBoxedLongs )
    {
        long measured = meter.measureDeep( table ) - measuredMemoryTracker;
        assertEquals( measured, memoryTracker.estimatedHeapMemory() + numberOfBoxedLongs * HeapEstimator.LONG_SIZE );
    }
}
