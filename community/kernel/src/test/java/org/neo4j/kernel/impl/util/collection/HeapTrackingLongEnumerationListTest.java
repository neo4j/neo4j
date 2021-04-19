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
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.neo4j.memory.HeapEstimator;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.kernel.impl.util.collection.HeapTrackingLongEnumerationList.DEFAULT_CHUNK_SIZE;
import static org.neo4j.kernel.impl.util.collection.HeapTrackingLongEnumerationListTest.ListOperation.ADD;
import static org.neo4j.kernel.impl.util.collection.HeapTrackingLongEnumerationListTest.ListOperation.REMOVE;

@ExtendWith( RandomExtension.class )
class HeapTrackingLongEnumerationListTest
{
    private final MemoryMeter meter = new MemoryMeter();
    private final MemoryTracker memoryTracker = new LocalMemoryTracker();
    private final long measuredMemoryTracker = meter.measureDeep( memoryTracker );

    @Inject
    private RandomRule random;

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

        assertEquals( 9999L, table.lastKey() );
        assertHeapUsageWithNumberOfLongs( 10000 );
    }

    @Test
    void put()
    {
        // When
        for ( long i = 0; i < 10000; i += 5 )
        {
            assertNull( table.put( i, i + 1 ) );
        }

        // Then
        for ( long i = 10000 - 1; i >= 0; i-- )
        {
            if ( i % 5 == 0 )
            {
                assertEquals( i + 1, table.get( i ) );
            }
            else
            {
                assertNull( table.get( i ) );
            }
        }

        assertEquals( 9995L, table.lastKey() );
        assertHeapUsageWithNumberOfLongs( 2000 );
    }

    @Test
    void shouldThrowIfPutBeforeFirstKey()
    {
        assertNull( table.put( 1, 1L ) );
        assertThrows( IndexOutOfBoundsException.class, () -> table.put( 0, 0L ) );
    }

    @Test
    void putOutOfOrder()
    {
        table.put( 10, 10L );
        table.put( 20, 20L );
        assertNull( table.put( 15, 14L ) );
        assertEquals( table.put( 15, 15L ), 14L ); // Replace
        assertContainsOnly( table, new long[]{10L, 15L, 20L} );
        assertThrows( IndexOutOfBoundsException.class, () -> table.put( 9, 9L ) );
    }

    @Test
    void putOutOfOrderSparse()
    {
        table.put( 1000, 1000L );
        table.put( 8000, 8000L );
        table.put( 4000, 4000L );
        assertContainsOnly( table, new long[]{1000L, 4000L, 8000L} );
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
        assertEquals( 2L, table.lastKey() );
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
        assertEquals( 3L, table.lastKey() );
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
        assertEquals( 6L, table.lastKey() );
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
        assertEquals( 6L, table.lastKey() );
        table.foreach( ( k, v ) -> fail() );
        assertFalse( table.valuesIterator().hasNext() );
        assertHeapUsageWithNumberOfLongs( 0 );
    }

    @Test
    void putNullShouldBeTheSameAsAddingAndRemoving()
    {
        // When
        table.put( 0L, null );
        table.put( 1L, null );
        table.put( 2L, null );

        // Then
        assertNull( table.get( 0 ) );
        assertNull( table.get( 1 ) );
        assertNull( table.get( 2 ) );
        assertNull( table.getFirst() );
        assertEquals( 2L, table.lastKey() );
        table.foreach( ( k, v ) -> fail() );
        assertFalse( table.valuesIterator().hasNext() );
        assertHeapUsageWithNumberOfLongs( 0 );

        // When
        table.put( 5L, 42L );

        // Then
        assertNull( table.get( 0 ) );
        assertNull( table.get( 1 ) );
        assertNull( table.get( 2 ) );
        assertNull( table.get( 3 ) );
        assertNull( table.get( 4 ) );
        assertEquals( 42L, table.get( 5 ) );
        assertEquals( 42L, table.getFirst() );
        assertEquals( 5L, table.lastKey() );
        table.foreach( ( k, v ) -> assertEquals( 42L, v ) );
        Iterator<Long> it = table.valuesIterator();
        assertTrue( it.hasNext() );
        assertEquals( 42L, it.next() );
        assertFalse( it.hasNext() );
        assertHeapUsageWithNumberOfLongs( 1 );

        // When
        table.put( 8L, null );
        table.put( 6L, null );
        table.put( 7L, null );

        // Then
        assertNull( table.get( 6 ) );
        assertNull( table.get( 7 ) );
        assertNull( table.get( 8 ) );
        assertEquals( 42L, table.getFirst() );
        assertEquals( 8L, table.lastKey() );
        table.foreach( ( k, v ) -> assertEquals( 42L, v ) );
        Iterator<Long> it2 = table.valuesIterator();
        assertTrue( it2.hasNext() );
        assertEquals( 42L, it2.next() );
        assertFalse( it2.hasNext() );
        assertHeapUsageWithNumberOfLongs( 1 );

        // When
        table.remove( 5 );

        // Then
        assertNull( table.get( 0 ) );
        assertNull( table.get( 1 ) );
        assertNull( table.get( 2 ) );
        assertNull( table.get( 3 ) );
        assertNull( table.get( 4 ) );
        assertNull( table.getFirst() );
        assertEquals( 8L, table.lastKey() );
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
        assertEquals( 4999L, table.lastKey() );
        assertHeapUsageWithNumberOfLongs( 5000 );

        // remove 0, .., 2499
        for ( long i = 0; i < size / 4; i++ )
        {
            table.remove( i );
        }
        assertEquals( 4999L, table.lastKey() );
        assertHeapUsageWithNumberOfLongs( 2500 );

        // add 5000, .., 9999
        for ( long i = size / 2; i < size; i++ )
        {
            table.add( i + 1 );
        }
        assertEquals( 9999L, table.lastKey() );
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
    void removeUntil()
    {
        long size = 10000;

        // add 0, .., 9999
        for ( long i = 0; i < size; i++ )
        {
            table.add( i + 100000 );
        }
        assertEquals( 9999L, table.lastKey() );
        assertHeapUsageWithNumberOfLongs( 10000 );

        // remove 2500-7500
        for ( long i = size / 4; i < (size - size / 4); i++ )
        {
            table.remove( i );
        }
        assertEquals( 9999L, table.lastKey() );
        assertHeapUsageWithNumberOfLongs( 5000 );

        // remove until 9000
        final long[] ia = new long[]{0L};
        table.removeUntil( 9000, ( removedKey, removedValue ) ->
        {
            assertEquals( ia[0], removedKey );
            assertEquals( ia[0] + 100000, removedValue );
            ia[0]++;
            if ( ia[0] == 2500 )
            {
                ia[0] += 5000;
            }
        } );
        assertEquals( 9999L, table.lastKey() );
        assertHeapUsageWithNumberOfLongs( 1000 );

        table.removeUntil( 20000L, ( removedKey, removedValue ) ->
        {
            assertEquals( ia[0], removedKey );
            assertEquals( ia[0] + 100000, removedValue );
            ia[0]++;
        } );
        assertEquals( 9999L, table.lastKey() );
        assertHeapUsageWithNumberOfLongs( 0 );
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
    void addRemoveScenario()
    {
        MemoryTracker memoryTracker = new LocalMemoryTracker();
        HeapTrackingLongEnumerationList<Long> table = HeapTrackingLongEnumerationList.create( memoryTracker, 4 );

        assertEmpty( table );
        table.add( 0L );
        table.add( 1L );
        table.add( 2L );
        assertContainsOnly( table, 0, 2 );

        table.remove( 0L );
        table.remove( 1L );
        assertContainsOnly( table, 2 );

        table.remove( 2 );
        assertEmpty( table );

        table.add( 3L );
        table.add( 4L );
        table.add( 5L );
        assertContainsOnly( table, 3, 5 );

        table.remove( 5 );
        assertContainsOnly( table, 3, 4 );

        table.remove( 4 );
        assertContainsOnly( table, 3 );

        long measured1 = meter.measureDeep( table ) - measuredMemoryTracker;
        assertEquals( measured1, memoryTracker.estimatedHeapMemory() + HeapEstimator.LONG_SIZE );

        table.add( 6L );
        assertContainsOnly( table, new long[]{3L, 6L} );
        long measured2 = meter.measureDeep( table ) - measuredMemoryTracker;
        assertEquals( measured2, memoryTracker.estimatedHeapMemory() + 2 * HeapEstimator.LONG_SIZE );
        assertEquals( HeapEstimator.LONG_SIZE, measured2 - measured1 );

        table.add( 7L ); // New chunk needed
        assertContainsOnly( table, new long[]{3L, 6, 7L} );
        table.add( 8L ); // New chunk allocated because of poor alignment in tail chunks
        assertContainsOnly( table, new long[]{3L, 6L, 7L, 8L} );

        table.remove( 6L );
        assertContainsOnly( table, new long[]{3L, 7L, 8L} );
        table.remove( 3L ); // Should recycle chunk
        assertContainsOnly( table, new long[]{7L, 8L} );

        table.remove( 7L ); // Should recycle chunk
        assertContainsOnly( table, 8L );

        // Memory should be back to one chunk + one value
        long measured3 = meter.measureDeep( table ) - measuredMemoryTracker;
        assertEquals( measured3, memoryTracker.estimatedHeapMemory() + HeapEstimator.LONG_SIZE );
        assertEquals( measured1, measured3 );

        table.add( 9L );
        table.add( 10L );
        table.add( 11L );
        assertContainsOnly( table, 8, 11 );
        table.remove( 8L );
        table.remove( 10L );
        table.remove( 11L );
        assertContainsOnly( table, 9 );
        table.add( 12L );
        assertContainsOnly( table, new long[]{9L, 12L} );
        table.remove( 9L );
        assertContainsOnly( table, 12 );

        // Memory should be one chunk + one value
        long measured4 = meter.measureDeep( table ) - measuredMemoryTracker;
        assertEquals( measured4, memoryTracker.estimatedHeapMemory() + HeapEstimator.LONG_SIZE );
        assertEquals( measured3, measured4 );

        table.close();
    }

    @Test
    void addPutRemoveScenario()
    {
        MemoryTracker memoryTracker = new LocalMemoryTracker();
        HeapTrackingLongEnumerationList<Long> table = HeapTrackingLongEnumerationList.create( memoryTracker, 4 );

        assertEmpty( table );
        table.add( 0L );
        table.put( 2L, 2L );
        table.put( 1L, 1L );
        assertContainsOnly( table, 0, 2 );

        table.remove( 0L );
        table.remove( 1L );
        assertContainsOnly( table, 2 );

        table.remove( 2 );
        assertEmpty( table );

        table.add( 9999L );
        table.put( 4L, 4L );
        assertEquals( table.put( 3L, 3L ), 9999L ); // Replace
        table.add( 5L );
        assertContainsOnly( table, 3, 5 );

        table.remove( 5 );
        assertContainsOnly( table, 3, 4 );

        table.remove( 4 );
        assertContainsOnly( table, 3 );

        long measured1 = meter.measureDeep( table ) - measuredMemoryTracker;
        assertEquals( measured1, memoryTracker.estimatedHeapMemory() + HeapEstimator.LONG_SIZE );

        table.put( 6L, 6L );
        assertContainsOnly( table, new long[]{3L, 6L} );
        long measured2 = meter.measureDeep( table ) - measuredMemoryTracker;
        assertEquals( measured2, memoryTracker.estimatedHeapMemory() + 2 * HeapEstimator.LONG_SIZE );
        assertEquals( HeapEstimator.LONG_SIZE, measured2 - measured1 );

        table.put( 8L, 8L ); // New chunk needed and extra chunk allocated because of poor alignment in tail chunks
        assertContainsOnly( table, new long[]{3L, 6L, 8L} );
        table.put( 7L, 7L );
        assertContainsOnly( table, new long[]{3L, 6L, 7L, 8L} );

        table.remove( 6L );
        assertContainsOnly( table, new long[]{3L, 7L, 8L} );
        table.remove( 3L ); // Should recycle chunk
        assertContainsOnly( table, new long[]{7L, 8L} );

        table.remove( 7L ); // Should recycle chunk
        assertContainsOnly( table, 8L );

        // Memory should be back to one chunk + one value
        long measured3 = meter.measureDeep( table ) - measuredMemoryTracker;
        assertEquals( measured3, memoryTracker.estimatedHeapMemory() + HeapEstimator.LONG_SIZE );
        assertEquals( measured1, measured3 );

        table.put( 10L, 10L );
        table.put( 9L, 9L );
        table.add( 11L );
        assertContainsOnly( table, 8, 11 );
        table.remove( 8L );
        table.remove( 10L );
        table.remove( 11L );
        assertContainsOnly( table, 9 );
        table.put( 12L, 12L );
        assertContainsOnly( table, new long[]{9L, 12L} );
        table.remove( 9L );
        assertContainsOnly( table, 12 );

        // Memory should be one chunk + one value
        long measured4 = meter.measureDeep( table ) - measuredMemoryTracker;
        assertEquals( measured4, memoryTracker.estimatedHeapMemory() + HeapEstimator.LONG_SIZE );
        assertEquals( measured3, measured4 );

        table.close();
    }

    @Test
    void addRemoveAllFirstCycle()
    {
        int nElements = 2500;
        long key = 0;

        assertNull( table.getFirst() );
        for ( int i = 0; i < nElements; i++ )
        {
            table.add( key++ );
        }
        long measuredAfterAdd1 = meter.measureDeep( table ) - measuredMemoryTracker;
        long estimatedAfterAdd1 = memoryTracker.estimatedHeapMemory() + nElements * HeapEstimator.LONG_SIZE;

        for ( int i = 0; i < nElements; i++ )
        {
            table.remove( i );
        }
        long measuredAfterRemove1 = meter.measureDeep( table ) - measuredMemoryTracker;
        long estimatedAfterRemove1 = memoryTracker.estimatedHeapMemory();

        assertNull( table.getFirst() );
        assertEquals( key - 1, table.lastKey() );
        table.add( key++ );
        assertEquals( key - 1, table.getFirst() );
        assertEquals( key - 1, table.lastKey() );
        for ( int i = 1; i < nElements; i++ )
        {
            table.add( key++ );
        }
        long measuredAfterAdd2 = meter.measureDeep( table ) - measuredMemoryTracker;
        long estimatedAfterAdd2 = memoryTracker.estimatedHeapMemory() + nElements * HeapEstimator.LONG_SIZE;

        for ( int i = 0; i < nElements; i++ )
        {
            table.remove( i + nElements );
        }
        long measuredAfterRemove2 = meter.measureDeep( table ) - measuredMemoryTracker;
        long estimatedAfterRemove2 = memoryTracker.estimatedHeapMemory();

        assertNull( table.getFirst() );
        assertEquals( key - 1, table.lastKey() );
        table.add( key++ );
        assertEquals( key - 1, table.getFirst() );
        assertEquals( key - 1, table.lastKey() );
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
    void addRemoveChunkFirstCycle()
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
    void addRemoveChunkFirstCycleWithOffset()
    {
        int size = DEFAULT_CHUNK_SIZE;
        long key = 0;

        long measuredEmpty = meter.measureDeep( table ) - measuredMemoryTracker;
        long estimatedEmpty = memoryTracker.estimatedHeapMemory();

        assertNull( table.getFirst() );

        table.add( key );
        table.remove( key );
        key++;
        assertNull( table.getFirst() );

        for ( long i = key; i < key + size; i++ )
        {
            table.add( i );
        }
        long measuredAfterAdd1 = meter.measureDeep( table ) - measuredMemoryTracker;
        long estimatedAfterAdd1 = memoryTracker.estimatedHeapMemory() + size * HeapEstimator.LONG_SIZE;

        for ( long i = key; i < key + size; i++ )
        {
            table.remove( i );
        }
        key += size;
        long measuredAfterRemove1 = meter.measureDeep( table ) - measuredMemoryTracker;
        long estimatedAfterRemove1 = memoryTracker.estimatedHeapMemory();

        assertNull( table.getFirst() );

        for ( long i = key; i < key + size; i++ )
        {
            table.add( i );
        }
        long measuredAfterAdd2 = meter.measureDeep( table ) - measuredMemoryTracker;
        long estimatedAfterAdd2 = memoryTracker.estimatedHeapMemory() + size * HeapEstimator.LONG_SIZE;

        for ( long i = key; i < key + size; i++ )
        {
            table.remove( i );
        }
        key += size;
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

    @Test
    void fuzzTest()
    {
        ArrayList<Long> referenceValues = new ArrayList<>();
        ArrayList<ListOperation> opList = new ArrayList<>();

        // If it fails, set the seed here to reproduce:
        // random.setSeed( 0L );

        int chunkSize = random.among( new Integer[]{1, 2, 4, 8, 16, DEFAULT_CHUNK_SIZE} );
        HeapTrackingLongEnumerationList<Long> table = HeapTrackingLongEnumerationList.create( memoryTracker, chunkSize );

        int iterations = random.intBetween( 1000, 2000 );
        int addPercentage = random.intBetween( 40, 70 );
        int nAdds = iterations * addPercentage / 100;
        int nRemoves = iterations - nAdds;

        for ( int i = 0; i < nAdds; i++ )
        {
            opList.add( ADD );
        }
        for ( int i = 0; i < nRemoves; i++ )
        {
            opList.add( REMOVE );
        }
        Collections.shuffle( opList, random.random() );

        int opCount = 0;
        long key = 0;
        try
        {
            for ( ListOperation op : opList )
            {
                switch ( op )
                {
                case ADD:
                    referenceValues.add( key );
                    table.add( key++ );
                    assertEquals( key - 1, table.get( key - 1 ) );
                    assertContainsOnly( table, referenceValues );
                    break;

                case REMOVE:
                    if ( !referenceValues.isEmpty() )
                    {
                        var i = random.intBetween( 0, referenceValues.size() - 1 );
                        int k = referenceValues.remove( i ).intValue();
                        var actual = table.remove( k );
                        assertEquals( k, actual );
                        assertContainsOnly( table, referenceValues );
                    }
                    else
                    {
                        assertEmpty( table );
                    }
                    break;

                default:
                    throw new UnsupportedOperationException();
                }
                assertEquals( key - 1, table.lastKey() );
                opCount++;
            }
            table.close();
        }
        catch ( Throwable t )
        {
            System.err.println( String.format( "Failed with chunk size %s after %s operations (last added key = %s)", chunkSize, opCount, key - 1 ) );
            throw t;
        }

        //System.out.println( String.format( "Succeeded with chunk size %s after %s operations (last added key = %s)", chunkSize, opCount, key - 1 ) );
    }

    enum ListOperation
    {
        ADD,
        REMOVE
    }

    private void assertHeapUsageWithNumberOfLongs( long numberOfBoxedLongs )
    {
        long measured = meter.measureDeep( table ) - measuredMemoryTracker;
        assertEquals( measured, memoryTracker.estimatedHeapMemory() + numberOfBoxedLongs * HeapEstimator.LONG_SIZE );
    }

    private void assertEmpty( HeapTrackingLongEnumerationList<Long> table )
    {
        assertNull( table.getFirst() );
        assertFalse( table.valuesIterator().hasNext() );
        table.foreach( ( k, v ) -> fail() );
        assertNull( table.get( 0 ) );
        assertNull( table.get( table.lastKey() ) );
    }

    private void assertContainsOnly( HeapTrackingLongEnumerationList<Long> table, long element )
    {
        assertContainsOnly( table, element, element );
    }

    private void assertContainsOnly( HeapTrackingLongEnumerationList<Long> table, long from, long to )
    {
        long until = to + 1;

        // getFirst
        assertEquals( from, table.getFirst() );

        // get
        for ( long i = from; i <= to; i++ )
        {
            assertEquals( i, table.get( i ) );
        }
        assertNull( table.get( from - 1 ) );
        assertNull( table.get( to + 1 ) );

        // foreach
        {
            long[] i = new long[1];

            i[0] = from;
            table.foreach( ( k, v ) ->
            {
                if ( i[0] >= until )
                {
                    fail( "foreach out of range" );
                }
                var expected = i[0];
                assertEquals( expected, k );
                assertEquals( expected, v );
                i[0]++;
            } );
            assertEquals( until, i[0] );
        }

        // valuesIterator
        {
            long i = from;
            var it = table.valuesIterator();
            while ( it.hasNext() )
            {
                long value = it.next();
                assertEquals( i, value );
                i++;
            }
            assertEquals( until, i );
        }
    }

    // NOTE: expected needs to be sorted incrementally
    private void assertContainsOnly( HeapTrackingLongEnumerationList<Long> table, long[] expected )
    {
        assertContainsOnly( table, Arrays.stream( expected ).boxed().collect( Collectors.toList() ) );
    }

    // NOTE: expected needs to be sorted incrementally
    private void assertContainsOnly( HeapTrackingLongEnumerationList<Long> table, List<Long> expected )
    {
        // getFirst
        if ( expected.size() > 0 )
        {
            assertEquals( expected.get( 0 ), table.getFirst() );
        }

        // get
        long nullFrom = -1;
        for ( long i : expected )
        {
            assertEquals( i, table.get( i ) );
            for ( long nullKey = nullFrom; nullKey < i; nullKey++ )
            {
                assertNull( table.get( nullKey ) );
            }
            nullFrom = i + 1;
        }
        if ( expected.size() > 0 )
        {
            assertNull( table.get( expected.get( 0 ) - 1 ) );
            assertNull( table.get( expected.get( expected.size() - 1 ) + 1 ) );
        }

        // foreach
        {
            int[] i = new int[1];
            table.foreach( ( k, v ) ->
            {
                if ( i[0] >= expected.size() )
                {
                    fail( "foreach out of range" );
                }
                var expect = expected.get( i[0] );
                assertEquals( expect, k );
                assertEquals( expect, v );
                i[0]++;
            } );
            assertEquals( expected.size(), i[0] );
        }

        // valuesIterator
        {
            int i = 0;
            var it = table.valuesIterator();
            while ( it.hasNext() )
            {
                long value = it.next();
                assertEquals( expected.get( i ), value );
                i++;
            }
            assertEquals( expected.size(), i );
        }
    }
}
