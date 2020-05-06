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

class EagerBufferTest
{
    private final MemoryMeter meter = new MemoryMeter();
    private final MemoryTracker memoryTracker = new LocalMemoryTracker();

    private final EagerBuffer<LongValue> eagerBuffer = EagerBuffer.createEagerBuffer( memoryTracker );

    @AfterEach
    void tearDown()
    {
        eagerBuffer.close();
        assertEquals( 0, memoryTracker.estimatedHeapMemory(), "Leaking memory" );
    }

    @Test
    void emptySize()
    {
        long actual = meter.measureDeep( eagerBuffer ) - meter.measureDeep( memoryTracker );
        assertEquals( actual, memoryTracker.estimatedHeapMemory() );
    }

    @Test
    void countInternalStructure()
    {
        // Allocate outside of buffer
        long externalAllocation = 113L;
        memoryTracker.allocateHeap( externalAllocation );

        eagerBuffer.add( Values.longValue( 1L ) );
        eagerBuffer.add( Values.longValue( 2L ) );
        eagerBuffer.add( Values.longValue( 3L ) );

        // Validate size
        long actualSize = meter.measureDeep( eagerBuffer ) - meter.measureDeep( memoryTracker );
        assertEquals( actualSize, memoryTracker.estimatedHeapMemory() - externalAllocation );

        // Validate content
        Iterator<LongValue> iterator = eagerBuffer.autoClosingIterator();
        assertTrue( iterator.hasNext() );
        assertEquals( 1, iterator.next().longValue() );
        assertTrue( iterator.hasNext() );
        assertEquals( 2, iterator.next().longValue() );
        assertTrue( iterator.hasNext() );
        assertEquals( 3, iterator.next().longValue() );
        assertFalse( iterator.hasNext() );

        // Exhausting the iterator should have closed the buffer automatically at this point
        assertEquals( externalAllocation, memoryTracker.estimatedHeapMemory() );
        memoryTracker.releaseHeap( externalAllocation );
    }

    @Test
    void closeShouldReleaseEverything()
    {
        // Allocate outside of buffer
        long externalAllocation = 113L;
        memoryTracker.allocateHeap( externalAllocation );

        eagerBuffer.add( Values.longValue( 1L ) );
        eagerBuffer.add( Values.longValue( 2L ) );
        eagerBuffer.add( Values.longValue( 3L ) );

        // Validate size
        long actualSize = meter.measureDeep( eagerBuffer ) - meter.measureDeep( memoryTracker );
        assertEquals( actualSize + externalAllocation, memoryTracker.estimatedHeapMemory() );

        // Close should release everything related to the table
        eagerBuffer.close();
        assertEquals( externalAllocation, memoryTracker.estimatedHeapMemory() );

        memoryTracker.releaseHeap( externalAllocation );
    }

    @Test
    void shouldHandleAddingMoreValuesThanCapacity()
    {
        EagerBuffer<LongValue> buffer1 = EagerBuffer.createEagerBuffer( memoryTracker, 16, 16, EagerBuffer.KEEP_CONSTANT_CHUNK_SIZE );
        EagerBuffer<LongValue> buffer2 = EagerBuffer.createEagerBuffer( memoryTracker, 4, 16, EagerBuffer.GROW_NEW_CHUNKS_BY_50_PCT );
        EagerBuffer<LongValue> buffer3 = EagerBuffer.createEagerBuffer( memoryTracker, 1, 16, EagerBuffer.GROW_NEW_CHUNKS_BY_100_PCT );

        var nValues = 64;

        for ( long i = 0; i < nValues; i++ )
        {
            buffer1.add( Values.longValue( i ) );
            buffer2.add( Values.longValue( i ) );
            buffer3.add( Values.longValue( i ) );
        }

        assertEquals( 4, buffer1.numberOfChunks() );
        assertEquals( 6, buffer2.numberOfChunks() );
        assertEquals( 8, buffer3.numberOfChunks() );

        Iterator<LongValue> iterator1 = buffer1.autoClosingIterator();
        Iterator<LongValue> iterator2 = buffer2.autoClosingIterator();
        Iterator<LongValue> iterator3 = buffer3.autoClosingIterator();

        for ( int i = 0; i < nValues; i++ )
        {
            assertTrue( iterator1.hasNext() );
            long value1 = iterator1.next().longValue();
            assertEquals( i, value1 );

            assertTrue( iterator2.hasNext() );
            long value2 = iterator2.next().longValue();
            assertEquals( i, value2 );

            assertTrue( iterator3.hasNext() );
            long value3 = iterator3.next().longValue();
            assertEquals( i, value3 );
        }

        assertFalse( iterator1.hasNext() );
        assertFalse( iterator2.hasNext() );
        assertFalse( iterator3.hasNext() );
    }
}
