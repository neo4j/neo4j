/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.collection;

import org.eclipse.collections.api.iterator.LongIterator;
import org.junit.jupiter.api.Test;

import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PrimitiveLongArrayQueueTest
{

    @Test
    void newQueueIsEmpty()
    {
        assertTrue( createQueue().isEmpty() );
    }

    @Test
    void growQueueOnElementOffer()
    {
        PrimitiveLongArrayQueue longArrayQueue = createQueue();
        for ( int i = 1; i < 1000; i++ )
        {
            longArrayQueue.enqueue( i );
            assertEquals( i, longArrayQueue.size() );
        }
    }

    @Test
    void addRemoveElementKeepQueueEmpty()
    {
        PrimitiveLongArrayQueue longArrayQueue = createQueue();
        for ( int i = 0; i < 1000; i++ )
        {
            longArrayQueue.enqueue( i );
            assertEquals( i, longArrayQueue.dequeue() );
            assertTrue( longArrayQueue.isEmpty() );
        }
    }

    @Test
    void offerLessThenQueueCapacityElements()
    {
        PrimitiveLongArrayQueue arrayQueue = createQueue();
        for ( int i = 1; i < 16; i++ )
        {
            arrayQueue.enqueue( i );
            assertEquals( i, arrayQueue.size() );
        }
    }

    @Test
    void failToRemoveElementFromNewEmptyQueue()
    {
        assertThrows( IllegalStateException.class, () -> createQueue().dequeue() );
    }

    @Test
    void offerMoreThenQueueCapacityElements()
    {
        PrimitiveLongArrayQueue arrayQueue = createQueue();
        for ( int i = 1; i < 1234; i++ )
        {
            arrayQueue.enqueue( i );
        }
        int currentValue = 1;
        while ( !arrayQueue.isEmpty() )
        {
            assertEquals( currentValue++, arrayQueue.dequeue() );
        }
    }

    @Test
    void emptyQueueAfterClear()
    {
        PrimitiveLongArrayQueue queue = createQueue();
        queue.enqueue( 2 );
        queue.enqueue( 3 );
        assertFalse( queue.isEmpty() );
        assertEquals( 2, queue.size() );

        queue.clear();

        assertTrue( queue.isEmpty() );
    }

    @Test
    void tailBeforeHeadCorrectSize()
    {
        PrimitiveLongArrayQueue queue = createQueue();
        for ( int i = 0; i < 14; i++ )
        {
            queue.enqueue( i );
        }
        for ( int i = 0; i < 10; i++ )
        {
            assertEquals( i, queue.dequeue() );
        }
        for ( int i = 14; i < 24 ; i++ )
        {
            queue.enqueue( i );
        }

        assertEquals( 14, queue.size() );
    }

    @Test
    void tailBeforeHeadCorrectResize()
    {
        PrimitiveLongArrayQueue queue = createQueue();
        for ( int i = 0; i < 14; i++ )
        {
            queue.enqueue( i );
        }
        for ( int i = 0; i < 10; i++ )
        {
            assertEquals( i, queue.dequeue() );
        }
        for ( int i = 14; i < 34 ; i++ )
        {
            queue.enqueue( i );
        }

        assertEquals( 24, queue.size() );
        for ( int j = 10; j < 34; j++ )
        {
            assertEquals( j, queue.dequeue() );
        }
    }

    @Test
    void tailBeforeHeadCorrectIteration()
    {
        PrimitiveLongArrayQueue queue = createQueue();
        for ( int i = 0; i < 14; i++ )
        {
            queue.enqueue( i );
        }
        for ( int i = 0; i < 10; i++ )
        {
            assertEquals( i, queue.dequeue() );
        }
        for ( int i = 14; i < 24 ; i++ )
        {
            queue.enqueue( i );
        }

        assertEquals( 14, queue.size() );
        LongIterator iterator = queue.longIterator();
        for ( int j = 10; j < 24; j++ )
        {
            assertTrue( iterator.hasNext() );
            assertEquals( j, iterator.next() );
        }
        assertFalse( iterator.hasNext() );
    }

    @Test
    void failToGetNextOnEmptyQueueIterator()
    {
        assertThrows( NoSuchElementException.class, () -> createQueue().longIterator().next() );
    }

    @Test
    void addAllElementsFromOtherQueue()
    {
        PrimitiveLongArrayQueue queue = createQueue();
        queue.enqueue( 1 );
        queue.enqueue( 2 );
        PrimitiveLongArrayQueue otherQueue = createQueue();
        otherQueue.enqueue( 3 );
        otherQueue.enqueue( 4 );
        queue.addAll( otherQueue );

        assertTrue( otherQueue.isEmpty() );
        assertEquals( 0, otherQueue.size() );
        assertEquals( 4, queue.size() );
        for ( int value = 1; value <= 4; value++ )
        {
            assertEquals( value, queue.dequeue() );
        }
        assertTrue( queue.isEmpty() );
    }

    @Test
    void doNotAllowCreationOfQueueWithRandomCapacity()
    {
        assertThrows( IllegalArgumentException.class, () -> new PrimitiveLongArrayQueue( 7 ) );
    }

    private PrimitiveLongArrayQueue createQueue()
    {
        return new PrimitiveLongArrayQueue();
    }
}
