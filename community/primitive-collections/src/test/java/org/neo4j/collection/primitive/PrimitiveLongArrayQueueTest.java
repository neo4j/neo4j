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
package org.neo4j.collection.primitive;

import org.junit.Test;

import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PrimitiveLongArrayQueueTest
{

    @Test
    public void newQueueIsEmpty()
    {
        assertTrue( createQueue().isEmpty() );
    }

    @Test
    public void growQueueOnElementOffer()
    {
        PrimitiveLongArrayQueue longArrayQueue = createQueue();
        for ( int i = 1; i < 1000; i++ )
        {
            longArrayQueue.enqueue( i );
            assertEquals( i, longArrayQueue.size() );
        }
    }

    @Test
    public void addRemoveElementKeepQueueEmpty()
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
    public void offerLessThenQueueCapacityElements()
    {
        PrimitiveLongArrayQueue arrayQueue = createQueue();
        for ( int i = 1; i < 16; i++ )
        {
            arrayQueue.enqueue( i );
            assertEquals( i, arrayQueue.size() );
        }
    }

    @Test( expected = IllegalStateException.class )
    public void failToRemoveElementFromNewEmptyQueue()
    {
        createQueue().dequeue();
    }

    @Test
    public void offerMoreThenQueueCapacityElements()
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
    public void emptyQueueAfterClear()
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
    public void tailBeforeHeadCorrectSize()
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
    public void tailBeforeHeadCorrectResize()
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
    public void tailBeforeHeadCorrectIteration()
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
        PrimitiveLongIterator iterator = queue.iterator();
        for ( int j = 10; j < 24; j++ )
        {
            assertTrue( iterator.hasNext() );
            assertEquals( j, iterator.next() );
        }
        assertFalse( iterator.hasNext() );
    }

    @Test( expected = NoSuchElementException.class )
    public void failToGetNextOnEmptyQueueIterator()
    {
        createQueue().iterator().next();
    }

    @Test
    public void addAllElementsFromOtherQueue()
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

    @Test( expected = AssertionError.class )
    public void doNotAllowCreationOfQueueWithRandomCapacity()
    {
        new PrimitiveLongArrayQueue( 7 );
    }

    private PrimitiveLongArrayQueue createQueue()
    {
        return new PrimitiveLongArrayQueue();
    }
}
