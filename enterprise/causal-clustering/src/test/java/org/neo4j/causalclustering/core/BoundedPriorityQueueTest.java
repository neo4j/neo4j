/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.causalclustering.core.BoundedPriorityQueue.Config;
import org.neo4j.causalclustering.core.BoundedPriorityQueue.Removable;

import static java.util.Comparator.comparingInt;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.causalclustering.core.BoundedPriorityQueue.Result.E_COUNT_EXCEEDED;
import static org.neo4j.causalclustering.core.BoundedPriorityQueue.Result.E_SIZE_EXCEEDED;
import static org.neo4j.causalclustering.core.BoundedPriorityQueue.Result.OK;

public class BoundedPriorityQueueTest
{
    private final Config BASE_CONFIG = new Config( 0, 5, 100 );
    private final Comparator<Integer> NO_PRIORITY = ( a, b ) -> 0;

    private final ThreadLocalRandom tlr = ThreadLocalRandom.current();

    @Test
    public void shouldReportTotalCountAndSize()
    {
        BoundedPriorityQueue<Integer> queue = new BoundedPriorityQueue<>( BASE_CONFIG, Integer::longValue, NO_PRIORITY );

        assertEquals( 0, queue.bytes() );
        assertEquals( 0, queue.count() );

        queue.offer( 10 );
        assertEquals( 1, queue.count() );
        assertEquals( 10, queue.bytes() );

        queue.offer( 20 );
        assertEquals( 2, queue.count() );
        assertEquals( 30, queue.bytes() );

        queue.poll();
        assertEquals( 1, queue.count() );
        assertEquals( 20, queue.bytes() );

        queue.poll();
        assertEquals( 0, queue.count() );
        assertEquals( 0, queue.bytes() );
    }

    @Test
    public void shouldNotAllowMoreThanMaxBytes()
    {
        BoundedPriorityQueue<Integer> queue = new BoundedPriorityQueue<>( BASE_CONFIG, Integer::longValue, NO_PRIORITY );

        assertEquals( E_SIZE_EXCEEDED, queue.offer( 101 ) );
        assertEquals( OK, queue.offer( 99 ) );
        assertEquals( OK, queue.offer( 1 ) );
        assertEquals( E_SIZE_EXCEEDED, queue.offer( 1 ) );
    }

    @Test
    public void shouldAllowMinCountDespiteSizeLimit()
    {
        Config config = new Config( 2, 5, 100 );
        BoundedPriorityQueue<Integer> queue = new BoundedPriorityQueue<>( config, Integer::longValue, NO_PRIORITY );

        assertEquals( OK, queue.offer( 101 ) );
        assertEquals( OK, queue.offer( 101 ) );
        assertEquals( E_SIZE_EXCEEDED, queue.offer( 1 ) );
    }

    @Test
    public void shouldAllowZeroSizedItemsDespiteSizeLimit()
    {
        BoundedPriorityQueue<Integer> queue = new BoundedPriorityQueue<>( BASE_CONFIG, Integer::longValue, NO_PRIORITY );

        assertEquals( OK, queue.offer( 100 ) );
        assertEquals( E_SIZE_EXCEEDED, queue.offer( 1 ) );

        assertEquals( OK, queue.offer( 0 ) );
        assertEquals( OK, queue.offer( 0 ) );
    }

    @Test
    public void shouldNotAllowMoreThanMaxCount()
    {
        BoundedPriorityQueue<Integer> queue = new BoundedPriorityQueue<>( BASE_CONFIG, Integer::longValue, NO_PRIORITY );

        assertEquals( OK, queue.offer( 1 ) );
        assertEquals( OK, queue.offer( 1 ) );
        assertEquals( OK, queue.offer( 1 ) );
        assertEquals( OK, queue.offer( 1 ) );
        assertEquals( OK, queue.offer( 1 ) );

        assertEquals( E_COUNT_EXCEEDED, queue.offer( 1 ) );
    }

    @Test
    public void shouldNotAllowMoreThanMaxCountDespiteZeroSize()
    {
        BoundedPriorityQueue<Integer> queue = new BoundedPriorityQueue<>( BASE_CONFIG, Integer::longValue, NO_PRIORITY );

        assertEquals( OK, queue.offer( 0 ) );
        assertEquals( OK, queue.offer( 0 ) );
        assertEquals( OK, queue.offer( 0 ) );
        assertEquals( OK, queue.offer( 0 ) );
        assertEquals( OK, queue.offer( 0 ) );

        assertEquals( E_COUNT_EXCEEDED, queue.offer( 0 ) );
    }

    @Test
    public void shouldBeAbleToPeekEntries()
    {
        BoundedPriorityQueue<Integer> queue = new BoundedPriorityQueue<>( BASE_CONFIG, Integer::longValue, NO_PRIORITY );

        assertEquals( OK, queue.offer( 1 ) );
        assertEquals( OK, queue.offer( 2 ) );
        assertEquals( OK, queue.offer( 3 ) );

        assertEquals( Optional.of( 1 ), queue.peek().map( Removable::get ) );
        assertEquals( Optional.of( 1 ), queue.peek().map( Removable::get ) );
        assertEquals( Optional.of( 1 ), queue.peek().map( Removable::get ) );

        assertEquals( 3, queue.count() );
        assertEquals( 6, queue.bytes() );
    }

    @Test
    public void shouldBeAbleToRemovePeekedEntries()
    {
        BoundedPriorityQueue<Integer> queue = new BoundedPriorityQueue<>( BASE_CONFIG, Integer::longValue, NO_PRIORITY );

        assertEquals( OK, queue.offer( 1 ) );
        assertEquals( OK, queue.offer( 2 ) );
        assertEquals( OK, queue.offer( 3 ) );
        assertEquals( 3, queue.count() );
        assertEquals( 6, queue.bytes() );

        assertTrue( queue.peek().isPresent() );
        assertTrue( queue.peek().get().remove() );
        assertEquals( 2, queue.count() );
        assertEquals( 5, queue.bytes() );

        assertTrue( queue.peek().isPresent() );
        assertTrue( queue.peek().get().remove() );
        assertEquals( 1, queue.count() );
        assertEquals( 3, queue.bytes() );

        assertTrue( queue.peek().isPresent() );
        assertTrue( queue.peek().get().remove() );
        assertEquals( 0, queue.count() );
        assertEquals( 0, queue.bytes() );

        assertFalse( queue.peek().isPresent() );
        try
        {
            queue.peek().get().remove();
            fail();
        }
        catch ( NoSuchElementException ignored )
        {
        }
    }

    @Test
    public void shouldRespectPriority()
    {
        int count = 100;
        Config config = new Config( 0, count, 0 );
        BoundedPriorityQueue<Integer> queue = new BoundedPriorityQueue<>( config, i -> 0L, Integer::compare );

        List<Integer> list = new ArrayList<>( count );
        for ( int i = 0; i < count; i++ )
        {
            list.add( i );
        }

        Collections.shuffle( list, tlr );
        list.forEach( queue::offer );

        for ( int i = 0; i < count; i++ )
        {
            assertEquals( Optional.of( i ), queue.poll() );
        }
    }

    @Test
    public void shouldHaveStablePriority()
    {
        int count = 100;
        int priorities = 3;

        Config config = new Config( 0, count, 0 );
        BoundedPriorityQueue<Element> queue = new BoundedPriorityQueue<>( config, i -> 0L,
                comparingInt( p -> p.priority ) );

        List<Element> insertionOrder = new ArrayList<>( count );
        for ( int i = 0; i < count; i++ )
        {
            insertionOrder.add( new Element( tlr.nextInt( priorities ) ) );
        }

        Collections.shuffle( insertionOrder, tlr );
        insertionOrder.forEach( queue::offer );

        for ( int p = 0; p < priorities; p++ )
        {
            ArrayList<Element> filteredInsertionOrder = new ArrayList<>();
            for ( Element element : insertionOrder )
            {
                if ( element.priority == p )
                {
                    filteredInsertionOrder.add( element );
                }
            }

            for ( Element element : filteredInsertionOrder )
            {
                assertEquals( Optional.of( element ), queue.poll() );
            }
        }
    }

    class Element
    {
        int priority;

        Element( int priority )
        {
            this.priority = priority;
        }

        @Override
        public boolean equals( Object o )
        {
            return this == o;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( priority );
        }
    }
}
