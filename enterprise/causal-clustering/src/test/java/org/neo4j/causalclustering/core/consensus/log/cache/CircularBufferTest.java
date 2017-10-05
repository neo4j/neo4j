/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.core.consensus.log.cache;

import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class CircularBufferTest
{
    private final ThreadLocalRandom tlr = ThreadLocalRandom.current();

    @Test
    public void shouldBeInitiallyEmpty() throws Exception
    {
        // when
        CircularBuffer<Object> buffer = new CircularBuffer<>( 3 );

        // then
        assertEquals( 0, buffer.size() );
        assertEquals( null, buffer.remove() );
        assertEquals( null, buffer.read( 0 ) );

        // again for idempotency check
        assertEquals( 0, buffer.size() );
        assertEquals( null, buffer.remove() );
        assertEquals( null, buffer.read( 0 ) );
    }

    @Test
    public void removeShouldReturnNullWhenEmpty() throws Exception
    {
        // given
        CircularBuffer<Object> buffer = new CircularBuffer<>( 3 );

        buffer.append( 1L );
        buffer.append( 2L );
        buffer.append( 3L );

        // when
        buffer.remove();
        buffer.remove();
        buffer.remove();

        // then
        assertEquals( null, buffer.remove() );
    }

    @Test
    public void shouldEvictElementsWhenClearing() throws Exception
    {
        // given
        CircularBuffer<Integer> buffer = new CircularBuffer<>( 3 );
        Integer[] evictions = new Integer[3];
        buffer.append( 1 );
        buffer.append( 2 );

        // when
        buffer.clear( evictions );

        // then
        assertEquals( 0, buffer.size() );
        assertArrayEquals( evictions, new Integer[]{1, 2, null} );
    }

    @Test
    public void shouldNullRemovedElements() throws Exception
    {
        // given
        CircularBuffer<Integer> buffer = new CircularBuffer<>( 3 );
        Integer[] evictions = new Integer[3];
        buffer.append( 1 );
        buffer.append( 2 );
        buffer.append( 3 );

        // when
        buffer.remove();
        buffer.remove();
        buffer.remove();

        // then
        assertNull( buffer.read( 0 ) );
        assertNull( buffer.read( 1 ) );
        assertNull( buffer.read( 2 ) );
    }

    @Test
    public void shouldNullClearedElements() throws Exception
    {
        // given
        CircularBuffer<Integer> buffer = new CircularBuffer<>( 3 );
        Integer[] evictions = new Integer[3];
        buffer.append( 1 );
        buffer.append( 2 );
        buffer.append( 3 );

        // when
        buffer.clear( evictions );

        // then
        assertNull( buffer.read( 0 ) );
        assertNull( buffer.read( 1 ) );
        assertNull( buffer.read( 2 ) );
    }

    @Test
    public void comprehensivelyTestAppendRemove() throws Exception
    {
        for ( int capacity = 1; capacity <= 128; capacity++ )
        {
            for ( int operations = 1; operations < capacity * 3; operations++ )
            {
                comprehensivelyTestAppendRemove( capacity, operations, new CircularBuffer<>( capacity ) );
            }
        }
    }

    @Test
    public void comprehensivelyTestAppendRemoveHead() throws Exception
    {
        for ( int capacity = 1; capacity <= 128; capacity++ )
        {
            for ( int operations = 1; operations < capacity * 3; operations++ )
            {
                comprehensivelyTestAppendRemoveHead( capacity, operations, new CircularBuffer<>( capacity ) );
            }
        }
    }

    @Test
    public void comprehensivelyTestAppendRemoveReusingBuffer() throws Exception
    {
        for ( int capacity = 1; capacity <= 128; capacity++ )
        {
            CircularBuffer<Integer> buffer = new CircularBuffer<>( capacity );
            for ( int operations = 1; operations <= capacity * 3; operations++ )
            {
                comprehensivelyTestAppendRemove( capacity, operations, buffer );
            }
        }
    }

    private void comprehensivelyTestAppendRemove( int capacity, int operations, CircularBuffer<Integer> buffer ) throws Exception
    {
        ArrayList<Integer> numbers = new ArrayList<>( operations );

        // when: adding a bunch of random numbers
        for ( int i = 0; i < operations; i++ )
        {
            int number = tlr.nextInt();
            numbers.add( number );
            buffer.append( number );
        }

        // then: these should have been knocked out
        for ( int i = 0; i < operations - capacity; i++ )
        {
            numbers.remove( 0 );
        }

        // and these should remain
        while ( !numbers.isEmpty() )
        {
            assertEquals( numbers.remove( 0 ), buffer.remove() );
        }

        assertEquals( 0, buffer.size() );
    }

    private void comprehensivelyTestAppendRemoveHead( int capacity, int operations, CircularBuffer<Integer> buffer ) throws Exception
    {
        ArrayList<Integer> numbers = new ArrayList<>( operations );

        // when: adding a bunch of random numbers
        for ( int i = 0; i < operations; i++ )
        {
            int number = tlr.nextInt();
            numbers.add( number );
            buffer.append( number );
        }

        // then: these should have been knocked out
        for ( int i = 0; i < operations - capacity; i++ )
        {
            numbers.remove( 0 );
        }

        // and these should remain
        while ( !numbers.isEmpty() )
        {
            assertEquals( numbers.remove( numbers.size() - 1 ), buffer.removeHead() );
        }

        assertEquals( 0, buffer.size() );
    }

    @Test
    public void comprehensivelyTestAppendRead() throws Exception
    {
        for ( int capacity = 1; capacity <= 128; capacity++ )
        {
            for ( int operations = 1; operations < capacity * 3; operations++ )
            {
                comprehensivelyTestAppendRead( capacity, operations );
            }
        }
    }

    private void comprehensivelyTestAppendRead( int capacity, int operations ) throws Exception
    {
        CircularBuffer<Integer> buffer = new CircularBuffer<>( capacity );
        ArrayList<Integer> numbers = new ArrayList<>( operations );

        // when: adding a bunch of random numbers
        for ( int i = 0; i < operations; i++ )
        {
            int number = tlr.nextInt();
            numbers.add( number );
            buffer.append( number );
        }

        // then: these should have been knocked out
        for ( int i = 0; i < operations - capacity; i++ )
        {
            numbers.remove( 0 );
        }

        // and these should remain
        int i = 0;
        while ( !numbers.isEmpty() )
        {
            assertEquals( numbers.remove( 0 ), buffer.read( i++ ) );
        }
    }
}
