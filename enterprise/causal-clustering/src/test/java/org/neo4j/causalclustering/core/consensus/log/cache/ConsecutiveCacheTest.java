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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith( Parameterized.class )
public class ConsecutiveCacheTest
{
    private final int capacity;
    private ConsecutiveCache<Integer> cache;
    private Integer[] evictions;

    public ConsecutiveCacheTest( int capacity )
    {
        this.capacity = capacity;
        this.evictions = new Integer[capacity];
    }

    @Parameterized.Parameters( name = "capacity={0}" )
    public static Collection<Object[]> data()
    {
        return Arrays.asList( new Object[][]{{1}, {2}, {3}, {4}, {8}, {1024}} );
    }

    @Before
    public void setup() throws Exception
    {
        cache = new ConsecutiveCache<>( capacity );
    }

    @Test
    public void testEmptyInvariants()
    {
        assertEquals( 0, cache.size() );
        for ( int i = 0; i < capacity; i++ )
        {
            assertNull( cache.get( i ) );
        }
    }

    @Test
    public void testCacheFill()
    {
        for ( int i = 0; i < capacity; i++ )
        {
            // when
            cache.put( i, i, evictions );
            assertTrue( Stream.of( evictions ).allMatch( Objects::isNull ) );

            // then
            assertEquals( i + 1, cache.size() );
        }

        // then
        for ( int i = 0; i < capacity; i++ )
        {
            assertEquals( i, cache.get( i ).intValue() );
        }
    }

    @Test
    public void testCacheMultipleFills()
    {
        // given
        for ( int i = 0; i < capacity; i++ )
        {
            cache.put( i, i, evictions );
        }

        for ( int i = capacity; i < 10 * capacity; i++ )
        {
            // when
            cache.put( i, i, evictions );

            // then
            assertEquals( i - capacity, evictions[0].intValue() );
            assertTrue( Stream.of( evictions ).skip( 1 ).allMatch( Objects::isNull ) );
            assertEquals( capacity, cache.size() );
        }
    }

    @Test
    public void testCacheClearing() throws Exception
    {
        // given
        for ( int i = 0; i < capacity; i++ )
        {
            cache.put( i, i, evictions );
        }

        // when
        cache.clear( evictions );

        // then
        for ( int i = 0; i < capacity; i++ )
        {
            assertEquals( i, evictions[i].intValue() );
            assertEquals( null, cache.get( i ) );
        }

        assertEquals( 0, cache.size() );
    }

    @Test
    public void testEntryOverride() throws Exception
    {
        // given
        for ( int i = 0; i < capacity; i++ )
        {
            cache.put( i, i, evictions );
        }

        // when
        cache.put( capacity / 2, 10000, evictions );

        // then
        for ( int i = 0; i < capacity; i++ )
        {
            if ( i == capacity / 2 )
            {
                continue;
            }

            assertEquals( i, evictions[i].intValue() );
            assertEquals( null, cache.get( i ) );
        }

        assertEquals( 10000, cache.get( capacity / 2 ).intValue() );
    }

    @Test
    public void testEntrySkip() throws Exception
    {
        // given
        for ( int i = 0; i < capacity; i++ )
        {
            cache.put( i, i, evictions );
        }

        // when
        cache.put( capacity + 1, 10000, evictions );

        // then
        for ( int i = 0; i < capacity; i++ )
        {
            assertEquals( i, evictions[i].intValue() );
            assertEquals( null, cache.get( i ) );
        }

        assertEquals( 10000, cache.get( capacity + 1 ).intValue() );
    }

    @Test
    public void testPruning() throws Exception
    {
        // given
        for ( int i = 0; i < capacity; i++ )
        {
            cache.put( i, i, evictions );
        }

        // when
        int upToIndex = capacity / 2;
        cache.prune( upToIndex, evictions );

        // then
        for ( int i = 0; i <= upToIndex; i++ )
        {
            assertEquals( null, cache.get( i ) );
            assertEquals( i, evictions[i].intValue() );
        }

        for ( int i = upToIndex + 1; i < capacity; i++ )
        {
            assertEquals( i, cache.get( i ).intValue() );
        }
    }

    @Test
    public void testRemoval() throws Exception
    {
        // given
        for ( int i = 0; i < capacity; i++ )
        {
            cache.put( i, i, evictions );
        }

        // then
        for ( int i = 0; i < capacity; i++ )
        {
            // when
            Integer removed = cache.remove();

            // then
            assertEquals( i, removed.intValue() );
        }

        assertNull( cache.remove() );
    }

    @Test
    public void testTruncation() throws Exception
    {
        // given
        for ( int i = 0; i < capacity; i++ )
        {
            cache.put( i, i, evictions );
        }

        // when
        int fromIndex = capacity / 2;
        cache.truncate( fromIndex, evictions );

        // then
        for ( int i = 0; i < fromIndex; i++ )
        {
            assertEquals( i, cache.get( i ).intValue() );
        }

        for ( int i = fromIndex; i < capacity; i++ )
        {
            assertEquals( null, cache.get( i ) );
            assertEquals( i, evictions[capacity - i - 1].intValue() );
        }
    }
}
