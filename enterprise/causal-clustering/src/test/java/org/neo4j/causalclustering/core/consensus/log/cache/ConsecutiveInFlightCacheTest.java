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

import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.state.machines.dummy.DummyRequest;

import static org.junit.Assert.assertEquals;

public class ConsecutiveInFlightCacheTest
{
    @Test
    public void shouldTrackUsedMemory() throws Exception
    {
        int capacity = 4;
        ConsecutiveInFlightCache cache = new ConsecutiveInFlightCache( capacity, 1000, InFlightCacheMonitor.VOID, true );

        for ( int i = 0; i < capacity; i++ )
        {
            // when
            cache.put( i, content( 100 ) );

            // then
            assertEquals( (i + 1) * 100, cache.totalBytes() );
        }

        // when
        cache.put( capacity, content( 100 ) );

        // then
        assertEquals( capacity, cache.elementCount() );
        assertEquals( capacity * 100, cache.totalBytes() );

        // when
        cache.put( capacity + 1, content( 500 ) );
        assertEquals( capacity, cache.elementCount() );
        assertEquals( 800, cache.totalBytes() );

        // when
        cache.put( capacity + 2, content( 500 ) );
        assertEquals( 2, cache.elementCount() );
        assertEquals( 1000, cache.totalBytes() );
    }

    @Test
    public void shouldReturnLatestItems() throws Exception
    {
        // given
        int capacity = 4;
        ConsecutiveInFlightCache cache = new ConsecutiveInFlightCache( capacity, 1000, InFlightCacheMonitor.VOID, true );

        // when
        for ( int i = 0; i < 3 * capacity; i++ )
        {
            cache.put( i, content( i ) );
        }

        // then
        for ( int i = 0; i < 3 * capacity; i++ )
        {
            if ( i < 2 * capacity )
            {
                assertEquals( null, cache.get( i ) );
            }
            else
            {
                assertEquals( i, cache.get( i ).content().size() );
            }
        }
    }

    @Test
    public void shouldRemovePrunedItems() throws Exception
    {
        // given
        int capacity = 20;
        ConsecutiveInFlightCache cache = new ConsecutiveInFlightCache( capacity, 1000, InFlightCacheMonitor.VOID, true );

        for ( int i = 0; i < capacity; i++ )
        {
            cache.put( i, content( i ) );
        }

        // when
        int upToIndex = capacity / 2 - 1;
        cache.prune( upToIndex );

        // then
        assertEquals( capacity / 2, cache.elementCount() );

        for ( int i = 0; i < capacity; i++ )
        {
            if ( i <= upToIndex )
            {
                assertEquals( null, cache.get( i ) );
            }
            else
            {
                assertEquals( i, cache.get( i ).content().size() );
            }
        }
    }

    @Test
    public void shouldRemoveTruncatedItems() throws Exception
    {
        // given
        int capacity = 20;
        ConsecutiveInFlightCache cache = new ConsecutiveInFlightCache( capacity, 1000, InFlightCacheMonitor.VOID, true );

        for ( int i = 0; i < capacity; i++ )
        {
            cache.put( i, content( i ) );
        }

        // when
        int fromIndex = capacity / 2;
        cache.truncate( fromIndex );

        // then
        assertEquals( fromIndex, cache.elementCount() );
        assertEquals( (fromIndex * (fromIndex - 1)) / 2, cache.totalBytes() );

        for ( int i = fromIndex; i < capacity; i++ )
        {
            assertEquals( null, cache.get( i ) );
        }
    }

    private RaftLogEntry content( int size )
    {
        return new RaftLogEntry( 0, new DummyRequest( new byte[size] ) );
    }
}
