/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.causalclustering.core.consensus.log.cache;

import org.junit.Test;

import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.state.machines.dummy.DummyRequest;

import static org.junit.Assert.assertEquals;

public class ConsecutiveInFlightCacheTest
{
    @Test
    public void shouldTrackUsedMemory()
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
    public void shouldReturnLatestItems()
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
    public void shouldRemovePrunedItems()
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
    public void shouldRemoveTruncatedItems()
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
