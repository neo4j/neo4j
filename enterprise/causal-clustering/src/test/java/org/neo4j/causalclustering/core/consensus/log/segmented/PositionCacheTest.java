/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.core.consensus.log.segmented;

import org.junit.Test;

import org.neo4j.causalclustering.core.consensus.log.LogPosition;

import static org.junit.Assert.assertEquals;

public class PositionCacheTest
{
    private final PositionCache cache = new PositionCache();
    private final LogPosition BEGINNING = new LogPosition( 0, SegmentHeader.SIZE );

    @Test
    public void shouldReturnSaneDefaultPosition()
    {
        // when
        LogPosition position = cache.lookup( 5 );

        // then
        assertEquals( BEGINNING, position );
    }

    @Test
    public void shouldReturnBestPosition()
    {
        // given
        cache.put( pos( 4 ) );
        cache.put( pos( 6 ) );

        // when
        LogPosition lookup = cache.lookup( 7 );

        // then
        assertEquals( pos( 6 ), lookup );
    }

    @Test
    public void shouldReturnExactMatch()
    {
        // given
        cache.put( pos( 4 ) );
        cache.put( pos( 6 ) );
        cache.put( pos( 8 ) );

        // when
        LogPosition lookup = cache.lookup( 6 );

        // then
        assertEquals( pos( 6 ), lookup );
    }

    @Test
    public void shouldNotReturnPositionAhead()
    {
        // given
        cache.put( pos( 4 ) );
        cache.put( pos( 6 ) );
        cache.put( pos( 8 ) );

        // when
        LogPosition lookup = cache.lookup( 7 );

        // then
        assertEquals( pos( 6 ), lookup );
    }

    @Test
    public void shouldPushOutOldEntries()
    {
        // given
        int count = PositionCache.CACHE_SIZE + 4;
        for ( int i = 0; i < count; i++ )
        {
            cache.put( pos( i ) );
        }

        // then
        for ( int i = 0; i < PositionCache.CACHE_SIZE; i++ )
        {
            int index = count - i - 1;
            assertEquals( pos( index ), cache.lookup( index ) );
        }

        int index = count - PositionCache.CACHE_SIZE - 1;
        assertEquals( BEGINNING, cache.lookup( index ) );
    }

    private LogPosition pos( int i )
    {
        return new LogPosition( i, 100 * i );
    }
}
