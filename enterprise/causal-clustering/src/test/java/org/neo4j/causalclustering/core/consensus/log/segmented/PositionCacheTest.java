/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
