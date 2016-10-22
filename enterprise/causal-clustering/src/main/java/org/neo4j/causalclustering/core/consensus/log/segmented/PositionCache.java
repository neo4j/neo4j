/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.causalclustering.core.consensus.log.LogPosition;

/**
 * Caches (offsetIndex) -> (byteOffset) mappings, which can be used to find an exact or
 * approximate byte position for an entry given an index. The index is defined as a relative
 * offset index starting from 0 for each segment, instead of the absolute logIndex in the
 * log file.
 *
 * The necessity and efficiency of this cache is understood by considering the values put into
 * it. When closing cursors the position after the last entry is cached so that when the next
 * batch of entries are to be read the position is already known.
 */
class PositionCache
{
    private static final LogPosition BEGINNING_OF_RECORDS = new LogPosition( 0, SegmentHeader.SIZE );
    static final int CACHE_SIZE = 8;

    private LogPosition[] cache = new LogPosition[CACHE_SIZE];
    private int pos;

    PositionCache()
    {
        for ( int i = 0; i < cache.length; i++ )
        {
            cache[i] = BEGINNING_OF_RECORDS;
        }
    }

    /**
     * Saves a known position in the cache.
     *
     * @param position The position which should interpreted as (offsetIndex, byteOffset).
     */
    public synchronized void put( LogPosition position )
    {
        cache[pos] = position;
        pos = (pos + 1) % CACHE_SIZE;
    }

    /**
     * Returns a position at or before the searched offsetIndex, the closest known.
     * Users will have to scan forward to reach the exact position.
     *
     * @param offsetIndex The relative index.
     * @return A position at or before the searched offsetIndex.
     */
    synchronized LogPosition lookup( long offsetIndex )
    {
        if ( offsetIndex == 0 )
        {
            return BEGINNING_OF_RECORDS;
        }

        LogPosition best = BEGINNING_OF_RECORDS;

        for ( int i = 0; i < CACHE_SIZE; i++ )
        {
            if ( cache[i].logIndex <= offsetIndex && cache[i].logIndex > best.logIndex )
            {
                best = cache[i];
            }
        }

        return best;
    }
}
