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
package org.neo4j.causalclustering.core.consensus.outcome;

import org.junit.jupiter.api.Test;

import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.consensus.log.cache.ConsecutiveInFlightCache;
import org.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.neo4j.causalclustering.core.consensus.ReplicatedInteger.valueOf;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class TruncateLogCommandTest
{
    @Test
    public void applyTo()
    {
        //Test that truncate commands correctly remove entries from the cache.

        //given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        Log log = logProvider.getLog( getClass() );
        long fromIndex = 2L;
        TruncateLogCommand truncateLogCommand = new TruncateLogCommand( fromIndex );
        InFlightCache inFlightCache = new ConsecutiveInFlightCache();

        inFlightCache.put( 0L, new RaftLogEntry( 0L, valueOf( 0 ) ) );
        inFlightCache.put( 1L, new RaftLogEntry( 1L, valueOf( 1 ) ) );
        inFlightCache.put( 2L, new RaftLogEntry( 2L, valueOf( 2 ) ) );
        inFlightCache.put( 3L, new RaftLogEntry( 3L, valueOf( 3 ) ) );

        //when
        truncateLogCommand.applyTo( inFlightCache, log );

        //then
        assertNotNull( inFlightCache.get( 0L ) );
        assertNotNull( inFlightCache.get( 1L ) );
        assertNull( inFlightCache.get( 2L ) );
        assertNull( inFlightCache.get( 3L ) );

        logProvider.assertAtLeastOnce( inLog( getClass() )
                .debug( "Start truncating in-flight-map from index %d. Current map:%n%s", fromIndex, inFlightCache ) );
    }

    @Test
    public void shouldTruncateWithGaps()
    {
        //given
        long fromIndex = 1L;
        TruncateLogCommand truncateLogCommand = new TruncateLogCommand( fromIndex );

        InFlightCache inFlightCache = new ConsecutiveInFlightCache();

        inFlightCache.put( 0L, new RaftLogEntry( 0L, valueOf( 0 ) ) );
        inFlightCache.put( 2L, new RaftLogEntry( 1L, valueOf( 1 ) ) );
        inFlightCache.put( 4L, new RaftLogEntry( 2L, valueOf( 2 ) ) );

        truncateLogCommand.applyTo( inFlightCache, NullLog.getInstance() );

        inFlightCache.put( 1L, new RaftLogEntry( 3L, valueOf( 1 ) ) );
        inFlightCache.put( 2L, new RaftLogEntry( 4L, valueOf( 2 ) ) );

        assertNotNull( inFlightCache.get( 1L ) );
        assertNotNull( inFlightCache.get( 2L ) );
    }
}
