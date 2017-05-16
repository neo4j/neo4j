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
package org.neo4j.causalclustering.core.consensus.outcome;

import org.junit.Test;

import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.consensus.log.segmented.InFlightMap;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.neo4j.causalclustering.core.consensus.ReplicatedInteger.valueOf;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class TruncateLogCommandTest
{
    @Test
    public void applyTo() throws Exception
    {
        //Test that truncate commands correctly remove entries from the cache.

        //given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        Log log = logProvider.getLog( getClass() );
        long fromIndex = 2L;
        TruncateLogCommand truncateLogCommand = new TruncateLogCommand( fromIndex );
        InFlightMap<RaftLogEntry> inFlightMap = new InFlightMap<>( true );

        inFlightMap.put( 0L, new RaftLogEntry( 0L, valueOf( 0 ) ) );
        inFlightMap.put( 1L, new RaftLogEntry( 1L, valueOf( 1 ) ) );
        inFlightMap.put( 2L, new RaftLogEntry( 2L, valueOf( 2 ) ) );
        inFlightMap.put( 3L, new RaftLogEntry( 3L, valueOf( 3 ) ) );

        //when
        truncateLogCommand.applyTo( inFlightMap, log );

        //then
        assertNotNull( inFlightMap.get( 0L ) );
        assertNotNull( inFlightMap.get( 1L ) );
        assertNull( inFlightMap.get( 2L ) );
        assertNull( inFlightMap.get( 3L ) );

        logProvider.assertAtLeastOnce( inLog( getClass() )
                .debug( "Start truncating in-flight-map from index %d. Current map:%n%s", fromIndex, inFlightMap ) );
    }

    @Test
    public void shouldTruncateWithGaps() throws Exception
    {
        //given
        long fromIndex = 1L;
        TruncateLogCommand truncateLogCommand = new TruncateLogCommand( fromIndex );

        InFlightMap<RaftLogEntry> inFlightMap = new InFlightMap<>( true );

        inFlightMap.put( 0L, new RaftLogEntry( 0L, valueOf( 0 ) ) );
        inFlightMap.put( 2L, new RaftLogEntry( 1L, valueOf( 1 ) ) );
        inFlightMap.put( 4L, new RaftLogEntry( 2L, valueOf( 2 ) ) );

        truncateLogCommand.applyTo( inFlightMap, NullLog.getInstance() );

        inFlightMap.put( 1L, new RaftLogEntry( 3L, valueOf( 1 ) ) );
        inFlightMap.put( 2L, new RaftLogEntry( 4L, valueOf( 2 ) ) );

        assertNotNull( inFlightMap.get( 0L ) );
        assertNotNull( inFlightMap.get( 1L ) );
        assertNotNull( inFlightMap.get( 2L ) );
    }
}
