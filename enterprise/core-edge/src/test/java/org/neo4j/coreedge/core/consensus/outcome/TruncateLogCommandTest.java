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
package org.neo4j.coreedge.core.consensus.outcome;

import org.junit.Test;

import org.neo4j.coreedge.core.consensus.log.RaftLogEntry;
import org.neo4j.coreedge.core.consensus.log.segmented.InFlightMap;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.neo4j.coreedge.core.consensus.ReplicatedInteger.valueOf;

public class TruncateLogCommandTest
{
    @Test
    public void applyTo() throws Exception
    {
        //Test that truncate commands correctly remove entries from the cache.

        //given
        long fromIndex = 2L;
        TruncateLogCommand truncateLogCommand = new TruncateLogCommand( fromIndex );

        InFlightMap<Long,RaftLogEntry> inFlightMap = new InFlightMap<>();

        inFlightMap.register( 0L, new RaftLogEntry( 0L, valueOf( 0 ) ) );
        inFlightMap.register( 1L, new RaftLogEntry( 1L, valueOf( 1 ) ) );
        inFlightMap.register( 2L, new RaftLogEntry( 2L, valueOf( 2 ) ) );
        inFlightMap.register( 3L, new RaftLogEntry( 3L, valueOf( 3 ) ) );

        //when
        truncateLogCommand.applyTo( inFlightMap );

        //then
        assertNotNull( inFlightMap.retrieve( 0L ) );
        assertNotNull( inFlightMap.retrieve( 1L ) );
        assertNull( inFlightMap.retrieve( 2L ) );
        assertNull( inFlightMap.retrieve( 3L ) );
    }
}
