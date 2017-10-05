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

import org.neo4j.causalclustering.core.consensus.log.cache.ConsecutiveInFlightCache;
import org.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;
import org.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.neo4j.causalclustering.core.consensus.ReplicatedInteger.valueOf;
import static org.neo4j.causalclustering.core.consensus.log.RaftLogHelper.readLogEntry;

public class BatchAppendLogEntriesTest
{
    private final Log log = NullLog.getInstance();
    private RaftLogEntry entryA = new RaftLogEntry( 0, valueOf( 100 ) );
    private RaftLogEntry entryB = new RaftLogEntry( 1, valueOf( 200 ) );
    private RaftLogEntry entryC = new RaftLogEntry( 2, valueOf( 300 ) );
    private RaftLogEntry entryD = new RaftLogEntry( 3, valueOf( 400 ) );

    @Test
    public void shouldApplyMultipleEntries() throws Exception
    {
        // given
        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        BatchAppendLogEntries batchAppendLogEntries =
                new BatchAppendLogEntries( 0, 0, new RaftLogEntry[]{entryA, entryB, entryC} );

        // when
        batchAppendLogEntries.applyTo( raftLog, log );

        // then
        assertEquals( entryA, readLogEntry( raftLog, 0 ) );
        assertEquals( entryB, readLogEntry( raftLog, 1 ) );
        assertEquals( entryC, readLogEntry( raftLog, 2 ) );
        assertEquals( 2, raftLog.appendIndex() );
    }

    @Test
    public void shouldApplyFromOffsetOnly() throws Exception
    {
        // given
        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        BatchAppendLogEntries batchAppendLogEntries =
                new BatchAppendLogEntries( 0, 2, new RaftLogEntry[]{entryA, entryB, entryC, entryD} );

        // when
        batchAppendLogEntries.applyTo( raftLog, log );

        // then
        assertEquals( entryC, readLogEntry( raftLog, 0 ) );
        assertEquals( entryD, readLogEntry( raftLog, 1 ) );
        assertEquals( 1, raftLog.appendIndex() );
    }

    @Test
    public void applyTo() throws Exception
    {
        //Test that batch commands apply entries to the cache.

        //given
        long baseIndex = 0;
        int offset = 1;
        RaftLogEntry[] entries =
                new RaftLogEntry[]{new RaftLogEntry( 0L, valueOf( 0 ) ), new RaftLogEntry( 1L, valueOf( 1 ) ),
                        new RaftLogEntry( 2L, valueOf( 2 ) ),};

        BatchAppendLogEntries batchAppend = new BatchAppendLogEntries( baseIndex, offset, entries );

        InFlightCache cache = new ConsecutiveInFlightCache();

        //when
        batchAppend.applyTo( cache, log );

        //then
        assertNull( cache.get( 0L ) );
        assertNotNull( cache.get( 1L ) );
        assertNotNull( cache.get( 2L ) );
    }
}
