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
package org.neo4j.coreedge.raft.outcome;

import org.junit.Test;

import org.neo4j.coreedge.raft.log.InMemoryRaftLog;
import org.neo4j.coreedge.raft.log.RaftLogEntry;

import static org.junit.Assert.assertEquals;
import static org.neo4j.coreedge.raft.ReplicatedInteger.valueOf;
import static org.neo4j.coreedge.raft.log.RaftLogHelper.readLogEntry;

public class BatchAppendLogEntriesTest
{
    private RaftLogEntry entryA = new RaftLogEntry( 0, valueOf( 100 ) );
    private RaftLogEntry entryB = new RaftLogEntry( 1, valueOf( 200 ) );
    private RaftLogEntry entryC = new RaftLogEntry( 2, valueOf( 300 ) );
    private RaftLogEntry entryD = new RaftLogEntry( 3, valueOf( 400 ) );

    @Test
    public void shouldApplyMultipleEntries() throws Exception
    {
        // given
        InMemoryRaftLog log = new InMemoryRaftLog();
        BatchAppendLogEntries batchAppendLogEntries =
                new BatchAppendLogEntries( 0, 0, new RaftLogEntry[]{entryA, entryB, entryC} );

        // when
        batchAppendLogEntries.applyTo( log );

        // then
        assertEquals( entryA, readLogEntry( log, 0 ) );
        assertEquals( entryB, readLogEntry( log, 1 ) );
        assertEquals( entryC, readLogEntry( log, 2 ) );
        assertEquals( 2, log.appendIndex() );
    }

    @Test
    public void shouldApplyFromOffsetOnly() throws Exception
    {
        // given
        InMemoryRaftLog log = new InMemoryRaftLog();
        BatchAppendLogEntries batchAppendLogEntries =
                new BatchAppendLogEntries( 0, 2, new RaftLogEntry[]{entryA, entryB, entryC, entryD} );

        // when
        batchAppendLogEntries.applyTo( log );

        // then
        assertEquals( entryC, readLogEntry( log, 0 ) );
        assertEquals( entryD, readLogEntry( log, 1 ) );
        assertEquals( 1, log.appendIndex() );
    }
}
