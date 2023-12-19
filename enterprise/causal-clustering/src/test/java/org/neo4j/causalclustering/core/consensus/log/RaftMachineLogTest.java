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
package org.neo4j.causalclustering.core.consensus.log;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.core.consensus.RaftMachineBuilder;
import org.neo4j.causalclustering.core.consensus.ReplicatedInteger;
import org.neo4j.causalclustering.core.consensus.membership.RaftTestGroup;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.identity.RaftTestMemberSetBuilder;

import static org.junit.Assert.assertEquals;
import static org.neo4j.causalclustering.core.consensus.ReplicatedInteger.valueOf;
import static org.neo4j.causalclustering.core.consensus.TestMessageBuilders.appendEntriesRequest;
import static org.neo4j.causalclustering.core.consensus.log.RaftLogHelper.readLogEntry;
import static org.neo4j.causalclustering.identity.RaftTestMember.member;

@RunWith( MockitoJUnitRunner.class )
public class RaftMachineLogTest
{
    @Mock
    RaftMachineBuilder.CommitListener commitListener;

    private MemberId myself = member( 0 );
    private ReplicatedContent content = valueOf( 1 );
    private RaftLog testEntryLog;

    private RaftMachine raft;

    @Before
    public void before() throws Exception
    {
        // given
        testEntryLog = new InMemoryRaftLog();
        testEntryLog.append( new RaftLogEntry( 0, new RaftTestGroup( myself ) ) );

        raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .raftLog( testEntryLog )
                .commitListener( commitListener )
                .build();
    }

    @Test
    public void shouldPersistAtSpecifiedLogIndex() throws Exception
    {
        // when
        raft.handle( appendEntriesRequest().leaderTerm( 0 ).prevLogIndex( 0 ).prevLogTerm( 0 )
                .logEntry( new RaftLogEntry( 0, content ) ).build() );

        // then
        assertEquals( 1, testEntryLog.appendIndex() );
        assertEquals( content, readLogEntry( testEntryLog, 1 ).content() );
    }

    @Test
    public void shouldOnlyPersistSameLogEntryOnce() throws Exception
    {
        // when
        raft.handle( appendEntriesRequest().leaderTerm( 0 ).prevLogIndex( 0 ).prevLogTerm( 0 )
                .logEntry( new RaftLogEntry( 0, content ) ).build() );
        raft.handle( appendEntriesRequest().leaderTerm( 0 ).prevLogIndex( 0 ).prevLogTerm( 0 )
                .logEntry( new RaftLogEntry( 0, content ) ).build() );

        // then
        assertEquals( 1, testEntryLog.appendIndex() );
        assertEquals( content, readLogEntry( testEntryLog, 1 ).content() );
    }

    @Test
    public void shouldRemoveLaterEntryFromLogConflictingWithNewEntry() throws Exception
    {
        testEntryLog.append( new RaftLogEntry( 1, valueOf( 1 ) ) );
        testEntryLog.append( new RaftLogEntry( 1, valueOf( 4 ) ) );
        testEntryLog.append( new RaftLogEntry( 1, valueOf( 7 ) ) ); /* conflicting entry */

        // when
        ReplicatedInteger newData = valueOf( 11 );
        raft.handle( appendEntriesRequest().leaderTerm( 2 ).prevLogIndex( 2 ).prevLogTerm( 1 )
                .logEntry( new RaftLogEntry( 2, newData ) ).build() );

        // then
        assertEquals( 3, testEntryLog.appendIndex() );
        assertEquals( newData, readLogEntry( testEntryLog, 3 ).content() );
    }

    @Test
    public void shouldNotTouchTheLogIfWeDoMatchEverywhere() throws Exception
    {
        testEntryLog.append( new RaftLogEntry( 1, valueOf( 99 ) ) ); // 0
        testEntryLog.append( new RaftLogEntry( 1, valueOf( 99 ) ) ); // 1
        testEntryLog.append( new RaftLogEntry( 1, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, valueOf( 99 ) ) ); // 5
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) ); // 10

        // when instance A as leader
        ReplicatedInteger newData = valueOf( 99 );

        // Matches everything in the given range
        raft.handle( appendEntriesRequest().leaderTerm( 8 ).prevLogIndex( 5 ).prevLogTerm( 2 )
                .logEntry( new RaftLogEntry( 2, newData ) )
                .logEntry( new RaftLogEntry( 3, newData ) )
                .logEntry( new RaftLogEntry( 3, newData ) )
                .logEntry( new RaftLogEntry( 3, newData ) )
                .build() );

        // then
        assertEquals( 11, testEntryLog.appendIndex() );
        assertEquals( 3, testEntryLog.readEntryTerm( 11 ) );
    }

    /* Figure 3.6 */
    @Test
    public void shouldNotTouchTheLogIfWeDoNotMatchAnywhere() throws Exception
    {
        testEntryLog.append( new RaftLogEntry( 1, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 1, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 1, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );

        // when instance A as leader
        ReplicatedInteger newData = valueOf( 99 );

        // Will not match as the entry at index 5 has term  2
        raft.handle( appendEntriesRequest().leaderTerm( 8 ).prevLogIndex( 6 ).prevLogTerm( 5 )
                .logEntry( new RaftLogEntry( 5, newData ) )
                .logEntry( new RaftLogEntry( 5, newData ) )
                .logEntry( new RaftLogEntry( 6, newData ) )
                .logEntry( new RaftLogEntry( 6, newData ) )
                .logEntry( new RaftLogEntry( 6, newData ) )
                .build() );

        // then
        assertEquals( 11, testEntryLog.appendIndex() );
        assertEquals( 3, testEntryLog.readEntryTerm( 11 ) );
    }

    @Test
    public void shouldTruncateOnFirstMismatchAndThenAppendOtherEntries() throws Exception
    {
        testEntryLog.append( new RaftLogEntry( 1, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 1, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 1, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );

        // when instance A as leader
        ReplicatedInteger newData = valueOf( 99 );

        raft.handle( appendEntriesRequest().leaderTerm( 8 ).prevLogIndex( 0 ).prevLogTerm( 0 )
                .logEntry( new RaftLogEntry( 1, newData ) )
                .logEntry( new RaftLogEntry( 1, newData ) )
                .logEntry( new RaftLogEntry( 1, newData ) )
                .logEntry( new RaftLogEntry( 4, newData ) ) // term mismatch - existing term is 2
                .logEntry( new RaftLogEntry( 4, newData ) )
                .logEntry( new RaftLogEntry( 5, newData ) )
                .logEntry( new RaftLogEntry( 5, newData ) )
                .logEntry( new RaftLogEntry( 6, newData ) )
                .logEntry( new RaftLogEntry( 6, newData ) )
                .logEntry( new RaftLogEntry( 6, newData ) )
                .build() );

        // then
        assertEquals( 10, testEntryLog.appendIndex() );
        assertEquals( 1, testEntryLog.readEntryTerm(1) );
        assertEquals( 1, testEntryLog.readEntryTerm(2) );
        assertEquals( 1, testEntryLog.readEntryTerm(3) );
        assertEquals( 4, testEntryLog.readEntryTerm(4) );
        assertEquals( 4, testEntryLog.readEntryTerm(5) );
        assertEquals( 5, testEntryLog.readEntryTerm(6) );
        assertEquals( 5, testEntryLog.readEntryTerm(7) );
        assertEquals( 6, testEntryLog.readEntryTerm(8) );
        assertEquals( 6, testEntryLog.readEntryTerm(9) );
        assertEquals( 6, testEntryLog.readEntryTerm(10) );
    }

    @Test
    public void shouldNotTruncateLogIfHistoryDoesNotMatch() throws Exception
    {
        testEntryLog.append( new RaftLogEntry( 1, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 1, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 1, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );

        // when instance A as leader
        ReplicatedInteger newData = valueOf( 99 );
        raft.handle( appendEntriesRequest().leaderTerm( 8 ).prevLogIndex( 4 ).prevLogTerm( 4 )
                .logEntry( new RaftLogEntry( 4, newData ) ) /* conflict */
                .logEntry( new RaftLogEntry( 5, newData ) )
                .logEntry( new RaftLogEntry( 5, newData ) )
                .logEntry( new RaftLogEntry( 6, newData ) )
                .logEntry( new RaftLogEntry( 6, newData ) )
                .logEntry( new RaftLogEntry( 6, newData ) )
                .build() );

        // then
        assertEquals( 11, testEntryLog.appendIndex() );
    }

    @Test
    public void shouldTruncateLogIfFirstEntryMatchesAndSecondEntryMismatchesOnTerm() throws Exception
    {
        testEntryLog.append( new RaftLogEntry( 1, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 1, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 1, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );

        // when instance A as leader
        ReplicatedInteger newData = valueOf( 99 );
        raft.handle( appendEntriesRequest().leaderTerm( 8 ).prevLogIndex( 2 ).prevLogTerm( 1 )
                .logEntry( new RaftLogEntry( 1, newData ) )
                .logEntry( new RaftLogEntry( 4, newData ) ) /* conflict */
                .logEntry( new RaftLogEntry( 4, newData ) )
                .logEntry( new RaftLogEntry( 5, newData ) )
                .logEntry( new RaftLogEntry( 5, newData ) )
                .logEntry( new RaftLogEntry( 6, newData ) )
                .logEntry( new RaftLogEntry( 6, newData ) )
                .logEntry( new RaftLogEntry( 6, newData ) )
                .build() );

        // then
        assertEquals( 10, testEntryLog.appendIndex() );

        // stay the same
        assertEquals( 1, testEntryLog.readEntryTerm( 1 ) );
        assertEquals( 1, testEntryLog.readEntryTerm( 2 ) );
        assertEquals( 1, testEntryLog.readEntryTerm( 3 ) );

        // replaced
        assertEquals( 4, testEntryLog.readEntryTerm( 4 ) );
        assertEquals( 4, testEntryLog.readEntryTerm( 5 ) );
        assertEquals( 5, testEntryLog.readEntryTerm( 6 ) );
        assertEquals( 5, testEntryLog.readEntryTerm( 7 ) );
        assertEquals( 6, testEntryLog.readEntryTerm( 8 ) );
        assertEquals( 6, testEntryLog.readEntryTerm( 9 ) );
        assertEquals( 6, testEntryLog.readEntryTerm( 10 ) );
    }
}
