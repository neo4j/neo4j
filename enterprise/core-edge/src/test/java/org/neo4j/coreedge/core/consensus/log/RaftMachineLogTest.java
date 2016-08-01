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
package org.neo4j.coreedge.core.consensus.log;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import org.neo4j.coreedge.core.consensus.RaftMachine;
import org.neo4j.coreedge.core.consensus.RaftMachineBuilder;
import org.neo4j.coreedge.core.consensus.ReplicatedInteger;
import org.neo4j.coreedge.core.replication.ReplicatedContent;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.coreedge.identity.RaftTestMemberSetBuilder;

import static org.junit.Assert.assertEquals;
import static org.neo4j.coreedge.core.consensus.ReplicatedInteger.valueOf;
import static org.neo4j.coreedge.core.consensus.TestMessageBuilders.appendEntriesRequest;
import static org.neo4j.coreedge.core.consensus.log.RaftLogHelper.readLogEntry;
import static org.neo4j.coreedge.identity.RaftTestMember.member;

@RunWith(MockitoJUnitRunner.class)
public class RaftMachineLogTest
{
    @Mock
    RaftMachineBuilder.CommitListener commitListener;

    private MemberId myself = member( 0 );
    private ReplicatedContent content = ReplicatedInteger.valueOf( 1 );
    private RaftLog testEntryLog;

    private RaftMachine raft;

    @Before
    public void before() throws Exception
    {
        // given
        testEntryLog = new InMemoryRaftLog();

        raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .raftLog( testEntryLog )
                .commitListener( commitListener )
                .build();
    }

    @Test
    public void shouldPersistAtSpecifiedLogIndex() throws Exception
    {
        // when
        raft.handle( appendEntriesRequest().leaderTerm( 0 ).prevLogIndex( -1 ).prevLogTerm( -1 )
                .logEntry( new RaftLogEntry( 0, content ) ).build() );

        // then
        assertEquals( 0, testEntryLog.appendIndex() );
        assertEquals( content, readLogEntry( testEntryLog, 0 ).content() );
    }

    @Test
    public void shouldOnlyPersistSameLogEntryOnce() throws Exception
    {
        // when
        raft.handle( appendEntriesRequest().leaderTerm( 0 ).prevLogIndex( -1 ).prevLogTerm( -1 )
                .logEntry( new RaftLogEntry( 0, content ) ).build() );
        raft.handle( appendEntriesRequest().leaderTerm( 0 ).prevLogIndex( -1 ).prevLogTerm( -1 )
                .logEntry( new RaftLogEntry( 0, content ) ).build() );

        // then
        assertEquals( 0, testEntryLog.appendIndex() );
        assertEquals( content, readLogEntry( testEntryLog, 0 ).content() );
    }

    @Test
    public void shouldRemoveFirstEntryConflictingWithNewEntry() throws Exception
    {
        // given
        testEntryLog.append( new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) ) );

        // when
        ReplicatedInteger newData = valueOf( 2 );
        raft.handle( appendEntriesRequest().leaderTerm( 2 ).prevLogIndex( -1 ).prevLogTerm( -1 )
                .logEntry( new RaftLogEntry( 2, newData ) ).build() );

        // then
        assertEquals( 0, testEntryLog.appendIndex() );
        assertEquals( newData, readLogEntry( testEntryLog, 0 ).content() );
    }

    @Test
    public void shouldRemoveLaterEntryFromLogConflictingWithNewEntry() throws Exception
    {
        testEntryLog.append( new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) ) );
        testEntryLog.append( new RaftLogEntry( 1, ReplicatedInteger.valueOf( 4 ) ) );
        testEntryLog.append( new RaftLogEntry( 1, ReplicatedInteger.valueOf( 7 ) ) ); /* conflicting entry */

        // when
        ReplicatedInteger newData = valueOf( 11 );
        raft.handle( appendEntriesRequest().leaderTerm( 2 ).prevLogIndex( 1 ).prevLogTerm( 1 )
                .logEntry( new RaftLogEntry( 2, newData ) ).build() );

        // then
        assertEquals( 2, testEntryLog.appendIndex() );
        assertEquals( newData, readLogEntry( testEntryLog, 2 ).content() );
    }

    @Test
    public void shouldNotTouchTheLogIfWeDoMatchEverywhere() throws Exception
    {
        testEntryLog.append( new RaftLogEntry( 1, ReplicatedInteger.valueOf( 99 ) ) ); // 0
        testEntryLog.append( new RaftLogEntry( 1, ReplicatedInteger.valueOf( 99 ) ) ); // 1
        testEntryLog.append( new RaftLogEntry( 1, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, ReplicatedInteger.valueOf( 99 ) ) ); // 5
        testEntryLog.append( new RaftLogEntry( 3, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, ReplicatedInteger.valueOf( 99 ) ) ); // 10

        // when instance A as leader
        ReplicatedInteger newData = valueOf( 99 );

        // Matches everything in the given range
        raft.handle( appendEntriesRequest().leaderTerm( 8 ).prevLogIndex( 4 ).prevLogTerm( 2 )
                .logEntry( new RaftLogEntry( 2, newData ) )
                .logEntry( new RaftLogEntry( 3, newData ) )
                .logEntry( new RaftLogEntry( 3, newData ) )
                .logEntry( new RaftLogEntry( 3, newData ) )
                .build() );

        // then
        assertEquals( 10, testEntryLog.appendIndex() );
        assertEquals( 3, testEntryLog.readEntryTerm( 10 ) );
    }

    /* Figure 3.6 */
    @Test
    public void shouldNotTouchTheLogIfWeDoNotMatchAnywhere() throws Exception
    {
        testEntryLog.append( new RaftLogEntry( 1, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 1, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 1, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, ReplicatedInteger.valueOf( 99 ) ) );

        // when instance A as leader
        ReplicatedInteger newData = valueOf( 99 );

        // Will not match as the entry at index 5 has term  2
        raft.handle( appendEntriesRequest().leaderTerm( 8 ).prevLogIndex( 5 ).prevLogTerm( 5 )
                .logEntry( new RaftLogEntry( 5, newData ) )
                .logEntry( new RaftLogEntry( 5, newData ) )
                .logEntry( new RaftLogEntry( 6, newData ) )
                .logEntry( new RaftLogEntry( 6, newData ) )
                .logEntry( new RaftLogEntry( 6, newData ) )
                .build() );

        // then
        assertEquals( 10, testEntryLog.appendIndex() );
        assertEquals( 3, testEntryLog.readEntryTerm( 10 ) );
    }

    @Test
    public void shouldTruncateOnFirstMismatchAndThenAppendOtherEntries() throws Exception
    {
        testEntryLog.append( new RaftLogEntry( 1, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 1, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 1, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, ReplicatedInteger.valueOf( 99 ) ) );

        // when instance A as leader
        ReplicatedInteger newData = valueOf( 99 );

        raft.handle( appendEntriesRequest().leaderTerm( 8 ).prevLogIndex( -1 ).prevLogTerm( -1 )
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
        assertEquals( 9, testEntryLog.appendIndex() );
        assertEquals( 1, testEntryLog.readEntryTerm(0) );
        assertEquals( 1, testEntryLog.readEntryTerm(1) );
        assertEquals( 1, testEntryLog.readEntryTerm(2) );
        assertEquals( 4, testEntryLog.readEntryTerm(3) );
        assertEquals( 4, testEntryLog.readEntryTerm(4) );
        assertEquals( 5, testEntryLog.readEntryTerm(5) );
        assertEquals( 5, testEntryLog.readEntryTerm(6) );
        assertEquals( 6, testEntryLog.readEntryTerm(7) );
        assertEquals( 6, testEntryLog.readEntryTerm(8) );
        assertEquals( 6, testEntryLog.readEntryTerm(9) );
    }

    @Test
    public void shouldNotTruncateLogIfHistoryDoesNotMatch() throws Exception
    {
        testEntryLog.append( new RaftLogEntry( 1, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 1, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 1, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, ReplicatedInteger.valueOf( 99 ) ) );

        // when instance A as leader
        ReplicatedInteger newData = valueOf( 99 );
        raft.handle( appendEntriesRequest().leaderTerm( 8 ).prevLogIndex( 3 ).prevLogTerm( 4 )
                .logEntry( new RaftLogEntry( 4, newData ) ) /* conflict */
                .logEntry( new RaftLogEntry( 5, newData ) )
                .logEntry( new RaftLogEntry( 5, newData ) )
                .logEntry( new RaftLogEntry( 6, newData ) )
                .logEntry( new RaftLogEntry( 6, newData ) )
                .logEntry( new RaftLogEntry( 6, newData ) )
                .build() );

        // then
        assertEquals( 10, testEntryLog.appendIndex() );
    }

    @Test
    public void shouldTruncateLogIfFirstEntryMatchesAndSecondEntryMismatchesOnTerm() throws Exception
    {
        testEntryLog.append( new RaftLogEntry( 1, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 1, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 1, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, ReplicatedInteger.valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, ReplicatedInteger.valueOf( 99 ) ) );

        // when instance A as leader
        ReplicatedInteger newData = valueOf( 99 );
        raft.handle( appendEntriesRequest().leaderTerm( 8 ).prevLogIndex( 1 ).prevLogTerm( 1 )
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
        assertEquals( 9, testEntryLog.appendIndex() );

        // stay the same
        assertEquals( 1, testEntryLog.readEntryTerm( 0 ) );
        assertEquals( 1, testEntryLog.readEntryTerm( 1 ) );
        assertEquals( 1, testEntryLog.readEntryTerm( 2 ) );

        // replaced
        assertEquals( 4, testEntryLog.readEntryTerm( 3 ) );
        assertEquals( 4, testEntryLog.readEntryTerm( 4 ) );
        assertEquals( 5, testEntryLog.readEntryTerm( 5 ) );
        assertEquals( 5, testEntryLog.readEntryTerm( 6 ) );
        assertEquals( 6, testEntryLog.readEntryTerm( 7 ) );
        assertEquals( 6, testEntryLog.readEntryTerm( 8 ) );
        assertEquals( 6, testEntryLog.readEntryTerm( 9 ) );
    }
}
