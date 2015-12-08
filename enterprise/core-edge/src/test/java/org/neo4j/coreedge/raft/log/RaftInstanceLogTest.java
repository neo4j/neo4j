/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.raft.log;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import org.neo4j.coreedge.raft.RaftInstance;
import org.neo4j.coreedge.raft.RaftInstanceBuilder;
import org.neo4j.coreedge.raft.ReplicatedInteger;
import org.neo4j.coreedge.raft.log.InMemoryRaftLog;
import org.neo4j.coreedge.raft.log.RaftLog;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.server.RaftTestMember;
import org.neo4j.coreedge.server.RaftTestMemberSetBuilder;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;

import static org.neo4j.coreedge.server.RaftTestMember.member;
import static org.neo4j.coreedge.raft.ReplicatedInteger.valueOf;
import static org.neo4j.coreedge.raft.TestMessageBuilders.appendEntriesRequest;

@RunWith(MockitoJUnitRunner.class)
public class RaftInstanceLogTest
{
    @Mock
    RaftLog.Listener entryConsumer;

    private RaftTestMember myself = member( 0 );
    private ReplicatedContent content = ReplicatedInteger.valueOf( 1 );
    private RaftLog testEntryLog;

    private RaftInstance<RaftTestMember> raft;

    @Before
    public void before() throws Exception
    {
        // given
        testEntryLog = new InMemoryRaftLog();
        testEntryLog.registerListener( entryConsumer );

        raft = new RaftInstanceBuilder<>( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .raftLog( testEntryLog )
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
        assertEquals( content, testEntryLog.readEntryContent( 0 ) );
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
        assertEquals( content, testEntryLog.readEntryContent( 0 ) );
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
        assertEquals( newData, testEntryLog.readEntryContent( 0 ) );
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
        assertEquals( newData, testEntryLog.readEntryContent( 2 ) );
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

        // Will not match as the entry at index 5 has term  2
        raft.handle( appendEntriesRequest().leaderTerm( 8 ).prevLogIndex( -1 ).prevLogTerm( -1 )
                .logEntry( new RaftLogEntry( 1, newData ) )
                .logEntry( new RaftLogEntry( 1, newData ) )
                .logEntry( new RaftLogEntry( 1, newData ) )
                .logEntry( new RaftLogEntry( 4, newData ) )
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

    // throw exception if trying to overwrite a position where a truncate hasn't happened
    @Test
    public void shouldThrowAnExceptionIfOverwritingTheLog() throws Exception
    {
        // given
        // Follower[ 1 2 2 3 ]
        // Leader  [ 1 4 4 ]
        // Follower will end up as [1 4 4 3 ]  [ 1 2 2 3 4 4 ]

        // when

        // then
    }


    // Leader   C-1 A3  [1,2,2,2] sends [1-3]
    // Follower C-1 A3  [1,1,1,1] => [1,2,2,2]

    // Match = Same term, Same index
    // None of the entries match
    // Some of the entries match
    // All of the entries match

    @Test
    public void shouldUpdateCommitIndexIfNecessary() throws Exception
    {
        //  If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

        // given

        testEntryLog.append( new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) ) );
        testEntryLog.append( new RaftLogEntry( 1, ReplicatedInteger.valueOf( 4 ) ) );
        testEntryLog.append( new RaftLogEntry( 1, ReplicatedInteger.valueOf( 7 ) ) );

        testEntryLog.commit( 1 );

        // when
        raft.handle(
                appendEntriesRequest().leaderTerm( 2 ).prevLogIndex( 2 ).prevLogTerm( 1 ).leaderCommit( 2 ).build() );

        // then
        assertEquals( 2, testEntryLog.commitIndex() );
        verify( entryConsumer ).onCommitted( eq( ReplicatedInteger.valueOf( 1 ) ), anyLong() );
    }
}
