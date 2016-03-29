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
package org.neo4j.coreedge.raft.roles;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.neo4j.coreedge.raft.RaftMessages.AppendEntries.Response;
import org.neo4j.coreedge.raft.ReplicatedString;
import org.neo4j.coreedge.raft.log.InMemoryRaftLog;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.outcome.BatchAppendLogEntries;
import org.neo4j.coreedge.raft.outcome.Outcome;
import org.neo4j.coreedge.raft.outcome.TruncateLogCommand;
import org.neo4j.coreedge.raft.state.RaftState;
import org.neo4j.coreedge.server.RaftTestMember;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import static org.neo4j.coreedge.raft.MessageUtils.messageFor;
import static org.neo4j.coreedge.raft.TestMessageBuilders.appendEntriesRequest;
import static org.neo4j.coreedge.raft.roles.AppendEntriesRequestTest.ContentGenerator.content;
import static org.neo4j.coreedge.raft.state.RaftStateBuilder.raftState;
import static org.neo4j.coreedge.server.RaftTestMember.member;

import org.hamcrest.Matchers;

@RunWith(Parameterized.class)
public class AppendEntriesRequestTest
{
    @Parameterized.Parameters(name = "{0} with leader {1} terms ahead.")
    public static Collection<Object[]> data()
    {
        return Arrays.asList( new Object[][]{
                {Role.FOLLOWER, 0}, {Role.FOLLOWER, 1}, {Role.LEADER, 1}, {Role.CANDIDATE, 1}
        } );
    }

    @Parameterized.Parameter(value = 0)
    public Role role;

    @Parameterized.Parameter(value = 1)
    public int leaderTermDifference;

    private RaftTestMember myself = member( 0 );
    private RaftTestMember leader = member( 1 );

    @Test
    public void shouldAcceptInitialEntry() throws Exception
    {
        RaftState<RaftTestMember> state = raftState()
                .myself( myself )
                .build();

        long leaderTerm = state.term() + leaderTermDifference;
        RaftLogEntry logEntry = new RaftLogEntry( leaderTerm, content() );

        // when
        Outcome<RaftTestMember> outcome = role.handler.handle( appendEntriesRequest()
                .from( leader )
                .leaderTerm( leaderTerm )
                .prevLogIndex( -1 )
                .prevLogTerm( -1 )
                .logEntry( logEntry )
                .build(), state, log() );

        // then
        assertTrue( ((Response) messageFor( outcome, leader )).success() );
        assertThat( outcome.getLogCommands(), hasItem( new BatchAppendLogEntries( 0, 0, new RaftLogEntry[]{ logEntry } ) ) );
    }

    @Test
    public void shouldAcceptInitialEntries() throws Exception
    {
        RaftState<RaftTestMember> state = raftState()
                .myself( myself )
                .build();

        long leaderTerm = state.term() + leaderTermDifference;
        RaftLogEntry logEntry1 = new RaftLogEntry( leaderTerm, content() );
        RaftLogEntry logEntry2 = new RaftLogEntry( leaderTerm, content() );

        // when
        Outcome<RaftTestMember> outcome = role.handler.handle( appendEntriesRequest()
                .from( leader )
                .leaderTerm( leaderTerm )
                .prevLogIndex( -1 )
                .prevLogTerm( -1 )
                .logEntry( logEntry1 )
                .logEntry( logEntry2 )
                .build(), state, log() );

        // then
        assertTrue( ((Response) messageFor( outcome, leader )).success() );
        assertThat( outcome.getLogCommands(), hasItem( new BatchAppendLogEntries( 0, 0,
                new RaftLogEntry[]{logEntry1, logEntry2} ) ) );
    }

    @Test
    public void shouldRejectDiscontinuousEntries() throws Exception
    {
        // given
        RaftState<RaftTestMember> state = raftState()
                .myself( myself )
                .build();

        long leaderTerm = state.term() + leaderTermDifference;

        // when
        Outcome<RaftTestMember> outcome = role.handler.handle( appendEntriesRequest()
                .from( leader )
                .leaderTerm( leaderTerm )
                .prevLogIndex( state.entryLog().appendIndex() + 1 )
                .prevLogTerm( leaderTerm )
                .logEntry( new RaftLogEntry( leaderTerm, content() ) )
                .build(), state, log() );

        // then
        Response response = (Response) messageFor( outcome, leader );
        assertEquals( state.entryLog().appendIndex(), response.appendIndex() );
        assertFalse( response.success() );
    }

    @Test
    public void shouldAcceptContinuousEntries() throws Exception
    {
        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        RaftState<RaftTestMember> state = raftState()
                .myself( myself )
                .entryLog( raftLog )
                .build();

        long leaderTerm = state.term() + leaderTermDifference;
        raftLog.append( new RaftLogEntry( leaderTerm, content() ) );

        // when
        Outcome<RaftTestMember> outcome = role.handler.handle( appendEntriesRequest()
                .from( leader )
                .leaderTerm( leaderTerm )
                .prevLogIndex( raftLog.appendIndex() )
                .prevLogTerm( leaderTerm )
                .logEntry( new RaftLogEntry( leaderTerm, content() ) )
                .build(), state, log() );

        // then
        assertTrue( ((Response) messageFor( outcome, leader )).success() );
    }

    @Test
    public void shouldTruncateOnReceiptOfConflictingEntry() throws Exception
    {
        // given
        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        RaftState<RaftTestMember> state = raftState()
                .myself( myself )
                .term( 5 )
                .entryLog( raftLog )
                .build();

        long leaderTerm = state.term() + leaderTermDifference;
        raftLog.append( new RaftLogEntry( state.term() - 1, content() ) );

        // when
        Outcome<RaftTestMember> outcome = role.handler.handle( appendEntriesRequest()
                .from( leader )
                .leaderTerm( leaderTerm )
                .prevLogIndex( raftLog.appendIndex() - 1 )
                .prevLogTerm( -1 )
                .logEntry( new RaftLogEntry( leaderTerm, content() ) )
                .build(), state, log() );

        // then
        assertTrue( ((Response) messageFor( outcome, leader )).success() );
        assertThat( outcome.getLogCommands(), hasItem( new TruncateLogCommand( 0 ) ) );
    }

    @Test
    public void shouldCommitEntry() throws Exception
    {
        // given
        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        RaftState<RaftTestMember> state = raftState()
                .entryLog( raftLog )
                .myself( myself )
                .build();

        long leaderTerm = state.term() + leaderTermDifference;
        raftLog.append( new RaftLogEntry( leaderTerm, content() ) );

        // when
        Outcome<RaftTestMember> outcome = role.handler.handle( appendEntriesRequest()
                .from( leader )
                .leaderTerm( leaderTerm )
                .prevLogIndex( raftLog.appendIndex() )
                .prevLogTerm( leaderTerm )
                .leaderCommit( 0 )
                .build(), state, log() );

        // then
        assertTrue( ((Response) messageFor( outcome, leader )).success() );
        assertThat( outcome.getCommitIndex(), Matchers.equalTo( 0L ) );
    }

    @Test
    public void shouldAppendNewEntryAndCommitPreviouslyAppendedEntry() throws Exception
    {
        // given
        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        RaftState<RaftTestMember> state = raftState()
                .entryLog( raftLog )
                .myself( myself )
                .build();

        long leaderTerm = state.term() + leaderTermDifference;
        RaftLogEntry previouslyAppendedEntry = new RaftLogEntry( leaderTerm, content() );
        raftLog.append( previouslyAppendedEntry );
        RaftLogEntry newLogEntry = new RaftLogEntry( leaderTerm, content() );

        // when
        Outcome<RaftTestMember> outcome = role.handler.handle( appendEntriesRequest()
                .from( leader )
                .leaderTerm( leaderTerm )
                .prevLogIndex( raftLog.appendIndex() )
                .prevLogTerm( leaderTerm )
                .logEntry( newLogEntry )
                .leaderCommit( 0 )
                .build(), state, log() );

        // then
        assertTrue( ((Response) messageFor( outcome, leader )).success() );
        assertThat( outcome.getCommitIndex(), Matchers.equalTo( 0L ) );
        assertThat( outcome.getLogCommands(), hasItem( new BatchAppendLogEntries( 1, 0,
                new RaftLogEntry[]{ newLogEntry } ) ) );
    }

    @Test
    public void shouldNotCommitAheadOfMatchingHistory() throws Exception
    {
        // given
        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        RaftState<RaftTestMember> state = raftState()
                .entryLog( raftLog )
                .myself( myself )
                .build();

        long leaderTerm = state.term() + leaderTermDifference;
        RaftLogEntry previouslyAppendedEntry = new RaftLogEntry( leaderTerm, content() );
        raftLog.append( previouslyAppendedEntry );

        // when
        Outcome<RaftTestMember> outcome = role.handler.handle( appendEntriesRequest()
                .from( leader )
                .leaderTerm( leaderTerm )
                .prevLogIndex( raftLog.appendIndex() + 1 )
                .prevLogTerm( leaderTerm )
                .leaderCommit( 0 )
                .build(), state, log() );


        // then
        assertFalse( ((Response) messageFor( outcome, leader )).success() );
        assertThat( outcome.getLogCommands(), empty() );
    }

    public RaftState<RaftTestMember> newState() throws IOException
    {
        return raftState().myself( myself ).build();
    }

    private Log log()
    {
        return NullLogProvider.getInstance().getLog( getClass() );
    }

    static class ContentGenerator
    {
        private static int count = 0;

        public static ReplicatedString content()
        {
            return new ReplicatedString( String.format( "content#%d", count++ ) );
        }
    }
}
