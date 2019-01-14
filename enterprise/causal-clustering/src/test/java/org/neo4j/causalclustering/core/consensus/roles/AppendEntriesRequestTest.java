/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core.consensus.roles;

import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.neo4j.causalclustering.core.consensus.RaftMessages.AppendEntries.Response;
import org.neo4j.causalclustering.core.consensus.ReplicatedString;
import org.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.consensus.outcome.BatchAppendLogEntries;
import org.neo4j.causalclustering.core.consensus.outcome.Outcome;
import org.neo4j.causalclustering.core.consensus.outcome.TruncateLogCommand;
import org.neo4j.causalclustering.core.consensus.state.RaftState;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.causalclustering.core.consensus.MessageUtils.messageFor;
import static org.neo4j.causalclustering.core.consensus.TestMessageBuilders.appendEntriesRequest;
import static org.neo4j.causalclustering.core.consensus.roles.AppendEntriesRequestTest.ContentGenerator.content;
import static org.neo4j.causalclustering.core.consensus.state.RaftStateBuilder.raftState;
import static org.neo4j.causalclustering.identity.RaftTestMember.member;

@RunWith( Parameterized.class )
public class AppendEntriesRequestTest
{

    @Parameterized.Parameters( name = "{0} with leader {1} terms ahead." )
    public static Collection<Object[]> data()
    {
        return Arrays.asList( new Object[][]{
                {Role.FOLLOWER, 0}, {Role.FOLLOWER, 1}, {Role.LEADER, 1}, {Role.CANDIDATE, 1}
        } );
    }

    @Parameterized.Parameter( value = 0 )
    public Role role;

    @Parameterized.Parameter( value = 1 )
    public int leaderTermDifference;

    private MemberId myself = member( 0 );
    private MemberId leader = member( 1 );

    @Test
    public void shouldAcceptInitialEntryAfterBootstrap() throws Exception
    {
        RaftLog raftLog = bootstrappedLog();
        RaftState state = raftState()
                .entryLog( raftLog )
                .myself( myself )
                .build();

        long leaderTerm = state.term() + leaderTermDifference;
        RaftLogEntry logEntry = new RaftLogEntry( leaderTerm, content() );

        // when
        Outcome outcome = role.handler.handle( appendEntriesRequest()
                .from( leader )
                .leaderTerm( leaderTerm )
                .prevLogIndex( 0 )
                .prevLogTerm( 0 )
                .logEntry( logEntry )
                .build(), state, log() );

        // then
        assertTrue( ((Response) messageFor( outcome, leader )).success() );
        assertThat( outcome.getLogCommands(), hasItem( new BatchAppendLogEntries( 1, 0, new RaftLogEntry[]{ logEntry } ) ) );
    }

    @Test
    public void shouldAcceptInitialEntriesAfterBootstrap() throws Exception
    {
        RaftLog raftLog = bootstrappedLog();
        RaftState state = raftState()
                .entryLog( raftLog )
                .myself( myself )
                .build();

        long leaderTerm = state.term() + leaderTermDifference;
        RaftLogEntry logEntry1 = new RaftLogEntry( leaderTerm, content() );
        RaftLogEntry logEntry2 = new RaftLogEntry( leaderTerm, content() );

        // when
        Outcome outcome = role.handler.handle( appendEntriesRequest()
                .from( leader )
                .leaderTerm( leaderTerm )
                .prevLogIndex( 0 )
                .prevLogTerm( 0 )
                .logEntry( logEntry1 )
                .logEntry( logEntry2 )
                .build(), state, log() );

        // then
        assertTrue( ((Response) messageFor( outcome, leader )).success() );
        assertThat( outcome.getLogCommands(), hasItem( new BatchAppendLogEntries( 1, 0,
                new RaftLogEntry[]{logEntry1, logEntry2} ) ) );
    }

    private RaftLog bootstrappedLog()
    {
        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        raftLog.append( new RaftLogEntry( 0, content() ) );
        return raftLog;
    }

    @Test
    public void shouldRejectDiscontinuousEntries() throws Exception
    {
        // given
        RaftState state = raftState()
                .myself( myself )
                .build();

        long leaderTerm = state.term() + leaderTermDifference;

        // when
        Outcome outcome = role.handler.handle( appendEntriesRequest()
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
        RaftState state = raftState()
                .myself( myself )
                .entryLog( raftLog )
                .build();

        long leaderTerm = state.term() + leaderTermDifference;
        raftLog.append( new RaftLogEntry( leaderTerm, content() ) );

        // when
        Outcome outcome = role.handler.handle( appendEntriesRequest()
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
        RaftState state = raftState()
                .myself( myself )
                .term( 5 )
                .entryLog( raftLog )
                .build();

        long leaderTerm = state.term() + leaderTermDifference;
        raftLog.append( new RaftLogEntry( state.term() - 1, content() ) );
        raftLog.append( new RaftLogEntry( state.term() - 1, content() ) );

        // when
        long previousIndex = raftLog.appendIndex() - 1;
        Outcome outcome = role.handler.handle( appendEntriesRequest()
                .from( leader )
                .leaderTerm( leaderTerm )
                .prevLogIndex( previousIndex )
                .prevLogTerm( raftLog.readEntryTerm( previousIndex ) )
                .logEntry( new RaftLogEntry( leaderTerm, content() ) )
                .build(), state, log() );

        // then
        assertTrue( ((Response) messageFor( outcome, leader )).success() );
        assertThat( outcome.getLogCommands(), hasItem( new TruncateLogCommand( 1 ) ) );
    }

    @Test
    public void shouldCommitEntry() throws Exception
    {
        // given
        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        RaftState state = raftState()
                .entryLog( raftLog )
                .myself( myself )
                .build();

        long leaderTerm = state.term() + leaderTermDifference;
        raftLog.append( new RaftLogEntry( leaderTerm, content() ) );

        // when
        Outcome outcome = role.handler.handle( appendEntriesRequest()
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
        RaftState state = raftState()
                .entryLog( raftLog )
                .myself( myself )
                .build();

        long leaderTerm = state.term() + leaderTermDifference;
        RaftLogEntry previouslyAppendedEntry = new RaftLogEntry( leaderTerm, content() );
        raftLog.append( previouslyAppendedEntry );
        RaftLogEntry newLogEntry = new RaftLogEntry( leaderTerm, content() );

        // when
        Outcome outcome = role.handler.handle( appendEntriesRequest()
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
        RaftState state = raftState()
                .entryLog( raftLog )
                .myself( myself )
                .build();

        long leaderTerm = state.term() + leaderTermDifference;
        RaftLogEntry previouslyAppendedEntry = new RaftLogEntry( leaderTerm, content() );
        raftLog.append( previouslyAppendedEntry );

        // when
        Outcome outcome = role.handler.handle( appendEntriesRequest()
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

    public RaftState newState() throws IOException
    {
        return raftState().myself( myself ).build();
    }

    private Log log()
    {
        return NullLogProvider.getInstance().getLog( getClass() );
    }

    static class ContentGenerator
    {
        private static int count;

        public static ReplicatedString content()
        {
            return new ReplicatedString( String.format( "content#%d", count++ ) );
        }
    }
}
