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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import org.neo4j.coreedge.raft.RaftMessages;
import org.neo4j.coreedge.raft.RaftMessages.RaftMessage;
import org.neo4j.coreedge.raft.RaftMessages.Timeout.Election;
import org.neo4j.coreedge.raft.ReplicatedString;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.net.Inbound;
import org.neo4j.coreedge.raft.outcome.Outcome;
import org.neo4j.coreedge.raft.state.RaftState;
import org.neo4j.coreedge.server.RaftTestMember;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.coreedge.raft.MessageUtils.messageFor;
import static org.neo4j.coreedge.raft.RaftMessages.AppendEntries;
import static org.neo4j.coreedge.raft.TestMessageBuilders.appendEntriesRequest;
import static org.neo4j.coreedge.raft.roles.Role.CANDIDATE;
import static org.neo4j.coreedge.raft.state.RaftStateBuilder.raftState;
import static org.neo4j.coreedge.server.RaftTestMember.member;
import static org.neo4j.helpers.collection.Iterators.asSet;

@RunWith(MockitoJUnitRunner.class)
public class FollowerTest
{
    private RaftTestMember myself = member( 0 );

    /* A few members that we use at will in tests. */
    private RaftTestMember member1 = member( 1 );
    private RaftTestMember member2 = member( 2 );

    @Mock
    private Inbound inbound;

    @Test
    public void followerShouldTransitToCandidateAndInstigateAnElectionAfterTimeout() throws Exception
    {
        // given
        RaftState<RaftTestMember> state = raftState()
                .myself( myself )
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();


        // when
        Outcome<RaftTestMember> outcome = new Follower().handle( new Election<>( myself ), state,
                log() );

        state.update( outcome );

        // then
        assertEquals( CANDIDATE, outcome.getNewRole() );

        assertNotNull( messageFor( outcome, member1 ) );
        assertNotNull( messageFor( outcome, member2 ) );
    }

    @Test
    public void shouldBecomeCandidateOnReceivingElectionTimeoutMessage() throws Exception
    {
        // given
        RaftState<RaftTestMember> state = raftState()
                .myself( myself )
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();

        Follower follower = new Follower();

        // when
        Outcome outcome = follower.handle( new Election<>( myself ), state, log() );

        // then
        assertEquals( CANDIDATE, outcome.getNewRole() );
    }

    @Test
    public void followerReceivingHeartbeatIndicatingClusterIsAheadShouldElicitAppendResponse() throws Exception
    {
        // given
        int term = 1;
        int followerAppendIndex = 9;
        RaftState<RaftTestMember> state = raftState()
                .myself( myself )
                .term( term )
                .build();

        Follower follower = new Follower();
        appendSomeEntriesToLog( state, follower, followerAppendIndex, term );

        AppendEntries.Request<RaftTestMember> heartbeat = appendEntriesRequest().from( member1 )
                .leader( member1 )
                .leaderTerm( term )
                .prevLogIndex( followerAppendIndex + 2 ) // leader has appended 2 ahead from this follower
                .prevLogTerm( term ) // in the same term
                .build(); // no entries, this is a heartbeat

        Outcome<RaftTestMember> outcome = follower.handle( heartbeat, state, log() );

        assertEquals( 1, outcome.getOutgoingMessages().size() );
        RaftMessage<RaftTestMember> outgoing = outcome.getOutgoingMessages().iterator().next().message();
        assertEquals( RaftMessages.Type.APPEND_ENTRIES_RESPONSE, outgoing.type() );
        RaftMessages.AppendEntries.Response response = (AppendEntries.Response) outgoing;
        assertFalse( response.success() );
    }

    @Test
    public void shouldTruncateIfTermDoesNotMatch() throws Exception
    {
        // given
        int term = 1;
        RaftState<RaftTestMember> state = raftState()
                .myself( myself )
                .term( term )
                .build();

        Follower follower = new Follower();

        state.update( follower.handle( new AppendEntries.Request<>( member1, 1, -1, -1,
                new RaftLogEntry[]{
                        new RaftLogEntry( 2, ContentGenerator.content() ),
                },
                -1 ), state, log() ) );


        RaftLogEntry[] entries = {
                new RaftLogEntry( 1, new ReplicatedString( "commit this!" ) ),
        };

        Outcome<RaftTestMember> outcome = follower.handle(
                new AppendEntries.Request<>( member1, 1, -1, -1, entries, -1 ), state, log() );
        state.update( outcome );

        // then
        assertEquals( 0, state.entryLog().appendIndex() );
        assertEquals( 1, state.entryLog().readEntryTerm( 0 ) );
    }

    @Test
    public void followerLearningAboutHigherCommitCausesValuesTobeAppliedToItsLog() throws Exception
    {
        // given
        RaftState<RaftTestMember> state = raftState()
                .myself( myself )
                .build();

        Follower follower = new Follower();

        appendSomeEntriesToLog( state, follower, 3, 0 );

        // when receiving AppEntries with high leader commit (3)
        Outcome<RaftTestMember> outcome = follower.handle( new AppendEntries.Request<>( myself, 0, 2, 0,
                new RaftLogEntry[] { new RaftLogEntry( 0, ContentGenerator.content() ) }, 3 ), state, log() );

        state.update( outcome );

        // then
        assertEquals( 3, state.entryLog().commitIndex() );
    }

    @Test
    public void shouldRenewElectionTimeoutOnReceiptOfHeartbeatInCurrentOrHigherTerm() throws Exception
    {
        // given
        RaftState<RaftTestMember> state = raftState()
                .myself( myself )
                .term(0)
                .build();

        Follower follower = new Follower();

        Outcome<RaftTestMember> outcome = follower.handle( new RaftMessages.Heartbeat<>( myself, 1, 1, 1 ),
                state, log() );

        // then
        assertTrue( outcome.electionTimeoutRenewed() );
    }

    @Test
    public void shouldNotRenewElectionTimeoutOnReceiptOfHeartbeatInLowerTerm() throws Exception
    {
        // given
        RaftState<RaftTestMember> state = raftState()
                .myself( myself )
                .term( 2 )
                .build();

        Follower follower = new Follower();

        Outcome<RaftTestMember> outcome = follower.handle( new RaftMessages.Heartbeat<>( myself, 1, 1, 1 ),
                state, log() );

        // then
        assertFalse( outcome.electionTimeoutRenewed() );
    }

    private void appendSomeEntriesToLog( RaftState<RaftTestMember> raft, Follower follower, int numberOfEntriesToAppend,
                                         int term ) throws IOException
    {
        for ( int i = 0; i < numberOfEntriesToAppend; i++ )
        {
            if ( i == 0 )
            {
                raft.update( follower.handle( new AppendEntries.Request<>( myself, term, i - 1, -1,
                        new RaftLogEntry[] { new RaftLogEntry( term, ContentGenerator.content() ) }, -1 ), raft, log() ) );
            }
            else
            {
                raft.update( follower.handle( new AppendEntries.Request<>( myself, term, i - 1, term,
                        new RaftLogEntry[]{new RaftLogEntry( term, ContentGenerator.content() )}, -1 ), raft, log() ) );
            }
        }
    }

    private static class ContentGenerator
    {
        private static int count = 0;

        public static ReplicatedString content()
        {
            return new ReplicatedString( String.format( "content#%d", count++ ) );
        }
    }

    private Log log()
    {
        return NullLogProvider.getInstance().getLog( getClass() );
    }
}
