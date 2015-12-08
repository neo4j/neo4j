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
package org.neo4j.coreedge.raft.roles;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import org.neo4j.coreedge.server.RaftTestMember;
import org.neo4j.coreedge.raft.ReplicatedString;
import org.neo4j.coreedge.raft.RaftMessages;
import org.neo4j.coreedge.raft.RaftMessages.AppendEntries;
import org.neo4j.coreedge.raft.RaftMessages.Timeout.Heartbeat;
import org.neo4j.coreedge.raft.log.InMemoryRaftLog;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.log.ReadableRaftLog;
import org.neo4j.coreedge.raft.outcome.ShipCommand;
import org.neo4j.coreedge.raft.net.Inbound;
import org.neo4j.coreedge.raft.net.Outbound;
import org.neo4j.coreedge.raft.outcome.AppendLogEntry;
import org.neo4j.coreedge.raft.outcome.CommitCommand;
import org.neo4j.coreedge.raft.outcome.Outcome;
import org.neo4j.coreedge.raft.state.FollowerState;
import org.neo4j.coreedge.raft.state.FollowerStates;
import org.neo4j.coreedge.raft.state.RaftState;
import org.neo4j.coreedge.raft.state.ReadableRaftState;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.coreedge.raft.MessageUtils.messageFor;
import static org.neo4j.coreedge.server.RaftTestMember.member;
import static org.neo4j.coreedge.raft.TestMessageBuilders.appendEntriesResponse;
import static org.neo4j.coreedge.raft.TestMessageBuilders.voteRequest;
import static org.neo4j.coreedge.raft.roles.Role.FOLLOWER;
import static org.neo4j.coreedge.raft.roles.Role.LEADER;
import static org.neo4j.coreedge.raft.state.RaftStateBuilder.raftState;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.helpers.collection.IteratorUtil.firstOrNull;
import static org.neo4j.helpers.collection.IteratorUtil.single;

@RunWith(MockitoJUnitRunner.class)
public class LeaderTest
{
    private RaftTestMember myself = member( 0 );

    /* A few members that we use at will in tests. */
    private RaftTestMember member1 = member( 1 );
    private RaftTestMember member2 = member( 2 );

    @Mock
    private Inbound inbound;

    @Mock
    private Outbound outbound;

    private LogProvider logProvider = NullLogProvider.getInstance();
    private static final int HIGHEST_TERM = 99;

    private static final ReplicatedString CONTENT = ReplicatedString.valueOf( "some-content-to-raft" );

    @Test
    public void leaderShouldUpdateTermToCurrentMessageAndBecomeFollower() throws Exception
    {
        // given
        RaftState<RaftTestMember> state = raftState().build();

        Leader leader = new Leader();

        // when
        Outcome outcome = leader.handle( voteRequest().from( member1 ).term( HIGHEST_TERM ).lastLogIndex( 0 )
                .lastLogTerm( -1 ).build(), state, log() );

        // then
        assertEquals( FOLLOWER, outcome.getNewRole() );
        assertEquals( HIGHEST_TERM, outcome.getTerm() );
    }

    @Test
    public void leaderShouldRejectVoteRequestWithNewerTermAndBecomeAFollower() throws Exception
    {
        // given
        RaftState<RaftTestMember> state = raftState().myself( myself ).build();

        Leader leader = new Leader();

        // when
        RaftMessages.Vote.Request<RaftTestMember> message = voteRequest()
                .from( member1 )
                .term( state.term() + 1 )
                .lastLogIndex( 0 )
                .lastLogTerm( -1 )
                .build();
        Outcome<RaftTestMember> outcome = leader.handle( message, state, log() );

        // then
        assertEquals( message, messageFor( outcome, myself ) );
        assertEquals( FOLLOWER, outcome.getNewRole() );
        assertEquals( 0, count( outcome.getLogCommands() ) );
        assertEquals( state.term() + 1, outcome.getTerm() );
    }

    @Test
    public void leaderShouldNotRespondToSuccessResponseFromFollowerThatWillSoonUpToDateViaInFlightMessages()
            throws Exception
    {
        // given
        /*
         * A leader who
         * - has an append index of 100
         * - knows about instance 2
         * - assumes that instance 2 is at an index less than 100 -say 84 but it has already been sent up to 100
         */
        Leader leader = new Leader();
        RaftTestMember instance2 = new RaftTestMember( 2 );
        FollowerState instance2State = createArtificialFollowerState( 84 );

        ReadableRaftState<RaftTestMember> state = mock( ReadableRaftState.class );

        FollowerStates<RaftTestMember> followerState = new FollowerStates<>();
        followerState = new FollowerStates<>( followerState, instance2, instance2State );

        ReadableRaftLog logMock = mock( ReadableRaftLog.class );
        when( logMock.appendIndex() ).thenReturn( 100l );

        when( state.entryLog() ).thenReturn( logMock );
        when( state.followerStates() ).thenReturn( followerState );
        when( state.term() ).thenReturn( 4l ); // both leader and follower are in the same term

        // when
        // that leader is asked to handle a response from that follower that says that the follower is up to date
        RaftMessages.AppendEntries.Response<RaftTestMember> response = appendEntriesResponse().success()
                .matchIndex( 90 ).term( 4 ).from( instance2 ).build();

        Outcome<RaftTestMember> outcome = leader.handle( response, state, mock( Log.class ) );

        // then
        // The leader should not be trying to send any messages to that instance
        assertTrue( outcome.getOutgoingMessages().isEmpty() );
        // And the follower state should be updated
        FollowerStates<RaftTestMember> leadersViewOfFollowerStates = outcome.getFollowerStates();
        assertEquals( 90, leadersViewOfFollowerStates.get( instance2 ).getMatchIndex() );
    }

    @Test
    public void leaderShouldNotRespondToSuccessResponseThatIndicatesUpToDateFollower() throws Exception
    {
        // given
        /*
         * A leader who
         * - has an append index of 100
         * - knows about instance 2
         * - assumes that instance 2 is at an index less than 100 -say 84
         */
        Leader leader = new Leader();
        RaftTestMember instance2 = new RaftTestMember( 2 );
        FollowerState instance2State = createArtificialFollowerState( 84 );

        ReadableRaftState<RaftTestMember> state = mock( ReadableRaftState.class );

        FollowerStates<RaftTestMember> followerState = new FollowerStates<>();
        followerState = new FollowerStates<>( followerState, instance2, instance2State );

        ReadableRaftLog logMock = mock( ReadableRaftLog.class );
        when( logMock.appendIndex() ).thenReturn( 100l );

        when( state.entryLog() ).thenReturn( logMock );
        when( state.followerStates() ).thenReturn( followerState );
        when( state.term() ).thenReturn( 4l ); // both leader and follower are in the same term

        // when
        // that leader is asked to handle a response from that follower that says that the follower is up to date
        RaftMessages.AppendEntries.Response<RaftTestMember> response = appendEntriesResponse().success()
                .matchIndex( 100 ).term( 4 ).from( instance2 ).build();

        Outcome<RaftTestMember> outcome = leader.handle( response, state, mock( Log.class ) );

        // then
        // The leader should not be trying to send any messages to that instance
        assertTrue( outcome.getOutgoingMessages().isEmpty() );
        // And the follower state should be updated
        FollowerStates<RaftTestMember> updatedFollowerStates = outcome.getFollowerStates();
        assertEquals( 100, updatedFollowerStates.get( instance2 ).getMatchIndex() );
    }

    @Test
    public void leaderShouldRespondToSuccessResponseThatIndicatesLaggingFollowerWithJustWhatItsMissing() throws
            Exception
    {
        // given
        /*
         * A leader who
         * - has an append index of 100
         * - knows about instance 2
         * - assumes that instance 2 is at an index less than 100 -say 50
         */
        Leader leader = new Leader();
        RaftTestMember instance2 = new RaftTestMember( 2 );
        FollowerState instance2State = createArtificialFollowerState( 50 );

        ReadableRaftState<RaftTestMember> state = mock( ReadableRaftState.class );

        FollowerStates<RaftTestMember> followerState = new FollowerStates<>();
        followerState = new FollowerStates<>( followerState, instance2, instance2State );

        ReadableRaftLog logMock = mock( ReadableRaftLog.class );
        when( logMock.appendIndex() ).thenReturn( 100l );
        when( logMock.commitIndex() ).thenReturn( 100l ); // assume that everything is committed, so we don't deal
        // with commit requests in this test

        when( state.entryLog() ).thenReturn( logMock );
        when( state.followerStates() ).thenReturn( followerState );
        when( state.term() ).thenReturn( 231l ); // both leader and follower are in the same term

        // when that leader is asked to handle a response from that follower that says that the follower is still
        // missing things
        RaftMessages.AppendEntries.Response<RaftTestMember> response = appendEntriesResponse()
                .success()
                .matchIndex( 89 )
                .term( 231 )
                .from( instance2 ).build();

        Outcome<RaftTestMember> outcome = leader.handle( response, state, mock( Log.class ) );

        // then
        int matchCount = 0;
        for ( ShipCommand shipCommand : outcome.getShipCommands() )
        {
            if ( shipCommand instanceof ShipCommand.Match )
            {
                matchCount++;
            }
        }

        assertThat( matchCount, greaterThan( 0 ) );
    }

    @Test
    public void leaderShouldIgnoreSuccessResponseThatIndicatesLaggingWhileLocalStateIndicatesFollowerIsCaughtUp()
            throws Exception
    {
        // given
        /*
         * A leader who
         * - has an append index of 100
         * - knows about instance 2
         * - assumes that instance 2 is fully caught up
         */
        Leader leader = new Leader();
        RaftTestMember instance2 = new RaftTestMember( 2 );
        int i = 101;
        int j = 100;
        FollowerState instance2State = createArtificialFollowerState( j );

        ReadableRaftState<RaftTestMember> state = mock( ReadableRaftState.class );

        FollowerStates<RaftTestMember> followerState = new FollowerStates<>();
        followerState = new FollowerStates<>( followerState, instance2, instance2State );

        ReadableRaftLog logMock = mock( ReadableRaftLog.class );
        when( logMock.appendIndex() ).thenReturn( 100l );
        when( logMock.commitIndex() ).thenReturn( 100l ); // assume that everything is committed, so we don't deal
        //  with commit requests in this test

        when( state.entryLog() ).thenReturn( logMock );
        when( state.followerStates() ).thenReturn( followerState );
        when( state.term() ).thenReturn( 4l ); // both leader and follower are in the same term

        // when that leader is asked to handle a response from that follower that says that the follower is still
        // missing things
        RaftMessages.AppendEntries.Response<RaftTestMember> response = appendEntriesResponse()
                .success()
                .matchIndex( 80 )
                .term( 4 )
                .from( instance2 ).build();

        Outcome<RaftTestMember> outcome = leader.handle( response, state, mock( Log.class ) );

        // then the leader should not send anything, since this is a delayed, out of order response to a previous append
        // request
        assertTrue( outcome.getOutgoingMessages().isEmpty() );
        // The follower state should not be touched
        FollowerStates<RaftTestMember> updatedFollowerStates = outcome.getFollowerStates();
        assertEquals( 100, updatedFollowerStates.get( instance2 ).getMatchIndex() );
    }

    private static FollowerState createArtificialFollowerState( long matchIndex )
    {
        return new FollowerState().onSuccessResponse( matchIndex );
    }

    // TODO: rethink this test, it does too much
    @Test
    public void leaderShouldSpawnMismatchCommandOnFailure() throws Exception
    {
        // given
        /*
         * A leader who
         * - has an append index of 100
         * - knows about instance 2
         * - assumes that instance 2 is fully caught up
         */
        Leader leader = new Leader();
        RaftTestMember instance2 = new RaftTestMember( 2 );
        FollowerState instance2State = createArtificialFollowerState( 100 );

        ReadableRaftState<RaftTestMember> state = mock( ReadableRaftState.class );

        FollowerStates<RaftTestMember> followerState = new FollowerStates<>();
        followerState = new FollowerStates<>( followerState, instance2, instance2State );

        ReadableRaftLog logMock = mock( ReadableRaftLog.class );
        when( logMock.appendIndex() ).thenReturn( 100l );
        when( logMock.commitIndex() ).thenReturn( 100l ); // assume that everything is committed, so we don't deal
        // with commit requests in this test

        when( state.entryLog() ).thenReturn( logMock );
        when( state.followerStates() ).thenReturn( followerState );
        when( state.term() ).thenReturn( 4l ); // both leader and follower are in the same term

        // when
        // that leader is asked to handle a response from that follower that says that the follower is still missing
        // things
        RaftMessages.AppendEntries.Response<RaftTestMember> response = appendEntriesResponse()
                .failure()
                .matchIndex( -1 )
                .term( 4 )
                .from( instance2 ).build();

        Outcome<RaftTestMember> outcome = leader.handle( response, state, mock( Log.class ) );

        // then
        int mismatchCount = 0;
        for ( ShipCommand shipCommand : outcome.getShipCommands() )
        {
            if ( shipCommand instanceof ShipCommand.Mismatch )
            {
                mismatchCount++;
            }
        }

        assertThat( mismatchCount, greaterThan( 0 ) );
    }

    @Test
    public void leaderShouldRejectAppendEntriesResponseWithNewerTermAndBecomeAFollower() throws Exception
    {
        // given
        RaftState<RaftTestMember> state = raftState().myself( myself ).build();

        Leader leader = new Leader();

        // when
        AppendEntries.Response<RaftTestMember> message = appendEntriesResponse()
                .from( member1 )
                .term( state.term() + 1 )
                .build();
        Outcome<RaftTestMember> outcome = leader.handle( message, state, log() );

        // then
        assertEquals( 0, count( outcome.getOutgoingMessages() ) );
        assertEquals( FOLLOWER, outcome.getNewRole() );
        assertEquals( 0, count( outcome.getLogCommands() ) );
        assertEquals( state.term() + 1, outcome.getTerm() );
    }

    // TODO: test that shows we don't commit for previous terms

    @Test
    public void leaderShouldRejectAnyMessageWithOldTerm() throws Exception
    {
        // given
        RaftState<RaftTestMember> state = raftState().build();

        Leader leader = new Leader();

        // when
        RaftMessages.Vote.Request<RaftTestMember> message = voteRequest()
                .from( member1 )
                .term( state.term() - 1 )
                .lastLogIndex( 0 )
                .lastLogTerm( -1 )
                .build();
        Outcome<RaftTestMember> outcome = leader.handle( message, state, log() );

        // then
        assertFalse( ((RaftMessages.Vote.Response<RaftTestMember>) messageFor( outcome, member1 ))
                .voteGranted() );
        assertEquals( LEADER, outcome.getNewRole() );
    }

    @Test
    public void leaderShouldSendHeartbeatsToAllClusterMembersOnReceiptOfHeartbeatTick() throws Exception
    {
        // given
        RaftState<RaftTestMember> state = raftState()
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();

        Leader leader = new Leader();

        // when
        Outcome<RaftTestMember> outcome = leader.handle( new Heartbeat<>( member1 ), state, log() );

        // then
        assertTrue( messageFor( outcome, member1 ) instanceof RaftMessages.Heartbeat );
        assertTrue( messageFor( outcome, member2 ) instanceof RaftMessages.Heartbeat );
    }

    @Test
    public void leaderShouldDecideToAppendToItsLogAndSendAppendEntriesMessageOnReceiptOfClientProposal()
            throws Exception
    {
        // given
        RaftState<RaftTestMember> state = raftState()
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();

        Leader leader = new Leader();

        RaftMessages.NewEntry.Request<RaftTestMember> newEntryRequest = new RaftMessages.NewEntry.Request<>( member( 9 ), CONTENT );

        // when
        Outcome<RaftTestMember> outcome = leader.handle( newEntryRequest, state, log() );
        //state.update( outcome );

        // then
        AppendLogEntry logCommand = (AppendLogEntry) single( outcome.getLogCommands() );
        assertEquals( 0, logCommand.index );
        assertEquals( 0, logCommand.entry.term() );

        ShipCommand.NewEntry shipCommand = (ShipCommand.NewEntry) single( outcome.getShipCommands() );

        assertEquals( shipCommand, new ShipCommand.NewEntry( -1, -1, new RaftLogEntry( 0, CONTENT ) ) );
    }

    @Test
    public void leaderShouldCommitOnMajorityResponse() throws Exception
    {
        // given
        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        raftLog.append( new RaftLogEntry( 0, new ReplicatedString( "lalalala" ) ) );

        RaftState<RaftTestMember> state = raftState().votingMembers( member1, member2 )
                .term( 0 )
                .lastLogIndexBeforeWeBecameLeader( -1 )
                .leader( myself )
                .leaderCommit( -1 )
                .entryLog( raftLog )
                .messagesSentToFollower( member1, raftLog.appendIndex() + 1 )
                .messagesSentToFollower( member2, raftLog.appendIndex() + 1 )
                .build();

        Leader leader = new Leader();

        // when a single instance responds (plus self == 2 out of 3 instances)
        Outcome<RaftTestMember> outcome = leader.handle(
                new RaftMessages.AppendEntries.Response<>( member1, 0, true, 0, 0 ), state, log() );

        // then
        assertThat( firstOrNull( outcome.getLogCommands() ), instanceOf( CommitCommand.class ) );
        assertEquals( 0, outcome.getLeaderCommit() );
    }

    @Test
    public void leaderShouldCommitAllPreviouslyAppendedEntriesWhenCommittingLaterEntryInSameTerm() throws Exception
    {
        // given
        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        raftLog.append( new RaftLogEntry( 0, new ReplicatedString( "first!" ) ) );
        raftLog.append( new RaftLogEntry( 0, new ReplicatedString( "second" ) ) );
        raftLog.append( new RaftLogEntry( 0, new ReplicatedString( "third" ) ) );

        RaftState<RaftTestMember> state = raftState()
                .votingMembers( myself, member1, member2 )
                .term( 0 ).entryLog( raftLog )
                .messagesSentToFollower( member1, raftLog.appendIndex() + 1 )
                .messagesSentToFollower( member2, raftLog.appendIndex() + 1 )
                .build();

        Leader leader = new Leader();

        // when
        Outcome<RaftTestMember> outcome =
                leader.handle( new AppendEntries.Response<>( member1, 0, true, 2, 2 ), state, log() );

        state.update( outcome );

        // then
        assertEquals( 2, raftLog.commitIndex() );
    }

    private Log log()
    {
        return logProvider.getLog( getClass() );
    }
}
