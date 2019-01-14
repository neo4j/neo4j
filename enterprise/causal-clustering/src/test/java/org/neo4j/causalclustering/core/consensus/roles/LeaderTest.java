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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.RaftMessages.AppendEntries;
import org.neo4j.causalclustering.core.consensus.RaftMessages.Timeout.Heartbeat;
import org.neo4j.causalclustering.core.consensus.ReplicatedInteger;
import org.neo4j.causalclustering.core.consensus.ReplicatedString;
import org.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.consensus.log.ReadableRaftLog;
import org.neo4j.causalclustering.core.consensus.outcome.AppendLogEntry;
import org.neo4j.causalclustering.core.consensus.outcome.BatchAppendLogEntries;
import org.neo4j.causalclustering.core.consensus.outcome.Outcome;
import org.neo4j.causalclustering.core.consensus.outcome.ShipCommand;
import org.neo4j.causalclustering.core.consensus.roles.follower.FollowerState;
import org.neo4j.causalclustering.core.consensus.roles.follower.FollowerStates;
import org.neo4j.causalclustering.core.consensus.state.RaftState;
import org.neo4j.causalclustering.core.consensus.state.ReadableRaftState;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.causalclustering.core.consensus.MessageUtils.messageFor;
import static org.neo4j.causalclustering.core.consensus.ReplicatedInteger.valueOf;
import static org.neo4j.causalclustering.core.consensus.TestMessageBuilders.appendEntriesResponse;
import static org.neo4j.causalclustering.core.consensus.roles.Role.FOLLOWER;
import static org.neo4j.causalclustering.core.consensus.roles.Role.LEADER;
import static org.neo4j.causalclustering.core.consensus.state.RaftStateBuilder.raftState;
import static org.neo4j.causalclustering.identity.RaftTestMember.member;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.helpers.collection.Iterators.asSet;

@RunWith( MockitoJUnitRunner.class )
public class LeaderTest
{
    private MemberId myself = member( 0 );

    /* A few members that we use at will in tests. */
    private MemberId member1 = member( 1 );
    private MemberId member2 = member( 2 );

    private LogProvider logProvider = NullLogProvider.getInstance();

    private static final ReplicatedString CONTENT = ReplicatedString.valueOf( "some-content-to-raft" );

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
        MemberId instance2 = member( 2 );
        FollowerState instance2State = createArtificialFollowerState( 84 );

        ReadableRaftState state = mock( ReadableRaftState.class );

        FollowerStates<MemberId> followerState = new FollowerStates<>();
        followerState = new FollowerStates<>( followerState, instance2, instance2State );

        ReadableRaftLog logMock = mock( ReadableRaftLog.class );
        when( logMock.appendIndex() ).thenReturn( 100L );

        when( state.commitIndex() ).thenReturn( -1L );
        when( state.entryLog() ).thenReturn( logMock );
        when( state.followerStates() ).thenReturn( followerState );
        when( state.term() ).thenReturn( 4L ); // both leader and follower are in the same term

        // when
        // that leader is asked to handle a response from that follower that says that the follower is up to date
        RaftMessages.AppendEntries.Response response =
                appendEntriesResponse().success().matchIndex( 90 ).term( 4 ).from( instance2 ).build();

        Outcome outcome = leader.handle( response, state, mock( Log.class ) );

        // then
        // The leader should not be trying to send any messages to that instance
        assertTrue( outcome.getOutgoingMessages().isEmpty() );
        // And the follower state should be updated
        FollowerStates<MemberId> leadersViewOfFollowerStates = outcome.getFollowerStates();
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
        MemberId instance2 = member( 2 );
        FollowerState instance2State = createArtificialFollowerState( 84 );

        ReadableRaftState state = mock( ReadableRaftState.class );

        FollowerStates<MemberId> followerState = new FollowerStates<>();
        followerState = new FollowerStates<>( followerState, instance2, instance2State );

        ReadableRaftLog logMock = mock( ReadableRaftLog.class );
        when( logMock.appendIndex() ).thenReturn( 100L );

        when( state.commitIndex() ).thenReturn( -1L );
        when( state.entryLog() ).thenReturn( logMock );
        when( state.followerStates() ).thenReturn( followerState );
        when( state.term() ).thenReturn( 4L ); // both leader and follower are in the same term

        // when
        // that leader is asked to handle a response from that follower that says that the follower is up to date
        RaftMessages.AppendEntries.Response response =
                appendEntriesResponse().success().matchIndex( 100 ).term( 4 ).from( instance2 ).build();

        Outcome outcome = leader.handle( response, state, mock( Log.class ) );

        // then
        // The leader should not be trying to send any messages to that instance
        assertTrue( outcome.getOutgoingMessages().isEmpty() );
        // And the follower state should be updated
        FollowerStates<MemberId> updatedFollowerStates = outcome.getFollowerStates();
        assertEquals( 100, updatedFollowerStates.get( instance2 ).getMatchIndex() );
    }

    @Test
    public void leaderShouldRespondToSuccessResponseThatIndicatesLaggingFollowerWithJustWhatItsMissing()
            throws Exception
    {
        // given
        /*
         * A leader who
         * - has an append index of 100
         * - knows about instance 2
         * - assumes that instance 2 is at an index less than 100 -say 50
         */
        Leader leader = new Leader();
        MemberId instance2 = member( 2 );
        FollowerState instance2State = createArtificialFollowerState( 50 );

        ReadableRaftState state = mock( ReadableRaftState.class );

        FollowerStates<MemberId> followerState = new FollowerStates<>();
        followerState = new FollowerStates<>( followerState, instance2, instance2State );

        ReadableRaftLog logMock = mock( ReadableRaftLog.class );
        when( logMock.appendIndex() ).thenReturn( 100L );
        // with commit requests in this test

        when( state.commitIndex() ).thenReturn( -1L );
        when( state.entryLog() ).thenReturn( logMock );
        when( state.followerStates() ).thenReturn( followerState );
        when( state.term() ).thenReturn( 231L ); // both leader and follower are in the same term

        // when that leader is asked to handle a response from that follower that says that the follower is still
        // missing things
        RaftMessages.AppendEntries.Response response = appendEntriesResponse()
                .success()
                .matchIndex( 89 )
                .term( 231 )
                .from( instance2 ).build();

        Outcome outcome = leader.handle( response, state, mock( Log.class ) );

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
        MemberId instance2 = member( 2 );
        int j = 100;
        FollowerState instance2State = createArtificialFollowerState( j );

        ReadableRaftState state = mock( ReadableRaftState.class );

        FollowerStates<MemberId> followerState = new FollowerStates<>();
        followerState = new FollowerStates<>( followerState, instance2, instance2State );

        ReadableRaftLog logMock = mock( ReadableRaftLog.class );
        when( logMock.appendIndex() ).thenReturn( 100L );
        //  with commit requests in this test

        when( state.commitIndex() ).thenReturn( -1L );
        when( state.entryLog() ).thenReturn( logMock );
        when( state.followerStates() ).thenReturn( followerState );
        when( state.term() ).thenReturn( 4L ); // both leader and follower are in the same term

        // when that leader is asked to handle a response from that follower that says that the follower is still
        // missing things
        RaftMessages.AppendEntries.Response response = appendEntriesResponse()
                .success()
                .matchIndex( 80 )
                .term( 4 )
                .from( instance2 ).build();

        Outcome outcome = leader.handle( response, state, mock( Log.class ) );

        // then the leader should not send anything, since this is a delayed, out of order response to a previous append
        // request
        assertTrue( outcome.getOutgoingMessages().isEmpty() );
        // The follower state should not be touched
        FollowerStates<MemberId> updatedFollowerStates = outcome.getFollowerStates();
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
        MemberId instance2 = member( 2 );
        FollowerState instance2State = createArtificialFollowerState( 100 );

        ReadableRaftState state = mock( ReadableRaftState.class );

        FollowerStates<MemberId> followerState = new FollowerStates<>();
        followerState = new FollowerStates<>( followerState, instance2, instance2State );

        RaftLog log = new InMemoryRaftLog();
        for ( int i = 0; i <= 100; i++ )
        {
            log.append( new RaftLogEntry( 0, valueOf( i ) ) );
        }

        when( state.commitIndex() ).thenReturn( -1L );
        when( state.entryLog() ).thenReturn( log );
        when( state.followerStates() ).thenReturn( followerState );
        when( state.term() ).thenReturn( 4L ); // both leader and follower are in the same term

        // when
        // that leader is asked to handle a response from that follower that says that the follower is still missing
        // things
        RaftMessages.AppendEntries.Response response = appendEntriesResponse()
                .failure()
                .appendIndex( 0 )
                .matchIndex( -1 )
                .term( 4 )
                .from( instance2 ).build();

        Outcome outcome = leader.handle( response, state, mock( Log.class ) );

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
    public void shouldSendCompactionInfoIfFailureWithNoEarlierEntries() throws Exception
    {
        // given
        Leader leader = new Leader();
        long term = 1;
        long leaderPrevIndex = 3;
        long followerIndex = leaderPrevIndex - 1;

        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        raftLog.skip( leaderPrevIndex, term );

        RaftState state = raftState()
                .term( term )
                .entryLog( raftLog )
                .build();

        RaftMessages.AppendEntries.Response incomingResponse = appendEntriesResponse()
                .failure()
                .term( term )
                .appendIndex( followerIndex )
                .from( member1 ).build();

        // when
        Outcome outcome = leader.handle( incomingResponse, state, log() );

        // then
        RaftMessages.RaftMessage outgoingMessage = messageFor( outcome, member1 );
        assertThat( outgoingMessage, instanceOf( RaftMessages.LogCompactionInfo.class ) );

        RaftMessages.LogCompactionInfo typedOutgoingMessage = (RaftMessages.LogCompactionInfo) outgoingMessage;
        assertThat( typedOutgoingMessage.prevIndex(), equalTo( leaderPrevIndex ) );
    }

    @Test
    public void shouldIgnoreAppendResponsesFromOldTerms() throws Exception
    {
        // given
        Leader leader = new Leader();
        long leaderTerm = 5;
        long followerTerm = 3;

        RaftState state = raftState()
                .term( leaderTerm )
                .build();

                RaftMessages.AppendEntries.Response incomingResponse = appendEntriesResponse()
                .failure()
                .term( followerTerm )
                .from( member1 ).build();

        // when
        Outcome outcome = leader.handle( incomingResponse, state, log() );

        // then
        assertThat( outcome.getTerm(), equalTo( leaderTerm ) );
        assertThat( outcome.getRole(), equalTo( LEADER ) );

        assertThat( outcome.getOutgoingMessages(), empty() );
        assertThat( outcome.getShipCommands(), empty() );
    }

    @Test
    public void leaderShouldRejectAppendEntriesResponseWithNewerTermAndBecomeAFollower() throws Exception
    {
        // given
        RaftState state = raftState().myself( myself ).build();

        Leader leader = new Leader();

        // when
        AppendEntries.Response message = appendEntriesResponse()
                .from( member1 )
                .term( state.term() + 1 )
                .build();
        Outcome outcome = leader.handle( message, state, log() );

        // then
        assertEquals( 0, count( outcome.getOutgoingMessages() ) );
        assertEquals( FOLLOWER, outcome.getRole() );
        assertEquals( 0, count( outcome.getLogCommands() ) );
        assertEquals( state.term() + 1, outcome.getTerm() );
    }

    // TODO: test that shows we don't commit for previous terms

    @Test
    public void leaderShouldSendHeartbeatsToAllClusterMembersOnReceiptOfHeartbeatTick() throws Exception
    {
        // given
        RaftState state = raftState()
                .votingMembers( myself, member1, member2 )
                .replicationMembers( myself, member1, member2 )
                .build();

        Leader leader = new Leader();
        leader.handle( new RaftMessages.HeartbeatResponse( member1 ), state, log() ); // make sure it has quorum.

        // when
        Outcome outcome = leader.handle( new Heartbeat( myself ), state, log() );

        // then
        assertTrue( messageFor( outcome, member1 ) instanceof RaftMessages.Heartbeat );
        assertTrue( messageFor( outcome, member2 ) instanceof RaftMessages.Heartbeat );
    }

    @Test
    public void leaderShouldStepDownWhenLackingHeartbeatResponses() throws Exception
    {
        // given
        RaftState state = raftState()
                .votingMembers( asSet( myself, member1, member2 ) )
                .leader( myself )
                .build();

        Leader leader = new Leader();
        leader.handle( new RaftMessages.Timeout.Election( myself ), state, log() );

        // when
        Outcome outcome = leader.handle( new RaftMessages.Timeout.Election( myself ), state, log() );

        // then
        assertThat( outcome.getRole(), not( LEADER ) );
        assertNull( outcome.getLeader() );
    }

    @Test
    public void leaderShouldNotStepDownWhenReceivedQuorumOfHeartbeatResponses() throws Exception
    {
        // given
        RaftState state = raftState()
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();

        Leader leader = new Leader();

        // when
        Outcome outcome = leader.handle( new RaftMessages.HeartbeatResponse( member1 ), state, log() );
        state.update( outcome );

        // we now have quorum and should not step down
        outcome = leader.handle( new RaftMessages.Timeout.Election( myself ), state, log() );

        // then
        assertThat( outcome.getRole(), is( LEADER ) );
    }

    @Test
    public void oldHeartbeatResponseShouldNotPreventStepdown() throws Exception
    {
        // given
        RaftState state = raftState()
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();

        Leader leader = new Leader();

        Outcome outcome = leader.handle( new RaftMessages.HeartbeatResponse( member1 ), state, log() );
        state.update( outcome );

        outcome = leader.handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome );

        assertThat( outcome.getRole(), is( LEADER ) );

        // when
        outcome = leader.handle( new RaftMessages.Timeout.Election( myself ), state, log() );

        // then
        assertThat( outcome.getRole(), is( FOLLOWER ) );
    }

    @Test
    public void leaderShouldDecideToAppendToItsLogAndSendAppendEntriesMessageOnReceiptOfClientProposal()
            throws Exception
    {
        // given
        RaftState state = raftState()
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();

        Leader leader = new Leader();

        RaftMessages.NewEntry.Request newEntryRequest = new RaftMessages.NewEntry.Request( member( 9 ), CONTENT );

        // when
        Outcome outcome = leader.handle( newEntryRequest, state, log() );
        //state.update( outcome );

        // then
        AppendLogEntry logCommand = (AppendLogEntry) single( outcome.getLogCommands() );
        assertEquals( 0, logCommand.index );
        assertEquals( 0, logCommand.entry.term() );

        ShipCommand.NewEntries shipCommand = (ShipCommand.NewEntries) single( outcome.getShipCommands() );

        assertEquals( shipCommand,
                new ShipCommand.NewEntries( -1, -1, new RaftLogEntry[]{new RaftLogEntry( 0, CONTENT )} ) );
    }

    @Test
    public void leaderShouldHandleBatch() throws Exception
    {
        // given
        RaftState state = raftState()
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();

        Leader leader = new Leader();

        final int BATCH_SIZE = 3;
        RaftMessages.NewEntry.BatchRequest batchRequest = new RaftMessages.NewEntry.BatchRequest( BATCH_SIZE );
        batchRequest.add( valueOf( 0 ) );
        batchRequest.add( valueOf( 1 ) );
        batchRequest.add( valueOf( 2 ) );

        // when
        Outcome outcome = leader.handle( batchRequest, state, log() );

        // then
        BatchAppendLogEntries logCommand = (BatchAppendLogEntries) single( outcome.getLogCommands() );

        assertEquals( 0, logCommand.baseIndex );
        for ( int i = 0; i < BATCH_SIZE; i++ )
        {
            assertEquals( 0, logCommand.entries[i].term() );
            assertEquals( i, ((ReplicatedInteger) logCommand.entries[i].content()).get() );
        }

        ShipCommand.NewEntries shipCommand = (ShipCommand.NewEntries) single( outcome.getShipCommands() );

        assertEquals( shipCommand, new ShipCommand.NewEntries( -1, -1, new RaftLogEntry[]{
                new RaftLogEntry( 0, valueOf( 0 ) ),
                new RaftLogEntry( 0, valueOf( 1 ) ),
                new RaftLogEntry( 0, valueOf( 2 ) )
        } ) );
    }

    @Test
    public void leaderShouldCommitOnMajorityResponse() throws Exception
    {
        // given
        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        raftLog.append( new RaftLogEntry( 0, new ReplicatedString( "lalalala" ) ) );

        RaftState state = raftState()
                .votingMembers( member1, member2 )
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
        Outcome outcome =
                leader.handle( new RaftMessages.AppendEntries.Response( member1, 0, true, 0, 0 ), state, log() );

        // then
        assertEquals( 0L, outcome.getCommitIndex() );
        assertEquals( 0L, outcome.getLeaderCommit() );
    }

    // TODO move this someplace else, since log no longer holds the commit
    @Test
    public void leaderShouldCommitAllPreviouslyAppendedEntriesWhenCommittingLaterEntryInSameTerm() throws Exception
    {
        // given
        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        raftLog.append( new RaftLogEntry( 0, new ReplicatedString( "first!" ) ) );
        raftLog.append( new RaftLogEntry( 0, new ReplicatedString( "second" ) ) );
        raftLog.append( new RaftLogEntry( 0, new ReplicatedString( "third" ) ) );

        RaftState state = raftState()
                .votingMembers( myself, member1, member2 )
                .term( 0 )
                .entryLog( raftLog )
                .messagesSentToFollower( member1, raftLog.appendIndex() + 1 )
                .messagesSentToFollower( member2, raftLog.appendIndex() + 1 )
                .build();

        Leader leader = new Leader();

        // when
        Outcome outcome = leader.handle( new AppendEntries.Response( member1, 0, true, 2, 2 ), state, log() );

        state.update( outcome );

        // then
        assertEquals( 2, state.commitIndex() );
    }

    @Test
    public void shouldSendNegativeResponseForVoteRequestFromTermNotGreaterThanLeader() throws Exception
    {
        // given
        long leaderTerm = 5;
        long leaderCommitIndex = 10;
        long rivalTerm = leaderTerm - 1;

        Leader leader = new Leader();
        RaftState state = raftState()
                .term( leaderTerm )
                .commitIndex( leaderCommitIndex )
                .build();

        // when
        Outcome outcome = leader.handle( new RaftMessages.Vote.Request( member1, rivalTerm, member1, leaderCommitIndex, leaderTerm ), state, log() );

        // then
        assertThat( outcome.getRole(), equalTo( LEADER) );
        assertThat( outcome.getTerm(), equalTo( leaderTerm ) );

        RaftMessages.RaftMessage response = messageFor( outcome, member1 );
        assertThat( response, instanceOf( RaftMessages.Vote.Response.class ) );
        RaftMessages.Vote.Response typedResponse = (RaftMessages.Vote.Response) response;
        assertThat( typedResponse.voteGranted(), equalTo( false ) );
    }

    @Test
    public void shouldStepDownIfReceiveVoteRequestFromGreaterTermThanLeader() throws Exception
    {
        // given
        long leaderTerm = 1;
        long leaderCommitIndex = 10;
        long rivalTerm = leaderTerm + 1;

        Leader leader = new Leader();
        RaftState state = raftState()
                .term( leaderTerm )
                .commitIndex( leaderCommitIndex )
                .build();

        // when
        Outcome outcome = leader.handle( new RaftMessages.Vote.Request( member1, rivalTerm, member1, leaderCommitIndex, leaderTerm ), state, log() );

        // then
        assertThat( outcome.getRole(), equalTo( FOLLOWER ) );
        assertThat( outcome.getLeader(), nullValue() );
        assertThat( outcome.getTerm(), equalTo( rivalTerm ) );

        RaftMessages.RaftMessage response = messageFor( outcome, member1 );
        assertThat( response, instanceOf( RaftMessages.Vote.Response.class ) );
        RaftMessages.Vote.Response typedResponse = (RaftMessages.Vote.Response) response;
        assertThat( typedResponse.voteGranted(), equalTo( true ) );
    }

    @Test
    public void shouldIgnoreHeartbeatFromOlderTerm() throws Exception
    {
        // given
        long leaderTerm = 5;
        long leaderCommitIndex = 10;
        long rivalTerm = leaderTerm - 1;

        Leader leader = new Leader();
        RaftState state = raftState()
                .term( leaderTerm )
                .commitIndex( leaderCommitIndex )
                .build();

        // when
        Outcome outcome = leader.handle( new RaftMessages.Heartbeat( member1, rivalTerm, leaderCommitIndex, leaderTerm ), state, log() );

        // then
        assertThat( outcome.getRole(), equalTo( LEADER) );
        assertThat( outcome.getTerm(), equalTo( leaderTerm ) );
    }

    @Test
    public void shouldStepDownIfHeartbeatReceivedWithGreaterOrEqualTerm() throws Exception
    {
        // given
        long leaderTerm = 1;
        long leaderCommitIndex = 10;
        long rivalTerm = leaderTerm + 1;

        Leader leader = new Leader();
        RaftState state = raftState()
                .term( leaderTerm )
                .commitIndex( leaderCommitIndex )
                .build();

        // when
        Outcome outcome = leader.handle( new RaftMessages.Heartbeat( member1, rivalTerm, leaderCommitIndex, leaderTerm ), state, log() );

        // then
        assertThat( outcome.getRole(), equalTo( FOLLOWER ) );
        assertThat( outcome.getLeader(), equalTo( member1 ) );
        assertThat( outcome.getTerm(), equalTo( rivalTerm ) );
    }

    @Test
    public void shouldRespondNegativelyToAppendEntriesRequestFromEarlierTerm() throws Exception
    {
        // given
        long leaderTerm = 5;
        long leaderCommitIndex = 10;
        long rivalTerm = leaderTerm - 1;
        long logIndex = 20;
        RaftLogEntry[] entries = { new RaftLogEntry( rivalTerm, ReplicatedInteger.valueOf( 99 ) ) };

        Leader leader = new Leader();
        RaftState state = raftState()
                .term( leaderTerm )
                .commitIndex( leaderCommitIndex )
                .build();

        // when
        Outcome outcome =
                leader.handle( new RaftMessages.AppendEntries.Request( member1, rivalTerm, logIndex, leaderTerm, entries, leaderCommitIndex ), state, log() );

        // then
        assertThat( outcome.getRole(), equalTo( LEADER) );
        assertThat( outcome.getTerm(), equalTo( leaderTerm ) );

        RaftMessages.RaftMessage response = messageFor( outcome, member1 );
        assertThat( response, instanceOf( RaftMessages.AppendEntries.Response.class ) );
        RaftMessages.AppendEntries.Response typedResponse = (RaftMessages.AppendEntries.Response) response;
        assertThat( typedResponse.term(), equalTo( leaderTerm ) );
        assertThat( typedResponse.success(), equalTo( false ) );
    }

    @Test
    public void shouldStepDownIfAppendEntriesRequestFromLaterTerm() throws Exception
    {
        // given
        long leaderTerm = 1;
        long leaderCommitIndex = 10;
        long rivalTerm = leaderTerm + 1;
        long logIndex = 20;
        RaftLogEntry[] entries = { new RaftLogEntry( rivalTerm, ReplicatedInteger.valueOf( 99 ) ) };

        Leader leader = new Leader();
        RaftState state = raftState()
                .term( leaderTerm )
                .commitIndex( leaderCommitIndex )
                .build();

        // when
        Outcome outcome =
                leader.handle( new RaftMessages.AppendEntries.Request( member1, rivalTerm, logIndex, leaderTerm, entries, leaderCommitIndex ), state, log() );

        // then
        assertThat( outcome.getRole(), equalTo( FOLLOWER ) );
        assertThat( outcome.getLeader(), equalTo( member1 ) );
        assertThat( outcome.getTerm(), equalTo( rivalTerm ) );

        RaftMessages.RaftMessage response = messageFor( outcome, member1 );
        assertThat( response, instanceOf( RaftMessages.AppendEntries.Response.class ) );
        RaftMessages.AppendEntries.Response typedResponse = (RaftMessages.AppendEntries.Response) response;
        assertThat( typedResponse.term(), equalTo( rivalTerm ) );
        // Not checking success or failure of append
    }

    private RaftState preElectionActive() throws IOException
    {
        return raftState()
                .myself( myself )
                .supportsPreVoting( true )
                .setPreElection( true )
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();
    }

    private Log log()
    {
        return logProvider.getLog( getClass() );
    }
}
