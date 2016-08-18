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
package org.neo4j.coreedge.core.consensus.roles;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import org.neo4j.coreedge.core.consensus.RaftMessages;
import org.neo4j.coreedge.core.consensus.RaftMessages.AppendEntries;
import org.neo4j.coreedge.core.consensus.RaftMessages.Timeout.Heartbeat;
import org.neo4j.coreedge.core.consensus.ReplicatedInteger;
import org.neo4j.coreedge.core.consensus.ReplicatedString;
import org.neo4j.coreedge.core.consensus.log.InMemoryRaftLog;
import org.neo4j.coreedge.core.consensus.log.RaftLog;
import org.neo4j.coreedge.core.consensus.log.RaftLogEntry;
import org.neo4j.coreedge.core.consensus.log.ReadableRaftLog;
import org.neo4j.coreedge.messaging.Inbound;
import org.neo4j.coreedge.messaging.Outbound;
import org.neo4j.coreedge.core.consensus.outcome.AppendLogEntry;
import org.neo4j.coreedge.core.consensus.outcome.BatchAppendLogEntries;
import org.neo4j.coreedge.core.consensus.outcome.Outcome;
import org.neo4j.coreedge.core.consensus.outcome.ShipCommand;
import org.neo4j.coreedge.core.consensus.state.RaftState;
import org.neo4j.coreedge.core.consensus.state.ReadableRaftState;
import org.neo4j.coreedge.core.consensus.roles.follower.FollowerState;
import org.neo4j.coreedge.core.consensus.roles.follower.FollowerStates;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.coreedge.core.consensus.MessageUtils.messageFor;
import static org.neo4j.coreedge.core.consensus.ReplicatedInteger.valueOf;
import static org.neo4j.coreedge.core.consensus.TestMessageBuilders.appendEntriesResponse;
import static org.neo4j.coreedge.core.consensus.roles.Role.FOLLOWER;
import static org.neo4j.coreedge.core.consensus.state.RaftStateBuilder.raftState;
import static org.neo4j.coreedge.identity.RaftTestMember.member;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.helpers.collection.Iterators.asSet;

@RunWith(MockitoJUnitRunner.class)
public class LeaderTest
{
    private MemberId myself = member( 0 );

    /* A few members that we use at will in tests. */
    private MemberId member1 = member( 1 );
    private MemberId member2 = member( 2 );

    @Mock
    private Inbound inbound;

    @Mock
    private Outbound outbound;

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
        RaftMessages.AppendEntries.Response response = appendEntriesResponse().success()
                .matchIndex( 90 ).term( 4 ).from( instance2 ).build();

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
        RaftMessages.AppendEntries.Response response = appendEntriesResponse().success()
                .matchIndex( 100 ).term( 4 ).from( instance2 ).build();

        Outcome outcome = leader.handle( response, state, mock( Log.class ) );

        // then
        // The leader should not be trying to send any messages to that instance
        assertTrue( outcome.getOutgoingMessages().isEmpty() );
        // And the follower state should be updated
        FollowerStates<MemberId> updatedFollowerStates = outcome.getFollowerStates();
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
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();

        Leader leader = new Leader();

        // when
        Outcome outcome = leader.handle( new Heartbeat( member1 ), state, log() );

        // then
        assertTrue( messageFor( outcome, member1 ) instanceof RaftMessages.Heartbeat );
        assertTrue( messageFor( outcome, member2 ) instanceof RaftMessages.Heartbeat );
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

        RaftMessages.NewEntry.Request newEntryRequest = new RaftMessages.NewEntry.Request(
                member( 9 ), CONTENT );

        // when
        Outcome outcome = leader.handle( newEntryRequest, state, log() );
        //state.update( outcome );

        // then
        AppendLogEntry logCommand = (AppendLogEntry) single( outcome.getLogCommands() );
        assertEquals( 0, logCommand.index );
        assertEquals( 0, logCommand.entry.term() );

        ShipCommand.NewEntries shipCommand = (ShipCommand.NewEntries) single( outcome.getShipCommands() );

        assertEquals( shipCommand, new ShipCommand.NewEntries( -1, -1, new RaftLogEntry[]{ new RaftLogEntry( 0, CONTENT ) } ) );
    }

    @Test
    public void leaderShouldHandleBatch() throws Exception
    {
        // given
        RaftState state = raftState()
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();

        Leader leader = new Leader();

        int BATCH_SIZE = 3;
        RaftMessages.NewEntry.BatchRequest batchRequest =
                new RaftMessages.NewEntry.BatchRequest( BATCH_SIZE );
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
            assertEquals( i, ((ReplicatedInteger)logCommand.entries[i].content()).get() );
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

        RaftState state = raftState().votingMembers( member1, member2 )
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
        Outcome outcome = leader.handle(
                new RaftMessages.AppendEntries.Response( member1, 0, true, 0, 0 ),
                state, log() );

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
                .term( 0 ).entryLog( raftLog )
                .messagesSentToFollower( member1, raftLog.appendIndex() + 1 )
                .messagesSentToFollower( member2, raftLog.appendIndex() + 1 )
                .build();

        Leader leader = new Leader();

        // when
        Outcome outcome =
                leader.handle( new AppendEntries.Response( member1, 0, true, 2, 2 ),
                        state, log() );

        state.update( outcome );

        // then
        assertEquals( 2, state.commitIndex() );
    }

    private Log log()
    {
        return logProvider.getLog( getClass() );
    }
}
