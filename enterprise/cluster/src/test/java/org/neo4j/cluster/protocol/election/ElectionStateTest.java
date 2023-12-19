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
package org.neo4j.cluster.protocol.election;

import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.protocol.MessageArgumentMatcher;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AtomicBroadcastMessage;
import org.neo4j.cluster.protocol.cluster.ClusterContext;
import org.neo4j.cluster.protocol.cluster.ClusterMessage.VersionedConfigurationStateChange;
import org.neo4j.logging.NullLog;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.cluster.protocol.cluster.ClusterConfiguration.COORDINATOR;
import static org.neo4j.cluster.protocol.election.ElectionMessage.demote;
import static org.neo4j.cluster.protocol.election.ElectionMessage.performRoleElections;
import static org.neo4j.cluster.protocol.election.ElectionMessage.voted;
import static org.neo4j.cluster.protocol.election.ElectionState.election;

public class ElectionStateTest
{
    @Test
    public void testElectionRequestIsRejectedIfNoQuorum() throws Throwable
    {
        ElectionContext context = mock( ElectionContext.class );
        ClusterContext clusterContextMock = mock( ClusterContext.class );
        when( context.getLog( ArgumentMatchers.any() ) ).thenReturn( NullLog.getInstance() );

        when( context.electionOk() ).thenReturn( false );
        when( clusterContextMock.getLog( ArgumentMatchers.any() ) ).thenReturn( NullLog.getInstance() );

        MessageHolder holder = mock( MessageHolder.class );

        election.handle( context, Message.internal( performRoleElections ), holder );

        verifyZeroInteractions( holder );
    }

    @Test
    public void testElectionFromDemoteIsRejectedIfNoQuorum() throws Throwable
    {
        ElectionContext context = mock( ElectionContext.class );
        ClusterContext clusterContextMock = mock( ClusterContext.class );

        when( context.electionOk() ).thenReturn( false );
        when( clusterContextMock.getLog( ArgumentMatchers.any() ) ).thenReturn( NullLog.getInstance() );
        when( context.getLog( ArgumentMatchers.any() ) ).thenReturn( NullLog.getInstance() );

        MessageHolder holder = mock( MessageHolder.class );

        election.handle( context, Message.internal( demote ), holder );

        verifyZeroInteractions( holder );
    }

    @Test
    public void electionShouldRemainLocalIfStartedBySingleInstanceWhichIsTheRoleHolder() throws Throwable
    {
        /*
         * Ensures that when an instance is alone in the cluster, elections for roles that it holds do not set
         * timeouts or try to reach other instances.
         */

        // Given
        ElectionContext context = mock( ElectionContext.class );
        ClusterContext clusterContextMock = mock( ClusterContext.class );

        when( clusterContextMock.getLog( ArgumentMatchers.any() ) ).thenReturn( NullLog.getInstance() );
        MessageHolder holder = mock( MessageHolder.class );

          // These mean the election can proceed normally, by us
        when( context.electionOk() ).thenReturn( true );
        when( context.isInCluster() ).thenReturn( true );
        when( context.isElector() ).thenReturn( true );

          // Like it says on the box, we are the only instance
        final InstanceId myInstanceId = new InstanceId( 1 );
        Map<InstanceId, URI> members = new HashMap<>();
        members.put( myInstanceId, URI.create( "ha://me" ) );
        when( context.getMembers() ).thenReturn( members );

          // Any role would do, just make sure we have it
        final String role = "master";
        ElectionContext.VoteRequest voteRequest = new ElectionContext.VoteRequest( role, 13 );
        when( context.getPossibleRoles() ).thenReturn(
                Collections.singletonList( new ElectionRole( role ) ) );
        when( context.getElected( role ) ).thenReturn( myInstanceId );
        when( context.voteRequestForRole( new ElectionRole( role ) ) ).thenReturn( voteRequest );

          // Required for logging
        when( context.getLog( Mockito.any() ) ).thenReturn( NullLog.getInstance() );

        // When
        election.handle( context, Message.internal( performRoleElections ), holder );

        // Then
          // Make sure that we asked ourselves to vote for that role and that no timer was set
        verify( holder, times(1) ).offer( ArgumentMatchers.argThat( new MessageArgumentMatcher<ElectionMessage>()
                .onMessageType( ElectionMessage.vote ).withPayload( voteRequest ) ) );
        verify( context, never() ).setTimeout( ArgumentMatchers.any(), ArgumentMatchers.any() );
    }

    @Test
    public void delayedVoteFromPreviousElectionMustNotCauseCurrentElectionToComplete() throws Throwable
    {
        // Given
        ElectionContext context = mock( ElectionContext.class );
        MessageHolder holder = mock( MessageHolder.class );

        when( context.getLog( Mockito.any() ) ).thenReturn( NullLog.getInstance() );

        final String role = "master";
        final InstanceId voter = new InstanceId( 2 );

        ElectionCredentials voteCredentialComparable = mock( ElectionCredentials.class );
        Message<ElectionMessage> vote = Message.internal( voted, new ElectionMessage.VersionedVotedData( role, voter,
                voteCredentialComparable, 4 ) );

        when( context.voted( role, voter, voteCredentialComparable, 4 ) ).thenReturn( false );

        // When
        election.handle( context, vote, holder );

        verify( context ).getLog( ArgumentMatchers.any() );
        verify( context ).voted( role, voter, voteCredentialComparable, 4 );

        // Then
        verifyNoMoreInteractions( context, holder );
    }

    @Test
    public void timeoutMakesElectionBeForgotten() throws Throwable
    {
        // Given
        String coordinatorRole = "coordinator";

        ElectionContext context = mock( ElectionContext.class );
        when( context.getLog( Mockito.any() ) ).thenReturn( NullLog.getInstance() );

        MessageHolder holder = mock( MessageHolder.class );

        Message<ElectionMessage> timeout = Message.timeout( ElectionMessage.electionTimeout,
                Message.internal( performRoleElections ),
                new ElectionState.ElectionTimeoutData( coordinatorRole, null ) );

        // When
        election.handle( context, timeout, holder );

        // Then
        verify( context, times( 1 ) ).forgetElection( coordinatorRole );
    }

    @Test
    public void electionCompletingMakesItBeForgotten() throws Throwable
    {
        // Given
        String coordinatorRole = "coordinator";
        InstanceId votingInstance = new InstanceId( 2 );
        ElectionCredentials voteCredentialComparable = mock( ElectionCredentials.class );

        ElectionContext context = mock( ElectionContext.class );
        when( context.getLog( Mockito.any() ) ).thenReturn( NullLog.getInstance() );
        when( context.getNeededVoteCount() ).thenReturn( 3 );
        when( context.getVoteCount( coordinatorRole ) ).thenReturn( 3 );
        when( context.voted( coordinatorRole, votingInstance, voteCredentialComparable, 4 ) ).thenReturn( true );
        MessageHolder holder = mock( MessageHolder.class );

        Message<ElectionMessage> vote = Message.to( ElectionMessage.voted, URI.create( "cluster://elector" ),
                new ElectionMessage.VersionedVotedData( coordinatorRole, votingInstance, voteCredentialComparable, 4 ) );

        // When
        election.handle( context, vote, holder );

        // Then
        verify( context, times( 1 ) ).forgetElection( coordinatorRole );
    }

    @Test
    public void voteResponseShouldHaveSameVersionAsVoteRequest() throws Throwable
    {
        final List<Message<?>> messages = new ArrayList<>( 1 );
        MessageHolder holder = messages::add;

        ElectionContext context = mock( ElectionContext.class );

        final int version = 14;
        Message<ElectionMessage> voteRequest = Message.to( ElectionMessage.vote, URI.create( "some://instance" ),
                new ElectionContext.VoteRequest( "coordinator", version )
        );
        voteRequest.setHeader( Message.HEADER_FROM, "some://other" );

        election.handle( context, voteRequest, holder );

        assertEquals( 1, messages.size() );
        Message<?> response = messages.get( 0 );
        assertEquals( ElectionMessage.voted, response.getMessageType() );
        ElectionMessage.VersionedVotedData payload = response.getPayload();
        assertEquals( version, payload.getVersion() );
    }

    @Test
    public void shouldSendAtomicBroadcastOnJoiningAClusterWithAnEstablishedCoordinator() throws Throwable
    {
        // Given
        String winnerURI = "some://winner";
        InstanceId winner = new InstanceId( 2 );

        final List<Message<?>> messages = new ArrayList<>( 1 );
        MessageHolder holder = messages::add;
        ElectionCredentials voteCredentialComparable = mock( ElectionCredentials.class );

        ElectionContext electionContext = mock( ElectionContext.class );
        when( electionContext.voted( eq( COORDINATOR ), eq( new InstanceId( 1 ) ), eq( voteCredentialComparable ),
                anyLong() ) ).thenReturn( true );
        when( electionContext.getVoteCount( COORDINATOR ) ).thenReturn( 3 );
        when( electionContext.getNeededVoteCount() ).thenReturn( 3 );
        when( electionContext.getElectionWinner( COORDINATOR ) ).thenReturn( winner );

        when( electionContext.getLog( any( Class.class ) ) ).thenReturn( NullLog.getInstance() );
        VersionedConfigurationStateChange stateChange = mock( VersionedConfigurationStateChange.class );
        when( electionContext.newConfigurationStateChange() ).thenReturn( stateChange );

        when( electionContext.getUriForId( winner ) ).thenReturn( URI.create( winnerURI ) );

        // When
        Message<ElectionMessage> votedMessage = Message.to(
                ElectionMessage.voted, URI.create( "some://instance" ),
                new ElectionMessage.VotedData( COORDINATOR, new InstanceId( 1 ), voteCredentialComparable ) );
        votedMessage.setHeader( Message.HEADER_FROM, "some://other" );

        election.handle( electionContext, votedMessage, holder );

        // Then
        assertEquals( 1, messages.size() );
        Message<?> message = messages.get( 0 );
        assertEquals( AtomicBroadcastMessage.broadcast, message.getMessageType() );
    }
}
