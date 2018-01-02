/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cluster.protocol.election;

import org.junit.Test;
import org.mockito.Matchers;
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
import org.neo4j.cluster.com.message.MessageType;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AtomicBroadcastMessage;
import org.neo4j.cluster.protocol.cluster.ClusterContext;
import org.neo4j.cluster.protocol.cluster.ClusterMessage;
import org.neo4j.cluster.protocol.MessageArgumentMatcher;
import org.neo4j.logging.NullLog;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
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

        when( context.electionOk() ).thenReturn( false );
        when( clusterContextMock.getLog( Matchers.<Class>any() ) ).thenReturn( NullLog.getInstance() );
//        when( context.getClusterContext() ).thenReturn( clusterContextMock );

        MessageHolder holder = mock( MessageHolder.class );

        election.handle( context,
                Message.<ElectionMessage>internal( performRoleElections ), holder );

        verifyZeroInteractions( holder );
    }

    @Test
    public void testElectionFromDemoteIsRejectedIfNoQuorum() throws Throwable
    {
        ElectionContext context = mock( ElectionContext.class );
        ClusterContext clusterContextMock = mock( ClusterContext.class );

        when( context.electionOk() ).thenReturn( false );
        when( clusterContextMock.getLog( Matchers.<Class>any() ) ).thenReturn( NullLog.getInstance() );
        when( context.getLog( Matchers.<Class>any() ) ).thenReturn( NullLog.getInstance() );

        MessageHolder holder = mock( MessageHolder.class );

        election.handle( context,
                Message.<ElectionMessage>internal( demote ), holder );

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

        when( clusterContextMock.getLog( Matchers.<Class>any() ) ).thenReturn( NullLog.getInstance() );
//        when( context.getClusterContext() ).thenReturn( clusterContextMock );
        MessageHolder holder = mock( MessageHolder.class );

          // These mean the election can proceed normally, by us
        when( context.electionOk() ).thenReturn( true );
        when( context.isInCluster() ).thenReturn( true );
        when( context.isElector() ).thenReturn( true );

          // Like it says on the box, we are the only instance
        final InstanceId myInstanceId = new InstanceId( 1 );
        Map<InstanceId, URI> members = new HashMap<InstanceId, URI>();
        members.put( myInstanceId, URI.create( "ha://me" ) );
        when( context.getMembers() ).thenReturn( members );

          // Any role would do, just make sure we have it
        final String role = "master";
        ElectionContext.VoteRequest voteRequest = new ElectionContext.VoteRequest( role, 13 );
        when( context.getPossibleRoles() ).thenReturn(
                Collections.<ElectionRole>singletonList( new ElectionRole( role ) ) );
        when( context.getElected( role ) ).thenReturn( myInstanceId );
        when( context.voteRequestForRole( new ElectionRole( role ) ) ).thenReturn( voteRequest );

          // Required for logging
        when( context.getLog( Mockito.<Class>any() ) ).thenReturn( NullLog.getInstance() );

        // When
        election.handle( context,
                Message.<ElectionMessage>internal( performRoleElections ), holder );

        // Then
          // Make sure that we asked ourselves to vote for that role and that no timer was set
        verify( holder, times(1) ).offer( Matchers.argThat( new MessageArgumentMatcher<ElectionMessage>()
                .onMessageType( ElectionMessage.vote ).withPayload( voteRequest ) ) );
        verify( context, times( 0 ) ).setTimeout( Matchers.<String>any(), Matchers.<Message>any() );
    }

    @Test
    public void delayedVoteFromPreviousElectionMustNotCauseCurrentElectionToComplete() throws Throwable
    {
        // Given
        ElectionContext context = mock( ElectionContext.class );
        MessageHolder holder = mock( MessageHolder.class );

        when( context.getLog( Mockito.<Class>any() ) ).thenReturn( NullLog.getInstance() );

        final String role = "master";
        final InstanceId voter = new InstanceId( 2 );

        Comparable<Object> voteCredentialComparable = new Comparable<Object>()
        {
            @Override
            public int compareTo( Object o )
            {
                return 0;
            }
        };
        Message vote = Message.internal( voted, new ElectionMessage.VersionedVotedData( role, voter,
                voteCredentialComparable
        , 4 ) );

        when( context.voted( role, voter, voteCredentialComparable, 4 ) ).thenReturn( false );

        // When
        election.handle( context, vote, holder );

        verify( context ).getLog( Matchers.<Class>any() );
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
        when( context.getLog( Mockito.<Class>any() ) ).thenReturn( NullLog.getInstance() );

        MessageHolder holder = mock( MessageHolder.class );

        Message timeout = Message.timeout( ElectionMessage.electionTimeout,
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
        Comparable<Object> voteCredentialComparable = new Comparable<Object>()
        {
            @Override
            public int compareTo( Object o )
            {
                return 0;
            }
        };

        ElectionContext context = mock( ElectionContext.class );
        when( context.getLog( Mockito.<Class>any() ) ).thenReturn( NullLog.getInstance() );
        when( context.getNeededVoteCount() ).thenReturn( 3 );
        when( context.getVoteCount( coordinatorRole ) ).thenReturn( 3 );
        when( context.voted( coordinatorRole, votingInstance, voteCredentialComparable, 4 ) ).thenReturn( true );
        MessageHolder holder = mock( MessageHolder.class );

        Message vote = Message.to( ElectionMessage.voted, URI.create( "cluster://elector" ),
                new ElectionMessage.VersionedVotedData( coordinatorRole, votingInstance, voteCredentialComparable, 4 ) );

        // When
        election.handle( context, vote, holder );

        // Then
        verify( context, times( 1 ) ).forgetElection( coordinatorRole );
    }

    @Test
    public void voteResponseShouldHaveSameVersionAsVoteRequest() throws Throwable
    {
        final List<Message> messages = new ArrayList<Message>( 1 );
        MessageHolder holder = new MessageHolder()
        {
            @Override
            public void offer( Message<? extends MessageType> message )
            {
                messages.add( message );
            }
        };

        ElectionContext context = mock( ElectionContext.class );

        final int version = 14;
        Message voteRequest = Message.to( ElectionMessage.vote, URI.create( "some://instance" ),
                new ElectionContext.VoteRequest( "coordinator", version )
        );
        voteRequest.setHeader( Message.FROM, "some://other" );

        election.handle( context, voteRequest, holder );

        assertEquals( 1, messages.size() );
        Message response = messages.get( 0 );
        assertEquals( ElectionMessage.voted, response.getMessageType() );
        ElectionMessage.VersionedVotedData payload = (ElectionMessage.VersionedVotedData) response.getPayload();
        assertEquals( version, payload.getVersion() );
    }

    @Test
    public void shouldSendAtomicBroadcastOnJoiningAClusterWithAnEstablishedCoordinator() throws Throwable
    {
        // Given
        String winnerURI = "some://winner";
        InstanceId winner = new InstanceId( 2 );

        final List<Message<?>> messages = new ArrayList<>( 1 );
        MessageHolder holder = new MessageHolder()
        {
            @Override
            public void offer( Message<? extends MessageType> message )
            {
                messages.add( message );
            }
        };
        Comparable<Object> voteCredentialComparable = new Comparable<Object>()
        {
            @Override
            public int compareTo( Object o )
            {
                return 0;
            }
        };

        ElectionContext electionContext = mock( ElectionContext.class );
        when( electionContext.voted( eq( COORDINATOR ), eq( new InstanceId( 1 ) ), eq( voteCredentialComparable ),
                anyLong() ) ).thenReturn( true );
        when( electionContext.getVoteCount( COORDINATOR ) ).thenReturn( 3 );
        when( electionContext.getNeededVoteCount() ).thenReturn( 3 );
        when( electionContext.getElectionWinner( COORDINATOR ) ).thenReturn( winner );

        when( electionContext.getLog( any( Class.class ) ) ).thenReturn( NullLog.getInstance() );
        when( electionContext.newConfigurationStateChange() )
                .thenReturn( mock( ClusterMessage.VersionedConfigurationStateChange.class ) );

        when( electionContext.getUriForId( winner ) ).thenReturn( URI.create( winnerURI ) );

        // When
        Message<ElectionMessage> votedMessage = Message.to(
                ElectionMessage.voted, URI.create( "some://instance" ),
                new ElectionMessage.VotedData( COORDINATOR, new InstanceId( 1 ), voteCredentialComparable ) );
        votedMessage.setHeader( Message.FROM, "some://other" );

        election.handle( electionContext, votedMessage, holder );

        // Then
        assertEquals( 1, messages.size() );
        Message<?> message = messages.get( 0 );
        assertEquals( AtomicBroadcastMessage.broadcast, message.getMessageType() );
    }
}
