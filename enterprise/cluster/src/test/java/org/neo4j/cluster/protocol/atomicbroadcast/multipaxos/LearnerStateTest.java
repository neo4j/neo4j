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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import org.junit.Test;
import org.mockito.Matchers;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.com.message.MessageType;
import org.neo4j.cluster.protocol.MessageArgumentMatcher;
import org.neo4j.cluster.statemachine.State;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class LearnerStateTest
{
    @Test
    public void shouldUseLastKnownOnlineClusterMemberAndSetTimeoutForCatchup() throws Throwable
    {
        // Given
        LearnerState state = LearnerState.learner;
        LearnerContext ctx = mock( LearnerContext.class );
        MessageHolder outgoing = mock( MessageHolder.class );
        org.neo4j.cluster.InstanceId upToDateClusterMember = new org.neo4j.cluster.InstanceId( 1 );

        // What we know
        when( ctx.getLastLearnedInstanceId() ).thenReturn( 0l );
        when( ctx.getPaxosInstance( new org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId( 1l ) ) )
                .thenReturn( new PaxosInstance( null, new org.neo4j.cluster.protocol.atomicbroadcast.multipaxos
                        .InstanceId( 1l ) ) );
        when( ctx.getLastKnownAliveUpToDateInstance() ).thenReturn( upToDateClusterMember );
        when( ctx.getUriForId( upToDateClusterMember ) ).thenReturn( new URI( "c:/1" ) );

        // What we know the cluster knows
        when( ctx.getLastKnownLearnedInstanceInCluster() ).thenReturn( 1l );

        // When
        Message<LearnerMessage> message = Message.to( LearnerMessage.catchUp, new URI( "c:/2" ), 2l )
                .setHeader( Message.FROM, "c:/2" ).setHeader( Message.INSTANCE_ID, "2" );
        State newState = state.handle( ctx, message, outgoing );

        // Then

        assertThat( newState, equalTo( (State) LearnerState.learner ) );
        verify( outgoing ).offer( Message.to( LearnerMessage.learnRequest, new URI( "c:/1" ),
                new LearnerMessage.LearnRequestState() ).setHeader(
                org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId.INSTANCE,
                Long.toString( 1l ) ) );
        verify( ctx ).setTimeout( "learn", Message.timeout( LearnerMessage.learnTimedout, message ) );
    }

    @Test
    public void learnerServingOldInstanceShouldNotLogErrorIfItDoesNotHaveIt() throws Throwable
    {
        // Given
        LearnerState state = LearnerState.learner;
        LearnerContext ctx = mock( LearnerContext.class );
        MessageHolder outgoing = mock( MessageHolder.class );
        // The instance will be asked for paxos instance 4...
        InstanceId paxosInstanceIdIDontHave = new InstanceId( 4 );
        Message<LearnerMessage> messageRequestingId = Message.to( LearnerMessage.learnRequest, URI.create( "c:/1" ) )
                .setHeader( Message.FROM, "c:/2" )
                .setHeader( InstanceId.INSTANCE, "4" );
        // ...but it does not have it yet
        when( ctx.getPaxosInstance( paxosInstanceIdIDontHave ) )
                .thenReturn( new PaxosInstance( mock( PaxosInstanceStore.class ), paxosInstanceIdIDontHave ) );

        // When
        state.handle( ctx, messageRequestingId, outgoing );

        // Then
        // verify there is no logging of the failure
        verify( ctx, times( 0 ) ).notifyLearnMiss( paxosInstanceIdIDontHave );
        // but the learn failed went out anyway
        verify( outgoing, times( 1 ) ).offer(
                Matchers.<Message<? extends MessageType>>argThat( new MessageArgumentMatcher()
                        .onMessageType( LearnerMessage.learnFailed ).to( URI.create( "c:/2" ) ) )
        );
    }

    @Test
    public void learnerReceivingLearnFailedShouldLogIt() throws Throwable
    {
        // Given
        LearnerState state = LearnerState.learner;
        LearnerContext ctx = mock( LearnerContext.class );
        MessageHolder outgoing = mock( MessageHolder.class );
        InstanceId paxosInstanceIdIAskedFor = new InstanceId( 4 );
        Message<LearnerMessage> theLearnFailure = Message.to( LearnerMessage.learnFailed, URI.create( "c:/1" ) )
                .setHeader( Message.FROM, "c:/2" )
                .setHeader( InstanceId.INSTANCE, "4" );
        when( ctx.getPaxosInstance( paxosInstanceIdIAskedFor ) )
                .thenReturn( new PaxosInstance( mock( PaxosInstanceStore.class ), paxosInstanceIdIAskedFor ) );
        when( ctx.getMemberURIs() ).thenReturn( Collections.singletonList( URI.create( "c:/2" ) ) );

        // When
        state.handle( ctx, theLearnFailure, outgoing );

        // Then
        // verify that the failure was logged
        verify( ctx, times( 1 ) ).notifyLearnMiss( paxosInstanceIdIAskedFor );
    }

    @Test
    public void learnerShouldAskAllAliveInstancesAndTheseOnlyForMissingValue() throws Throwable
    {
        // Given

        List<URI> allMembers = new ArrayList<URI>( 3 );
        URI instance1 = URI.create( "c:/1" ); // this one is failed
        URI instance2 = URI.create( "c:/2" ); // this one is ok and will respond
        URI instance3 = URI.create( "c:/3" ); // this one is the requesting instance
        URI instance4 = URI.create( "c:/4" ); // and this one is ok and will respond too
        allMembers.add( instance1 );
        allMembers.add( instance2 );
        allMembers.add( instance3 );
        allMembers.add( instance4 );

        Set<org.neo4j.cluster.InstanceId> aliveInstanceIds = new HashSet<org.neo4j.cluster.InstanceId>();
        org.neo4j.cluster.InstanceId id2 = new org.neo4j.cluster.InstanceId( 2 );
        org.neo4j.cluster.InstanceId id4 = new org.neo4j.cluster.InstanceId( 4 );
        aliveInstanceIds.add( id2 );
        aliveInstanceIds.add( id4 );

        LearnerState state = LearnerState.learner;
        LearnerContext ctx = mock( LearnerContext.class );
        MessageHolder outgoing = mock( MessageHolder.class );
        InstanceId paxosInstanceIdIAskedFor = new InstanceId( 4 );

        when( ctx.getLastDeliveredInstanceId() ).thenReturn( 3l );
        when( ctx.getLastKnownLearnedInstanceInCluster() ).thenReturn( 5l );
        when( ctx.getMemberURIs() ).thenReturn( allMembers );
        when( ctx.getAlive() ).thenReturn( aliveInstanceIds );
        when( ctx.getUriForId( id2 ) ).thenReturn( instance2 );
        when( ctx.getUriForId( id4 ) ).thenReturn( instance4 );
        when( ctx.getPaxosInstance( paxosInstanceIdIAskedFor ) )
                .thenReturn( new PaxosInstance( mock( PaxosInstanceStore.class ), paxosInstanceIdIAskedFor ) );

        Message<LearnerMessage> theCause = Message.to( LearnerMessage.catchUp, instance2 ); // could be anything, really

        // When
        state.handle( ctx, Message.timeout( LearnerMessage.learnTimedout, theCause ), outgoing );

        // Then
        verify( outgoing, times( 1 ) ).offer(
                Matchers.<Message<? extends MessageType>>argThat( new MessageArgumentMatcher()
                        .onMessageType( LearnerMessage.learnRequest ).to( instance2 ) )
        );
        verify( outgoing, times( 1 ) ).offer(
                Matchers.<Message<? extends MessageType>>argThat( new MessageArgumentMatcher()
                        .onMessageType( LearnerMessage.learnRequest ).to( instance4 ) )
        );
        verifyNoMoreInteractions( outgoing );
    }

    @Test
    public void shouldHandleLocalLearnMessagesWithoutInstanceIdInTheMessageHeaderWhenCatchingUp() throws Throwable
    {
        // Given
        LearnerState learner = LearnerState.learner;
        org.neo4j.cluster.InstanceId instanceId = new org.neo4j.cluster.InstanceId( 42 );
        long payload = 12l;

        LearnerContext context = mock( LearnerContext.class );
        when( context.getMyId() ).thenReturn( instanceId );
        when( context.getLastKnownLearnedInstanceInCluster() ).thenReturn( 11l );
        when( context.getLastLearnedInstanceId() ).thenReturn( payload );

        @SuppressWarnings( "unchecked" )
        Message<LearnerMessage> message = mock( Message.class );
        when( message.getMessageType() ).thenReturn( LearnerMessage.catchUp );
        when( message.hasHeader( Message.INSTANCE_ID )).thenReturn( false );
        when( message.getHeader( Message.INSTANCE_ID ) ).thenThrow( new IllegalArgumentException() );
        when( message.getPayload() ).thenReturn( payload );

        // When
        State<?,?> state = learner.handle( context, message, mock( MessageHolder.class ) );

        // Then
        assertSame( state, learner );
        verify( context, times(1) ).setLastKnownLearnedInstanceInCluster( payload, instanceId );
    }
}
