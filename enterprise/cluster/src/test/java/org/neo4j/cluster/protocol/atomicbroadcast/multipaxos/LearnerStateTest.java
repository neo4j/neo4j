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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.mockito.ArgumentMatchers;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.com.message.MessageType;
import org.neo4j.cluster.protocol.MessageArgumentMatcher;
import org.neo4j.cluster.statemachine.State;
import org.neo4j.logging.Log;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
        when( ctx.getLastLearnedInstanceId() ).thenReturn( 0L );
        when( ctx.getPaxosInstance( new org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId( 1L ) ) )
                .thenReturn( new PaxosInstance( null, new org.neo4j.cluster.protocol.atomicbroadcast.multipaxos
                        .InstanceId( 1L ) ) );
        when( ctx.getLastKnownAliveUpToDateInstance() ).thenReturn( upToDateClusterMember );
        when( ctx.getUriForId( upToDateClusterMember ) ).thenReturn( new URI( "c:/1" ) );

        // What we know the cluster knows
        when( ctx.getLastKnownLearnedInstanceInCluster() ).thenReturn( 1L );

        // When
        Message<LearnerMessage> message = Message.to( LearnerMessage.catchUp, new URI( "c:/2" ), 2L )
                .setHeader( Message.HEADER_FROM, "c:/2" ).setHeader( Message.HEADER_INSTANCE_ID, "2" );
        State newState = state.handle( ctx, message, outgoing );

        // Then

        assertThat( newState, equalTo( LearnerState.learner ) );
        verify( outgoing ).offer( Message.to( LearnerMessage.learnRequest, new URI( "c:/1" ),
                new LearnerMessage.LearnRequestState() ).setHeader(
                org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId.INSTANCE,
                Long.toString( 1L ) ) );
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
                .setHeader( Message.HEADER_FROM, "c:/2" )
                .setHeader( InstanceId.INSTANCE, "4" );
        // ...but it does not have it yet
        when( ctx.getPaxosInstance( paxosInstanceIdIDontHave ) )
                .thenReturn( new PaxosInstance( mock( PaxosInstanceStore.class ), paxosInstanceIdIDontHave ) );

        // When
        state.handle( ctx, messageRequestingId, outgoing );

        // Then
        // verify there is no logging of the failure
        verify( ctx, never() ).notifyLearnMiss( paxosInstanceIdIDontHave );
        // but the learn failed went out anyway
        verify( outgoing, times( 1 ) ).offer(
                ArgumentMatchers.<Message<? extends MessageType>>argThat( new MessageArgumentMatcher()
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
                .setHeader( Message.HEADER_FROM, "c:/2" )
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

        List<URI> allMembers = new ArrayList<>( 3 );
        URI instance1 = URI.create( "c:/1" ); // this one is failed
        URI instance2 = URI.create( "c:/2" ); // this one is ok and will respond
        URI instance3 = URI.create( "c:/3" ); // this one is the requesting instance
        URI instance4 = URI.create( "c:/4" ); // and this one is ok and will respond too
        allMembers.add( instance1 );
        allMembers.add( instance2 );
        allMembers.add( instance3 );
        allMembers.add( instance4 );

        Set<org.neo4j.cluster.InstanceId> aliveInstanceIds = new HashSet<>();
        org.neo4j.cluster.InstanceId id2 = new org.neo4j.cluster.InstanceId( 2 );
        org.neo4j.cluster.InstanceId id4 = new org.neo4j.cluster.InstanceId( 4 );
        aliveInstanceIds.add( id2 );
        aliveInstanceIds.add( id4 );

        LearnerState state = LearnerState.learner;
        LearnerContext ctx = mock( LearnerContext.class );
        MessageHolder outgoing = mock( MessageHolder.class );
        InstanceId paxosInstanceIdIAskedFor = new InstanceId( 4 );

        when( ctx.getLastDeliveredInstanceId() ).thenReturn( 3L );
        when( ctx.getLastKnownLearnedInstanceInCluster() ).thenReturn( 5L );
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
                ArgumentMatchers.<Message<? extends MessageType>>argThat( new MessageArgumentMatcher()
                        .onMessageType( LearnerMessage.learnRequest ).to( instance2 ) )
        );
        verify( outgoing, times( 1 ) ).offer(
                ArgumentMatchers.<Message<? extends MessageType>>argThat( new MessageArgumentMatcher()
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
        long payload = 12L;

        LearnerContext context = mock( LearnerContext.class );
        when( context.getMyId() ).thenReturn( instanceId );
        when( context.getLastKnownLearnedInstanceInCluster() ).thenReturn( 11L );
        when( context.getLastLearnedInstanceId() ).thenReturn( payload );

        @SuppressWarnings( "unchecked" )
        Message<LearnerMessage> message = mock( Message.class );
        when( message.getMessageType() ).thenReturn( LearnerMessage.catchUp );
        when( message.hasHeader( Message.HEADER_INSTANCE_ID )).thenReturn( false );
        when( message.getHeader( Message.HEADER_INSTANCE_ID ) ).thenThrow( new IllegalArgumentException() );
        when( message.getPayload() ).thenReturn( payload );

        // When
        State<?,?> state = learner.handle( context, message, mock( MessageHolder.class ) );

        // Then
        assertSame( state, learner );
        verify( context, times(1) ).setLastKnownLearnedInstanceInCluster( payload, instanceId );
    }

    @Test
    public void shouldCloseTheGapIfItsTheCoordinator() throws Throwable
    {
        // Given
        // A coordinator that knows that the last Paxos instance delivered is 3
        LearnerState learner = LearnerState.learner;
        org.neo4j.cluster.InstanceId memberId = new org.neo4j.cluster.InstanceId( 42 );
        long lastDelivered = 3L;

        LearnerContext context = mock( LearnerContext.class );
        when( context.isMe( any() ) ).thenReturn( true );
        when( context.getCoordinator() ).thenReturn( memberId ); // so it's the coordinator
        when( context.getLastDeliveredInstanceId() ).thenReturn( lastDelivered );
        // and has this list of pending instances (up to id 14)
        List<PaxosInstance> pendingInstances = new LinkedList<>();
        for ( int i = 1; i < 12; i++ ) // start at 1 because instance 3 is already delivered
        {
            InstanceId instanceId = new InstanceId( lastDelivered + i );
            PaxosInstance value = new PaxosInstance( mock( PaxosInstanceStore.class ), instanceId );
            value.closed( "", "" );
            when( context.getPaxosInstance( instanceId ) ).thenReturn( value );
            pendingInstances.add( value );
        }
        when( context.getLog( any() ) ).thenReturn( mock( Log.class ) );

        Message<LearnerMessage> incomingInstance = Message.to( LearnerMessage.learn, URI.create( "c:/1" ), new LearnerMessage.LearnState( new Object() ) )
                .setHeader( Message.HEADER_FROM, "c:/2" )
                .setHeader( Message.HEADER_CONVERSATION_ID, "conversation-id" )
                .setHeader( InstanceId.INSTANCE, "" + ( lastDelivered + LearnerContext.LEARN_GAP_THRESHOLD + 1 ) );

        // When
        // it receives a message with Paxos instance id 1 greater than the threshold
        learner.handle( context, incomingInstance, mock( MessageHolder.class ) );

        // Then
        // it delivers everything pending and marks the context appropriately
        for ( PaxosInstance pendingInstance : pendingInstances )
        {
            assertTrue( pendingInstance.isState( PaxosInstance.State.delivered ) );
            verify( context, times(1 ) ).setLastDeliveredInstanceId( pendingInstance.id.id );
        }
    }

    @Test
    public void shouldNotCloseTheGapIfItsTheCoordinatorAndTheGapIsSmallerThanTheThreshold() throws Throwable
    {
        // Given
        // A coordinator that knows that the last Paxos instance delivered is 3
        long lastDelivered = 3L;
        LearnerState learner = LearnerState.learner;
        org.neo4j.cluster.InstanceId memberId = new org.neo4j.cluster.InstanceId( 42 );

        LearnerContext context = mock( LearnerContext.class );
        when( context.isMe( any() ) ).thenReturn( true );
        when( context.getCoordinator() ).thenReturn( memberId ); // so it's the coordinator
        when( context.getLastDeliveredInstanceId() ).thenReturn( lastDelivered );
        // and has this list of pending instances (up to id 14)
        List<PaxosInstance> pendingInstances = new LinkedList<>();
        for ( int i = 1; i < 12; i++ ) // start at 1 because instance 3 is already delivered
        {
            InstanceId instanceId = new InstanceId( lastDelivered + i );
            PaxosInstance value = new PaxosInstance( mock( PaxosInstanceStore.class ), instanceId );
            value.closed( "", "" );
            when( context.getPaxosInstance( instanceId ) ).thenReturn( value );
            pendingInstances.add( value );
        }
        when( context.getLog( any() ) ).thenReturn( mock( Log.class ) );

        Message<LearnerMessage> incomingInstance = Message.to( LearnerMessage.learn, URI.create( "c:/1" ), new LearnerMessage.LearnState( new Object() ) )
                .setHeader( Message.HEADER_FROM, "c:/2" )
                .setHeader( Message.HEADER_CONVERSATION_ID, "conversation-id" )
                .setHeader( InstanceId.INSTANCE, "" + ( lastDelivered + LearnerContext.LEARN_GAP_THRESHOLD ) );

        // When
        // it receives a message with Paxos instance id at the threshold
        learner.handle( context, incomingInstance, mock( MessageHolder.class ) );

        // Then
        // it waits and doesn't deliver anything
        for ( PaxosInstance pendingInstance : pendingInstances )
        {
            assertFalse( pendingInstance.isState( PaxosInstance.State.delivered ) );
        }
        verify( context, times( 0 ) ).setLastDeliveredInstanceId( anyLong() );
    }
}
