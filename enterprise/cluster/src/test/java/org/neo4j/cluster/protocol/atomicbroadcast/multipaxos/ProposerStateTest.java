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

import java.io.Serializable;
import java.net.URI;

import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.com.message.MessageType;
import org.neo4j.cluster.com.message.TrackingMessageHolder;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.PaxosInstance.State;
import org.neo4j.cluster.protocol.MessageArgumentMatcher;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.logging.NullLog;

import static java.lang.Integer.parseInt;
import static java.net.URI.create;
import static java.util.Arrays.asList;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.neo4j.cluster.com.message.Message.to;
import static org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId.INSTANCE;
import static org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.ProposerMessage.phase1Timeout;
import static org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.ProposerMessage.promise;
import static org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.ProposerMessage.propose;
import static org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.ProposerMessage.rejectAccept;

public class ProposerStateTest
{
    @SuppressWarnings( { "unchecked", "rawtypes" } )
    @Test
    public void ifProposingWithClosedInstanceThenRetryWithNextInstance() throws Throwable
    {
        ProposerContext context = Mockito.mock(ProposerContext.class);
        when(context.getLog( any( Class.class ) )).thenReturn( NullLog.getInstance() );

        org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId instanceId = new org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId( 42 );
        PaxosInstanceStore paxosInstanceStore = new PaxosInstanceStore();

        // The instance is closed
        PaxosInstance paxosInstance = new PaxosInstance( paxosInstanceStore, instanceId ); // the instance
        paxosInstance.closed( instanceId, "1/15#" ); // is closed for that conversation, not really important
        when( context.unbookInstance( instanceId ) ).thenReturn( Message.internal( ProposerMessage.accepted, "the closed payload" ) );

        when( context.getPaxosInstance( instanceId ) ).thenReturn( paxosInstance ); // required for

        // But in the meantime it was reused and has now (of course) timed out
        String theTimedoutPayload = "the timed out payload";
        Message message = Message.internal( ProposerMessage.phase1Timeout, theTimedoutPayload );
        message.setHeader( org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId.INSTANCE, instanceId.toString() );

        // Handle it
        MessageHolder mockHolder = mock( MessageHolder.class );
        ProposerState.proposer.handle( context, message, mockHolder );

        // Verify it was resent as a propose with the same value
        verify( mockHolder, times(1) ).offer(
                Matchers.<Message<? extends MessageType>>argThat(
                        new MessageArgumentMatcher().onMessageType( ProposerMessage.propose ).withPayload( theTimedoutPayload )
                ) );
        verify( context, times(1) ).unbookInstance( instanceId );
    }

    @Test
    public void something() throws Throwable
    {
        Object acceptorValue = new Object();
        Object bookedValue = new Object();

        org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId instanceId = new org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId( 42 );

        PaxosInstanceStore paxosInstanceStore = new PaxosInstanceStore();

        ProposerContext context = Mockito.mock(ProposerContext.class);
        when(context.getPaxosInstance( instanceId )).thenReturn( paxosInstanceStore.getPaxosInstance( instanceId ) );
        when(context.getMinimumQuorumSize( Mockito.anyList() )).thenReturn( 2 );

        // The instance is closed
        PaxosInstance paxosInstance = new PaxosInstance( paxosInstanceStore, instanceId ); // the instance
        paxosInstance.propose( 2001, Iterables.toList(
                Iterables.<URI, URI>iterable( create( "http://something1" ), create( "http://something2" ),
                        create( "http://something3" ) ) ) );

        Message message = Message.to( ProposerMessage.promise, create( "http://something1" ), new ProposerMessage.PromiseState( 2001, acceptorValue ) );
        message.setHeader( org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId.INSTANCE, instanceId.toString() );

        MessageHolder mockHolder = mock( MessageHolder.class );
        ProposerState.proposer.handle(context, message, mockHolder);



    }

    @Test
    public void proposer_proposePhase1TimeoutShouldCarryOnPayload() throws Throwable
    {
        // GIVEN
        PaxosInstance instance = mock( PaxosInstance.class );
        ProposerContext context = mock( ProposerContext.class );
        when( context.getPaxosInstance( any( org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId.class ) ) ).thenReturn( instance );
        when( context.getMyId() ).thenReturn( new org.neo4j.cluster.InstanceId( 0 ) );
        TrackingMessageHolder outgoing = new TrackingMessageHolder();
        String instanceId = "1";
        Serializable payload = "myPayload";
        Message<ProposerMessage> message = to( propose, create( "http://something" ), payload )
                .setHeader( INSTANCE, instanceId );

        // WHEN
        ProposerState.proposer.handle( context, message, outgoing );

        // THEN
        verify( context ).setTimeout( eq( new org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId( instanceId ) ),
                argThat( new MessageArgumentMatcher<>().withPayload( payload ) ) );
    }

    @Test
    public void proposer_phase1TimeoutShouldCarryOnPayload() throws Throwable
    {
        // GIVEN
        PaxosInstance instance = mock( PaxosInstance.class );
        when( instance.isState( State.p1_pending ) ).thenReturn( true );
        ProposerContext context = mock( ProposerContext.class );
        when( context.getPaxosInstance( any( org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId.class ) ) ).thenReturn( instance );
        TrackingMessageHolder outgoing = new TrackingMessageHolder();
        String instanceId = "1";
        Serializable payload = "myPayload";
        Message<ProposerMessage> message = to( phase1Timeout, create( "http://something" ), payload )
                .setHeader( INSTANCE, instanceId );

        // WHEN
        ProposerState.proposer.handle( context, message, outgoing );

        // THEN
        verify( context ).setTimeout( eq( new org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId( instanceId ) ),
                argThat( new MessageArgumentMatcher<>().withPayload( payload ) ) );
    }

    @Test
    public void proposer_rejectAcceptShouldCarryOnPayload() throws Throwable
    {
        // GIVEN
        String instanceId = "1";
        PaxosInstance instance = new PaxosInstance( mock( PaxosInstanceStore.class ), new org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId( instanceId ) );
        Serializable payload = "myPayload";
        instance.propose( 1, asList( create( "http://some-guy" ) ) );
        instance.ready( payload, true );
        instance.pending();
        ProposerContext context = mock( ProposerContext.class );
        when( context.getLog( any( Class.class ) ) ).thenReturn( NullLog.getInstance() );
        when( context.getPaxosInstance( any( org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId.class ) ) ).thenReturn( instance );
        when( context.getMyId() ).thenReturn( new org.neo4j.cluster.InstanceId( parseInt( instanceId ) ) );
        TrackingMessageHolder outgoing = new TrackingMessageHolder();
        Message<ProposerMessage> message = to( rejectAccept, create( "http://something" ),
                new ProposerMessage.RejectAcceptState() )
                .setHeader( INSTANCE, instanceId );

        // WHEN
        ProposerState.proposer.handle( context, message, outgoing );

        // THEN
        verify( context ).setTimeout( eq( new org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId( instanceId ) ),
                argThat( new MessageArgumentMatcher<>().withPayload( payload ) ) );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void proposer_promiseShouldCarryOnPayloadToPhase2Timeout() throws Throwable
    {
        // GIVEN
        String instanceId = "1";
        Serializable payload = "myPayload";
        PaxosInstance instance = new PaxosInstance( mock( PaxosInstanceStore.class ), new org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId( instanceId ) );
        instance.propose( 1, asList( create( "http://some-guy" ) ) );
        instance.value_2 = payload; // don't blame me for making it package access.
        ProposerContext context = mock( ProposerContext.class );
        when( context.getPaxosInstance( any( org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId.class ) ) ).thenReturn( instance );
        when( context.getMinimumQuorumSize( anyList() ) ).thenReturn( 1 );
        TrackingMessageHolder outgoing = new TrackingMessageHolder();
        Message<ProposerMessage> message = to( promise, create( "http://something" ),
                new ProposerMessage.PromiseState( 1, payload ) ).setHeader( INSTANCE, instanceId );

        // WHEN
        ProposerState.proposer.handle( context, message, outgoing );

        // THEN
        verify( context ).setTimeout( eq( new org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId( instanceId ) ),
                argThat( new MessageArgumentMatcher<>().withPayload( payload ) ) );
    }
}
