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
package org.neo4j.cluster.protocol.omega;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.protocol.omega.payload.CollectPayload;
import org.neo4j.cluster.protocol.omega.payload.CollectResponsePayload;
import org.neo4j.cluster.protocol.omega.payload.RefreshAckPayload;
import org.neo4j.cluster.protocol.omega.payload.RefreshPayload;
import org.neo4j.cluster.protocol.omega.state.EpochNumber;
import org.neo4j.cluster.protocol.omega.state.State;
import org.neo4j.cluster.protocol.omega.state.View;

public class OmegaStateTest
{
    @Test
    public void testStartTransition() throws Throwable
    {
        OmegaContext context = Mockito.mock( OmegaContext.class );
        Message<OmegaMessage> message = Message.internal( OmegaMessage.start );
        MessageHolder outgoing = Mockito.mock( MessageHolder.class );
        OmegaState result = (OmegaState) OmegaState.start.handle( context, message, outgoing );
        // Assert we move to operational state
        assertEquals( OmegaState.omega, result );
        // And that timers started
        Mockito.verify( context ).startTimers();
    }

    @Test
    public void testRefreshTimeoutResponse() throws Throwable
    {
        OmegaContext context = Mockito.mock( OmegaContext.class );
        Message<OmegaMessage> message = Message.internal( OmegaMessage.refresh_timeout );
        MessageHolder outgoing = Mockito.mock( MessageHolder.class );

        State state = new State( new EpochNumber() );
        Mockito.when( context.getMyState() ).thenReturn( state );

        Set<URI> servers = new HashSet<URI>();
        servers.add( new URI( "localhost:80" ) );
        servers.add( new URI( "localhost:81" ) );
        Mockito.when( context.getServers() ).thenReturn( (Collection) servers );

        OmegaState result = (OmegaState) OmegaState.omega.handle( context, message, outgoing );

        assertEquals( OmegaState.omega, result );
        Mockito.verify( context ).getServers();
        Mockito.verify( outgoing, Mockito.times( servers.size() ) ).offer( Matchers.isA( Message.class ) );
        Mockito.verify( context ).startRefreshRound();
    }

    @Test
    public void testRefreshSuccess() throws Throwable
    {
        OmegaContext context = Mockito.mock( OmegaContext.class );
        Message<OmegaMessage> message = Message.internal( OmegaMessage.refresh_ack, RefreshAckPayload.forRefresh( new
                RefreshPayload( 1, 2, 3, 1 ) ) );
        MessageHolder outgoing = Mockito.mock( MessageHolder.class );

        Mockito.when( context.getClusterNodeCount() ).thenReturn( 3 );
        Mockito.when( context.getAckCount( 1 ) ).thenReturn( 2 );

        State state = new State( new EpochNumber() );
        Mockito.when( context.getMyState() ).thenReturn( state );

        OmegaState.omega.handle( context, message, outgoing );

        Mockito.verify( context ).roundDone( 1 );
        assertEquals( 1, state.getFreshness() );
    }

    @Test
    public void testRoundTripTimeoutAkaAdvanceEpoch() throws Throwable
    {
        OmegaContext context = Mockito.mock( OmegaContext.class );
        Message<OmegaMessage> message = Message.internal( OmegaMessage.round_trip_timeout );
        MessageHolder outgoing = Mockito.mock( MessageHolder.class );

        State state = new State( new EpochNumber() );
        Mockito.when( context.getMyState() ).thenReturn( state );

        View myView = new View( state );
        Mockito.when( context.getMyView() ).thenReturn( myView );

        OmegaState result = (OmegaState) OmegaState.omega.handle( context, message, outgoing );

        assertEquals( OmegaState.omega, result );
        Mockito.verify( context ).getMyState();
        Mockito.verify( context ).getMyView();
        Mockito.verify( context, Mockito.never() ).roundDone( Matchers.anyInt() );
        assertTrue( myView.isExpired() );

        // Most important things to test - no update on freshness and serial num incremented
        assertEquals( 1, state.getEpochNum().getSerialNum() );
        assertEquals( 0, state.getFreshness() );

    }

    private static final String fromString = "neo4j://from";

    private void testRefreshResponseOnState( boolean newer ) throws Throwable
    {
        OmegaContext context = Mockito.mock( OmegaContext.class );
        Message<OmegaMessage> message = Mockito.mock( Message.class );
        MessageHolder outgoing = Mockito.mock( MessageHolder.class );

        // Value here is not important, we override the compareTo() method anyway
        RefreshPayload payload = new RefreshPayload( 1, 1, 1, 1 );

        Mockito.when( message.getHeader( Message.FROM ) ).thenReturn( fromString );
        Mockito.when( message.getPayload() ).thenReturn( payload );
        Mockito.when( message.getMessageType() ).thenReturn( OmegaMessage.refresh );

        URI fromURI = new URI( fromString );

        Map<URI, State> registry = Mockito.mock( Map.class );
        State fromState = Mockito.mock( State.class );
        Mockito.when( registry.get( fromURI ) ).thenReturn( fromState );
        Mockito.when( context.getRegistry() ).thenReturn( registry );
        if ( newer )
        {
            Mockito.when( fromState.compareTo( Matchers.any( State.class ) ) ).thenReturn( -1 );
        }
        else
        {
            Mockito.when( fromState.compareTo( Matchers.any( State.class ) ) ).thenReturn( 1 );
        }

        OmegaState.omega.handle( context, message, outgoing );

        Mockito.verify( context, Mockito.atLeastOnce() ).getRegistry();
        Mockito.verify( registry ).get( fromURI );
        Mockito.verify( fromState ).compareTo( Matchers.isA( State.class ) ); // existing compared to the one from the message
        if ( newer )
        {
            Mockito.verify( registry ).put( Matchers.eq( fromURI ), Matchers.isA( State.class ) );
        }
        else
        {
            Mockito.verify( registry, Mockito.never() ).put( Matchers.eq( fromURI ), Matchers.isA( State.class ) );
        }

        Mockito.verify( outgoing ).offer( Matchers.argThat( new MessageArgumentMatcher<OmegaMessage>().to( fromURI
        ).onMessageType(
                OmegaMessage.refresh_ack ) ) );
    }

    @Test
    public void testRefreshResponseOnOlderState() throws Throwable
    {
        testRefreshResponseOnState( false );
    }

    @Test
    public void testRefreshResponseOnNewerState() throws Throwable
    {
        testRefreshResponseOnState( true );
    }

    @Test
    public void testCollectRoundStartsOnReadTimeout() throws Throwable
    {
        OmegaContext context = Mockito.mock( OmegaContext.class );
        Message<OmegaMessage> message = Mockito.mock( Message.class );
        MessageHolder outgoing = Mockito.mock( MessageHolder.class );

        Set<URI> servers = new HashSet<URI>();
        servers.add( new URI( "localhost:80" ) );
        servers.add( new URI( "localhost:81" ) );
        servers.add( new URI( "localhost:82" ) );

        Mockito.when( context.getServers() ).thenReturn( (Collection) servers );
        Mockito.when( message.getMessageType() ).thenReturn( OmegaMessage.read_timeout );
        Mockito.when( context.getMyProcessId() ).thenReturn( 1 );

        OmegaState.omega.handle( context, message, outgoing );

        Mockito.verify( context, Mockito.atLeastOnce() ).getServers();
        Mockito.verify( context ).startCollectionRound();
        for ( URI server : servers )
        {
            Mockito.verify( outgoing ).offer( Matchers.argThat( new MessageArgumentMatcher<OmegaMessage>().to(
                    server )
                    .onMessageType( OmegaMessage.collect ).withPayload( new CollectPayload( 0 ) ) ) );
        }
    }

    @Test
    public void testResponseOnCollectRequest() throws Throwable
    {
        OmegaContext context = Mockito.mock( OmegaContext.class );
        Message<OmegaMessage> message = Mockito.mock( Message.class );
        MessageHolder outgoing = Mockito.mock( MessageHolder.class );

        Map<URI, State> dummyState = new HashMap<URI, State>();
        Mockito.when( context.getRegistry() ).thenReturn( dummyState );

        Mockito.when( message.getHeader( Message.FROM ) ).thenReturn( fromString );
        Mockito.when( message.getPayload() ).thenReturn( new CollectPayload( 1 ) );
        Mockito.when( message.getMessageType() ).thenReturn( OmegaMessage.collect );

        OmegaState.omega.handle( context, message, outgoing );

        Mockito.verify( context ).getRegistry();
        Mockito.verify( outgoing ).offer( Matchers.argThat( new MessageArgumentMatcher<OmegaMessage>().to( new URI(
                fromString ) )
                .onMessageType( OmegaMessage.status ).withPayload( new CollectResponsePayload( new URI[]{},
                        new RefreshPayload[]{}, 1 ) ) ) );
    }


    private void testStatusResponseHandling( boolean done ) throws Throwable
    {
        OmegaContext context = Mockito.mock( OmegaContext.class );
        Message<OmegaMessage> message = Mockito.mock( Message.class );
        MessageHolder outgoing = Mockito.mock( MessageHolder.class );

        URI fromUri = new URI( fromString );

        Map<URI, State> thePayloadContents = new HashMap<URI, State>();
        thePayloadContents.put( fromUri, new State( new EpochNumber( 1, 1 ), 1 ) );
        CollectResponsePayload thePayload = CollectResponsePayload.fromRegistry( thePayloadContents, 3 /*== readNum*/ );

        Mockito.when( message.getHeader( Message.FROM ) ).thenReturn( fromString );
        Mockito.when( message.getPayload() ).thenReturn( thePayload );
        Mockito.when( message.getMessageType() ).thenReturn( OmegaMessage.status );

        Mockito.when( context.getViews() ).thenReturn( new HashMap<URI, View>() );
        Mockito.when( context.getStatusResponsesForRound( 3 ) ).thenReturn( done ? 3 : 1 ); // less than half, not done
        Mockito.when( context.getClusterNodeCount() ).thenReturn( 5 );

        OmegaState.omega.handle( context, message, outgoing );

        Mockito.verify( context ).responseReceivedForRound( 3, fromUri, thePayloadContents );
        Mockito.verify( context ).getStatusResponsesForRound( 3 /*== readNum*/ );
        Mockito.verify( context ).getClusterNodeCount();
        if ( done )
        {
            Mockito.verify( context ).collectionRoundDone( 3 );
        }
        Mockito.verifyNoMoreInteractions( context );
        // Receiving status response sends no messages anywhere, just alters context state
        Mockito.verifyZeroInteractions( outgoing );
    }

    @Test
    public void testStatusResponseHandlingRoundNotDone() throws Throwable
    {
        testStatusResponseHandling( false );
    }

    @Test
    public void testStatusResponseHandlingRoundDone() throws Throwable
    {
        testStatusResponseHandling( true );
    }
}
