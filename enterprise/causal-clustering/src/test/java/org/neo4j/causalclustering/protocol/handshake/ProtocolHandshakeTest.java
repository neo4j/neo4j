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
package org.neo4j.causalclustering.protocol.handshake;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.neo4j.causalclustering.messaging.Channel;
import org.neo4j.causalclustering.protocol.Protocol;
import org.neo4j.causalclustering.protocol.handshake.TestProtocols.TestApplicationProtocols;
import org.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class ProtocolHandshakeTest
{
    private HandshakeClient handshakeClient;
    private ProtocolRepository<Protocol.ApplicationProtocol> applicationProtocolRepository;
    private ProtocolRepository<Protocol.ModifierProtocol> modifierProtocolRepository;
    private HandshakeServer handshakeServer;
    private FakeChannelWrapper clientChannel;

    @Before
    public void setUp()
    {
        handshakeClient = new HandshakeClient();
        applicationProtocolRepository = new ProtocolRepository<>( TestApplicationProtocols.values() );
        modifierProtocolRepository = new ProtocolRepository<>( TestModifierProtocols.values() );
        handshakeServer = new HandshakeServer( new FakeClientChannel( handshakeClient ), applicationProtocolRepository, modifierProtocolRepository,
                Protocol.ApplicationProtocolIdentifier.RAFT );
        clientChannel = new FakeServerChannel( handshakeServer );
    }

    @Test
    public void shouldSuccessfullyHandshakeKnownProtocolOnClient() throws Exception
    {
        // when
        CompletableFuture<ProtocolStack> clientHandshakeFuture = handshakeClient.initiate(
                clientChannel, applicationProtocolRepository, Protocol.ApplicationProtocolIdentifier.RAFT,
                modifierProtocolRepository, asSet( Protocol.ModifierProtocolIdentifier.COMPRESSION ) );
        handshakeServer.protocolStackFuture();

        // then
        assertFalse( clientChannel.isClosed() );
        ProtocolStack clientProtocolStack = clientHandshakeFuture.get( 1, TimeUnit.SECONDS );
        assertThat( clientProtocolStack.applicationProtocol(), equalTo( TestApplicationProtocols.latest( Protocol.ApplicationProtocolIdentifier.RAFT ) ) );
        assertThat( clientProtocolStack.modifierProtocols(), contains( TestModifierProtocols.latest( Protocol.ModifierProtocolIdentifier.COMPRESSION ) ) );
    }

    @Test
    public void shouldSuccessfullyHandshakeKnownProtocolOnServer() throws Exception
    {
        // when
        handshakeClient.initiate(
                clientChannel, applicationProtocolRepository, Protocol.ApplicationProtocolIdentifier.RAFT,
                modifierProtocolRepository, asSet( Protocol.ModifierProtocolIdentifier.COMPRESSION ) );
        CompletableFuture<ProtocolStack> serverHandshakeFuture = handshakeServer.protocolStackFuture();

        // then
        assertFalse( clientChannel.isClosed() );
        ProtocolStack serverProtocolStack = serverHandshakeFuture.get( 1, TimeUnit.SECONDS );
        assertThat( serverProtocolStack.applicationProtocol(), equalTo( TestApplicationProtocols.latest( Protocol.ApplicationProtocolIdentifier.RAFT ) ) );
        assertThat( serverProtocolStack.modifierProtocols(), contains( TestModifierProtocols.latest( Protocol.ModifierProtocolIdentifier.COMPRESSION ) ) );
    }

    @Test( expected = ClientHandshakeException.class )
    public void shouldFailHandshakeForUnknownProtocolOnClient() throws Throwable
    {
        // when
        applicationProtocolRepository = new ProtocolRepository<>( new Protocol.ApplicationProtocol[]{Protocol.ApplicationProtocols.RAFT_1} );
        CompletableFuture<ProtocolStack> clientHandshakeFuture = handshakeClient.initiate(
                clientChannel, applicationProtocolRepository, Protocol.ApplicationProtocolIdentifier.CATCHUP,
                modifierProtocolRepository, asSet( Protocol.ModifierProtocolIdentifier.COMPRESSION ) );

        // then
        try
        {
            clientHandshakeFuture.get( 1, TimeUnit.SECONDS );
        }
        catch ( ExecutionException ex )
        {
            throw ex.getCause();
        }
    }

    @Test( expected = ServerHandshakeException.class )
    public void shouldFailHandshakeForUnknownProtocolOnServer() throws Throwable
    {
        // when
        applicationProtocolRepository = new ProtocolRepository<>( new Protocol.ApplicationProtocol[]{Protocol.ApplicationProtocols.RAFT_1} );
        handshakeServer = new HandshakeServer(
                new FakeClientChannel( handshakeClient ), applicationProtocolRepository, modifierProtocolRepository, Protocol.ApplicationProtocolIdentifier.RAFT
        );
        clientChannel = new FakeServerChannel( handshakeServer );
        handshakeClient.initiate(
                clientChannel, applicationProtocolRepository, Protocol.ApplicationProtocolIdentifier.CATCHUP,
                modifierProtocolRepository, asSet( Protocol.ModifierProtocolIdentifier.COMPRESSION ) );
        CompletableFuture<ProtocolStack> serverHandshakeFuture = handshakeServer.protocolStackFuture();

        // then
        try
        {
            serverHandshakeFuture.get( 1, TimeUnit.SECONDS );
        }
        catch ( ExecutionException ex )
        {
            throw ex.getCause();
        }
    }

    abstract class FakeChannelWrapper implements Channel
    {
        private boolean closed;

        public boolean isDisposed()
        {
            return closed;
        }

        public void dispose()
        {
            closed = true;
        }

        @Override
        public boolean isOpen()
        {
            return true;
        }

        public abstract CompletableFuture<Void> write( Object msg );

        public CompletableFuture<Void> writeAndFlush( Object msg )
        {
            return write( msg );
        }

        boolean isClosed()
        {
            return closed;
        }
    }

    private class FakeClientChannel extends FakeChannelWrapper
    {
        private final HandshakeClient handshakeClient;

        FakeClientChannel( HandshakeClient handshakeClient )
        {
            super();
            this.handshakeClient = handshakeClient;
        }

        @Override
        public CompletableFuture<Void> write( Object msg )
        {
            ((ClientMessage) msg).dispatch( handshakeClient );
            return CompletableFuture.completedFuture( null );
        }
    }

    private class FakeServerChannel extends FakeChannelWrapper
    {
        private final HandshakeServer handshakeServer;

        FakeServerChannel( HandshakeServer handshakeServer )
        {
            super();
            this.handshakeServer = handshakeServer;
        }

        @Override
        public CompletableFuture<Void> write( Object msg )
        {
            ((ServerMessage) msg).dispatch( handshakeServer );
            return CompletableFuture.completedFuture( null );
        }
    }
}
