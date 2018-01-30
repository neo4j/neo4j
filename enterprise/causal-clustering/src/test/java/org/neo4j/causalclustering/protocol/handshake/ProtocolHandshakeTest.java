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

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class ProtocolHandshakeTest
{
    private HandshakeClient handshakeClient;
    private ProtocolRepository protocolRepository;
    private HandshakeServer handshakeServer;
    private FakeChannelWrapper clientChannel;

    @Before
    public void setUp()
    {
        handshakeClient = new HandshakeClient();
        protocolRepository = new ProtocolRepository( TestProtocols.values() );
        handshakeServer = new HandshakeServer( new FakeClientChannel( handshakeClient ), protocolRepository, Protocol.Protocols.Identifier.RAFT );
        clientChannel = new FakeServerChannel( handshakeServer );
    }

    @Test
    public void shouldSuccessfullyHandshakeKnownProtocolOnClient() throws Exception
    {
        // when
        CompletableFuture<ProtocolStack> clientHandshakeFuture = handshakeClient.initiate( clientChannel, protocolRepository, TestProtocols.Identifier.RAFT );
        handshakeServer.protocolStackFuture();

        // then
        assertFalse( clientChannel.isClosed() );
        Protocol clientProtocol = clientHandshakeFuture.get( 1, TimeUnit.SECONDS ).applicationProtocol();
        assertThat( clientProtocol, equalTo( TestProtocols.RAFT_3 ) );
    }

    @Test
    public void shouldSuccessfullyHandshakeKnownProtocolOnServer() throws Exception
    {
        // when
        handshakeClient.initiate( clientChannel, protocolRepository, TestProtocols.Identifier.RAFT );
        CompletableFuture<ProtocolStack> serverHandshakeFuture = handshakeServer.protocolStackFuture();

        // then
        assertFalse( clientChannel.isClosed() );
        Protocol serverProtocol = serverHandshakeFuture.get( 1, TimeUnit.SECONDS ).applicationProtocol();
        assertThat( serverProtocol, equalTo( TestProtocols.RAFT_3 ) );
    }

    @Test( expected = ClientHandshakeException.class )
    public void shouldFailHandshakeForUnknownProtocolOnClient() throws Throwable
    {
        // when
        protocolRepository = new ProtocolRepository( new Protocol[]{TestProtocols.Protocols.RAFT_1} );
        CompletableFuture<ProtocolStack> clientHandshakeFuture =
                handshakeClient.initiate( clientChannel, protocolRepository, TestProtocols.Identifier.CATCHUP );

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
        protocolRepository = new ProtocolRepository( new Protocol[]{TestProtocols.Protocols.RAFT_1} );
        handshakeServer = new HandshakeServer( new FakeClientChannel( handshakeClient ), protocolRepository, Protocol.Protocols.Identifier.RAFT );
        clientChannel = new FakeServerChannel( handshakeServer );
        handshakeClient.initiate( clientChannel, protocolRepository, Protocol.Identifier.CATCHUP );
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
