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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.neo4j.causalclustering.messaging.Channel;
import org.neo4j.causalclustering.protocol.Protocol;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.causalclustering.protocol.Protocol.Identifier.CATCHUP;
import static org.neo4j.causalclustering.protocol.Protocol.Identifier.RAFT;
import static org.neo4j.causalclustering.protocol.Protocol.Protocols.RAFT_1;
import static org.neo4j.causalclustering.protocol.handshake.TestProtocols.RAFT_3;

public class ProtocolHandshakeTest
{
    private HandshakeClient handshakeClient;
    private ProtocolRepository protocolRepository;
    private HandshakeServer handshakeServer;
    private FakeChannelWrapper clientChannel;

    @BeforeEach
    public void setUp()
    {
        handshakeClient = new HandshakeClient();
        protocolRepository = new ProtocolRepository( TestProtocols.values() );
        handshakeServer = new HandshakeServer( new FakeClientChannel( handshakeClient ), protocolRepository, RAFT );
        clientChannel = new FakeServerChannel( handshakeServer );
    }

    @Test
    public void shouldSuccessfullyHandshakeKnownProtocolOnClient() throws Exception
    {
        // when
        CompletableFuture<ProtocolStack> clientHandshakeFuture = handshakeClient.initiate( clientChannel, protocolRepository, RAFT );
        handshakeServer.protocolStackFuture();

        // then
        assertFalse( clientChannel.isClosed() );
        Protocol clientProtocol = clientHandshakeFuture.get( 1, TimeUnit.SECONDS ).applicationProtocol();
        assertThat( clientProtocol, equalTo( TestProtocols.RAFT_LATEST ) );
    }

    @Test
    public void shouldSuccessfullyHandshakeKnownProtocolOnServer() throws Exception
    {
        // when
        handshakeClient.initiate( clientChannel, protocolRepository, RAFT );
        CompletableFuture<ProtocolStack> serverHandshakeFuture = handshakeServer.protocolStackFuture();

        // then
        assertFalse( clientChannel.isClosed() );
        Protocol serverProtocol = serverHandshakeFuture.get( 1, TimeUnit.SECONDS ).applicationProtocol();
        assertThat( serverProtocol, equalTo( TestProtocols.RAFT_LATEST ) );
    }

    @Test
    public void shouldFailHandshakeForUnknownProtocolOnClient()
    {
        assertThrows( ClientHandshakeException.class, () -> {
            // when
            protocolRepository = new ProtocolRepository( new Protocol[]{RAFT_1} );
            CompletableFuture<ProtocolStack> clientHandshakeFuture =
                    handshakeClient.initiate( clientChannel, protocolRepository, CATCHUP );

            // then
            try
            {
                clientHandshakeFuture.get( 1, SECONDS );
            }
            catch ( ExecutionException ex )
            {
                throw ex.getCause();
            }
        } );
    }

    @Test
    public void shouldFailHandshakeForUnknownProtocolOnServer()
    {
        assertThrows( ServerHandshakeException.class, () -> {
            // when
            protocolRepository = new ProtocolRepository( new Protocol[]{RAFT_1} );
            handshakeServer = new HandshakeServer( new FakeClientChannel( handshakeClient ), protocolRepository, RAFT );
            clientChannel = new FakeServerChannel( handshakeServer );
            handshakeClient.initiate( clientChannel, protocolRepository, CATCHUP );
            CompletableFuture<ProtocolStack> serverHandshakeFuture = handshakeServer.protocolStackFuture();

            // then
            try
            {
                serverHandshakeFuture.get( 1, SECONDS );
            }
            catch ( ExecutionException ex )
            {
                throw ex.getCause();
            }
        } );
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
            return completedFuture( null );
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
            return completedFuture( null );
        }
    }
}
