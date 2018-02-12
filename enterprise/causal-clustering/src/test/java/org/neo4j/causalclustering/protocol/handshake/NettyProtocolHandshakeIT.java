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

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.neo4j.causalclustering.messaging.SimpleNettyChannel;
import org.neo4j.causalclustering.protocol.Protocol;
import org.neo4j.logging.NullLog;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class NettyProtocolHandshakeIT
{
    private ProtocolRepository protocolRepository = new ProtocolRepository( TestProtocols.values() );

    private Server server;
    private HandshakeClient handshakeClient;
    private Client client;
    private HandshakeServer handshakeServer;

    @Before
    public void setUp()
    {
        server = new Server();
        server.start();

        handshakeClient = new HandshakeClient();

        client = new Client( handshakeClient );
        client.connect( server.port() );
    }

    @After
    public void tearDown()
    {
        client.disconnect();
        server.stop();
    }

    @Test
    public void shouldSuccessfullyHandshakeKnownProtocolOnClient() throws Exception
    {
        // when
        CompletableFuture<ProtocolStack> clientHandshakeFuture =
                handshakeClient.initiate( new SimpleNettyChannel( client.channel, NullLog.getInstance() ), protocolRepository, Protocol.Identifier.RAFT );

        // then
        ProtocolStack clientProtocolStack = clientHandshakeFuture.get( 1, TimeUnit.MINUTES );
        assertThat( clientProtocolStack.applicationProtocol(), equalTo( TestProtocols.RAFT_LATEST ) );
    }

    @Test
    public void shouldSuccessfullyHandshakeKnownProtocolOnServer() throws Exception
    {
        // when
        CompletableFuture<ProtocolStack> clientFuture =
                handshakeClient.initiate( new SimpleNettyChannel( client.channel, NullLog.getInstance() ), protocolRepository, Protocol.Identifier.RAFT );
        CompletableFuture<ProtocolStack> serverHandshakeFuture = getServerHandshakeFuture( clientFuture );

        // then
        ProtocolStack serverProtocolStack = serverHandshakeFuture.get( 1, TimeUnit.MINUTES );
        assertThat( serverProtocolStack.applicationProtocol(), equalTo( TestProtocols.RAFT_LATEST ) );
    }

    @Test( expected = ClientHandshakeException.class )
    public void shouldFailHandshakeForUnknownProtocolOnClient() throws Throwable
    {
        // when
        protocolRepository = new ProtocolRepository( new Protocol[]{TestProtocols.Protocols.RAFT_1} );
        CompletableFuture<ProtocolStack> clientHandshakeFuture =
                handshakeClient.initiate( new SimpleNettyChannel( client.channel, NullLog.getInstance() ), protocolRepository, Protocol.Identifier.CATCHUP );

        // then
        try
        {
            clientHandshakeFuture.get( 1, TimeUnit.MINUTES );
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
        CompletableFuture<ProtocolStack> clientFuture =
                handshakeClient.initiate( new SimpleNettyChannel( client.channel, NullLog.getInstance() ), protocolRepository, Protocol.Identifier.CATCHUP );

        CompletableFuture<ProtocolStack> serverHandshakeFuture = getServerHandshakeFuture( clientFuture );

        // then
        try
        {
            serverHandshakeFuture.get( 1, TimeUnit.MINUTES );
        }
        catch ( ExecutionException ex )
        {
            throw ex.getCause();
        }
    }

    /**
     * Only attempt to access handshakeServer when client has completed, and do so whether client has completed normally or exceptionally
     * This is to avoid NullPointerException if handshakeServer accessed too soon
     */
    private CompletableFuture<ProtocolStack> getServerHandshakeFuture( CompletableFuture<ProtocolStack> clientFuture )
    {
        return clientFuture
                .handle( ( ignoreSuccess, ignoreFailure ) -> null )
                .thenCompose( ignored -> handshakeServer.protocolStackFuture() );
    }

    private class Server
    {
        Channel channel;
        NioEventLoopGroup eventLoopGroup;

        private void start()
        {
            eventLoopGroup = new NioEventLoopGroup();
            ServerBootstrap bootstrap = new ServerBootstrap().group( eventLoopGroup ).channel( NioServerSocketChannel.class ).option(
                    ChannelOption.SO_REUSEADDR, true ).localAddress( 0 ).childHandler( new ChannelInitializer<SocketChannel>()
            {
                @Override
                protected void initChannel( SocketChannel ch )
                {
                    ChannelPipeline pipeline = ch.pipeline();
                    pipeline.addLast( "frameEncoder", new LengthFieldPrepender( 4 ) );
                    pipeline.addLast( "frameDecoder", new LengthFieldBasedFrameDecoder( Integer.MAX_VALUE, 0, 4, 0, 4 ) );
                    pipeline.addLast( "responseMessageEncoder", new ServerMessageEncoder() );
                    pipeline.addLast( "requestMessageDecoder", new ServerMessageDecoder() );
                    handshakeServer = new HandshakeServer( new SimpleNettyChannel( ch, NullLog.getInstance() ), protocolRepository, Protocol.Identifier.RAFT );
                    pipeline.addLast( new NettyHandshakeServer( handshakeServer ) );
                }
            } );

            channel = bootstrap.bind().syncUninterruptibly().channel();
        }

        private void stop()
        {
            channel.close().awaitUninterruptibly();
            channel = null;
            eventLoopGroup.shutdownGracefully( 0, 0, SECONDS );
        }

        private int port()
        {
            return ((InetSocketAddress) channel.localAddress()).getPort();
        }
    }

    private class Client
    {
        Bootstrap bootstrap;
        NioEventLoopGroup eventLoopGroup;
        Channel channel;

        Client( HandshakeClient handshakeClient )
        {
            eventLoopGroup = new NioEventLoopGroup();
            bootstrap = new Bootstrap().group( eventLoopGroup ).channel( NioSocketChannel.class ).handler( new ClientInitializer( handshakeClient ) );
        }

        @SuppressWarnings( "SameParameterValue" )
        void connect( int port )
        {
            ChannelFuture channelFuture = bootstrap.connect( "localhost", port ).awaitUninterruptibly();
            channel = channelFuture.channel();
        }

        void disconnect()
        {
            if ( channel != null )
            {
                channel.close().awaitUninterruptibly();
                eventLoopGroup.shutdownGracefully( 0, 0, SECONDS );
            }
        }
    }

    public class ClientInitializer extends ChannelInitializer<SocketChannel>
    {
        private final HandshakeClient handshakeClient;

        ClientInitializer( HandshakeClient handshakeClient )
        {
            this.handshakeClient = handshakeClient;
        }

        @Override
        protected void initChannel( SocketChannel channel )
        {
            ChannelPipeline pipeline = channel.pipeline();
            pipeline.addLast( "frameEncoder", new LengthFieldPrepender( 4 ) );
            pipeline.addLast( "frameDecoder", new LengthFieldBasedFrameDecoder( Integer.MAX_VALUE, 0, 4, 0, 4 ) );
            pipeline.addLast( "requestMessageEncoder", new ClientMessageEncoder() );
            pipeline.addLast( "responseMessageDecoder", new ClientMessageDecoder() );
            pipeline.addLast( new NettyHandshakeClient( handshakeClient ) );
        }
    }
}
