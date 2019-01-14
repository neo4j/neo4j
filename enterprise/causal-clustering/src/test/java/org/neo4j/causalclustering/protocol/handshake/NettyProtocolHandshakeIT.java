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
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.neo4j.causalclustering.messaging.SimpleNettyChannel;
import org.neo4j.causalclustering.protocol.handshake.TestProtocols.TestApplicationProtocols;
import org.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols;
import org.neo4j.logging.NullLog;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocolCategory.CATCHUP;
import static org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocolCategory.RAFT;
import static org.neo4j.causalclustering.protocol.Protocol.ModifierProtocolCategory.COMPRESSION;

public class NettyProtocolHandshakeIT
{
    private ApplicationSupportedProtocols supportedRaftApplicationProtocol =
            new ApplicationSupportedProtocols( RAFT, emptyList() );
    private ApplicationSupportedProtocols supportedCatchupApplicationProtocol =
            new ApplicationSupportedProtocols( CATCHUP, emptyList() );
    private Collection<ModifierSupportedProtocols> supportedCompressionModifierProtocols =
            asList( new ModifierSupportedProtocols( COMPRESSION, TestModifierProtocols.listVersionsOf( COMPRESSION ) ) );
    private Collection<ModifierSupportedProtocols> noSupportedModifierProtocols = emptyList();

    private ApplicationProtocolRepository raftApplicationProtocolRepository =
            new ApplicationProtocolRepository( TestApplicationProtocols.values(), supportedRaftApplicationProtocol );
    private ApplicationProtocolRepository catchupApplicationProtocolRepository =
            new ApplicationProtocolRepository( TestApplicationProtocols.values(), supportedCatchupApplicationProtocol );
    private ModifierProtocolRepository compressionModifierProtocolRepository =
            new ModifierProtocolRepository( TestModifierProtocols.values(), supportedCompressionModifierProtocols );
    private ModifierProtocolRepository unsupportingModifierProtocolRepository =
            new ModifierProtocolRepository( TestModifierProtocols.values(), noSupportedModifierProtocols );

    private Server server;
    private HandshakeClient handshakeClient;
    private Client client;

    @Before
    public void setUp()
    {
        server = new Server();
        server.start( raftApplicationProtocolRepository, compressionModifierProtocolRepository );

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
    public void shouldSuccessfullyHandshakeKnownProtocolOnClientWithCompression() throws Exception
    {
        // when
        CompletableFuture<ProtocolStack> clientHandshakeFuture = handshakeClient.initiate(
                new SimpleNettyChannel( client.channel, NullLog.getInstance() ), raftApplicationProtocolRepository, compressionModifierProtocolRepository );

        // then
        ProtocolStack clientProtocolStack = clientHandshakeFuture.get( 1, TimeUnit.MINUTES );
        assertThat( clientProtocolStack.applicationProtocol(), equalTo( TestApplicationProtocols.latest( RAFT) ) );
        assertThat( clientProtocolStack.modifierProtocols(), contains( TestModifierProtocols.latest( COMPRESSION ) ) );
    }

    @Test
    public void shouldSuccessfullyHandshakeKnownProtocolOnServerWithCompression() throws Exception
    {
        // when
        CompletableFuture<ProtocolStack> clientFuture = handshakeClient.initiate(
                new SimpleNettyChannel( client.channel, NullLog.getInstance() ), raftApplicationProtocolRepository, compressionModifierProtocolRepository );
        CompletableFuture<ProtocolStack> serverHandshakeFuture = getServerHandshakeFuture( clientFuture );

        // then
        ProtocolStack serverProtocolStack = serverHandshakeFuture.get( 1, TimeUnit.MINUTES );
        assertThat( serverProtocolStack.applicationProtocol(), equalTo( TestApplicationProtocols.latest( RAFT ) ) );
        assertThat( serverProtocolStack.modifierProtocols(), contains( TestModifierProtocols.latest( COMPRESSION ) ) );
    }

    @Test
    public void shouldSuccessfullyHandshakeKnownProtocolOnClientNoModifiers() throws Exception
    {
        // when
        CompletableFuture<ProtocolStack> clientHandshakeFuture = handshakeClient.initiate(
                new SimpleNettyChannel( client.channel, NullLog.getInstance() ), raftApplicationProtocolRepository, unsupportingModifierProtocolRepository );

        // then
        ProtocolStack clientProtocolStack = clientHandshakeFuture.get( 1, TimeUnit.MINUTES );
        assertThat( clientProtocolStack.applicationProtocol(), equalTo( TestApplicationProtocols.latest( RAFT) ) );
        assertThat( clientProtocolStack.modifierProtocols(), empty() );
    }

    @Test
    public void shouldSuccessfullyHandshakeKnownProtocolOnServerNoModifiers() throws Exception
    {
        // when
        CompletableFuture<ProtocolStack> clientFuture = handshakeClient.initiate(
                new SimpleNettyChannel( client.channel, NullLog.getInstance() ), raftApplicationProtocolRepository, unsupportingModifierProtocolRepository );
        CompletableFuture<ProtocolStack> serverHandshakeFuture = getServerHandshakeFuture( clientFuture );

        // then
        ProtocolStack serverProtocolStack = serverHandshakeFuture.get( 1, TimeUnit.MINUTES );
        assertThat( serverProtocolStack.applicationProtocol(), equalTo( TestApplicationProtocols.latest( RAFT ) ) );
        assertThat( serverProtocolStack.modifierProtocols(), empty() );
    }

    @Test( expected = ClientHandshakeException.class )
    public void shouldFailHandshakeForUnknownProtocolOnClient() throws Throwable
    {
        // when
        CompletableFuture<ProtocolStack> clientHandshakeFuture = handshakeClient.initiate(
                new SimpleNettyChannel( client.channel, NullLog.getInstance() ), catchupApplicationProtocolRepository, compressionModifierProtocolRepository );

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
        CompletableFuture<ProtocolStack> clientFuture = handshakeClient.initiate(
                new SimpleNettyChannel( client.channel, NullLog.getInstance() ), catchupApplicationProtocolRepository, compressionModifierProtocolRepository );

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
        return clientFuture.handle( ( ignoreSuccess, ignoreFailure ) -> null ).thenCompose( ignored -> server.handshakeServer.protocolStackFuture() );
    }

    private static class Server
    {
        Channel channel;
        NioEventLoopGroup eventLoopGroup;
        HandshakeServer handshakeServer;

        void start( final ApplicationProtocolRepository applicationProtocolRepository, final ModifierProtocolRepository modifierProtocolRepository )
        {
            eventLoopGroup = new NioEventLoopGroup();
            ServerBootstrap bootstrap = new ServerBootstrap().group( eventLoopGroup )
                    .channel( NioServerSocketChannel.class )
                    .option( ChannelOption.SO_REUSEADDR, true )
                    .localAddress( 0 )
                    .childHandler( new ChannelInitializer<SocketChannel>()
                    {
                        @Override
                        protected void initChannel( SocketChannel ch )
                        {
                            ChannelPipeline pipeline = ch.pipeline();
                            handshakeServer = new HandshakeServer(
                                    applicationProtocolRepository, modifierProtocolRepository, new SimpleNettyChannel( ch, NullLog.getInstance() ) );
                            pipeline.addLast( "frameEncoder", new LengthFieldPrepender( 4 ) );
                            pipeline.addLast( "frameDecoder", new LengthFieldBasedFrameDecoder( Integer.MAX_VALUE, 0, 4, 0, 4 ) );
                            pipeline.addLast( "responseMessageEncoder", new ServerMessageEncoder() );
                            pipeline.addLast( "requestMessageDecoder", new ServerMessageDecoder() );
                            pipeline.addLast( new NettyHandshakeServer( handshakeServer ) );
                        }
                    } );

            channel = bootstrap.bind().syncUninterruptibly().channel();
        }

        void stop()
        {
            channel.close().awaitUninterruptibly();
            channel = null;
            eventLoopGroup.shutdownGracefully( 0, 0, SECONDS );
        }

        int port()
        {
            return ((InetSocketAddress) channel.localAddress()).getPort();
        }
    }

    private static class Client
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

    static class ClientInitializer extends ChannelInitializer<SocketChannel>
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
