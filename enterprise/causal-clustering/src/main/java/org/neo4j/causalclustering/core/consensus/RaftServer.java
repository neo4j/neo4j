/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.core.consensus;

import java.net.BindException;
import java.util.concurrent.TimeUnit;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import org.neo4j.causalclustering.VersionDecoder;
import org.neo4j.causalclustering.VersionPrepender;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.handlers.ExceptionLoggingHandler;
import org.neo4j.causalclustering.handlers.ExceptionMonitoringHandler;
import org.neo4j.causalclustering.handlers.ExceptionSwallowingHandler;
import org.neo4j.causalclustering.handlers.PipelineHandlerAppender;
import org.neo4j.causalclustering.messaging.Inbound;
import org.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;
import org.neo4j.causalclustering.messaging.marshalling.RaftMessageDecoder;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

public class RaftServer extends LifecycleAdapter implements Inbound<RaftMessages.ClusterIdAwareMessage>
{
    private static final Setting<ListenSocketAddress> setting = CausalClusteringSettings.raft_listen_address;
    private final ChannelMarshal<ReplicatedContent> marshal;
    private final PipelineHandlerAppender pipelineAppender;
    private final ListenSocketAddress listenAddress;
    private final LogProvider logProvider;
    private final Log log;
    private final Log userLog;
    private final Monitors monitors;

    private MessageHandler<RaftMessages.ClusterIdAwareMessage> messageHandler;
    private EventLoopGroup workerGroup;
    private Channel channel;

    private final NamedThreadFactory threadFactory = new NamedThreadFactory( "raft-server" );

    public RaftServer( ChannelMarshal<ReplicatedContent> marshal, PipelineHandlerAppender pipelineAppender, Config config,
                       LogProvider logProvider, LogProvider userLogProvider, Monitors monitors )
    {
        this.marshal = marshal;
        this.pipelineAppender = pipelineAppender;
        this.listenAddress = config.get( setting );
        this.logProvider = logProvider;
        this.log = logProvider.getLog( getClass() );
        this.userLog = userLogProvider.getLog( getClass() );
        this.monitors = monitors;
    }

    @Override
    public synchronized void start() throws Throwable
    {
        startNettyServer();
    }

    @Override
    public synchronized void stop() throws Throwable
    {
        log.info( "RaftServer stopping and unbinding from " + listenAddress );
        try
        {
            channel.close().sync();
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            log.warn( "Interrupted while closing channel." );
        }

        if ( workerGroup.shutdownGracefully( 2, 5, TimeUnit.SECONDS ).awaitUninterruptibly( 10, TimeUnit.SECONDS ) )
        {
            log.warn( "Worker group not shutdown within 10 seconds." );
        }
    }

    private void startNettyServer()
    {
        workerGroup = new NioEventLoopGroup( 0, threadFactory );

        log.info( "Starting server at: " + listenAddress );

        ServerBootstrap bootstrap = new ServerBootstrap()
                .group( workerGroup )
                .channel( NioServerSocketChannel.class )
                .option( ChannelOption.SO_REUSEADDR, true )
                .localAddress( listenAddress.socketAddress() )
                .childHandler( new ChannelInitializer<SocketChannel>()
                {
                    @Override
                    protected void initChannel( SocketChannel ch ) throws Exception
                    {
                        ChannelPipeline pipeline = ch.pipeline();

                        pipelineAppender.addPipelineHandlerForServer( pipeline, ch );

                        pipeline.addLast( new LengthFieldBasedFrameDecoder( Integer.MAX_VALUE, 0, 4, 0, 4 ) );
                        pipeline.addLast( new LengthFieldPrepender( 4 ) );

                        pipeline.addLast( new VersionDecoder( logProvider ) );
                        pipeline.addLast( new VersionPrepender() );

                        pipeline.addLast( new RaftMessageDecoder( marshal ) );
                        pipeline.addLast( new RaftMessageHandler() );

                        pipeline.addLast( new ExceptionLoggingHandler( log ) );
                        pipeline.addLast( new ExceptionMonitoringHandler(
                                monitors.newMonitor( ExceptionMonitoringHandler.Monitor.class, RaftServer.class ) ) );
                        pipeline.addLast( new ExceptionSwallowingHandler() );
                    }
                } );

        try
        {
            channel = bootstrap.bind().syncUninterruptibly().channel();
        }
        catch ( Exception e )
        {
            // thanks to netty we need to catch everything and do an instanceof because it does not declare properly
            // checked exception but it still throws them with some black magic at runtime.
            //noinspection ConstantConditions
            if ( e instanceof BindException )
            {
                userLog.error( "Address is already bound for setting: " + setting + " with value: " + listenAddress );
                log.error( "Address is already bound for setting: " + setting + " with value: " + listenAddress, e );
                throw e;
            }
        }
    }

    @Override
    public void registerHandler( Inbound.MessageHandler<RaftMessages.ClusterIdAwareMessage> handler )
    {
        this.messageHandler = handler;
    }

    private class RaftMessageHandler extends SimpleChannelInboundHandler<RaftMessages.ClusterIdAwareMessage>
    {
        @Override
        protected void channelRead0( ChannelHandlerContext channelHandlerContext,
                                     RaftMessages.ClusterIdAwareMessage clusterIdAwareMessage ) throws Exception
        {
            try
            {
                messageHandler.handle( clusterIdAwareMessage );
            }
            catch ( Exception e )
            {
                log.error( format( "Failed to process message %s", clusterIdAwareMessage ), e );
            }
        }
    }

}
