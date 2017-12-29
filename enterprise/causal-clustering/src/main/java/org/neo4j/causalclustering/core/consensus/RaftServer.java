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
package org.neo4j.causalclustering.core.consensus;

import io.netty.bootstrap.ServerBootstrap;
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

import java.net.InetSocketAddress;
import java.time.Clock;

import org.neo4j.causalclustering.VersionDecoder;
import org.neo4j.causalclustering.VersionPrepender;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.core.server.AbstractNettyApplication;
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
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

public class RaftServer extends AbstractNettyApplication<ServerBootstrap>
        implements Inbound<RaftMessages.ReceivedInstantClusterIdAwareMessage>
{
    private static final Setting<ListenSocketAddress> setting = CausalClusteringSettings.raft_listen_address;
    private final ChannelMarshal<ReplicatedContent> marshal;
    private final PipelineHandlerAppender pipelineAppender;
    private final ListenSocketAddress listenAddress;
    private final LogProvider logProvider;
    private final Log log;
    private final Log userLog;
    private final Monitors monitors;
    private final Clock clock;

    private MessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage> messageHandler;
    private EventLoopGroup workerGroup;

    private final NamedThreadFactory threadFactory = new NamedThreadFactory( "raft-server" );

    public RaftServer( ChannelMarshal<ReplicatedContent> marshal, PipelineHandlerAppender pipelineAppender,
            Config config, LogProvider logProvider,
            LogProvider userLogProvider, Monitors monitors, Clock clock )
    {
        super( logProvider, userLogProvider );
        this.marshal = marshal;
        this.pipelineAppender = pipelineAppender;
        this.listenAddress = config.get( setting );
        this.logProvider = logProvider;
        this.log = logProvider.getLog( getClass() );
        this.userLog = userLogProvider.getLog( getClass() );
        this.monitors = monitors;
        this.clock = clock;
    }

    @Override
    protected EventLoopGroup getEventLoopGroup()
    {
        if ( workerGroup == null )
        {
            workerGroup = new NioEventLoopGroup( 0, threadFactory );
        }
        return workerGroup;
    }

    @Override
    protected ServerBootstrap bootstrap()
    {
        log.info( "Starting server at: " + listenAddress );

        return new ServerBootstrap()
                .group( getEventLoopGroup() )
                .channel( NioServerSocketChannel.class )
                .option( ChannelOption.SO_REUSEADDR, true )
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

                        pipeline.addLast( new RaftMessageDecoder( marshal, clock ) );
                        pipeline.addLast( new RaftMessageHandler() );

                        pipeline.addLast( new ExceptionLoggingHandler( log ) );
                        pipeline.addLast( new ExceptionMonitoringHandler(
                                monitors.newMonitor( ExceptionMonitoringHandler.Monitor.class,
                                        RaftServer.class ) ) );
                        pipeline.addLast( new ExceptionSwallowingHandler() );
                    }
                } );
    }

    @Override
    protected InetSocketAddress bindAddress()
    {
        return listenAddress.socketAddress();
    }

    @Override
    public void registerHandler( Inbound.MessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage> handler )
    {
        this.messageHandler = handler;
    }

    private class RaftMessageHandler
            extends SimpleChannelInboundHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage>
    {
        @Override
        protected void channelRead0( ChannelHandlerContext channelHandlerContext,
                RaftMessages.ReceivedInstantClusterIdAwareMessage incomingMessage ) throws Exception
        {
            try
            {
                messageHandler.handle( incomingMessage );
            }
            catch ( Exception e )
            {
                log.error( format( "Failed to process message %s", incomingMessage ), e );
            }
        }
    }

}
