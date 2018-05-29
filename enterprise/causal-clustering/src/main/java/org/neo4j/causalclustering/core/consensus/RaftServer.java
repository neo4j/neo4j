/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.causalclustering.core.consensus;

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

import java.net.BindException;
import java.time.Clock;
import java.util.concurrent.TimeUnit;

import org.neo4j.causalclustering.VersionDecoder;
import org.neo4j.causalclustering.VersionPrepender;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.handlers.ExceptionLoggingHandler;
import org.neo4j.causalclustering.handlers.ExceptionMonitoringHandler;
import org.neo4j.causalclustering.handlers.ExceptionSwallowingHandler;
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
import org.neo4j.ssl.SslPolicy;

import static java.lang.String.format;

public class RaftServer extends LifecycleAdapter implements Inbound<RaftMessages.ReceivedInstantClusterIdAwareMessage>
{
    private static final Setting<ListenSocketAddress> setting = CausalClusteringSettings.raft_listen_address;
    private final ChannelMarshal<ReplicatedContent> marshal;
    private final SslPolicy sslPolicy;
    private final ListenSocketAddress listenAddress;
    private final LogProvider logProvider;
    private final Log log;
    private final Log userLog;
    private final Monitors monitors;
    private final Clock clock;

    private MessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage> messageHandler;
    private EventLoopGroup workerGroup;
    private Channel channel;

    private final NamedThreadFactory threadFactory = new NamedThreadFactory( "raft-server" );

    public RaftServer( ChannelMarshal<ReplicatedContent> marshal, SslPolicy sslPolicy, Config config, LogProvider logProvider, LogProvider userLogProvider,
            Monitors monitors, Clock clock )
    {
        this.marshal = marshal;
        this.sslPolicy = sslPolicy;
        this.listenAddress = config.get( setting );
        this.logProvider = logProvider;
        this.log = logProvider.getLog( getClass() );
        this.userLog = userLogProvider.getLog( getClass() );
        this.monitors = monitors;
        this.clock = clock;
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

                        if ( sslPolicy != null )
                        {
                            pipeline.addLast( sslPolicy.nettyServerHandler( ch ) );
                        }

                        pipeline.addLast( new LengthFieldBasedFrameDecoder( Integer.MAX_VALUE, 0, 4, 0, 4 ) );
                        pipeline.addLast( new LengthFieldPrepender( 4 ) );

                        pipeline.addLast( new VersionDecoder( logProvider ) );
                        pipeline.addLast( new VersionPrepender() );

                        pipeline.addLast( new RaftMessageDecoder( marshal, clock ) );
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
    public void registerHandler( Inbound.MessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage> handler )
    {
        this.messageHandler = handler;
    }

    private class RaftMessageHandler extends SimpleChannelInboundHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage>
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
