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
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.net.BindException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.neo4j.causalclustering.protocol.handshake.ProtocolStack;
import org.neo4j.causalclustering.protocol.handshake.ServerHandshakeFinishedEvent;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.helpers.SocketAddress;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.causalclustering.core.CausalClusteringSettings.raft_listen_address;

public class RaftServer extends LifecycleAdapter
{
    public static final String RAFT_SERVER = "RaftServer";

    private final ChannelInitializer<SocketChannel> channelInitializer;

    private final ListenSocketAddress listenAddress;
    private final Log log;
    private final Log userLog;

    private EventLoopGroup workerGroup;
    private Channel channel;
    private ConcurrentMap<SocketAddress,ProtocolStack> installedProtocols = new ConcurrentHashMap<>();

    private final NamedThreadFactory threadFactory = new NamedThreadFactory( "raft-server" );

    public RaftServer( ChannelInitializer<SocketChannel> channelInitializer, Config config, LogProvider logProvider, LogProvider userLogProvider )
    {
        this.channelInitializer = channelInitializer;
        this.listenAddress = config.get( raft_listen_address );
        this.log = logProvider.getLog( getClass() );
        this.userLog = userLogProvider.getLog( getClass() );
    }

    @Override
    public synchronized void start()
    {
        startNettyServer();
    }

    @Override
    public synchronized void stop()
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

        if ( !workerGroup.shutdownGracefully( 2, 5, TimeUnit.SECONDS ).awaitUninterruptibly( 10, TimeUnit.SECONDS ) )
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
                .handler( new ChannelInboundHandlerAdapter()
                {
                    @Override
                    public void userEventTriggered( ChannelHandlerContext ctx, Object evt ) throws Exception
                    {
                        if ( evt instanceof ServerHandshakeFinishedEvent.Created )
                        {
                            ServerHandshakeFinishedEvent.Created created = (ServerHandshakeFinishedEvent.Created) evt;
                            installedProtocols.put( created.advertisedSocketAddress, created.protocolStack );
                        }
                        else if ( evt instanceof ServerHandshakeFinishedEvent.Closed )
                        {
                            ServerHandshakeFinishedEvent.Closed closed = (ServerHandshakeFinishedEvent.Closed) evt;
                            installedProtocols.remove( closed.advertisedSocketAddress );
                        }
                        else
                        {
                            super.userEventTriggered( ctx, evt );
                        }
                    }
                } )
                .childHandler( channelInitializer );

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
                userLog.error( "Address is already bound for setting: " + raft_listen_address + " with value: " + listenAddress );
                log.error( "Address is already bound for setting: " + raft_listen_address + " with value: " + listenAddress, e );
                throw e;
            }
        }
    }

    public Stream<Pair<SocketAddress,ProtocolStack>> installedProtocols()
    {
        return installedProtocols.entrySet().stream().map( entry -> Pair.of( entry.getKey(), entry.getValue() ) );
    }
}
