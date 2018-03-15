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
package org.neo4j.causalclustering.catchup;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.net.BindException;
import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.causalclustering.core.CausalClusteringSettings.transaction_listen_address;

public class CatchupServer extends LifecycleAdapter
{
    private final Log log;
    private final Log userLog;

    private final NamedThreadFactory threadFactory = new NamedThreadFactory( "catchup-server" );
    private final ChannelInitializer<SocketChannel> channelInitializer;
    private final ListenSocketAddress listenAddress;

    private EventLoopGroup workerGroup;
    private Channel channel;

    public CatchupServer( ChannelInitializer<SocketChannel> channelInitializer, LogProvider logProvider, LogProvider userLogProvider,
            ListenSocketAddress listenAddress )
    {
        this.channelInitializer = channelInitializer;
        this.listenAddress = listenAddress;
        this.log = logProvider.getLog( getClass() );
        this.userLog = userLogProvider.getLog( getClass() );
    }

    @Override
    public synchronized void start()
    {
        if ( channel != null )
        {
            return;
        }

        workerGroup = new NioEventLoopGroup( 0, threadFactory );

        ServerBootstrap bootstrap = new ServerBootstrap()
                .group( workerGroup )
                .channel( NioServerSocketChannel.class )
                .option( ChannelOption.SO_REUSEADDR, Boolean.TRUE )
                .localAddress( listenAddress.socketAddress() )
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
                String message = String.format( "Address is already bound for setting: %s with value: %s", transaction_listen_address, listenAddress );
                userLog.error( message );
                log.error( message, e );
                throw e;
            }
        }
    }

    @Override
    public synchronized void stop()
    {
        if ( channel == null )
        {
            return;
        }

        log.info( "CatchupServer stopping and unbinding from " + listenAddress );
        try
        {
            channel.close().sync();
            channel = null;
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            log.warn( "Interrupted while closing channel." );
        }

        if ( workerGroup != null &&
                workerGroup.shutdownGracefully( 2, 5, TimeUnit.SECONDS ).awaitUninterruptibly( 10, TimeUnit.SECONDS ) )
        {
            log.warn( "Worker group not shutdown within 10 seconds." );
        }
        workerGroup = null;
    }
}
