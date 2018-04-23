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
package org.neo4j.causalclustering.net;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.net.BindException;
import java.util.concurrent.TimeUnit;

import org.neo4j.causalclustering.helper.SuspendableLifeCycle;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static java.lang.String.format;

public class Server extends SuspendableLifeCycle
{
    private final Log debugLog;
    private final Log userLog;
    private final String serverName;

    private final NamedThreadFactory threadFactory;
    private final ChildInitializer childInitializer;
    private final ChannelInboundHandler parentHandler;
    private final ListenSocketAddress listenAddress;

    private EventLoopGroup workerGroup;
    private Channel channel;

    public Server( ChildInitializer childInitializer, LogProvider debugLogProvider, LogProvider userLogProvider, ListenSocketAddress listenAddress,
                   String serverName )
    {
        this( childInitializer, null, debugLogProvider, userLogProvider, listenAddress, serverName );
    }

    public Server( ChildInitializer childInitializer, ChannelInboundHandler parentHandler, LogProvider debugLogProvider, LogProvider userLogProvider,
                   ListenSocketAddress listenAddress, String serverName )
    {
        super( debugLogProvider.getLog( Server.class ) );
        this.childInitializer = childInitializer;
        this.parentHandler = parentHandler;
        this.listenAddress = listenAddress;
        this.debugLog = debugLogProvider.getLog( getClass() );
        this.userLog = userLogProvider.getLog( getClass() );
        this.serverName = serverName;
        this.threadFactory = new NamedThreadFactory( serverName );
    }

    public Server( ChildInitializer childInitializer, ListenSocketAddress listenAddress, String serverName )
    {
        this( childInitializer, null, NullLogProvider.getInstance(), NullLogProvider.getInstance(), listenAddress, serverName );
    }

    @Override
    protected void init0()
    {
        // do nothing
    }

    @Override
    protected void start0()
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
                .childHandler( childInitializer.asChannelInitializer() );

        if ( parentHandler != null )
        {
            bootstrap.handler( parentHandler );
        }

        try
        {
            channel = bootstrap.bind().syncUninterruptibly().channel();
            debugLog.info( serverName + ": bound to " + listenAddress );
        }
        catch ( Exception e )
        {
            //noinspection ConstantConditions netty sneaky throw
            if ( e instanceof BindException )
            {
                String message = serverName + ": address is already bound: " + listenAddress;
                userLog.error( message );
                debugLog.error( message, e );
            }
            throw e;
        }
    }

    @Override
    protected void stop0()
    {
        if ( channel == null )
        {
            return;
        }

        debugLog.info( serverName + ": stopping and unbinding from: " + listenAddress );
        try
        {
            channel.close().sync();
            channel = null;
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            debugLog.warn( "Interrupted while closing channel." );
        }

        if ( workerGroup != null && workerGroup.shutdownGracefully( 2, 5, TimeUnit.SECONDS ).awaitUninterruptibly( 10, TimeUnit.SECONDS ) )
        {
            debugLog.warn( "Worker group not shutdown within 10 seconds." );
        }
        workerGroup = null;
    }

    @Override
    protected void shutdown0()
    {
        // do nothing
    }

    public ListenSocketAddress address()
    {
        return listenAddress;
    }

    @Override
    public String toString()
    {
        return format( "Server[%s]", serverName );
    }
}
