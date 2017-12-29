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
package org.neo4j.causalclustering.core.server;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;

import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public abstract class AbstractNettyApplication<T extends AbstractBootstrap> extends LifecycleAdapter
{
    private static final int SHUTDOWN_TIMEOUT = 170;
    private static final int TIMEOUT = 180;
    private static final int QUIET_PERIOD = 2;
    private static final TimeUnit TIME_UNIT = TimeUnit.SECONDS;
    private final Log log;
    private final Log userLog;
    private boolean hasShutDown = false;
    private T bootstrap;
    private Channel channel;

    public AbstractNettyApplication( LogProvider logProvider, LogProvider userLogProvider )
    {
        this.log = logProvider.getLog( getClass() );
        this.userLog = userLogProvider.getLog( getClass() );
    }

    protected abstract EventLoopGroup getEventLoopGroup();

    protected abstract T bootstrap();

    protected abstract InetSocketAddress bindAddress();

    @Override
    public synchronized void init() throws Throwable
    {
        if ( hasShutDown )
        {
            throw new IllegalStateException( "Cannot initiate application. Already shutdown" );
        }
        if ( bootstrap != null )
        {
            throw new IllegalStateException( "Application is already initialized" );
        }
        bootstrap = bootstrap();
    }

    @Override
    public synchronized void start() throws Throwable
    {
        if ( hasShutDown )
        {
            throw new IllegalStateException( "Cannot start application. Already shutdown" );
        }
        if ( bootstrap == null )
        {
            throw new IllegalStateException( "Cannot start application. Need to be initialised first" );
        }
        if ( channel != null )
        {
            log.info( "Already running" );
            return;
        }

        ChannelFuture channelFuture = bootstrap.bind( bindAddress() ).awaitUninterruptibly();
        if ( channelFuture.isSuccess() )
        {
            channel = channelFuture.channel();
        }
        else
        {
            handleUnsuccesfulBindCause( channelFuture.cause() );
        }
    }

    @Override
    public synchronized void stop() throws Throwable
    {
        if ( channel == null )
        {
            log.info( "Already stopped" );
            return;
        }
        try
        {
            channel.close().syncUninterruptibly();
        }
        finally
        {
            channel = null;
        }
    }

    @Override
    public synchronized void shutdown() throws Throwable
    {
        hasShutDown = true;
        EventLoopGroup eventLoopGroup = getEventLoopGroup();
        if ( eventLoopGroup == null )
        {
            throw new IllegalArgumentException( "EventLoopGroup cannot be null." );
        }
        eventLoopGroup.shutdownGracefully( QUIET_PERIOD, SHUTDOWN_TIMEOUT, TIME_UNIT ).get( TIMEOUT, TIME_UNIT );
    }

    private void handleUnsuccesfulBindCause( Throwable cause ) throws Exception
    {
        if ( cause == null )
        {
            throw new IllegalArgumentException( "Cause cannot be null" );
        }
        if ( cause instanceof BindException )
        {
            userLog.error(
                    "Address is already bound for setting: " + CausalClusteringSettings.transaction_listen_address +
                    " with value: " + bootstrap.config().localAddress() );
            log.error(
                    "Address is already bound for setting: " + CausalClusteringSettings.transaction_listen_address +
                    " with value: " + bootstrap.config().localAddress(), cause );
            throw (BindException) cause;
        }
        else
        {
            log.error( "Failed to start application", cause );
            throw new RuntimeException( cause );
        }
    }
}
