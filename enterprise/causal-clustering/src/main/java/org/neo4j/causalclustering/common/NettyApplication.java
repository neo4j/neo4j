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
package org.neo4j.causalclustering.common;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class NettyApplication<C extends Channel> extends LifecycleAdapter
{
    private static final int SHUTDOWN_TIMEOUT = 170;
    private static final int TIMEOUT = 180;
    private static final int QUIET_PERIOD = 2;
    private static final TimeUnit TIME_UNIT = TimeUnit.SECONDS;
    private final ChannelService<?,C> channelService;
    private final Supplier<EventLoopContext<C>> eventLoopContextSupplier;
    private boolean hasShutDown;
    private EventLoopGroup eventExecutors;

    public NettyApplication( ChannelService<?,C> channelService,
            Supplier<EventLoopContext<C>> eventLoopContextSupplier )
    {
        this.channelService = channelService;
        this.eventLoopContextSupplier = eventLoopContextSupplier;
    }

    @Override
    public synchronized void init() throws Throwable
    {
        if ( hasShutDown )
        {
            throw new IllegalStateException( "Cannot initiate application. Already shutdown" );
        }
        if ( eventExecutors != null )
        {
            throw new IllegalStateException( "Cannot initiate application. Already initiated" );
        }
        EventLoopContext<C> eventLoopContext = eventLoopContextSupplier.get();
        this.eventExecutors = eventLoopContext.eventExecutors();
        channelService.bootstrap( eventLoopContext );
    }

    @Override
    public synchronized void start() throws Throwable
    {
        if ( hasShutDown )
        {
            throw new IllegalStateException( "Cannot start application. Already shutdown" );
        }
        channelService.start();
    }

    @Override
    public synchronized void stop() throws Throwable
    {
        if ( hasShutDown )
        {
            return;
        }
        channelService.closeChannels();
    }

    @Override
    public synchronized void shutdown() throws Throwable
    {
        hasShutDown = true;
        if ( eventExecutors == null )
        {
            throw new IllegalArgumentException( "EventLoopGroup cannot be null." );
        }
        eventExecutors.shutdownGracefully( QUIET_PERIOD, SHUTDOWN_TIMEOUT, TIME_UNIT ).get( TIMEOUT, TIME_UNIT );
    }
}
