/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.ndp.transport.socket;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.util.Collection;

import org.neo4j.function.BiConsumer;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * Simple wrapper around Netty boss and selector threads, which allows multiple ports and protocols to be handled
 * by the same set of common worker threads.
 */
public class NettyServer extends LifecycleAdapter
{
    private final Collection<BiConsumer<EventLoopGroup,EventLoopGroup>> bootstrappers;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    /**
     * @param bootstrappers functions that bootstrap protocols we should support
     */
    public NettyServer( Collection<BiConsumer<EventLoopGroup, EventLoopGroup>> bootstrappers )
    {
        this.bootstrappers = bootstrappers;
    }

    @Override
    public void start() throws Throwable
    {
        // TODO: This should circle back to Neo4j thread scheduler, which needs to provide some sort of thread
        // factory for us to use here.
        bossGroup = new NioEventLoopGroup( 1 );
        workerGroup = new NioEventLoopGroup();

        for ( BiConsumer<EventLoopGroup,EventLoopGroup> bootstrapper : bootstrappers )
        {
            bootstrapper.accept( bossGroup, workerGroup );
        }
    }

    @Override
    public void stop() throws Throwable
    {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }
}
