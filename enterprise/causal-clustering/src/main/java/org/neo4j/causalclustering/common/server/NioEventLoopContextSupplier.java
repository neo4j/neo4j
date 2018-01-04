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
package org.neo4j.causalclustering.common.server;

import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;

import org.neo4j.causalclustering.common.EventLoopContext;

public class NioEventLoopContextSupplier implements Supplier<EventLoopContext<NioServerSocketChannel>>
{
    private final ThreadFactory threadFactory;
    private final int threads;

    public NioEventLoopContextSupplier( ThreadFactory threadFactory, int threads )
    {
        this.threadFactory = threadFactory;
        this.threads = threads;
    }

    public NioEventLoopContextSupplier( ThreadFactory threadFactory )
    {
        this( threadFactory, 0 );
    }

    @Override
    public EventLoopContext<NioServerSocketChannel> get()
    {
        return new EventLoopContext<>( new NioEventLoopGroup( threads, threadFactory ), NioServerSocketChannel.class );
    }
}
