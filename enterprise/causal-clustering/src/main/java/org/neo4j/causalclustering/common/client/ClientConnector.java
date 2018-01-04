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
package org.neo4j.causalclustering.common.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOutboundInvoker;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

import org.neo4j.causalclustering.common.ChannelService;
import org.neo4j.causalclustering.common.EventLoopContext;
import org.neo4j.concurrent.Futures;

import static java.util.stream.Collectors.toList;

public class ClientConnector<C extends Channel> implements ChannelService<Bootstrap,C>
{
    private final Function<EventLoopContext<C>,Bootstrap> bootstrapper;
    private Bootstrap bootstrap;
    private final List<Channel> connect = new LinkedList<>();

    public ClientConnector( Function<EventLoopContext<C>,Bootstrap> bootstrapper )
    {
        this.bootstrapper = bootstrapper;
    }

    @Override
    public void bootstrap( EventLoopContext<C> eventLoopContext )
    {
        this.bootstrap = bootstrapper.apply( eventLoopContext );
    }

    public synchronized Channel connect( InetSocketAddress socketAddress )
    {
        return connect( bootstrap, socketAddress );
    }

    public synchronized Channel connect( InetSocketAddress socketAddress,
            Function<Bootstrap,Bootstrap> additionalConfig )
    {
        return connect( additionalConfig.apply( bootstrap.clone() ), socketAddress );
    }

    private Channel connect( Bootstrap bootstrap, InetSocketAddress socketAddress )
    {
        Channel channel = bootstrap.connect( socketAddress ).awaitUninterruptibly().channel();
        connect.add( channel );
        return channel;
    }

    @Override
    public void start() throws Throwable
    {
        // do nothing
    }

    @Override
    public synchronized void closeChannels() throws Throwable
    {
        Futures.combine( connect.stream().map( ChannelOutboundInvoker::close ).collect( toList() ) ).get();
        connect.clear();
    }
}
