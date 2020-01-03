/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.bolt.transport;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.ssl.SslContext;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.logging.LogProvider;

/**
 * Implements a transport for the Neo4j Messaging Protocol that uses good old regular sockets.
 */
public class SocketTransport implements NettyServer.ProtocolInitializer
{
    private final String connector;
    private final ListenSocketAddress address;
    private final SslContext sslCtx;
    private final boolean encryptionRequired;
    private final LogProvider logging;
    private final TransportThrottleGroup throttleGroup;
    private final BoltProtocolFactory boltProtocolFactory;
    private final NetworkConnectionTracker connectionTracker;

    public SocketTransport( String connector, ListenSocketAddress address, SslContext sslCtx, boolean encryptionRequired,
            LogProvider logging, TransportThrottleGroup throttleGroup,
            BoltProtocolFactory boltProtocolFactory, NetworkConnectionTracker connectionTracker )
    {
        this.connector = connector;
        this.address = address;
        this.sslCtx = sslCtx;
        this.encryptionRequired = encryptionRequired;
        this.logging = logging;
        this.throttleGroup = throttleGroup;
        this.boltProtocolFactory = boltProtocolFactory;
        this.connectionTracker = connectionTracker;
    }

    @Override
    public ChannelInitializer<Channel> channelInitializer()
    {
        return new ChannelInitializer<Channel>()
        {
            @Override
            public void initChannel( Channel ch )
            {
                ch.config().setAllocator( PooledByteBufAllocator.DEFAULT );

                BoltChannel boltChannel = newBoltChannel( ch );
                connectionTracker.add( boltChannel );
                ch.closeFuture().addListener( future -> connectionTracker.remove( boltChannel ) );

                // install throttles
                throttleGroup.install( ch );

                // add a close listener that will uninstall throttles
                ch.closeFuture().addListener( future -> throttleGroup.uninstall( ch ) );

                TransportSelectionHandler transportSelectionHandler = new TransportSelectionHandler( boltChannel, sslCtx,
                        encryptionRequired, false, logging, boltProtocolFactory );

                ch.pipeline().addLast( transportSelectionHandler );
            }
        };
    }

    @Override
    public ListenSocketAddress address()
    {
        return address;
    }

    private BoltChannel newBoltChannel( Channel ch )
    {
        return new BoltChannel( connectionTracker.newConnectionId( connector ), connector, ch );
    }
}
