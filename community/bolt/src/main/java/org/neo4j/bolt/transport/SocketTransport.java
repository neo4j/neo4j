/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;

import org.neo4j.bolt.logging.BoltMessageLogging;
import org.neo4j.helpers.ListenSocketAddress;
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
    private final BoltMessageLogging boltLogging;
    private final TransportThrottleGroup throttleGroup;
    private final BoltProtocolPipelineInstallerFactory handlerFactory;

    public SocketTransport( String connector, ListenSocketAddress address, SslContext sslCtx, boolean encryptionRequired,
                            LogProvider logging, BoltMessageLogging boltLogging,
                            TransportThrottleGroup throttleGroup,
                            BoltProtocolPipelineInstallerFactory handlerFactory )
    {
        this.connector = connector;
        this.address = address;
        this.sslCtx = sslCtx;
        this.encryptionRequired = encryptionRequired;
        this.logging = logging;
        this.boltLogging = boltLogging;
        this.throttleGroup = throttleGroup;
        this.handlerFactory = handlerFactory;
    }

    @Override
    public ChannelInitializer<SocketChannel> channelInitializer()
    {
        return new ChannelInitializer<SocketChannel>()
        {
            @Override
            public void initChannel( SocketChannel ch )
            {
                ch.config().setAllocator( PooledByteBufAllocator.DEFAULT );

                // install throttles
                throttleGroup.install( ch );

                // add a close listener that will uninstall throttles
                ch.closeFuture().addListener( future -> throttleGroup.uninstall( ch ) );

                TransportSelectionHandler transportSelectionHandler = new TransportSelectionHandler( connector, sslCtx,
                        encryptionRequired, false, logging, handlerFactory, boltLogging );

                ch.pipeline().addLast( transportSelectionHandler );
            }
        };
    }

    @Override
    public ListenSocketAddress address()
    {
        return address;
    }
}
