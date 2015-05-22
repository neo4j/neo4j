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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.function.BiConsumer;
import org.neo4j.function.Factory;
import org.neo4j.function.Function;
import org.neo4j.helpers.HostnamePort;

/**
 * Implements a transport for the Neo4j Messaging Protocol that uses good old regular sockets.
 */
public class SocketTransport implements BiConsumer<EventLoopGroup, EventLoopGroup>
{
    private final HostnamePort address;
    private final PrimitiveLongObjectMap<Function<Channel, SocketProtocol>> protocolVersions;

    public SocketTransport( HostnamePort address, PrimitiveLongObjectMap<Function<Channel, SocketProtocol>> protocolVersions)
    {
        this.address = address;
        this.protocolVersions = protocolVersions;
    }

    public HostnamePort address()
    {
        return address;
    }

    @Override
    public void accept( EventLoopGroup bossGroup, EventLoopGroup workerGroup )
    {
        ServerBootstrap b = new ServerBootstrap();
        b.option( ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT )
         .group( bossGroup, workerGroup )
         .channel( NioServerSocketChannel.class )
         .childHandler( new ChannelInitializer<SocketChannel>()
                {
                    @Override
                    public void initChannel( SocketChannel ch ) throws Exception
                    {
                        ch.pipeline().addLast( new SocketTransportHandler(
                                new SocketTransportHandler.ProtocolChooser( protocolVersions ) ) );
                    }
                } );

        // Bind and start to accept incoming connections.
        try
        {
            b.bind( address.getHost(), address.getPort() ).sync();
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
    }
}
