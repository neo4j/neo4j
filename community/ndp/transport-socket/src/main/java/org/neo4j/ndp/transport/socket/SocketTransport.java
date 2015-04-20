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
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.function.Factory;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.ndp.runtime.Sessions;

/**
 * Implements a transport for the Neo4j Messaging Protocol that uses good old regular sockets.
 */
public class SocketTransport extends LifecycleAdapter
{
    private final HostnamePort address;
    private final PrimitiveLongObjectMap<Factory<SocketProtocol>> availableProtocols;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    public SocketTransport( HostnamePort address, final Log log, final Sessions sessions )
    {
        this.address = address;

        this.availableProtocols = Primitive.longObjectMap();
        this.availableProtocols.put( SocketProtocolV1.VERSION, new Factory<SocketProtocol>()
        {
            @Override
            public SocketProtocol newInstance()
            {
                return new SocketProtocolV1( log, sessions.newSession() );
            }
        } );
    }

    @Override
    public void init() throws Throwable
    {
        bossGroup = new NioEventLoopGroup( 1 );
        workerGroup = new NioEventLoopGroup();
    }

    @Override
    public void start() throws Throwable
    {
        ServerBootstrap b = new ServerBootstrap();
        b.group( bossGroup, workerGroup )
                .channel( NioServerSocketChannel.class )
                .handler( new LoggingHandler( LogLevel.INFO ) )
                .childHandler( new ChannelInitializer<SocketChannel>()
                {
                    @Override
                    public void initChannel( SocketChannel ch ) throws Exception
                    {
                        ch.pipeline().addLast( new SocketTransportHandler( new SocketTransportHandler.ProtocolChooser(
                                availableProtocols ) ) );
                    }
                } );

        // Bind and start to accept incoming connections.
        b.bind( address.getHost(), address.getPort() ).sync();
    }

    @Override
    public void stop() throws Throwable
    {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

    public HostnamePort address()
    {
        return address;
    }
}
