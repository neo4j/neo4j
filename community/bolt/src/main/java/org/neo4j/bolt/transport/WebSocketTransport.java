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
package org.neo4j.bolt.transport;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.ssl.SslContext;

import java.util.function.Function;

import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.logging.LogProvider;

/**
 * Carries the Neo4j protocol over websockets. Apart from the initial websocket handshake, this works with the exact
 * same protocol as {@link SocketTransport}.
 */
public class WebSocketTransport implements NettyServer.ProtocolInitializer
{
    private static final int MAX_WEBSOCKET_HANDSHAKE_SIZE = 65536;

    private final HostnamePort address;
    private final SslContext sslCtx;
    private LogProvider logging;
    private final PrimitiveLongObjectMap<Function<Channel,BoltProtocol>> availableVersions;

    public WebSocketTransport( HostnamePort address, SslContext sslCtx, LogProvider logging,
            PrimitiveLongObjectMap<Function<Channel,BoltProtocol>> protocolVersions )
    {
        this.address = address;
        this.sslCtx = sslCtx;
        this.logging = logging;
        this.availableVersions = protocolVersions;
    }

    @Override
    public ChannelInitializer<SocketChannel> channelInitializer()
    {
        return new ChannelInitializer<SocketChannel>()
        {
            @Override
            public void initChannel( SocketChannel ch ) throws Exception
            {
                ch.config().setAllocator( PooledByteBufAllocator.DEFAULT );

                if ( sslCtx != null )
                {
                    ch.pipeline().addLast( sslCtx.newHandler( ch.alloc() ) );
                }

                ch.pipeline().addLast(
                        new HttpServerCodec(),
                        new HttpObjectAggregator( MAX_WEBSOCKET_HANDSHAKE_SIZE ),
                        new WebSocketServerProtocolHandler( "" ),
                        new WebSocketFrameTranslator(),
                        new SocketTransportHandler(
                                new SocketTransportHandler.ProtocolChooser( availableVersions ), logging ) );
            }
        };
    }

    @Override
    public HostnamePort address()
    {
        return address;
    }
}
