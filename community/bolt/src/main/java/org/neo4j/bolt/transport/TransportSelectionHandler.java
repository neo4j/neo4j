/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;

import java.util.List;
import java.util.function.BiFunction;

import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.logging.LogProvider;

import static org.neo4j.bolt.transport.SocketTransportHandler.ProtocolChooser.BOLT_MAGIC_PREAMBLE;

public class TransportSelectionHandler extends ByteToMessageDecoder
{
    private static final String WEBSOCKET_MAGIC = "GET ";
    private static final int MAX_WEBSOCKET_HANDSHAKE_SIZE = 65536;

    private final SslContext sslCtx;
    private final boolean isEncrypted;
    private final LogProvider logging;
    private final PrimitiveLongObjectMap<BiFunction<Channel,Boolean,BoltProtocol>> protocolVersions;

    public TransportSelectionHandler( SslContext sslCtx, LogProvider logging,
            PrimitiveLongObjectMap<BiFunction<Channel,Boolean,BoltProtocol>> protocolVersions )
    {
        this( sslCtx, false, logging, protocolVersions );
    }

    private TransportSelectionHandler( SslContext sslCtx, boolean isEncrypted, LogProvider logging,
            PrimitiveLongObjectMap<BiFunction<Channel,Boolean,BoltProtocol>> protocolVersions )
    {
        this.sslCtx = sslCtx;
        this.isEncrypted = isEncrypted;
        this.logging = logging;
        this.protocolVersions = protocolVersions;
    }

    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out ) throws Exception
    {
        // Will use the first five bytes to detect a protocol.
        if ( in.readableBytes() < 5 )
        {
            return;
        }

        if ( detectSsl( in ) )
        {
            enableSsl( ctx );
        }
        else if ( isHttp( in ) )
        {
            switchToWebsocket( ctx );
        }
        else if( isBoltPreamble( in ) )
        {
            switchToSocket( ctx );
        }
        else
        {
            // TODO: send a alert_message for a ssl connection to terminate the handshake
            in.clear();
            ctx.close();
        }
    }

    private boolean isBoltPreamble( ByteBuf in )
    {
        return in.getInt( 0 ) == BOLT_MAGIC_PREAMBLE;
    }

    private boolean detectSsl( ByteBuf buf )
    {
        return sslCtx != null && SslHandler.isEncrypted( buf );
    }

    private boolean isHttp( ByteBuf buf )
    {
        for ( int i = 0; i < WEBSOCKET_MAGIC.length(); ++i )
        {
            if ( buf.getUnsignedByte( buf.readerIndex() + i ) != WEBSOCKET_MAGIC.charAt( i ) )
            {
                return false;
            }
        }
        return true;
    }

    private void enableSsl( ChannelHandlerContext ctx )
    {
        ChannelPipeline p = ctx.pipeline();
        p.addLast( sslCtx.newHandler( ctx.alloc() ) );
        p.addLast( new TransportSelectionHandler( null, true, logging, protocolVersions ) );
        p.remove( this );
    }

    private void switchToSocket( ChannelHandlerContext ctx )
    {
        ChannelPipeline p = ctx.pipeline();
        p.addLast( new SocketTransportHandler(
                new SocketTransportHandler.ProtocolChooser( protocolVersions, isEncrypted ), logging ) );
        p.remove( this );
    }

    private void switchToWebsocket( ChannelHandlerContext ctx )
    {
        ChannelPipeline p = ctx.pipeline();
        p.addLast(
                new HttpServerCodec(),
                new HttpObjectAggregator( MAX_WEBSOCKET_HANDSHAKE_SIZE ),
                new WebSocketServerProtocolHandler( "" ),
                new WebSocketFrameTranslator(),
                new SocketTransportHandler(
                        new SocketTransportHandler.ProtocolChooser( protocolVersions, isEncrypted ), logging ) );
        p.remove( this );
    }
}
