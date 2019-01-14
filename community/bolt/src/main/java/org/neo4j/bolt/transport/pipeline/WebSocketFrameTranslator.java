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
package org.neo4j.bolt.transport.pipeline;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;

/**
 * Translates websocket frames to bytebufs, and bytebufs to frames. Intermediary layer between our binary protocol
 * and nettys built-in websocket handlers.
 */
public class WebSocketFrameTranslator extends ChannelDuplexHandler
{
    @Override
    public void channelRead( ChannelHandlerContext ctx, Object msg )
    {
        if ( msg instanceof BinaryWebSocketFrame )
        {
            ctx.fireChannelRead( ((BinaryWebSocketFrame) msg).content() );
        }
        else
        {
            ctx.fireChannelRead( msg );
        }
    }

    @Override
    public void write( ChannelHandlerContext ctx, Object msg, ChannelPromise promise )
    {
        if ( msg instanceof ByteBuf )
        {
            ctx.write( new BinaryWebSocketFrame( (ByteBuf) msg ), promise );
        }
        else
        {
            ctx.write( msg, promise );
        }
    }
}
