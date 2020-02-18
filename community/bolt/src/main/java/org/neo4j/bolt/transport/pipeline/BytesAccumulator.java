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
package org.neo4j.bolt.transport.pipeline;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import org.neo4j.bolt.runtime.BoltConnectionFatality;

import static java.lang.String.format;

public class BytesAccumulator extends ChannelInboundHandlerAdapter
{
    private final long limit;
    private long count;

    public BytesAccumulator( long limit )
    {
        this.limit = limit;
    }

    @Override
    public void channelRead( ChannelHandlerContext ctx, Object msg ) throws Exception
    {
        ByteBuf buf = (ByteBuf) msg;
        count += buf.readableBytes();
        if ( count < 0 )
        {
            count = Long.MAX_VALUE;
        }
        if ( count > limit )
        {
            ctx.channel().close();
            throw new BoltConnectionFatality( format(
                    "A connection '%s' is terminated because too many inbound bytes received " +
                    "before the client is authenticated. Max bytes allowed: %s. Bytes received: %s.",
                    ctx.channel(), limit, count ), null );
        }

        super.channelRead( ctx, msg );
    }
}
