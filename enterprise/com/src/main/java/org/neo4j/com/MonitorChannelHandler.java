/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.com;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;

/**
 * This Netty handler will report through a monitor how many bytes are read/written.
 */
public class MonitorChannelHandler extends ChannelDuplexHandler
{
    private ByteCounterMonitor byteCounterMonitor;

    public MonitorChannelHandler( ByteCounterMonitor byteCounterMonitor )
    {
        this.byteCounterMonitor = byteCounterMonitor;
    }

    @Override
    public void channelRead( ChannelHandlerContext ctx, Object msg ) throws Exception
    {
        if (msg instanceof ByteBuf )
        {
            byteCounterMonitor.bytesRead( ((ByteBuf)msg).readableBytes() );
        }

        super.channelRead( ctx, msg );
    }

    @Override
    public void write( ChannelHandlerContext ctx, Object msg, ChannelPromise promise ) throws Exception
    {
        if (msg instanceof ByteBuf )
        {
            byteCounterMonitor.bytesWritten( ((ByteBuf)msg).readableBytes() );
        }

        super.write( ctx, msg, promise );
    }
}