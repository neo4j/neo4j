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
package org.neo4j.com;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

import org.neo4j.kernel.monitoring.ByteCounterMonitor;

/**
 * This Netty handler will report through a monitor how many bytes are read/written.
 */
public class MonitorChannelHandler extends SimpleChannelHandler
{
    private ByteCounterMonitor byteCounterMonitor;

    public MonitorChannelHandler( ByteCounterMonitor byteCounterMonitor )
    {
        this.byteCounterMonitor = byteCounterMonitor;
    }

    @Override
    public void messageReceived( ChannelHandlerContext ctx, MessageEvent e ) throws Exception
    {
        if (e.getMessage() instanceof ChannelBuffer )
        {
            byteCounterMonitor.bytesRead( ((ChannelBuffer)e.getMessage()).readableBytes() );
        }

        super.messageReceived( ctx, e );
    }

    @Override
    public void writeRequested( ChannelHandlerContext ctx, MessageEvent e ) throws Exception
    {
        if (e.getMessage() instanceof ChannelBuffer )
        {
            byteCounterMonitor.bytesWritten( ((ChannelBuffer)e.getMessage()).readableBytes() );
        }

        super.writeRequested( ctx, e );
    }
}
