/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
        if ( e.getMessage() instanceof ChannelBuffer )
        {
            byteCounterMonitor.bytesRead( ((ChannelBuffer) e.getMessage()).readableBytes() );
        }

        super.messageReceived( ctx, e );
    }

    @Override
    public void writeRequested( ChannelHandlerContext ctx, MessageEvent e ) throws Exception
    {
        if ( e.getMessage() instanceof ChannelBuffer )
        {
            byteCounterMonitor.bytesWritten( ((ChannelBuffer) e.getMessage()).readableBytes() );
        }

        super.writeRequested( ctx, e );
    }
}
