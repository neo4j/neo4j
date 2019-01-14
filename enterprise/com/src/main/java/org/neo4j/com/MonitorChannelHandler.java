/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
