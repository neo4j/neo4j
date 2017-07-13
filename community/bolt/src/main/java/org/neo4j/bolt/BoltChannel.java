/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

package org.neo4j.bolt;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

import java.net.SocketAddress;

/**
 * A channel through which Bolt messaging can occur.
 */
public class BoltChannel implements AutoCloseable, BoltConnectionDescriptor
{
    private final ChannelHandlerContext channelHandlerContext;
    private final BoltMessageLogger messageLogger;

    public static BoltChannel open( ChannelHandlerContext channelHandlerContext,
                                    BoltMessageLogger messageLogger )
    {
        return new BoltChannel( channelHandlerContext, messageLogger );
    }

    private BoltChannel( ChannelHandlerContext channelHandlerContext,
                         BoltMessageLogger messageLogger )
    {
        this.channelHandlerContext = channelHandlerContext;
        this.messageLogger = messageLogger;
        messageLogger.serverEvent( "OPEN" );
    }

    public ChannelHandlerContext channelHandlerContext()
    {
        return channelHandlerContext;
    }

    public Channel rawChannel()
    {
        return channelHandlerContext.channel();
    }

    public BoltMessageLogger log()
    {
        return messageLogger;
    }

    @Override
    public void close()
    {
        messageLogger.serverEvent( "CLOSE" );
        try
        {
            rawChannel().close();
        }
        catch ( Exception e )
        {
            //
        }
    }

    @Override
    public SocketAddress clientAddress()
    {
        return channelHandlerContext.channel().remoteAddress();
    }

    @Override
    public SocketAddress serverAddress()
    {
        return channelHandlerContext.channel().localAddress();
    }
}
