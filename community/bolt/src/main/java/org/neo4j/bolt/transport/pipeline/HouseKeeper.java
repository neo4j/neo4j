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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.concurrent.EventExecutorGroup;

import org.neo4j.bolt.runtime.BoltConnection;
import org.neo4j.helpers.Exceptions;
import org.neo4j.logging.Log;

public class HouseKeeper extends ChannelInboundHandlerAdapter
{
    private final BoltConnection connection;
    private final Log log;
    private boolean failed;

    public HouseKeeper( BoltConnection connection, Log log )
    {
        this.connection = connection;
        this.log = log;
    }

    @Override
    public void channelInactive( ChannelHandlerContext ctx )
    {
        connection.stop();
    }

    @Override
    public void exceptionCaught( ChannelHandlerContext ctx, Throwable cause )
    {
        if ( failed || isShuttingDown( ctx ) )
        {
            return;
        }
        failed = true; // log only the first exception to not pollute the log

        try
        {
            // Netty throws a NativeIoException on connection reset - directly importing that class
            // caused a host of linking errors, because it depends on JNI to work. Hence, we just
            // test on the message we know we'll get.
            if ( Exceptions.contains( cause, e -> e.getMessage().contains( "Connection reset by peer" ) ) )
            {
                log.warn( "Fatal error occurred when handling a client connection, " +
                        "remote peer unexpectedly closed connection: %s", ctx.channel() );
            }
            else
            {
                log.error( "Fatal error occurred when handling a client connection: " + ctx.channel(),
                        cause );
            }
        }
        finally
        {
            ctx.close();
        }
    }

    private static boolean isShuttingDown( ChannelHandlerContext ctx )
    {
        EventExecutorGroup eventLoopGroup = ctx.executor().parent();
        return eventLoopGroup != null && eventLoopGroup.isShuttingDown();
    }
}
