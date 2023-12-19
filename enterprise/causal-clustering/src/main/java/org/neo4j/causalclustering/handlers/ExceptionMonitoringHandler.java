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
package org.neo4j.causalclustering.handlers;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

public class ExceptionMonitoringHandler  extends ChannelHandlerAdapter
{
    public interface Monitor
    {
        void exceptionCaught( Channel channel, Throwable cause );
    }

    private final Monitor monitor;

    public ExceptionMonitoringHandler( Monitor monitor )
    {
        this.monitor = monitor;
    }

    @Override
    public void exceptionCaught( ChannelHandlerContext ctx, Throwable cause )
    {
        if ( ctx != null )
        {
            monitor.exceptionCaught( ctx.channel(), cause );
            ctx.fireExceptionCaught( cause );
        }
        else
        {
            monitor.exceptionCaught( null, cause );
        }
    }
}
