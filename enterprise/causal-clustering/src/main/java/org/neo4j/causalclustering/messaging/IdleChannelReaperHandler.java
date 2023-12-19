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
package org.neo4j.causalclustering.messaging;

import java.net.InetSocketAddress;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleStateEvent;

import org.neo4j.helpers.AdvertisedSocketAddress;

public class IdleChannelReaperHandler extends ChannelDuplexHandler
{
    private ReconnectingChannels channels;

    public IdleChannelReaperHandler( ReconnectingChannels channels )
    {
        this.channels = channels;
    }

    @Override
    public void userEventTriggered( ChannelHandlerContext ctx, Object evt )
    {
        if ( evt instanceof IdleStateEvent && evt == IdleStateEvent.ALL_IDLE_STATE_EVENT )
        {
            final InetSocketAddress socketAddress = (InetSocketAddress) ctx.channel().remoteAddress();
            final AdvertisedSocketAddress address =
                    new AdvertisedSocketAddress( socketAddress.getHostName(), socketAddress.getPort() );

            channels.remove( address );
        }
    }
}
