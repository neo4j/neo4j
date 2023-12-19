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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleStateEvent;
import org.junit.Test;

import java.net.InetSocketAddress;

import org.neo4j.helpers.AdvertisedSocketAddress;

import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IdleChannelReaperHandlerTest
{
    @Test
    public void shouldRemoveChannelViaCallback()
    {
        // given
        AdvertisedSocketAddress address = new AdvertisedSocketAddress( "localhost", 1984 );
        ReconnectingChannels channels = new ReconnectingChannels();
        channels.putIfAbsent( address, mock( ReconnectingChannel.class) );

        IdleChannelReaperHandler reaper = new IdleChannelReaperHandler( channels );

        final InetSocketAddress socketAddress = address.socketAddress();

        final Channel channel = mock( Channel.class );
        when( channel.remoteAddress() ).thenReturn( socketAddress );

        final ChannelHandlerContext context = mock( ChannelHandlerContext.class );
        when( context.channel() ).thenReturn( channel );

        // when
        reaper.userEventTriggered( context, IdleStateEvent.ALL_IDLE_STATE_EVENT );

        // then
        assertNull( channels.get( address ) );
    }
}
