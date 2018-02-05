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
