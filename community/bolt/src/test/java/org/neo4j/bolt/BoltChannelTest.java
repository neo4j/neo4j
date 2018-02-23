/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;

import org.neo4j.bolt.logging.BoltMessageLogger;
import org.neo4j.test.extension.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith( MockitoExtension.class )
class BoltChannelTest
{
    private final String connector = "default";
    @Mock
    private ChannelHandlerContext channelHandlerContext;
    @Mock
    private BoltMessageLogger messageLogger;

    @Test
    void shouldLogWhenOpened()
    {
        BoltChannel boltChannel = BoltChannel.open( connector, channelHandlerContext, messageLogger );
        assertNotNull( boltChannel );

        verify( messageLogger ).serverEvent( "OPEN" );
    }

    @Test
    void shouldLogWhenClosed()
    {
        Channel channel = channelMock( true );
        when( channelHandlerContext.channel() ).thenReturn( channel );
        BoltChannel boltChannel = BoltChannel.open( connector, channelHandlerContext, messageLogger );
        assertNotNull( boltChannel );

        boltChannel.close();

        InOrder inOrder = inOrder( messageLogger );
        inOrder.verify( messageLogger ).serverEvent( "OPEN" );
        inOrder.verify( messageLogger ).serverEvent( "CLOSE" );
    }

    @Test
    void shouldCloseUnderlyingChannelWhenItIsOpen()
    {
        Channel channel = channelMock( true );
        when( channelHandlerContext.channel() ).thenReturn( channel );
        BoltChannel boltChannel = BoltChannel.open( connector, channelHandlerContext, messageLogger );

        boltChannel.close();

        verify( channel ).close();
    }

    @Test
    void shouldNotCloseUnderlyingChannelWhenItIsClosed()
    {
        Channel channel = channelMock( false );
        when( channelHandlerContext.channel() ).thenReturn( channel );
        BoltChannel boltChannel = BoltChannel.open( connector, channelHandlerContext, messageLogger );

        boltChannel.close();

        verify( channel, never() ).close();
    }

    private static Channel channelMock( boolean open )
    {
        Channel channel = mock( Channel.class );
        when( channel.isOpen() ).thenReturn( open );
        when( channel.close() ).thenReturn( mock( ChannelFuture.class ) );
        return channel;
    }
}
