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
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import org.neo4j.bolt.logging.BoltMessageLogger;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class BoltChannelTest
{
    @Mock
    private ChannelHandlerContext channelHandlerContext;
    @Mock
    private BoltMessageLogger messageLogger;

    @Test
    public void shouldLogWhenOpened()
    {
        BoltChannel boltChannel = BoltChannel.open( channelHandlerContext, messageLogger );
        assertNotNull( boltChannel );

        verify( messageLogger ).serverEvent( "OPEN" );
    }

    @Test
    public void shouldLogWhenClosed()
    {
        Channel channel = channelMock( true );
        when( channelHandlerContext.channel() ).thenReturn( channel );
        BoltChannel boltChannel = BoltChannel.open( channelHandlerContext, messageLogger );
        assertNotNull( boltChannel );

        boltChannel.close();

        InOrder inOrder = inOrder( messageLogger );
        inOrder.verify( messageLogger ).serverEvent( "OPEN" );
        inOrder.verify( messageLogger ).serverEvent( "CLOSE" );
    }

    @Test
    public void shouldCloseUnderlyingChannelWhenItIsOpen()
    {
        Channel channel = channelMock( true );
        when( channelHandlerContext.channel() ).thenReturn( channel );
        BoltChannel boltChannel = BoltChannel.open( channelHandlerContext, messageLogger );

        boltChannel.close();

        verify( channel ).close();
    }

    @Test
    public void shouldNotCloseUnderlyingChannelWhenItIsClosed()
    {
        Channel channel = channelMock( false );
        when( channelHandlerContext.channel() ).thenReturn( channel );
        BoltChannel boltChannel = BoltChannel.open( channelHandlerContext, messageLogger );

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
