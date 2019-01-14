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
package org.neo4j.bolt;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
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
    private final String connector = "default";
    @Mock
    private Channel channel;
    @Mock
    private BoltMessageLogger messageLogger;

    @Test
    public void shouldLogWhenOpened()
    {
        BoltChannel boltChannel = BoltChannel.open( connector, channel, messageLogger );
        assertNotNull( boltChannel );

        verify( messageLogger ).serverEvent( "OPEN" );
    }

    @Test
    public void shouldLogWhenClosed()
    {
        Channel channel = channelMock( true );
        BoltChannel boltChannel = BoltChannel.open( connector, channel, messageLogger );
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
        BoltChannel boltChannel = BoltChannel.open( connector, channel, messageLogger );

        boltChannel.close();

        verify( channel ).close();
    }

    @Test
    public void shouldNotCloseUnderlyingChannelWhenItIsClosed()
    {
        Channel channel = channelMock( false );
        BoltChannel boltChannel = BoltChannel.open( connector, channel, messageLogger );

        boltChannel.close();

        verify( channel, never() ).close();
    }

    private static Channel channelMock( boolean open )
    {
        Channel channel = mock( Channel.class );
        when( channel.isOpen() ).thenReturn( open );
        ChannelFuture channelFuture = mock( ChannelFuture.class );
        when( channel.close() ).thenReturn( channelFuture );
        return channel;
    }
}
