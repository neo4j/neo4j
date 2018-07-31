/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BoltChannelTest
{
    private final Channel channel = mock( Channel.class );

    @Test
    void shouldCloseUnderlyingChannelWhenItIsOpen()
    {
        Channel channel = channelMock( true );
        BoltChannel boltChannel = new BoltChannel( "bolt-1", "bolt", channel );

        boltChannel.close();

        verify( channel ).close();
    }

    @Test
    void shouldNotCloseUnderlyingChannelWhenItIsClosed()
    {
        Channel channel = channelMock( false );
        BoltChannel boltChannel = new BoltChannel( "bolt-1", "bolt", channel );

        boltChannel.close();

        verify( channel, never() ).close();
    }

    @Test
    void shouldHaveId()
    {
        BoltChannel boltChannel = new BoltChannel( "bolt-42", "bolt", channel );

        assertEquals( "bolt-42", boltChannel.id() );
    }

    @Test
    void shouldHaveConnector()
    {
        BoltChannel boltChannel = new BoltChannel( "bolt-1", "my-bolt", channel );

        assertEquals( "my-bolt", boltChannel.connector() );
    }

    @Test
    void shouldHaveConnectTime()
    {
        BoltChannel boltChannel = new BoltChannel( "bolt-1", "my-bolt", channel );

        assertThat( boltChannel.connectTime(), greaterThan( 0L ) );
    }

    @Test
    void shouldHaveUser()
    {
        BoltChannel boltChannel = new BoltChannel( "bolt-1", "my-bolt", channel );

        assertNull( boltChannel.user() );
        boltChannel.updateUser( "hello" );
        assertEquals( "hello", boltChannel.user() );
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
