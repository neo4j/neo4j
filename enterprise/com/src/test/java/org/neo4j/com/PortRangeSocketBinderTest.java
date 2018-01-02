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
package org.neo4j.com;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelException;
import org.junit.Test;

import java.net.InetSocketAddress;

import org.neo4j.helpers.HostnamePort;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class PortRangeSocketBinderTest
{
    @Test
    public void shouldReThrowExceptionIfCannotBindToPort()
    {
        // given
        HostnamePort localhost = new HostnamePort( "localhost", 9000 );
        ServerBootstrap bootstrap = mock( ServerBootstrap.class );

        when( bootstrap.bind( new InetSocketAddress( "localhost", 9000 ) ) ).thenThrow( new ChannelException() );

        try
        {
            // when
            new PortRangeSocketBinder( bootstrap ).bindToFirstAvailablePortInRange( localhost );
            fail( "should have thrown ChannelException" );
        }
        catch ( ChannelException ignored )
        {
            // expected
        }
    }

    @Test
    public void shouldReThrowExceptionIfCannotBindToAnyOfThePortsInTheRange()
    {
        // given
        HostnamePort localhost = new HostnamePort( "localhost", 9000, 9002 );
        ServerBootstrap bootstrap = mock( ServerBootstrap.class );

        when( bootstrap.bind( new InetSocketAddress( "localhost", 9000 ) ) ).thenThrow(
                new ChannelException("Failed to bind to: 9000") );
        when( bootstrap.bind( new InetSocketAddress( "localhost", 9001 ) ) ).thenThrow(
                new ChannelException("Failed to bind to: 9001") );
        when( bootstrap.bind( new InetSocketAddress( "localhost", 9002 ) ) ).thenThrow(
                new ChannelException("Failed to bind to: 9002") );

        try
        {
            // when
            new PortRangeSocketBinder( bootstrap ).bindToFirstAvailablePortInRange( localhost );
            fail( "should have thrown ChannelException" );
        }
        catch ( ChannelException ex )
        {
            // expected
            assertEquals(2, suppressedExceptions( ex ));
        }
    }

    private int suppressedExceptions( Throwable throwable ) {
        int suppressed = 0;
        for ( Throwable ignored : throwable.getSuppressed() )
        {
            suppressed++;
            suppressed = suppressed + suppressedExceptions( ignored );

        }
        return suppressed;
    }

    @Test
    public void shouldReturnChannelAndSocketIfPortIsFree()
    {
        // given
        HostnamePort localhost = new HostnamePort( "localhost", 9000 );
        ServerBootstrap bootstrap = mock( ServerBootstrap.class );
        Channel channel = mock( Channel.class );

        when( bootstrap.bind( new InetSocketAddress( "localhost", 9000 ) ) ).thenReturn( channel );

        // when
        Connection connection = new PortRangeSocketBinder( bootstrap ).bindToFirstAvailablePortInRange( localhost );

        //then
        assertEquals( channel, connection.getChannel() );
        assertEquals( new InetSocketAddress( "localhost", 9000 ), connection.getSocketAddress() );
    }

    @Test
    public void shouldReturnChannelAndSocketIfAnyPortsAreFree()
    {
        // given
        HostnamePort localhost = new HostnamePort( "localhost", 9000, 9001 );
        ServerBootstrap bootstrap = mock( ServerBootstrap.class );
        Channel channel = mock( Channel.class );

        when( bootstrap.bind( new InetSocketAddress( "localhost", 9000 ) ) ).thenThrow( new ChannelException() );
        when( bootstrap.bind( new InetSocketAddress( "localhost", 9001 ) ) ).thenReturn( channel );

        // when
        Connection connection = new PortRangeSocketBinder( bootstrap ).bindToFirstAvailablePortInRange( localhost );

        //then
        assertEquals( channel, connection.getChannel() );
        assertEquals( new InetSocketAddress( localhost.getHost(), 9001 ), connection.getSocketAddress() );
    }

    @Test
    public void shouldReturnChannelAndSocketIfPortRangeIsInverted()
    {
        // given
        HostnamePort localhost = new HostnamePort( "localhost", 9001, 9000 );
        ServerBootstrap bootstrap = mock( ServerBootstrap.class );
        Channel channel = mock( Channel.class );

        when( bootstrap.bind( new InetSocketAddress( "localhost", 9001 ) ) ).thenReturn( channel );

        // when
        Connection connection = new PortRangeSocketBinder( bootstrap ).bindToFirstAvailablePortInRange( localhost );

        //then
        assertEquals( channel, connection.getChannel() );
        assertEquals( new InetSocketAddress( localhost.getHost(), 9001 ), connection.getSocketAddress() );

    }
}
