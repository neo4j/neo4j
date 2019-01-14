/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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

        when( bootstrap.bind( new InetSocketAddress( "localhost", 9000 ) ) )
                .thenThrow( new ChannelException( "Failed to bind to: 9000" ) );
        when( bootstrap.bind( new InetSocketAddress( "localhost", 9001 ) ) )
                .thenThrow( new ChannelException( "Failed to bind to: 9001" ) );
        when( bootstrap.bind( new InetSocketAddress( "localhost", 9002 ) ) )
                .thenThrow( new ChannelException( "Failed to bind to: 9002" ) );

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

    private int suppressedExceptions( Throwable throwable )
    {
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
