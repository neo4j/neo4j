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
package org.neo4j.bolt.v1.transport;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;

import org.neo4j.bolt.transport.NettyServer;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.helpers.PortBindException;

import static java.util.Arrays.asList;

public class NettyServerTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldGivePortConflictErrorWithPortNumberInIt() throws Throwable
    {
        // Given an occupied port
        int port = 16000;
        try ( ServerSocketChannel ignore = ServerSocketChannel.open().bind( new InetSocketAddress( "localhost", port ) ) )
        {
            final ListenSocketAddress address = new ListenSocketAddress( "localhost", port );

            // Expect
            exception.expect( PortBindException.class );
            exception.expectMessage( "Address localhost:16000 is already in use" );

            // When
            new NettyServer( new NamedThreadFactory( "mythreads" ), asList( protocolOnAddress( address ) ) ).start();
        }
    }

    private NettyServer.ProtocolInitializer protocolOnAddress( final ListenSocketAddress address )
    {
        return new NettyServer.ProtocolInitializer()
        {
            @Override
            public ChannelInitializer<SocketChannel> channelInitializer()
            {
                return new ChannelInitializer<SocketChannel>()
                {
                    @Override
                    public void initChannel( SocketChannel ch ) throws Exception
                    {
                    }
                };
            }

            @Override
            public ListenSocketAddress address()
            {
                return address;
            }
        };
    }
}
