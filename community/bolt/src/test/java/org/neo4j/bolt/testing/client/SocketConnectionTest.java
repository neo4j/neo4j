/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.bolt.testing.client;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

import org.neo4j.internal.helpers.HostnamePort;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SocketConnectionTest
{
    @Test
    void shouldOnlyReadOnceIfAllBytesAreRead() throws Exception
    {
        // GIVEN
        Socket socket = mock( Socket.class );
        InputStream stream = mock( InputStream.class );
        when(socket.getInputStream()).thenReturn( stream );
        when(stream.read( any(byte[].class), anyInt(), anyInt() )).thenReturn( 4 );
        SocketConnection connection = new SocketConnection( socket );
        connection.connect( new HostnamePort( "my.domain", 1234 ) );

        // WHEN
        connection.recv( 4 );

        // THEN
        verify( stream ).read( any( byte[].class ), anyInt(), anyInt() );
    }

    @Test
    void shouldOnlyReadUntilAllBytesAreRead() throws Exception
    {
        // GIVEN
        Socket socket = mock( Socket.class );
        InputStream stream = mock( InputStream.class );
        when(socket.getInputStream()).thenReturn( stream );
        when(stream.read( any(byte[].class), anyInt(), anyInt() ))
                .thenReturn( 4 )
                .thenReturn( 4 )
                .thenReturn( 2 )
                .thenReturn( -1 );
        SocketConnection connection = new SocketConnection( socket );
        connection.connect( new HostnamePort( "my.domain", 1234 ) );

        // WHEN
        connection.recv( 10 );

        // THEN
        verify(stream, times(3)).read( any(byte[].class), anyInt(), anyInt() );
    }

    @Test
    void shouldThrowIfNotEnoughBytesAreRead() throws Exception
    {
        // GIVEN
        Socket socket = mock( Socket.class );
        InputStream stream = mock( InputStream.class );
        when(socket.getInputStream()).thenReturn( stream );
        when(stream.read( any(byte[].class), anyInt(), anyInt() ))
                .thenReturn( 4 )
                .thenReturn( -1 );
        SocketConnection connection = new SocketConnection( socket );
        connection.connect( new HostnamePort( "my.domain", 1234 ) );

        // WHEN
        assertThrows(IOException.class, () -> connection.recv( 10 ) );
    }
}
