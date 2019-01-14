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
package org.neo4j.bolt.v1.transport.socket.client;

import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

import org.neo4j.test.rule.SuppressOutput;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WebSocketConnectionTest
{

    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Rule
    public SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    @Test
    public void shouldNotThrowAnyExceptionWhenDataReceivedBeforeClose() throws Throwable
    {
        // Given
        WebSocketClient client = mock( WebSocketClient.class );
        WebSocketConnection conn = new WebSocketConnection( client );
        when( client.isStopped() ).thenReturn( true );

        byte[] data = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

        // When
        conn.onWebSocketBinary( data, 0, 10 );
        conn.recv( 10 );

        // Then
        // no exception
    }

    @Test
    public void shouldThrowIOExceptionWhenNotEnoughDataReceivedBeforeClose() throws Throwable
    {
        // Given
        WebSocketClient client = mock( WebSocketClient.class );
        WebSocketConnection conn = new WebSocketConnection( client );
        when( client.isStopped() ).thenReturn( true, true );

        byte[] data = {0, 1, 2, 3};

        // When && Then
        conn.onWebSocketBinary( data, 0, 4 );

        expectedException.expect( IOException.class );
        expectedException.expectMessage( "Connection closed while waiting for data from the server." );
        conn.recv( 10 );
    }
}
