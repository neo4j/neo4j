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

import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.client.WebSocketClient;

import java.net.URI;
import java.util.function.Supplier;

public class SecureWebSocketConnection extends WebSocketConnection
{
    public SecureWebSocketConnection()
    {
        super( createTestClientSupplier(), address -> URI.create( "wss://" + address.getHost() + ":" + address.getPort() ) );
    }

    private static Supplier<WebSocketClient> createTestClientSupplier()
    {
        return () ->
        {
            SslContextFactory sslContextFactory = new SslContextFactory( /* trustall= */ true );
            /* remove extra filters added by jetty on cipher suites */
            sslContextFactory.setExcludeCipherSuites();
            return new WebSocketClient( sslContextFactory );
        };
    }
}
