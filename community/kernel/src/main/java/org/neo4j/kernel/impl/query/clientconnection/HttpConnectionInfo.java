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
package org.neo4j.kernel.impl.query.clientconnection;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import static org.neo4j.helpers.SocketAddress.format;

/**
 * @see ClientConnectionInfo Parent class for documentation and tests.
 */
public class HttpConnectionInfo extends ClientConnectionInfo
{
    private final String connectionId;
    private final String protocol;
    private final SocketAddress clientAddress;
    private final SocketAddress serverAddress;
    private final String requestPath;

    public HttpConnectionInfo(
            String connectionId,
            String protocol,
            SocketAddress clientAddress,
            SocketAddress serverAddress,
            String requestPath )
    {
        this.connectionId = connectionId;
        this.protocol = protocol;
        this.clientAddress = clientAddress;
        this.serverAddress = serverAddress;
        this.requestPath = requestPath;
    }

    @Override
    public String asConnectionDetails()
    {
        return String.join( "\t", "server-session", protocol, getHostString( clientAddress ), requestPath );
    }

    @Override
    public String protocol()
    {
        return protocol;
    }

    @Override
    public String connectionId()
    {
        return connectionId;
    }

    @Override
    public String clientAddress()
    {
        return format( clientAddress );
    }

    @Override
    public String requestURI()
    {
        return serverAddress == null
               ? requestPath
               : protocol + "://" + format( serverAddress ) + requestPath;
    }

    private static String getHostString( SocketAddress address )
    {
        return address instanceof InetSocketAddress ? ((InetSocketAddress) address).getHostString() : address.toString();
    }
}
