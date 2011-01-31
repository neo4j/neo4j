/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.remote.transports;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import org.neo4j.remote.CustomGraphDatabaseServer;
import org.neo4j.remote.RemoteConnection;
import org.neo4j.remote.ConnectionTarget;

/**
 * A {@link ConnectionTarget} that connects to a graph database server through a custom
 * communication protocol.
 * 
 * @author Tobias Ivarsson
 */
final class CustomProtocolTarget implements ConnectionTarget
{
    private final SocketAddress remote;
    private final boolean useSSL;

    /**
     * Create a new {@link ConnectionTarget} that connects to a graph database server through a
     * custom communication protocol.
     * @param host
     *            The server host to connect to.
     * @param port
     *            The port to connect to on the server.
     * @param useSSL
     *            <code>true</code> if the server is using SSL to secure the
     *            communication.
     */
    public CustomProtocolTarget( String host, int port, boolean useSSL )
    {
        this.remote = new InetSocketAddress( host, port );
        this.useSSL = useSSL;
    }

    public RemoteConnection connect()
    {
        return CustomGraphDatabaseServer.connect( remote, useSSL );
    }

    public RemoteConnection connect( String username, String password )
    {
        return CustomGraphDatabaseServer.connect( remote, useSSL, username, password );
    }
}
