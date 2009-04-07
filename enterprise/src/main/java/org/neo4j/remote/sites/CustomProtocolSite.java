/*
 * Copyright 2008 Network Engine for Objects in Lund AB [neotechnology.com]
 * 
 * This program is free software: you can redistribute it and/or modify
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.remote.sites;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import org.neo4j.remote.CustomNeoServer;
import org.neo4j.remote.RemoteConnection;
import org.neo4j.remote.RemoteSite;

/**
 * A {@link RemoteSite} that connects to a Neo server through a custom
 * communication protocol.
 * 
 * @author Tobias Ivarsson
 */
public final class CustomProtocolSite implements RemoteSite
{
    private final SocketAddress remote;
    private final boolean useSSL;

    /**
     * Create a new {@link RemoteSite} that connects to a Neo server through a
     * custom communication protocol.
     * @param host
     *            The server host to connect to.
     * @param port
     *            The port to connect to on the server.
     * @param useSSL
     *            <code>true</code> if the server is using SSL to secure the
     *            communication.
     */
    public CustomProtocolSite( String host, int port, boolean useSSL )
    {
        this.remote = new InetSocketAddress( host, port );
        this.useSSL = useSSL;
    }

    public RemoteConnection connect()
    {
        return CustomNeoServer.connect( remote, useSSL );
    }

    public RemoteConnection connect( String username, String password )
    {
        return CustomNeoServer.connect( remote, useSSL, username, password );
    }
}
