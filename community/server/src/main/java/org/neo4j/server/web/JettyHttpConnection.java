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
package org.neo4j.server.web;

import org.eclipse.jetty.http.HttpCompliance;
import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnection;

import java.net.SocketAddress;

import org.neo4j.kernel.api.net.TrackedNetworkConnection;

/**
 * Extension of the default Jetty {@link HttpConnection} which contains additional properties like id, connect time, user, etc.
 * It is bound of the Jetty worker thread when active.
 *
 * @see HttpConnection#getCurrentConnection()
 */
public class JettyHttpConnection extends HttpConnection implements TrackedNetworkConnection
{
    private final String id;
    private final long connectTime;

    private volatile String user;

    public JettyHttpConnection( String id, HttpConfiguration config, Connector connector, EndPoint endPoint,
            HttpCompliance compliance, boolean recordComplianceViolations )
    {
        super( config, connector, endPoint, compliance, recordComplianceViolations );
        this.id = id;
        this.connectTime = System.currentTimeMillis();
    }

    @Override
    public String id()
    {
        return id;
    }

    @Override
    public long connectTime()
    {
        return connectTime;
    }

    @Override
    public String connector()
    {
        return getConnector().getName();
    }

    @Override
    public SocketAddress serverAddress()
    {
        return getEndPoint().getLocalAddress();
    }

    @Override
    public SocketAddress clientAddress()
    {
        return getEndPoint().getRemoteAddress();
    }

    @Override
    public String user()
    {
        return user;
    }

    @Override
    public void updateUser( String user )
    {
        this.user = user;
    }

    public static void updateUserForCurrentConnection( String user )
    {
        HttpConnection currentConnection = HttpConnection.getCurrentConnection();
        if ( currentConnection instanceof JettyHttpConnection )
        {
            ((JettyHttpConnection) currentConnection).updateUser( user );
        }
    }
}
