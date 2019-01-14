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
package org.neo4j.server.rest.web;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import javax.servlet.http.HttpServletRequest;

import org.neo4j.kernel.impl.query.clientconnection.ClientConnectionInfo;
import org.neo4j.kernel.impl.query.clientconnection.HttpConnectionInfo;
import org.neo4j.server.web.JettyHttpConnection;

public class HttpConnectionInfoFactory
{
    private HttpConnectionInfoFactory()
    {
    }

    public static ClientConnectionInfo create( HttpServletRequest request )
    {
        String connectionId;
        String protocol = request.getScheme();
        SocketAddress clientAddress;
        SocketAddress serverAddress;
        String requestURI = request.getRequestURI();

        JettyHttpConnection connection = JettyHttpConnection.getCurrentJettyHttpConnection();
        if ( connection != null )
        {
            connectionId = connection.id();
            clientAddress = connection.clientAddress();
            serverAddress = connection.serverAddress();
        }
        else
        {
            // connection is unknown, connection object can't be extracted or is missing from the Jetty thread-local
            // get all the available information directly from the request
            connectionId = null;
            clientAddress = new InetSocketAddress( request.getRemoteAddr(), request.getRemotePort() );
            serverAddress = new InetSocketAddress( request.getServerName(), request.getServerPort() );
        }

        return new HttpConnectionInfo( connectionId, protocol, clientAddress, serverAddress, requestURI );
    }
}
