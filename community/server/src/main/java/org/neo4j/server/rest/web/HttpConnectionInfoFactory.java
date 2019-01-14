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
import javax.servlet.http.HttpServletRequest;

import org.neo4j.kernel.impl.query.clientconnection.ClientConnectionInfo;
import org.neo4j.kernel.impl.query.clientconnection.HttpConnectionInfo;

import static javax.ws.rs.core.HttpHeaders.USER_AGENT;

public class HttpConnectionInfoFactory
{
    private HttpConnectionInfoFactory()
    {
    }

    public static ClientConnectionInfo create( HttpServletRequest request )
    {
        return new HttpConnectionInfo(
                request.getScheme(),
                request.getHeader( USER_AGENT ),
                new InetSocketAddress( request.getRemoteAddr(), request.getRemotePort() ),
                new InetSocketAddress( request.getServerName(), request.getServerPort() ),
                request.getRequestURI() );
    }
}
