/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.server.rest.web;

import javax.servlet.http.HttpServletRequest;

import org.neo4j.kernel.impl.query.QuerySession;
import org.neo4j.kernel.impl.query.TransactionalContext;

import static java.lang.String.format;

public class ServerQuerySession extends QuerySession
{
    private final HttpServletRequest request;

    public ServerQuerySession( HttpServletRequest request, TransactionalContext transactionalContext )
    {
        super( transactionalContext );
        this.request = request;
    }

    @Override
    public String toString()
    {
        return request == null ?
               "server-session" :
               format("server-session\thttp\t%s\t%s", request.getRemoteAddr(), request.getRequestURI() );
    }
}
