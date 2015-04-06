/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.ndp.transport.http;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.Methods;
import io.undertow.util.StatusCodes;

import static java.lang.String.format;

public class SessionCollectionHandler implements HttpHandler
{
    private final SessionRegistry sessionRegistry;

    public SessionCollectionHandler( SessionRegistry sessionRegistry )
    {
        this.sessionRegistry = sessionRegistry;
    }

    @Override
    public void handleRequest( final HttpServerExchange exchange ) throws Exception
    {
//        System.err.println( "< " + exchange.getRequestMethod().toString() + " " + exchange.getRequestPath() );
        if ( exchange.getRequestMethod().equals( Methods.POST ) )
        {
            String sessionKey = sessionRegistry.create();
            exchange.setResponseCode( StatusCodes.CREATED );
            exchange.getResponseHeaders().put( Headers.LOCATION,
                    format( "/session/%s", sessionKey ) );
        }
        else
        {
            exchange.setResponseCode( StatusCodes.METHOD_NOT_ALLOWED );
        }
//        System.err.println( "> " + exchange.getResponseCode() );
    }
}
