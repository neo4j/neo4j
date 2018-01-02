/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerResponse;
import com.sun.jersey.spi.container.ContainerResponseFilter;

/**
 * This filter adds the header "Access-Control-Allow-Origin : *" to all
 * responses that goes through it. This allows modern browsers to do cross-site
 * requests to us via javascript.
 */
public class AllowAjaxFilter implements ContainerResponseFilter
{
    private static final String ACCESS_CONTROL_ALLOW_ORIGIN = "Access-Control-Allow-Origin";
    private static final String ACCESS_CONTROL_ALLOW_METHODS = "Access-Control-Allow-Methods";
    private static final String ACCESS_CONTROL_ALLOW_HEADERS = "Access-Control-Allow-Headers";
    private static final String ACCESS_CONTROL_REQUEST_METHOD = "access-control-request-method";
    private static final String ACCESS_CONTROL_REQUEST_HEADERS = "access-control-request-headers";

    public ContainerResponse filter( ContainerRequest request, ContainerResponse response )
    {

        response.getHttpHeaders()
                .add( ACCESS_CONTROL_ALLOW_ORIGIN, "*" );

        // Allow all forms of requests
        if ( request.getRequestHeaders()
                .containsKey( ACCESS_CONTROL_REQUEST_METHOD ) )
        {

            for ( String value : request.getRequestHeaders()
                    .get( ACCESS_CONTROL_REQUEST_METHOD ) )
            {
                response.getHttpHeaders()
                        .add( ACCESS_CONTROL_ALLOW_METHODS, value );
            }
        }

        // Allow all types of headers
        if ( request.getRequestHeaders()
                .containsKey( ACCESS_CONTROL_REQUEST_HEADERS ) )
        {
            for ( String value : request.getRequestHeaders()
                    .get( ACCESS_CONTROL_REQUEST_HEADERS ) )
            {
                response.getHttpHeaders()
                        .add( ACCESS_CONTROL_ALLOW_HEADERS, value );
            }
        }

        return response;
    }

}
