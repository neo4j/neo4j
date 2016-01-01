/**
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
package org.neo4j.server.web;

import java.net.URI;
import java.net.URISyntaxException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.UriBuilder;

import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerRequestFilter;

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.status;

/**
 * Changes the value of the base and request URIs to match the provided
 * X-Forwarded-Host and X-Forwarded-Proto header values.
 * <p/>
 * In doing so, it means Neo4j server can use those URIs as if they were the
 * actual request URIs.
 */
public class XForwardFilter implements ContainerRequestFilter
{
    private static final String X_FORWARD_HOST_HEADER_KEY = "X-Forwarded-Host";
    private static final String X_FORWARD_PROTO_HEADER_KEY = "X-Forwarded-Proto";

    @Override
    public ContainerRequest filter( ContainerRequest containerRequest )
    {
        try
        {
            containerRequest.setUris( assembleExternalUri( containerRequest.getBaseUri(), containerRequest ),
                    assembleExternalUri( containerRequest.getRequestUri(), containerRequest ) );
        }
        catch ( URISyntaxException e )
        {
            throw new WebApplicationException( status( INTERNAL_SERVER_ERROR )
                    .entity( e.getMessage() )
                    .build() );
        }
        return containerRequest;
    }

    private URI assembleExternalUri( URI theUri, ContainerRequest containerRequest ) throws URISyntaxException
    {
        UriBuilder builder = UriBuilder.fromUri( theUri );

        ForwardedHost xForwardedHost = new ForwardedHost( containerRequest.getHeaderValue( X_FORWARD_HOST_HEADER_KEY
        ) );
        ForwardedProto xForwardedProto = new ForwardedProto( containerRequest.getHeaderValue(
                X_FORWARD_PROTO_HEADER_KEY ) );

        if ( xForwardedHost.isValid )
        {
            builder.host( xForwardedHost.getHost() );

            if ( xForwardedHost.hasExplicitlySpecifiedPort() )
            {
                builder.port( xForwardedHost.getPort() );
            }
        }

        if ( xForwardedProto.isValid() )
        {
            builder.scheme( xForwardedProto.getScheme() );
        }

        return builder.build();
    }

    private class ForwardedHost
    {
        private String host;
        private int port = -1;
        private boolean isValid = false;

        public ForwardedHost( String headerValue )
        {
            if ( headerValue == null )
            {
                this.isValid = false;
                return;
            }

            String firstHostInXForwardedHostHeader = headerValue.split( "," )[0].trim();

            try
            {
                UriBuilder.fromUri( firstHostInXForwardedHostHeader ).build();
            }
            catch ( IllegalArgumentException ex )
            {
                this.isValid = false;
                return;
            }

            String[] strings = firstHostInXForwardedHostHeader.split( ":" );
            if ( strings.length > 0 )
            {
                this.host = strings[0];
                isValid = true;
            }
            if ( strings.length > 1 )
            {
                this.port = Integer.valueOf( strings[1] );
                isValid = true;
            }
            if ( strings.length > 2 )
            {
                this.isValid = false;
            }
        }

        public boolean hasExplicitlySpecifiedPort()
        {
            return port >= 0;
        }

        public String getHost()
        {
            return host;
        }

        public int getPort()
        {
            return port;
        }
    }

    private class ForwardedProto
    {
        private final String headerValue;

        public ForwardedProto( String headerValue )
        {
            if ( headerValue != null )
            {
                this.headerValue = headerValue;
            }
            else
            {
                this.headerValue = "";
            }
        }

        public boolean isValid()
        {
            return headerValue.toLowerCase().equals( "http" ) || headerValue.toLowerCase().equals( "https" );
        }

        public String getScheme()
        {
            return headerValue;
        }
    }
}
