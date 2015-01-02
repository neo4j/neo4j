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
package org.neo4j.server.rest.repr;

import java.net.URI;
import java.net.URISyntaxException;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.ext.Provider;

import com.sun.jersey.api.core.HttpContext;
import org.neo4j.server.database.InjectableProvider;

@Provider
public final class OutputFormatProvider extends InjectableProvider<OutputFormat>
{
    private final RepresentationFormatRepository repository;

    public OutputFormatProvider( RepresentationFormatRepository repository )
    {
        super( OutputFormat.class );
        this.repository = repository;
    }

    @Override
    public OutputFormat getValue( HttpContext context )
    {
        try
        {
            return repository.outputFormat( context.getRequest()
                    .getAcceptableMediaTypes(), getBaseUri( context ), context.getRequest().getRequestHeaders() );
        }
        catch ( MediaTypeNotSupportedException e )
        {
            throw new WebApplicationException( Response.status( Status.NOT_ACCEPTABLE )
                    .entity( e.getMessage() )
                    .build() );
        }
        catch ( URISyntaxException e )
        {
            throw new WebApplicationException( Response.status( Status.INTERNAL_SERVER_ERROR )
                    .entity( e.getMessage() )
                    .build() );
        }
    }

    private URI getBaseUri( HttpContext context ) throws URISyntaxException
    {
        UriBuilder builder = UriBuilder.fromUri( context.getRequest().getBaseUri() );

        ForwardedHost forwardedHost = new ForwardedHost( context.getRequest().getHeaderValue( "X-Forwarded-Host" ) );
        ForwardedProto xForwardedProto = new ForwardedProto(
                context.getRequest().getHeaderValue( "X-Forwarded-Proto" ) );


        if ( forwardedHost.isValid )
        {
            builder.host( forwardedHost.getHost() );

            if ( forwardedHost.hasExplicitlySpecifiedPort() )
            {
                builder.port( forwardedHost.getPort() );
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
