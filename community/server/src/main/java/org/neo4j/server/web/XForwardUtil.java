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
package org.neo4j.server.web;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;

public class XForwardUtil
{
    public static final String X_FORWARD_HOST_HEADER_KEY = "X-Forwarded-Host";
    public static final String X_FORWARD_PROTO_HEADER_KEY = "X-Forwarded-Proto";

    public static String externalUri( String internalUri, String xForwardedHost, String xForwardedProto )
    {
        return externalUri( UriBuilder.fromUri( internalUri ), xForwardedHost, xForwardedProto ).toString();
    }

    public static URI externalUri( URI internalUri, String xForwardedHost, String xForwardedProto )
    {
        return externalUri( UriBuilder.fromUri( internalUri ), xForwardedHost, xForwardedProto );
    }

    private static URI externalUri( UriBuilder builder, String xForwardedHost, String xForwardedProto )
    {
        ForwardedHost forwardedHost = new ForwardedHost( xForwardedHost );
        ForwardedProto forwardedProto = new ForwardedProto( xForwardedProto );

        if ( forwardedHost.isValid )
        {
            builder.host( forwardedHost.getHost() );

            if ( forwardedHost.hasExplicitlySpecifiedPort() )
            {
                builder.port( forwardedHost.getPort() );
            }
        }

        if ( forwardedProto.isValid() )
        {
            builder.scheme( forwardedProto.getScheme() );
        }

        return builder.build();
    }

    private static final class ForwardedHost
    {
        String host;
        int port = -1;
        boolean isValid = false;

        ForwardedHost( String headerValue )
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
            } catch ( IllegalArgumentException ex )
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

        boolean hasExplicitlySpecifiedPort()
        {
            return port >= 0;
        }

        String getHost()
        {
            return host;
        }

        int getPort()
        {
            return port;
        }
    }

    private static final class ForwardedProto
    {
        final String headerValue;

        ForwardedProto( String headerValue )
        {
            if ( headerValue != null )
            {
                this.headerValue = headerValue;
            } else
            {
                this.headerValue = "";
            }
        }

        boolean isValid()
        {
            return headerValue.toLowerCase().equals( "http" ) || headerValue.toLowerCase().equals( "https" );
        }

        String getScheme()
        {
            return headerValue;
        }
    }
}
