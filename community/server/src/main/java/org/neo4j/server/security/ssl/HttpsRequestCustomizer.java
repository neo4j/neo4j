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
package org.neo4j.server.security.ssl;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpScheme;
import org.eclipse.jetty.http.PreEncodedHttpField;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.Request;

import org.neo4j.kernel.configuration.Config;

import static org.eclipse.jetty.http.HttpHeader.STRICT_TRANSPORT_SECURITY;
import static org.neo4j.server.configuration.ServerSettings.http_public_key_pins;
import static org.neo4j.server.configuration.ServerSettings.http_strict_transport_security;

public class HttpsRequestCustomizer implements HttpConfiguration.Customizer
{
    public static final String PUBLIC_KEY_PINS_HTTP_HEADER = "Public-Key-Pins";

    private final HttpField hstsResponseField;
    private final HttpField hpkpResponseField;

    public HttpsRequestCustomizer( Config config )
    {
        hstsResponseField = createHstsResponseField( config );
        hpkpResponseField = createHpkpResponseField( config );
    }

    @Override
    public void customize( Connector connector, HttpConfiguration channelConfig, Request request )
    {
        request.setScheme( HttpScheme.HTTPS.asString() );

        addResponseFieldIfConfigured( request, hstsResponseField );
        addResponseFieldIfConfigured( request, hpkpResponseField );
    }

    private static void addResponseFieldIfConfigured( Request request, HttpField field )
    {
        if ( field != null )
        {
            request.getResponse().getHttpFields().add( field );
        }
    }

    private static HttpField createHstsResponseField( Config config )
    {
        String configuredValue = config.get( http_strict_transport_security );
        if ( StringUtils.isBlank( configuredValue ) )
        {
            return null;
        }
        return new PreEncodedHttpField( STRICT_TRANSPORT_SECURITY, configuredValue );
    }

    private static HttpField createHpkpResponseField( Config config )
    {
        String configuredValue = config.get( http_public_key_pins );
        if ( StringUtils.isBlank( configuredValue ) )
        {
            return null;
        }
        // unable to use PreEncodedHttpField because of a bug, see https://github.com/eclipse/jetty.project/pull/2488
        // should be possible to handle HSTS and HPKP field creation after bug is fixed in jetty version we depend on
        return new HttpField( PUBLIC_KEY_PINS_HTTP_HEADER, configuredValue );
    }
}
