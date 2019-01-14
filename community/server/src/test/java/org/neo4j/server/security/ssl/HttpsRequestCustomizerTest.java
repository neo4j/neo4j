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
package org.neo4j.server.security.ssl;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpChannel;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpInput;
import org.eclipse.jetty.server.HttpOutput;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.junit.Test;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.server.configuration.ServerSettings;

import static org.eclipse.jetty.http.HttpHeader.STRICT_TRANSPORT_SECURITY;
import static org.eclipse.jetty.http.HttpScheme.HTTPS;
import static org.eclipse.jetty.server.HttpConfiguration.Customizer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HttpsRequestCustomizerTest
{
    @Test
    public void shouldSetRequestSchemeToHttps()
    {
        Customizer customizer = newCustomizer();
        Request request = mock( Request.class );

        customize( customizer, request );

        verify( request ).setScheme( HTTPS.asString() );
    }

    @Test
    public void shouldAddHstsHeaderWhenConfigured()
    {
        String configuredValue = "max-age=3600; includeSubDomains";
        Customizer customizer = newCustomizer( configuredValue );
        Request request = newRequest();

        customize( customizer, request );

        String receivedValue = request.getResponse().getHttpFields().get( STRICT_TRANSPORT_SECURITY );
        assertEquals( configuredValue, receivedValue );
    }

    @Test
    public void shouldNotAddHstsHeaderWhenNotConfigured()
    {
        Customizer customizer = newCustomizer();
        Request request = newRequest();

        customize( customizer, request );

        String hstsValue = request.getResponse().getHttpFields().get( STRICT_TRANSPORT_SECURITY );
        assertNull( hstsValue );
    }

    private static void customize( Customizer customizer, Request request )
    {
        customizer.customize( mock( Connector.class ), new HttpConfiguration(), request );
    }

    private static Request newRequest()
    {
        HttpChannel channel = mock( HttpChannel.class );
        Response response = new Response( channel, mock( HttpOutput.class ) );
        Request request = new Request( channel, mock( HttpInput.class ) );
        when( channel.getRequest() ).thenReturn( request );
        when( channel.getResponse() ).thenReturn( response );
        return request;
    }

    private static Customizer newCustomizer()
    {
        return newCustomizer( null );
    }

    private static Customizer newCustomizer( String hstsValue )
    {
        Config config = Config.defaults( ServerSettings.http_strict_transport_security, hstsValue );
        return new HttpsRequestCustomizer( config );
    }
}
