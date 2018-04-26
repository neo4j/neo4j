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

import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpChannel;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpInput;
import org.eclipse.jetty.server.HttpOutput;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.junit.Test;

import java.util.Map;

import org.neo4j.kernel.configuration.Config;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static org.eclipse.jetty.http.HttpHeader.STRICT_TRANSPORT_SECURITY;
import static org.eclipse.jetty.http.HttpScheme.HTTPS;
import static org.eclipse.jetty.server.HttpConfiguration.Customizer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.server.configuration.ServerSettings.http_public_key_pins;
import static org.neo4j.server.configuration.ServerSettings.http_strict_transport_security;
import static org.neo4j.server.security.ssl.HttpsRequestCustomizer.PUBLIC_KEY_PINS_HTTP_HEADER;

public class HttpsRequestCustomizerTest
{
    private static final Map<String,String> settingNameToHttpHeader = unmodifiableMap( stringMap(
            http_strict_transport_security.name(), STRICT_TRANSPORT_SECURITY.asString(),
            http_public_key_pins.name(), PUBLIC_KEY_PINS_HTTP_HEADER
    ) );

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
        testHeadersPresence( stringMap( http_strict_transport_security.name(), "max-age=3600; includeSubDomains" ) );
    }

    @Test
    public void shouldNotAddHstsHeaderWhenNotConfigured()
    {
        testHeaderNotPresentWhenConfigurationMissing( STRICT_TRANSPORT_SECURITY.asString() );
    }

    @Test
    public void shouldAddHpkpHeaderWhenConfigured()
    {
        testHeadersPresence( stringMap( http_public_key_pins.name(), "pin-sha256=\"cUPcTAZWKaASuYWhhneDttWpY3oBAkE3h2+soZS7sWs=\"; " +
                                                                     "pin-sha256=\"M8HztCzM3elUxkcjR2S5P4hhyBNf6lHkmjAHKhpGPWE=\"; " +
                                                                     "max-age=5184000; includeSubDomains;" ) );
    }

    @Test
    public void shouldNotAddHpkpHeaderWhenNotConfigured()
    {
        testHeaderNotPresentWhenConfigurationMissing( PUBLIC_KEY_PINS_HTTP_HEADER );
    }

    @Test
    public void shouldAddBothHstsAndHpkpHeadersWhenConfigured()
    {
        testHeadersPresence( stringMap(
                http_strict_transport_security.name(), "max-age=31536000; includeSubDomains; preload",
                http_public_key_pins.name(), "pin-sha256=\"cUPcTAZWKaASuYWhhneDttWpY3oBAkE3h2+soZS7sWs=\"; " +
                                             "pin-sha256=\"M8HztCzM3elUxkcjR2S5P4hhyBNf6lHkmjAHKhpGPWE=\"; " +
                                             "max-age=5184000; includeSubDomains; " +
                                             "report-uri=\"https://www.example.org/hpkp-report\"" ) );
    }

    private static void testHeadersPresence( Map<String,String> settingsWithValues )
    {
        Customizer customizer = newCustomizer( settingsWithValues );
        Request request = newRequest();

        customize( customizer, request );

        HttpFields httpFields = request.getResponse().getHttpFields();
        for ( Map.Entry<String,String> entry : settingsWithValues.entrySet() )
        {
            String settingName = entry.getKey();
            String settingValue = entry.getValue();
            String headerName = settingNameToHttpHeader.get( settingName );
            assertEquals( settingValue, httpFields.get( headerName ) );
        }
    }

    private static void testHeaderNotPresentWhenConfigurationMissing( String header )
    {
        Customizer customizer = newCustomizer();
        Request request = newRequest();

        customize( customizer, request );

        assertNull( request.getResponse().getHttpFields().get( header ) );
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
        return newCustomizer( emptyMap() );
    }

    private static Customizer newCustomizer( Map<String,String> settingsWithValues )
    {
        Config config = Config.defaults( settingsWithValues );
        return new HttpsRequestCustomizer( config );
    }
}
