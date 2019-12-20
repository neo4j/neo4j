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
package org.neo4j.server;

import org.junit.After;
import org.junit.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.helpers.TestWebContainer;
import org.neo4j.test.server.ExclusiveWebContainerTestBase;
import org.neo4j.test.server.InsecureTrustManager;

import static java.net.http.HttpRequest.BodyPublishers.noBody;
import static java.net.http.HttpResponse.BodyHandlers.discarding;
import static java.util.Collections.emptyList;
import static javax.ws.rs.core.HttpHeaders.ACCEPT;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.eclipse.jetty.http.HttpHeader.SERVER;
import static org.eclipse.jetty.http.HttpHeader.STRICT_TRANSPORT_SECURITY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.neo4j.server.helpers.CommunityWebContainerBuilder.serverOnRandomPorts;

public class HttpHeadersIT extends ExclusiveWebContainerTestBase
{
    private static final String HSTS_HEADER_VALUE = "max-age=31536000; includeSubDomains; preload";

    private TestWebContainer testWebContainer;

    @After
    public void tearDown() throws Exception
    {
        if ( testWebContainer != null )
        {
            testWebContainer.shutdown();
        }
    }

    @Test
    public void shouldNotSendJettyVersionWithHttpResponseHeaders() throws Exception
    {
        startServer();
        testNoJettyVersionInResponseHeaders( httpUri() );
    }

    @Test
    public void shouldNotSendJettyVersionWithHttpsResponseHeaders() throws Exception
    {
        startServer();
        testNoJettyVersionInResponseHeaders( httpsUri() );
    }

    @Test
    public void shouldNotSendHstsHeaderWithHttpResponse() throws Exception
    {
        startServer( HSTS_HEADER_VALUE );
        assertNull( runRequestAndGetHstsHeaderValue( httpUri() ) );
    }

    @Test
    public void shouldSendHstsHeaderWithHttpsResponse() throws Exception
    {
        startServer( HSTS_HEADER_VALUE );
        assertEquals( HSTS_HEADER_VALUE, runRequestAndGetHstsHeaderValue( httpsUri() ) );
    }

    @Test
    public void shouldNotSendHstsHeaderWithHttpsResponseWhenNotConfigured() throws Exception
    {
        startServer();
        assertNull( runRequestAndGetHstsHeaderValue( httpsUri() ) );
    }

    private void startServer() throws Exception
    {
        startServer( null );
    }

    private void startServer( String hstsValue ) throws Exception
    {
        testWebContainer = buildServer( hstsValue );
    }

    private TestWebContainer buildServer( String hstsValue ) throws Exception
    {
        var builder = serverOnRandomPorts()
                .withHttpsEnabled()
                .usingDataDir( folder.directory( name.getMethodName() ).getAbsolutePath() );

        if ( hstsValue != null )
        {
            builder.withProperty( ServerSettings.http_strict_transport_security.name(), hstsValue );
        }

        return builder.build();
    }

    private URI httpUri()
    {
        return testWebContainer.getBaseUri();
    }

    private URI httpsUri()
    {
        return testWebContainer.httpsUri().orElseThrow( IllegalStateException::new );
    }

    private static void testNoJettyVersionInResponseHeaders( URI baseUri ) throws Exception
    {
        var headers = runRequestAndGetHeaders( baseUri );

        assertNull( headers.get( SERVER.asString() ) ); // no 'Server' header

        for ( var values : headers.values() )
        {
            assertFalse( values.stream().anyMatch( value -> value.toLowerCase().contains( "jetty" ) ) ); // no 'jetty' in other header values
        }
    }

    private static String runRequestAndGetHstsHeaderValue( URI baseUri ) throws Exception
    {
        return runRequestAndGetHeaderValue( baseUri, STRICT_TRANSPORT_SECURITY.asString() );
    }

    private static String runRequestAndGetHeaderValue( URI baseUri, String header ) throws Exception
    {
        var values = runRequestAndGetHeaderValues( baseUri, header );
        if ( values.isEmpty() )
        {
            return null;
        }
        else if ( values.size() == 1 )
        {
            return values.get( 0 );
        }
        else
        {
            throw new IllegalStateException( "Unexpected number of " + STRICT_TRANSPORT_SECURITY.asString() + " header values: " + values );
        }
    }

    private static List<String> runRequestAndGetHeaderValues( URI baseUri, String header ) throws Exception
    {
        return runRequestAndGetHeaders( baseUri ).getOrDefault( header, emptyList() );
    }

    private static Map<String,List<String>> runRequestAndGetHeaders( URI baseUri ) throws Exception
    {
        var uri = baseUri.resolve( txCommitEndpoint() );

        var request = HttpRequest.newBuilder( uri )
                .header( ACCEPT, APPLICATION_JSON )
                .POST( noBody() )
                .build();

        var trustAllSslContext = SSLContext.getInstance( "TLS" );
        trustAllSslContext.init( null, new TrustManager[]{new InsecureTrustManager()}, null );

        var client = HttpClient.newBuilder()
                .sslContext( trustAllSslContext )
                .connectTimeout( Duration.ofMinutes( 1 ) )
                .build();

        var response = client.sendAsync( request, discarding() ).get( 1, TimeUnit.MINUTES );

        assertEquals( 200, response.statusCode() );

        return response.headers().map();
    }
}
