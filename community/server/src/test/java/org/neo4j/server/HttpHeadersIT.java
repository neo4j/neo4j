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
package org.neo4j.server;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientRequest;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.client.urlconnection.HTTPSProperties;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.List;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.ws.rs.core.MultivaluedMap;

import org.neo4j.test.server.ExclusiveServerTestBase;
import org.neo4j.test.server.InsecureTrustManager;

import static com.sun.jersey.client.urlconnection.HTTPSProperties.PROPERTY_HTTPS_PROPERTIES;
import static org.eclipse.jetty.http.HttpHeader.SERVER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.neo4j.server.helpers.CommunityServerBuilder.serverOnRandomPorts;

public class HttpHeadersIT extends ExclusiveServerTestBase
{
    private CommunityNeoServer server;

    @Before
    public void setUp() throws Exception
    {
        server = serverOnRandomPorts().withHttpsEnabled()
                .usingDataDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .build();

        server.start();
    }

    @After
    public void tearDown() throws Exception
    {
        if ( server != null )
        {
            server.stop();
        }
    }

    @Test
    public void shouldNotSendJettyVersionWithHttpResponseHeaders() throws Exception
    {
        URI httpUri = server.baseUri();
        testNoJettyVersionInResponseHeaders( httpUri );
    }

    @Test
    public void shouldNotSendJettyVersionWithHttpsResponseHeaders() throws Exception
    {
        URI httpsUri = server.httpsUri().orElseThrow( IllegalStateException::new );
        testNoJettyVersionInResponseHeaders( httpsUri );
    }

    private static void testNoJettyVersionInResponseHeaders( URI baseUri ) throws Exception
    {
        URI uri = baseUri.resolve( "db/data/transaction/commit" );
        ClientRequest request = createClientRequest( uri );

        ClientResponse response = createClient().handle( request );

        assertEquals( 200, response.getStatus() );

        MultivaluedMap<String,String> headers = response.getHeaders();
        assertNull( headers.get( SERVER.name() ) ); // no 'Server' header
        for ( List<String> values : headers.values() )
        {
            assertFalse( values.stream().anyMatch( value -> value.toLowerCase().contains( "jetty" ) ) ); // no 'jetty' in other header values
        }
    }

    private static ClientRequest createClientRequest( URI uri )
    {
        return ClientRequest.create()
                .header( "Accept", "application/json" )
                .build( uri, "POST" );
    }

    private static Client createClient() throws Exception
    {
        HostnameVerifier hostnameVerifier = HttpsURLConnection.getDefaultHostnameVerifier();
        ClientConfig config = new DefaultClientConfig();
        SSLContext ctx = SSLContext.getInstance( "TLS" );
        ctx.init( null, new TrustManager[]{new InsecureTrustManager()}, null );
        config.getProperties().put( PROPERTY_HTTPS_PROPERTIES, new HTTPSProperties( hostnameVerifier, ctx ) );
        return Client.create( config );
    }
}
