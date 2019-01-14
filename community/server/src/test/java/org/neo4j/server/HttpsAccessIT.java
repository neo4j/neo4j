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

import org.apache.http.client.utils.URIBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.security.SecureRandom;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.configuration.ConnectorPortRegister;
import org.neo4j.server.helpers.CommunityServerBuilder;
import org.neo4j.test.server.ExclusiveServerTestBase;
import org.neo4j.test.server.HTTP;
import org.neo4j.test.server.InsecureTrustManager;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.neo4j.server.helpers.CommunityServerBuilder.serverOnRandomPorts;
import static org.neo4j.test.server.HTTP.GET;
import static org.neo4j.test.server.HTTP.POST;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

public class HttpsAccessIT extends ExclusiveServerTestBase
{
    private SSLSocketFactory originalSslSocketFactory;
    private CommunityNeoServer server;

    @Before
    public void setUp()
    {
        originalSslSocketFactory = HttpsURLConnection.getDefaultSSLSocketFactory();
    }

    @After
    public void tearDown()
    {
        HttpsURLConnection.setDefaultSSLSocketFactory( originalSslSocketFactory );
        server.stop();
    }

    @Test
    public void serverShouldSupportSsl() throws Exception
    {
        startServer();

        assertThat( GET( httpsUri() ).status(), is( 200 ) );
        assertThat( GET(server.baseUri().toString()).status(), is( 200 ) );
    }

    @Test
    public void txEndpointShouldReplyWithHttpsWhenItReturnsURLs() throws Exception
    {
        startServer();

        String baseUri = server.baseUri().toString();
        HTTP.Response response = POST( baseUri + "db/data/transaction", quotedJson( "{'statements':[]}" ) );

        assertThat( response.location(), startsWith( baseUri ) );
        assertThat( response.get( "commit" ).asText(), startsWith( baseUri ) );
    }

    @Test
    public void shouldExposeBaseUriWhenHttpEnabledAndHttpsDisabled() throws Exception
    {
        startServer( true, false );

        URI uri = server.baseUri();

        assertEquals( "http", uri.getScheme() );
        HostnamePort expectedHostPort = addressForConnector( "http" );
        assertEquals( expectedHostPort.getHost(), uri.getHost() );
        assertEquals( expectedHostPort.getPort(), uri.getPort() );
    }

    @Test
    public void shouldExposeBaseUriWhenHttpDisabledAndHttpsEnabled() throws Exception
    {
        startServer( false, true );

        URI uri = server.baseUri();

        assertEquals( "https", uri.getScheme() );
        HostnamePort expectedHostPort = addressForConnector( "https" );
        assertEquals( expectedHostPort.getHost(), uri.getHost() );
        assertEquals( expectedHostPort.getPort(), uri.getPort() );
    }

    private void startServer() throws Exception
    {
        startServer( true, true );
    }

    private void startServer( boolean httpEnabled, boolean httpsEnabled ) throws Exception
    {
        CommunityServerBuilder serverBuilder = serverOnRandomPorts().usingDataDir( folder.directory( name.getMethodName() ).getAbsolutePath() );
        if ( !httpEnabled )
        {
            serverBuilder.withHttpDisabled();
        }
        if ( httpsEnabled )
        {
            serverBuilder.withHttpsEnabled();
        }

        server = serverBuilder.build();
        server.start();

        // Because we are generating a non-CA-signed certificate, we need to turn off verification in the client.
        // This is ironic, since there is no proper verification on the CA side in the first place, but I digress.
        TrustManager[] trustAllCerts = {new InsecureTrustManager()};

        // Install the all-trusting trust manager
        SSLContext sc = SSLContext.getInstance( "TLS" );
        sc.init( null, trustAllCerts, new SecureRandom() );
        HttpsURLConnection.setDefaultSSLSocketFactory( sc.getSocketFactory() );
    }

    private String httpsUri() throws Exception
    {
        HostnamePort hostPort = addressForConnector( "https" );
        assertNotNull( hostPort );

        return new URIBuilder()
                .setScheme( "https" )
                .setHost( hostPort.getHost() )
                .setPort( hostPort.getPort() )
                .build()
                .toString();
    }

    private HostnamePort addressForConnector( String name )
    {
        DependencyResolver resolver = server.database.getGraph().getDependencyResolver();
        ConnectorPortRegister portRegister = resolver.resolveDependency( ConnectorPortRegister.class );
        return portRegister.getLocalAddress( name );
    }
}
