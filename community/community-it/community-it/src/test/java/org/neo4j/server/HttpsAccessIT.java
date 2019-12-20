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
import org.junit.Before;
import org.junit.Test;

import java.security.SecureRandom;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;

import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.server.helpers.CommunityWebContainerBuilder;
import org.neo4j.server.helpers.TestWebContainer;
import org.neo4j.test.PortUtils;
import org.neo4j.test.server.ExclusiveWebContainerTestBase;
import org.neo4j.test.server.HTTP;
import org.neo4j.test.server.InsecureTrustManager;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.server.helpers.CommunityWebContainerBuilder.serverOnRandomPorts;
import static org.neo4j.test.server.HTTP.GET;
import static org.neo4j.test.server.HTTP.POST;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

public class HttpsAccessIT extends ExclusiveWebContainerTestBase
{
    private SSLSocketFactory originalSslSocketFactory;
    private TestWebContainer testWebContainer;

    @Before
    public void setUp()
    {
        originalSslSocketFactory = HttpsURLConnection.getDefaultSSLSocketFactory();
    }

    @After
    public void tearDown()
    {
        HttpsURLConnection.setDefaultSSLSocketFactory( originalSslSocketFactory );
        testWebContainer.shutdown();
    }

    @Test
    public void serverShouldSupportSsl() throws Exception
    {
        startServer();

        assertThat( GET( testWebContainer.httpsUri().get().toString() ).status(), is( 200 ) );
        assertThat( GET( testWebContainer.getBaseUri().toString() ).status(), is( 200 ) );
    }

    @Test
    public void txEndpointShouldReplyWithHttpsWhenItReturnsURLs() throws Exception
    {
        startServer();

        String baseUri = testWebContainer.getBaseUri().toString();
        HTTP.Response response = POST( baseUri + txEndpoint(), quotedJson( "{'statements':[]}" ) );

        assertThat( response.location(), startsWith( baseUri ) );
        assertThat( response.get( "commit" ).asText(), startsWith( baseUri ) );
    }

    @Test
    public void shouldExposeBaseUriWhenHttpEnabledAndHttpsDisabled() throws Exception
    {
        startServer( true, false );
        shouldInstallConnector( "http" );
        shouldExposeCorrectSchemeInDiscoveryService( "http" );
    }

    @Test
    public void shouldExposeBaseUriWhenHttpDisabledAndHttpsEnabled() throws Exception
    {
        startServer( false, true );
        shouldInstallConnector( "https" );
        shouldExposeCorrectSchemeInDiscoveryService( "https" );
    }

    private void shouldInstallConnector( String scheme )
    {
        var uri = testWebContainer.getBaseUri();
        assertEquals( scheme, uri.getScheme() );
        HostnamePort expectedHostPort = PortUtils.getConnectorAddress( testWebContainer.getDefaultDatabase(), scheme );
        assertEquals( expectedHostPort.getHost(), uri.getHost() );
        assertEquals( expectedHostPort.getPort(), uri.getPort() );
    }

    private void shouldExposeCorrectSchemeInDiscoveryService( String scheme ) throws Exception
    {
        var response = GET( testWebContainer.getBaseUri().toString() );

        assertThat( response.status(), is( 200 ) );
        assertThat( response.stringFromContent( "transaction" ), startsWith( scheme + "://" ) );
    }

    private void startServer() throws Exception
    {
        startServer( true, true );
    }

    private void startServer( boolean httpEnabled, boolean httpsEnabled ) throws Exception
    {
        CommunityWebContainerBuilder serverBuilder = serverOnRandomPorts().usingDataDir( folder.directory( name.getMethodName() ).getAbsolutePath() );
        if ( !httpEnabled )
        {
            serverBuilder.withHttpDisabled();
        }
        if ( httpsEnabled )
        {
            serverBuilder.withHttpsEnabled();
        }

        testWebContainer = serverBuilder.build();

        // Because we are generating a non-CA-signed certificate, we need to turn off verification in the client.
        // This is ironic, since there is no proper verification on the CA side in the first place, but I digress.
        TrustManager[] trustAllCerts = {new InsecureTrustManager()};

        // Install the all-trusting trust manager
        SSLContext sc = SSLContext.getInstance( "TLS" );
        sc.init( null, trustAllCerts, new SecureRandom() );
        HttpsURLConnection.setDefaultSSLSocketFactory( sc.getSocketFactory() );
    }
}
