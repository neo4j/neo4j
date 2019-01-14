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

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

import org.neo4j.test.server.ExclusiveServerTestBase;
import org.neo4j.test.server.HTTP;
import org.neo4j.test.server.InsecureTrustManager;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.neo4j.server.helpers.CommunityServerBuilder.serverOnRandomPorts;
import static org.neo4j.test.server.HTTP.GET;
import static org.neo4j.test.server.HTTP.POST;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

public class HttpsAccessIT extends ExclusiveServerTestBase
{
    private CommunityNeoServer server;

    @After
    public void stopTheServer()
    {
        server.stop();
    }

    @Before
    public void startServer() throws NoSuchAlgorithmException, KeyManagementException, IOException
    {
        server = serverOnRandomPorts().withHttpsEnabled()
                .usingDataDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .build();

        // Because we are generating a non-CA-signed certificate, we need to turn off verification in the client.
        // This is ironic, since there is no proper verification on the CA side in the first place, but I digress.
        TrustManager[] trustAllCerts = {new InsecureTrustManager()};

        // Install the all-trusting trust manager
        SSLContext sc = SSLContext.getInstance( "TLS" );
        sc.init( null, trustAllCerts, new SecureRandom() );
        HttpsURLConnection.setDefaultSSLSocketFactory( sc.getSocketFactory() );
    }

    @Test
    public void serverShouldSupportSsl()
    {
        // When
        server.start();

        // Then
        assertThat( server.httpsIsEnabled(), is( true ) );
        assertThat( GET(server.baseUri().toString()).status(), is( 200 ) );
    }

    @Test
    public void txEndpointShouldReplyWithHttpsWhenItReturnsURLs() throws Exception
    {
        // Given
        server.start();

        // When
        String baseUri = server.baseUri().toString();
        HTTP.Response response = POST( baseUri + "db/data/transaction", quotedJson( "{'statements':[]}" ) );

        // Then
        assertThat( response.location(), startsWith( baseUri ) );
        assertThat( response.get( "commit" ).asText(), startsWith( baseUri ) );
    }
}
