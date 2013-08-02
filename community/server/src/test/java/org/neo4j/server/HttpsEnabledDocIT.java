/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import org.junit.After;
import org.junit.Test;

import org.neo4j.test.server.ExclusiveServerTestBase;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import static org.neo4j.server.helpers.CommunityServerBuilder.server;

public class HttpsEnabledDocIT extends ExclusiveServerTestBase
{

    private CommunityNeoServer server;

    @After
    public void stopTheServer()
    {
        server.stop();
    }

    @Test
    public void serverShouldSupportSsl() throws Exception
    {
        server = server().withHttpsEnabled()
                .usingDatabaseDir( folder.getRoot().getAbsolutePath() )
                .build();
        server.start();

        assertThat( server.getHttpsEnabled(), is( true ) );

        trustAllSslCerts();

        Client client = Client.create();
        ClientResponse r = client.resource( server.httpsUri() ).get( ClientResponse.class );

        assertThat( r.getStatus(), is( 200 ) );
    }

    @Test
    public void webadminShouldBeRetrievableViaSsl() throws Exception
    {
        server = server().withHttpsEnabled()
                .usingDatabaseDir( folder.getRoot().getAbsolutePath() )
                .build();
        server.start();

        assertThat( server.getHttpsEnabled(), is( true ) );

        trustAllSslCerts();

        Client client = Client.create();
        ClientResponse r = client.resource( server.httpsUri().toASCIIString() + "webadmin/" ).get( ClientResponse
                .class );

        assertThat( r.getStatus(), is( 200 ) );
    }

    private void trustAllSslCerts()
    {

        TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager()
        {
            public void checkClientTrusted( X509Certificate[] arg0, String arg1 )
                    throws CertificateException
            {
            }

            public void checkServerTrusted( X509Certificate[] arg0, String arg1 )
                    throws CertificateException
            {
            }

            public X509Certificate[] getAcceptedIssuers()
            {
                return null;
            }
        }};

        // Install the all-trusting trust manager
        try
        {
            SSLContext sc = SSLContext.getInstance( "TLS" );
            sc.init( null, trustAllCerts, new SecureRandom() );
            HttpsURLConnection.setDefaultSSLSocketFactory( sc.getSocketFactory() );
        }
        catch ( Exception e )
        {
            ;
        }
    }

}
