/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.ssl;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.neo4j.ssl.SslContextFactory.SslParameters;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.ssl.SslContextFactory.SslParameters.protocols;
import static org.neo4j.ssl.SslContextFactory.makeSslContext;
import static org.neo4j.ssl.SslResourceBuilder.selfSignedKeyId;

@RunWith( Parameterized.class )
public class SslNegotiationTest
{
    private static final String OLD_CIPHER_A = "SSL_RSA_WITH_NULL_SHA";
    private static final String OLD_CIPHER_B = "SSL_RSA_WITH_RC4_128_MD5";
    private static final String OLD_CIPHER_C = "SSL_RSA_WITH_3DES_EDE_CBC_SHA";

    private static final String NEW_CIPHER_A = "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA";
    private static final String NEW_CIPHER_B = "TLS_RSA_WITH_AES_128_CBC_SHA256";
    private static final String NEW_CIPHER_C = "TLS_DHE_RSA_WITH_AES_128_CBC_SHA256";

    private static final String TLSv10 = "TLSv1";
    private static final String TLSv11 = "TLSv1.1";
    private static final String TLSv12 = "TLSv1.2";

    @Rule
    public TestDirectory testDir = TestDirectory.testDirectory();

    @Rule
    public DefaultFileSystemRule fsRule = new DefaultFileSystemRule();

    @Parameterized.Parameter
    public TestSetup setup;

    private SecureServer server;
    private SecureClient client;

    @Parameterized.Parameters( name = "{0}" )
    public static Object[] params()
    {
        return new TestSetup[]{
                // succeeding exact matches
                new TestSetup(
                        protocols( TLSv10 ).ciphers( OLD_CIPHER_A ),
                        protocols( TLSv10 ).ciphers( OLD_CIPHER_A ),
                        true, TLSv10, OLD_CIPHER_A ),
                new TestSetup(
                        protocols( TLSv10 ).ciphers( NEW_CIPHER_A ),
                        protocols( TLSv10 ).ciphers( NEW_CIPHER_A ),
                        true, TLSv10, NEW_CIPHER_A ),
                new TestSetup(
                        protocols( TLSv11 ).ciphers( OLD_CIPHER_A ),
                        protocols( TLSv11 ).ciphers( OLD_CIPHER_A ),
                        true, TLSv11, OLD_CIPHER_A ),
                new TestSetup(
                        protocols( TLSv11 ).ciphers( NEW_CIPHER_A ),
                        protocols( TLSv11 ).ciphers( NEW_CIPHER_A ),
                        true, TLSv11, NEW_CIPHER_A ),
                new TestSetup(
                        protocols( TLSv12 ).ciphers( NEW_CIPHER_A ),
                        protocols( TLSv12 ).ciphers( NEW_CIPHER_A ),
                        true, TLSv12, NEW_CIPHER_A ),

                // failing protocol matches
                new TestSetup(
                        protocols( TLSv10 ).ciphers( OLD_CIPHER_A ),
                        protocols( TLSv11 ).ciphers( OLD_CIPHER_A ),
                        false ),
                new TestSetup(
                        protocols( TLSv11 ).ciphers( OLD_CIPHER_A ),
                        protocols( TLSv10 ).ciphers( OLD_CIPHER_A ),
                        false ),
                new TestSetup(
                        protocols( TLSv11 ).ciphers( NEW_CIPHER_A ),
                        protocols( TLSv12 ).ciphers( NEW_CIPHER_A ),
                        false ),
                new TestSetup(
                        protocols( TLSv12 ).ciphers( NEW_CIPHER_A ),
                        protocols( TLSv11 ).ciphers( NEW_CIPHER_A ),
                        false ),

                // failing cipher matches
                new TestSetup(
                        protocols( TLSv10 ).ciphers( OLD_CIPHER_A ),
                        protocols( TLSv10 ).ciphers( OLD_CIPHER_B ),
                        false ),
                new TestSetup(
                        protocols( TLSv11 ).ciphers( NEW_CIPHER_A ),
                        protocols( TLSv11 ).ciphers( NEW_CIPHER_B ),
                        false ),
                new TestSetup(
                        protocols( TLSv12 ).ciphers( NEW_CIPHER_A ),
                        protocols( TLSv12 ).ciphers( NEW_CIPHER_B ),
                        false ),

                // overlapping cipher success
                new TestSetup(
                        protocols( TLSv10 ).ciphers( OLD_CIPHER_B, OLD_CIPHER_A ),
                        protocols( TLSv10 ).ciphers( OLD_CIPHER_C, OLD_CIPHER_A ),
                        true, TLSv10, OLD_CIPHER_A ),
                new TestSetup(
                        protocols( TLSv11 ).ciphers( NEW_CIPHER_B, NEW_CIPHER_A ),
                        protocols( TLSv11 ).ciphers( NEW_CIPHER_C, NEW_CIPHER_A ),
                        true, TLSv11, NEW_CIPHER_A ),
                new TestSetup(
                        protocols( TLSv12 ).ciphers( NEW_CIPHER_B, NEW_CIPHER_A ),
                        protocols( TLSv12 ).ciphers( NEW_CIPHER_C, NEW_CIPHER_A ),
                        true, TLSv12, NEW_CIPHER_A ),

                // overlapping protocol success
                new TestSetup(
                        protocols( TLSv10, TLSv11 ).ciphers( OLD_CIPHER_A ),
                        protocols( TLSv11, TLSv12 ).ciphers( OLD_CIPHER_A ),
                        true, TLSv11, OLD_CIPHER_A ),
                new TestSetup(
                        protocols( TLSv11, TLSv12 ).ciphers( OLD_CIPHER_A ),
                        protocols( TLSv10, TLSv11 ).ciphers( OLD_CIPHER_A ),
                        true, TLSv11, OLD_CIPHER_A ),
                new TestSetup(
                        protocols( TLSv10, TLSv11, TLSv12 ).ciphers( NEW_CIPHER_B ),
                        protocols( TLSv10, TLSv11, TLSv12 ).ciphers( NEW_CIPHER_B ),
                        true, TLSv12, NEW_CIPHER_B ),
        };
    }

    @After
    public void cleanup()
    {
        if ( client != null )
        {
            client.disconnect();
        }
        if ( server != null )
        {
            server.stop();
        }
    }

    @Test
    public void shouldNegotiateCorrectly() throws Exception
    {
        SslResource sslServerResource = selfSignedKeyId( 0 ).trustKeyId( 1 ).install( testDir.directory( "server" ) );
        SslResource sslClientResource = selfSignedKeyId( 1 ).trustKeyId( 0 ).install( testDir.directory( "client" ) );

        server = new SecureServer( makeSslContext( sslServerResource, true, setup.serverParams ) );

        server.start();
        client = new SecureClient( makeSslContext( sslClientResource, false, setup.clientParams ) );
        client.connect( server.port() );

        assertTrue( client.sslHandshakeFuture().await( 1, MINUTES ) );

        if ( setup.expectedSuccess )
        {
            assertNull( client.sslHandshakeFuture().cause() );
            assertEquals( setup.expectedProtocol, client.protocol() );
            assertEquals( setup.expectedCipher.substring( 4 ), client.ciphers().substring( 4 ) ); // cut away SSL_ or TLS_
        }
        else
        {
            assertNotNull( client.sslHandshakeFuture().cause() );
        }
    }

    private static class TestSetup
    {
        private final SslParameters serverParams;
        private final SslParameters clientParams;

        private final boolean expectedSuccess;
        private final String expectedProtocol;
        private final String expectedCipher;

        private TestSetup( SslParameters serverParams, SslParameters clientParams, boolean expectedSuccess )
        {
            this( serverParams, clientParams, expectedSuccess, null, null );
        }

        private TestSetup( SslParameters serverParams, SslParameters clientParams, boolean expectedSuccess, String expectedProtocol, String expectedCipher )
        {
            this.serverParams = serverParams;
            this.clientParams = clientParams;
            this.expectedSuccess = expectedSuccess;
            this.expectedProtocol = expectedProtocol;
            this.expectedCipher = expectedCipher;
        }

        @Override
        public String toString()
        {
            return "TestSetup{" + "serverParams=" + serverParams + ", clientParams=" + clientParams + ", expectedSuccess=" + expectedSuccess +
                    ", expectedProtocol='" + expectedProtocol + '\'' + ", expectedCipher='" + expectedCipher + '\'' + '}';
        }
    }
}
