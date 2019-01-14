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
package org.neo4j.bolt.v1.transport.integration;

import org.bouncycastle.operator.OperatorCreationException;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Set;

import org.neo4j.bolt.v1.messaging.Neo4jPackV1;
import org.neo4j.bolt.v1.transport.socket.client.SecureSocketConnection;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.ssl.PkiUtils;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.neo4j.bolt.v1.transport.integration.Neo4jWithSocket.DEFAULT_CONNECTOR_KEY;
import static org.neo4j.kernel.configuration.ssl.LegacySslPolicyConfig.tls_certificate_file;
import static org.neo4j.kernel.configuration.ssl.LegacySslPolicyConfig.tls_key_file;

public class CertificatesIT
{
    private static File keyFile;
    private static File certFile;
    private static PkiUtils certFactory;
    private static TransportTestUtil util;

    @Rule
    public Neo4jWithSocket server = new Neo4jWithSocket( getClass(), settings ->
    {
        settings.put( tls_certificate_file.name(), certFile.getAbsolutePath() );
        settings.put( tls_key_file.name(), keyFile.getAbsolutePath() );
        settings.put( new BoltConnector( DEFAULT_CONNECTOR_KEY ).type.name(), "BOLT" );
        settings.put( new BoltConnector( DEFAULT_CONNECTOR_KEY ).enabled.name(), "true" );
        settings.put( new BoltConnector( DEFAULT_CONNECTOR_KEY ).listen_address.name(), "localhost:0" );
    } );

    @Test
    public void shouldUseConfiguredCertificate() throws Exception
    {
        // GIVEN
        SecureSocketConnection connection = new SecureSocketConnection();
        try
        {
            // WHEN
            connection.connect( server.lookupConnector( DEFAULT_CONNECTOR_KEY ) )
                    .send( util.acceptedVersions( 1, 0, 0, 0 ) );

            // THEN
            Set<X509Certificate> certificatesSeen = connection.getServerCertificatesSeen();
            assertThat( certificatesSeen, contains( loadCertificateFromDisk() ) );
        }
        finally
        {
            connection.disconnect();
        }
    }

    private X509Certificate loadCertificateFromDisk() throws CertificateException, IOException
    {
        Certificate[] certificates = certFactory.loadCertificates( certFile );
        assertThat( certificates.length, equalTo( 1 ) );

        return (X509Certificate) certificates[0];
    }

    @BeforeClass
    public static void setUp() throws IOException, GeneralSecurityException, OperatorCreationException
    {
        certFactory = new PkiUtils();
        keyFile = File.createTempFile( "key", "pem" );
        certFile = File.createTempFile( "key", "pem" );
        keyFile.deleteOnExit();
        certFile.deleteOnExit();

        // make sure files are not there
        keyFile.delete();
        certFile.delete();

        certFactory.createSelfSignedCertificate( certFile, keyFile, "my.domain" );

        util = new TransportTestUtil( new Neo4jPackV1() );
    }

}
