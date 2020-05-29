/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.bolt.transport;

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

import org.neo4j.bolt.testing.TransportTestUtil;
import org.neo4j.bolt.testing.client.SecureSocketConnection;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.configuration.ssl.ClientAuth;
import org.neo4j.configuration.ssl.SslPolicyConfig;
import org.neo4j.ssl.PkiUtils;
import org.neo4j.test.ssl.SelfSignedCertificateFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.OPTIONAL;
import static org.neo4j.configuration.ssl.SslPolicyScope.BOLT;
import static org.neo4j.configuration.ssl.SslPolicyScope.CLUSTER;

public class RoutingConnectorCertificatesIT
{
    private static File externalKeyFile;
    private static File externalCertFile;
    private static File internalKeyFile;
    private static File internalCertFile;
    private static SelfSignedCertificateFactory certFactory;
    private static TransportTestUtil util;

    @Rule
    public Neo4jWithSocket server = new Neo4jWithSocket( getClass(), settings ->
    {
        SslPolicyConfig externalPolicy = SslPolicyConfig.forScope( BOLT );
        settings.put( externalPolicy.enabled, true );
        settings.put( externalPolicy.public_certificate, externalCertFile.toPath().toAbsolutePath() );
        settings.put( externalPolicy.private_key, externalKeyFile.toPath().toAbsolutePath() );

        SslPolicyConfig internalPolicy = SslPolicyConfig.forScope( CLUSTER );
        settings.put( internalPolicy.enabled, true );
        settings.put( internalPolicy.client_auth, ClientAuth.NONE );
        settings.put( internalPolicy.public_certificate, internalCertFile.toPath().toAbsolutePath() );
        settings.put( internalPolicy.private_key, internalKeyFile.toPath().toAbsolutePath() );

        settings.put( BoltConnector.enabled, true );
        settings.put( BoltConnector.encryption_level, OPTIONAL );
        settings.put( BoltConnector.listen_address, new SocketAddress( "localhost", 0 ) );

        settings.put( GraphDatabaseSettings.routing_enabled, true );
        settings.put( GraphDatabaseSettings.routing_listen_address, new SocketAddress( "localhost", 0 ) );
    } );

    @Test
    public void shouldUseConfiguredCertificate() throws Exception
    {
        // GIVEN
        SecureSocketConnection externalConnection = new SecureSocketConnection();
        SecureSocketConnection internalConnection = new SecureSocketConnection();
        try
        {
            // WHEN
            externalConnection.connect( server.lookupConnector( BoltConnector.NAME ) )
                    .send( util.defaultAcceptedVersions() );
            internalConnection.connect( server.lookupConnector( BoltConnector.INTERNAL_NAME ) )
                              .send( util.defaultAcceptedVersions() );

            // THEN
            Set<X509Certificate> externalCertificatesSeen = externalConnection.getServerCertificatesSeen();
            Set<X509Certificate> internalCertificatesSeen = internalConnection.getServerCertificatesSeen();

            assertThat( externalCertificatesSeen ).isNotEqualTo( internalCertificatesSeen );

            assertThat( externalCertificatesSeen ).containsExactly( loadCertificateFromDisk( externalCertFile ) );
            assertThat( internalCertificatesSeen ).containsExactly( loadCertificateFromDisk( internalCertFile ) );
        }
        finally
        {
            externalConnection.disconnect();
            internalConnection.disconnect();
        }
    }

    private X509Certificate loadCertificateFromDisk( File certFile ) throws CertificateException, IOException
    {
        Certificate[] certificates = PkiUtils.loadCertificates( certFile );
        assertThat( certificates.length ).isEqualTo( 1 );

        return (X509Certificate) certificates[0];
    }

    @BeforeClass
    public static void setUp() throws IOException, GeneralSecurityException, OperatorCreationException
    {
        certFactory = new SelfSignedCertificateFactory();
        externalKeyFile = File.createTempFile( "key", "pem" );
        externalCertFile = File.createTempFile( "key", "pem" );
        externalKeyFile.deleteOnExit();
        externalCertFile.deleteOnExit();

        // make sure files are not there
        externalKeyFile.delete();
        externalCertFile.delete();

        certFactory.createSelfSignedCertificate( externalCertFile, externalKeyFile, "my.domain" );

        internalKeyFile = File.createTempFile( "key", "pem" );
        internalCertFile = File.createTempFile( "key", "pem" );
        internalKeyFile.deleteOnExit();
        internalCertFile.deleteOnExit();

        // make sure files are not there
        internalKeyFile.delete();
        internalCertFile.delete();

        certFactory.createSelfSignedCertificate( internalCertFile, internalKeyFile, "my.domain" );

        util = new TransportTestUtil();
    }

}
