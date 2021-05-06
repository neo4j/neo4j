/*
 * Copyright (c) "Neo4j"
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

import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.ocsp.BasicOCSPResp;
import org.bouncycastle.cert.ocsp.BasicOCSPRespBuilder;
import org.bouncycastle.cert.ocsp.CertificateID;
import org.bouncycastle.cert.ocsp.CertificateStatus;
import org.bouncycastle.cert.ocsp.OCSPResp;
import org.bouncycastle.cert.ocsp.OCSPRespBuilder;
import org.bouncycastle.cert.ocsp.RespID;
import org.bouncycastle.cert.ocsp.jcajce.JcaBasicOCSPRespBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.DigestCalculatorProvider;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.operator.jcajce.JcaDigestCalculatorProviderBuilder;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHandler;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.neo4j.bolt.testing.TransportTestUtil;
import org.neo4j.bolt.testing.client.CertConfiguredSecureSocketConnection;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.configuration.ssl.SslPolicyConfig;
import org.neo4j.ssl.PkiUtils;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.ssl.CertificateChainFactory;

import static java.util.Objects.isNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.OPTIONAL;
import static org.neo4j.configuration.ssl.SslPolicyScope.BOLT;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
class OcspStaplingIT
{
    private static Path endUserKeyFile;
    private static Path endUserCertFile;
    private static Path intKeyFile;
    private static Path rootCertFile;
    private static Path rootKeyFile;

    private static TransportTestUtil util;

    @Inject
    private Neo4jWithSocket server;

    @BeforeEach
    void setup( TestInfo testInfo ) throws IOException
    {
        server.setConfigure( settings ->
                             {
                                 SslPolicyConfig policy = SslPolicyConfig.forScope( BOLT );
                                 settings.put( policy.enabled, true );
                                 settings.put( policy.public_certificate, endUserCertFile.toAbsolutePath() );
                                 settings.put( policy.private_key, endUserKeyFile.toAbsolutePath() );
                                 settings.put( BoltConnector.enabled, true );
                                 settings.put( BoltConnector.encryption_level, OPTIONAL );
                                 settings.put( BoltConnector.ocsp_enabled, true );
                                 settings.put( BoltConnector.listen_address, new SocketAddress( "localhost", 0 ) );
                             } );

        server.init( testInfo );
    }

    @Test
    void shouldReturnCertificatesWithStapledOcspResponses() throws Exception
    {
        // Given
        X509Certificate rootCertificate = loadCertificateFromDisk();
        CertConfiguredSecureSocketConnection connection =
                new CertConfiguredSecureSocketConnection( rootCertificate );

        // When
        connection.connect( server.lookupConnector( BoltConnector.NAME ) )
                  .send( util.defaultAcceptedVersions() );

        // Then
        Set<X509Certificate> certificatesSeen = connection.getServerCertificatesSeen();
        List<BigInteger> certificateSerialNumbersSeen = certificatesSeen.stream().map( X509Certificate::getSerialNumber ).collect( Collectors.toList() );
        assertThat( certificatesSeen.size() ).isEqualTo( 3 );

        Set<BasicOCSPResp> ocspResponsesSeen = connection.getSeenOcspResponses();
        assertThat( ocspResponsesSeen.size() ).isEqualTo( 2 );

        // checking all responses come back OK ( null means the certificate hasn't been revoked! )
        assertTrue( ocspResponsesSeen.stream().allMatch( basicOCSPResp -> isNull( basicOCSPResp.getResponses()[0].getCertStatus() ) ) );
        // responses match the certificates seen
        assertTrue( ocspResponsesSeen.stream().allMatch(
                basicOCSPResp -> certificateSerialNumbersSeen.contains( basicOCSPResp.getResponses()[0].getCertID().getSerialNumber() ) ) );
    }

    private static X509Certificate loadCertificateFromDisk() throws CertificateException, IOException
    {
        X509Certificate[] certificates = PkiUtils.loadCertificates( rootCertFile );
        assertThat( certificates.length ).isEqualTo( 1 );

        return certificates[0];
    }

    @BeforeAll
    static void setup() throws Exception
    {
        int jettyServerPortNo = startOcspMock();

        endUserKeyFile = Files.createTempFile( "end_key", "pem" );
        endUserCertFile = Files.createTempFile( "end_key", "pem" );

        intKeyFile = Files.createTempFile( "int_key", "pem" );
        Path intCertFile = Files.createTempFile( "int_key", "pem" );

        rootCertFile = Files.createTempFile( "root_key", "pem" );
        rootKeyFile = Files.createTempFile( "root_key", "pem" );

        // make sure files are not there
        Files.delete( endUserKeyFile );
        Files.delete( endUserCertFile );
        Files.delete( rootKeyFile );
        Files.delete( rootCertFile );
        Files.delete( intKeyFile );
        Files.delete( intCertFile );

        var bouncyCastleProvider = new BouncyCastleProvider();
        CertificateChainFactory.createCertificateChain( endUserCertFile, endUserKeyFile, intCertFile, intKeyFile,
                                            rootCertFile, rootKeyFile, jettyServerPortNo, bouncyCastleProvider );

        util = new TransportTestUtil();
    }

    private static int startOcspMock() throws Exception
    {
        Server jettyServer = new Server( 0 );
        ServletHandler handler = new ServletHandler();
        jettyServer.setHandler( handler );

        jettyServer.setDumpBeforeStop( true );
        handler.addServletWithMapping( EndUserOcspResponderServlet.class, "/endUserCA" );
        handler.addServletWithMapping( IntOcspResponderServlet.class, "/intCA" );
        jettyServer.start();

        return jettyServer.getURI().getPort();
    }

    public static class EndUserOcspResponderServlet extends HttpServlet
    {
        @Override
        protected void doPost( HttpServletRequest httpRequest, HttpServletResponse httpResponse )
        {
            try
            {
                X509Certificate[] issueCert = PkiUtils.loadCertificates( endUserCertFile );
                PrivateKey privateKey = PkiUtils.loadPrivateKey( intKeyFile, null );

                X509CertificateHolder[] certChain = new X509CertificateHolder[]
                        {new X509CertificateHolder( issueCert[0].getEncoded() ),
                         new X509CertificateHolder( issueCert[1].getEncoded() )};

                DigestCalculatorProvider digCalcProv = new JcaDigestCalculatorProviderBuilder().build();

                BasicOCSPRespBuilder respGen = new JcaBasicOCSPRespBuilder( issueCert[0].getPublicKey(), digCalcProv.get( RespID.HASH_SHA1 ) );

                CertificateID certificateID = new CertificateID( digCalcProv.get( RespID.HASH_SHA1 ),
                                                                 certChain[1], issueCert[0].getSerialNumber() );

                respGen.addResponse( certificateID, CertificateStatus.GOOD );

                BasicOCSPResp resp = respGen.build( new JcaContentSignerBuilder( "SHA1withRSA" ).build( privateKey ), certChain, new Date() );
                OCSPRespBuilder rGen = new OCSPRespBuilder();

                OCSPResp ocspResp = rGen.build( OCSPRespBuilder.SUCCESSFUL, resp );

                httpResponse.setStatus( 200 );
                httpResponse.getOutputStream().write( ocspResp.getEncoded() );
            }
            catch ( Exception e )
            {
                fail( "Error whilst responding to intermediate certificates OCSP request" );
            }
        }
    }

    public static class IntOcspResponderServlet extends HttpServlet
    {
        @Override
        protected void doPost( HttpServletRequest httpRequest, HttpServletResponse httpResponse )
        {
            try
            {
                X509Certificate[] issueCert = PkiUtils.loadCertificates( endUserCertFile );
                PrivateKey privateKey = PkiUtils.loadPrivateKey( rootKeyFile, null );

                X509CertificateHolder[] certChain = new X509CertificateHolder[]
                        {new X509CertificateHolder( issueCert[0].getEncoded() ),
                         new X509CertificateHolder( issueCert[1].getEncoded() ),
                         new X509CertificateHolder( issueCert[2].getEncoded() )};

                DigestCalculatorProvider digCalcProv = new JcaDigestCalculatorProviderBuilder().build();

                BasicOCSPRespBuilder respGen = new JcaBasicOCSPRespBuilder( issueCert[1].getPublicKey(), digCalcProv.get( RespID.HASH_SHA1 ) );

                CertificateID certificateID = new CertificateID( digCalcProv.get( RespID.HASH_SHA1 ),
                                                                 certChain[2], issueCert[1].getSerialNumber() );

                respGen.addResponse( certificateID, CertificateStatus.GOOD );

                BasicOCSPResp resp = respGen.build( new JcaContentSignerBuilder( "SHA1withRSA" ).build( privateKey ), certChain, new Date() );
                OCSPRespBuilder rGen = new OCSPRespBuilder();

                OCSPResp ocspResp = rGen.build( OCSPRespBuilder.SUCCESSFUL, resp );

                httpResponse.setStatus( 200 );
                httpResponse.getOutputStream().write( ocspResp.getEncoded() );
            }
            catch ( Exception e )
            {
                fail( "Error whilst responding to intermediate certificates OCSP request" );
            }
        }
    }
}
