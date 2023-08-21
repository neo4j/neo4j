/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.tls;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.neo4j.configuration.ssl.SslPolicyScope.BOLT;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.Map;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.transport.IncludeTransport;
import org.neo4j.bolt.test.annotation.setup.SettingsFunction;
import org.neo4j.bolt.test.annotation.test.TransportTest;
import org.neo4j.bolt.testing.client.TransportType;
import org.neo4j.bolt.testing.client.tls.CertConfiguredSecureSocketConnection;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.configuration.connectors.CommonConnectorConfig;
import org.neo4j.configuration.ssl.SslPolicyConfig;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.pki.PkiUtils;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.ssl.CertificateChainFactory;

/**
 * Evaluates whether Bolt correctly staples OCSP responses to connections.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
class OcspStaplingIT {
    private Server server;

    private static Path endUserCertFile;
    private Path endUserKeyFile;
    private Path intCertFile;
    private static Path intKeyFile;
    private Path rootCertFile;
    private static Path rootKeyFile;

    private X509Certificate certificate;

    @Inject
    private FileSystemAbstraction fileSystem;

    @BeforeAll
    void prepare() throws Exception {
        endUserCertFile = Files.createTempFile("end_key", "pem");
        endUserKeyFile = Files.createTempFile("end_key", "pem");

        intCertFile = Files.createTempFile("int_key", "pem");
        intKeyFile = Files.createTempFile("int_key", "pem");

        rootCertFile = Files.createTempFile("root_key", "pem");
        rootKeyFile = Files.createTempFile("root_key", "pem");

        server = startMockOCSPResponder();

        var bouncyCastleProvider = new BouncyCastleProvider();
        CertificateChainFactory.createCertificateChain(
                endUserCertFile,
                endUserKeyFile,
                intCertFile,
                intKeyFile,
                rootCertFile,
                rootKeyFile,
                server.getURI().getPort(),
                bouncyCastleProvider);

        var certificates = PkiUtils.loadCertificates(fileSystem, rootCertFile);
        assertThat(certificates.length).isEqualTo(1);

        this.certificate = certificates[0];
    }

    @AfterAll
    void cleanup() throws IOException {
        Files.deleteIfExists(endUserKeyFile);
        Files.deleteIfExists(endUserCertFile);
        Files.deleteIfExists(rootKeyFile);
        Files.deleteIfExists(rootCertFile);
        Files.deleteIfExists(intKeyFile);
        Files.deleteIfExists(intCertFile);

        try {
            server.stop();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @SettingsFunction
    void customizeSettings(Map<Setting<?>, Object> settings) {
        var policy = SslPolicyConfig.forScope(BOLT);
        settings.put(policy.enabled, true);
        settings.put(policy.public_certificate, endUserCertFile.toAbsolutePath());
        settings.put(policy.private_key, endUserKeyFile.toAbsolutePath());

        settings.put(CommonConnectorConfig.ocsp_stapling_enabled, true);
    }

    @TransportTest
    @IncludeTransport(TransportType.TCP_TLS)
    void shouldReturnCertificatesWithStapledOcspResponses(SocketAddress address) throws Exception {
        try (var connection = new CertConfiguredSecureSocketConnection(address, this.certificate)) {
            connection.connect().sendDefaultProtocolVersion();

            var certificatesSeen = connection.getServerCertificatesSeen();
            var certificateSerialNumbersSeen = certificatesSeen.stream()
                    .map(X509Certificate::getSerialNumber)
                    .toList();

            assertThat(certificatesSeen).hasSize(3);

            var ocspResponsesSeen = connection.getSeenOcspResponses();

            assertThat(ocspResponsesSeen).hasSize(2).allSatisfy(response -> assertThat(response.getResponses()[0])
                    .satisfies(res -> {
                        // checking all responses come back OK ( null means the certificate hasn't been revoked! )
                        assertThat(res.getCertStatus()).isNull();

                        // responses match the certificates seen
                        assertThat(res.getCertID().getSerialNumber()).isIn(certificateSerialNumbersSeen);
                    }));
        }
    }

    private static Server startMockOCSPResponder() throws Exception {
        var jettyServer = new Server(0);
        var handler = new ServletHandler();
        jettyServer.setHandler(handler);

        jettyServer.setDumpBeforeStop(true);
        handler.addServletWithMapping(EndUserOcspResponderServlet.class, "/endUserCA/*");
        handler.addServletWithMapping(IntOcspResponderServlet.class, "/intCA/*");
        jettyServer.start();

        return jettyServer;
    }

    public static class EndUserOcspResponderServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse httpResponse) {
            serveResponse(httpResponse);
        }

        @Override
        protected void doPost(HttpServletRequest httpRequest, HttpServletResponse httpResponse) {
            serveResponse(httpResponse);
        }

        private void serveResponse(HttpServletResponse httpResponse) {
            try {
                DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();
                X509Certificate[] issueCert = PkiUtils.loadCertificates(fs, endUserCertFile);
                PrivateKey privateKey = PkiUtils.loadPrivateKey(fs, intKeyFile);

                X509CertificateHolder[] certChain = new X509CertificateHolder[] {
                    new X509CertificateHolder(issueCert[0].getEncoded()),
                    new X509CertificateHolder(issueCert[1].getEncoded())
                };

                DigestCalculatorProvider digCalcProv = new JcaDigestCalculatorProviderBuilder().build();

                BasicOCSPRespBuilder respGen =
                        new JcaBasicOCSPRespBuilder(issueCert[0].getPublicKey(), digCalcProv.get(RespID.HASH_SHA1));

                CertificateID certificateID = new CertificateID(
                        digCalcProv.get(RespID.HASH_SHA1), certChain[1], issueCert[0].getSerialNumber());

                respGen.addResponse(certificateID, CertificateStatus.GOOD);

                BasicOCSPResp resp = respGen.build(
                        new JcaContentSignerBuilder("SHA256withRSA").build(privateKey), certChain, new Date());
                OCSPRespBuilder rGen = new OCSPRespBuilder();

                OCSPResp ocspResp = rGen.build(OCSPRespBuilder.SUCCESSFUL, resp);

                httpResponse.setStatus(200);
                httpResponse.getOutputStream().write(ocspResp.getEncoded());
            } catch (Exception e) {
                fail("Error whilst responding to intermediate certificates OCSP request");
            }
        }
    }

    public static class IntOcspResponderServlet extends HttpServlet {
        @Override
        protected void doPost(HttpServletRequest httpRequest, HttpServletResponse httpResponse) {
            serveResponse(httpResponse);
        }

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse httpResponse) {
            serveResponse(httpResponse);
        }

        private void serveResponse(HttpServletResponse httpResponse) {
            try {
                DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();
                X509Certificate[] issueCert = PkiUtils.loadCertificates(fs, endUserCertFile);
                PrivateKey privateKey = PkiUtils.loadPrivateKey(fs, rootKeyFile);

                X509CertificateHolder[] certChain = new X509CertificateHolder[] {
                    new X509CertificateHolder(issueCert[0].getEncoded()),
                    new X509CertificateHolder(issueCert[1].getEncoded()),
                    new X509CertificateHolder(issueCert[2].getEncoded())
                };

                DigestCalculatorProvider digCalcProv = new JcaDigestCalculatorProviderBuilder().build();

                BasicOCSPRespBuilder respGen =
                        new JcaBasicOCSPRespBuilder(issueCert[1].getPublicKey(), digCalcProv.get(RespID.HASH_SHA1));

                CertificateID certificateID = new CertificateID(
                        digCalcProv.get(RespID.HASH_SHA1), certChain[2], issueCert[1].getSerialNumber());

                respGen.addResponse(certificateID, CertificateStatus.GOOD);

                BasicOCSPResp resp = respGen.build(
                        new JcaContentSignerBuilder("SHA256withRSA").build(privateKey), certChain, new Date());
                OCSPRespBuilder rGen = new OCSPRespBuilder();

                OCSPResp ocspResp = rGen.build(OCSPRespBuilder.SUCCESSFUL, resp);

                httpResponse.setStatus(200);
                httpResponse.getOutputStream().write(ocspResp.getEncoded());
            } catch (Exception e) {
                fail("Error whilst responding to intermediate certificates OCSP request");
            }
        }
    }
}
