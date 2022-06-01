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

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.OPTIONAL;
import static org.neo4j.configuration.ssl.SslPolicyScope.BOLT;

import java.io.IOException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import org.bouncycastle.operator.OperatorCreationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.neo4j.bolt.testing.client.tls.SecureSocketConnection;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorType;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.configuration.ssl.SslPolicyConfig;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.pki.PkiUtils;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.ssl.SelfSignedCertificateFactory;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
@Neo4jWithSocketExtension
class CertificatesIT {
    private Path keyFile;
    private Path certFile;

    @Inject
    private Neo4jWithSocket server;

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private FileSystemAbstraction fileSystem;

    @BeforeEach
    void setup(TestInfo testInfo) throws IOException, GeneralSecurityException, OperatorCreationException {
        keyFile = testDirectory.file("key.pem");
        certFile = testDirectory.file("key.crt");

        // make sure files are not there
        fileSystem.delete(keyFile);
        fileSystem.delete(certFile);

        new SelfSignedCertificateFactory().createSelfSignedCertificate(fileSystem, certFile, keyFile, "my.domain");

        server.setConfigure(settings -> {
            SslPolicyConfig policy = SslPolicyConfig.forScope(BOLT);
            settings.put(policy.enabled, true);
            settings.put(policy.public_certificate, certFile.toAbsolutePath());
            settings.put(policy.private_key, keyFile.toAbsolutePath());
            settings.put(BoltConnector.enabled, true);
            settings.put(BoltConnector.encryption_level, OPTIONAL);
            settings.put(BoltConnector.listen_address, new SocketAddress("localhost", 0));
        });

        server.init(testInfo);
    }

    @Test
    void shouldUseConfiguredCertificate() throws Exception {
        // GIVEN
        SecureSocketConnection connection = new SecureSocketConnection(server.lookupConnector(ConnectorType.BOLT));
        try {
            // WHEN
            connection.connect().sendDefaultProtocolVersion();

            // THEN
            var certificatesSeen = connection.getServerCertificatesSeen();

            assertThat(certificatesSeen).containsExactly(loadCertificateFromDisk());
        } finally {
            connection.disconnect();
        }
    }

    private X509Certificate loadCertificateFromDisk() throws CertificateException, IOException {
        X509Certificate[] certificates = PkiUtils.loadCertificates(fileSystem, certFile);
        assertThat(certificates.length).isEqualTo(1);

        return certificates[0];
    }
}
