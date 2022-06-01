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
import static org.neo4j.configuration.ssl.SslPolicyScope.CLUSTER;

import java.io.IOException;
import java.nio.file.Path;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.neo4j.bolt.testing.client.tls.SecureSocketConnection;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorType;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.configuration.ssl.ClientAuth;
import org.neo4j.configuration.ssl.SslPolicyConfig;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.pki.PkiUtils;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.ssl.SelfSignedCertificateFactory;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
@Neo4jWithSocketExtension
public class RoutingConnectorCertificatesIT {
    private Path externalKeyFile;
    private Path externalCertFile;
    private Path internalKeyFile;
    private Path internalCertFile;

    @Inject
    public Neo4jWithSocket server;

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private FileSystemAbstraction fileSystem;

    @BeforeEach
    void setup(TestInfo testInfo) throws Exception {
        SelfSignedCertificateFactory certFactory = new SelfSignedCertificateFactory();
        externalKeyFile = testDirectory.file("key.pem");
        externalCertFile = testDirectory.file("key.crt");

        // make sure files are not there
        fileSystem.delete(externalKeyFile);
        fileSystem.delete(externalCertFile);

        certFactory.createSelfSignedCertificate(fileSystem, externalCertFile, externalKeyFile, "my.domain");

        internalKeyFile = testDirectory.file("internal_key.pem");
        internalCertFile = testDirectory.file("internal_key.crt");

        // make sure files are not there
        fileSystem.delete(internalKeyFile);
        fileSystem.delete(internalCertFile);

        certFactory.createSelfSignedCertificate(fileSystem, internalCertFile, internalKeyFile, "my.domain");

        server.setConfigure(getSettingsFunction());
        server.init(testInfo);
    }

    private Consumer<Map<Setting<?>, Object>> getSettingsFunction() {
        return settings -> {
            SslPolicyConfig externalPolicy = SslPolicyConfig.forScope(BOLT);
            settings.put(externalPolicy.enabled, true);
            settings.put(externalPolicy.public_certificate, externalCertFile.toAbsolutePath());
            settings.put(externalPolicy.private_key, externalKeyFile.toAbsolutePath());

            SslPolicyConfig internalPolicy = SslPolicyConfig.forScope(CLUSTER);
            settings.put(internalPolicy.enabled, true);
            settings.put(internalPolicy.client_auth, ClientAuth.NONE);
            settings.put(internalPolicy.public_certificate, internalCertFile.toAbsolutePath());
            settings.put(internalPolicy.private_key, internalKeyFile.toAbsolutePath());

            settings.put(BoltConnector.enabled, true);
            settings.put(BoltConnector.encryption_level, OPTIONAL);
            settings.put(BoltConnector.listen_address, new SocketAddress("localhost", 0));

            settings.put(GraphDatabaseSettings.routing_enabled, true);
            settings.put(GraphDatabaseSettings.routing_listen_address, new SocketAddress("localhost", 0));
        };
    }

    @Test
    void shouldUseConfiguredCertificate() throws Exception {
        // GIVEN
        SecureSocketConnection externalConnection =
                new SecureSocketConnection(server.lookupConnector(ConnectorType.BOLT));
        SecureSocketConnection internalConnection =
                new SecureSocketConnection(server.lookupConnector(ConnectorType.INTRA_BOLT));
        try {
            // WHEN
            externalConnection.connect().sendDefaultProtocolVersion();
            internalConnection.connect().sendDefaultProtocolVersion();

            // THEN
            var externalCertificatesSeen = externalConnection.getServerCertificatesSeen();
            var internalCertificatesSeen = internalConnection.getServerCertificatesSeen();

            assertThat(externalCertificatesSeen)
                    .isNotEqualTo(internalCertificatesSeen)
                    .containsExactly(loadCertificateFromDisk(externalCertFile));

            assertThat(internalCertificatesSeen).containsExactly(loadCertificateFromDisk(internalCertFile));
        } finally {
            externalConnection.disconnect();
            internalConnection.disconnect();
        }
    }

    private X509Certificate loadCertificateFromDisk(Path certFile) throws CertificateException, IOException {
        var certificates = PkiUtils.loadCertificates(fileSystem, certFile);

        assertThat(certificates).hasSize(1);

        return certificates[0];
    }
}
