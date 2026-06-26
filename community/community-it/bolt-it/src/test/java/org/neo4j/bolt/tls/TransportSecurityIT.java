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
import static org.neo4j.configuration.ssl.SslPolicyScope.BOLT;

import java.nio.file.Path;
import java.security.cert.X509Certificate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.VersionSelected;
import org.neo4j.bolt.test.annotation.connection.transport.IncludeTransport;
import org.neo4j.bolt.test.annotation.setup.SettingsFunction;
import org.neo4j.bolt.test.annotation.test.TransportTest;
import org.neo4j.bolt.test.connection.setup.SettingBuilder;
import org.neo4j.bolt.testing.client.SecureSocketConnection;
import org.neo4j.bolt.testing.client.TransportType;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.configuration.ssl.SslPolicyConfig;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.pki.PkiUtils;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.ssl.SelfSignedCertificateFactory;
import org.neo4j.test.utils.TestDirectory;

/**
 * Evaluates whether Bolt correctly negotiates TLS connections.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
class TransportSecurityIT {

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private FileSystemAbstraction fileSystem;

    private Path keyFile;
    private Path certFile;

    private X509Certificate certificate;

    @SettingsFunction
    void customizeSettings(SettingBuilder settings) {
        var policy = SslPolicyConfig.forScope(BOLT);

        settings.set(policy.enabled, true)
                .set(policy.public_certificate, this.certFile.toAbsolutePath())
                .set(policy.private_key, this.keyFile.toAbsolutePath());
    }

    @BeforeAll
    void prepare() throws Exception {
        this.keyFile = this.testDirectory.file("key.pem");
        this.certFile = this.testDirectory.file("key.crt");

        this.fileSystem.delete(keyFile);
        this.fileSystem.delete(certFile);

        var certificateFactory = new SelfSignedCertificateFactory();
        certificateFactory.createSelfSignedCertificate(this.fileSystem, this.certFile, this.keyFile, "my.domain");

        var certificates = PkiUtils.loadCertificates(fileSystem, certFile);
        assertThat(certificates.length).isOne();

        this.certificate = certificates[0];
    }

    @TransportTest
    @IncludeTransport(TransportType.TCP_TLS)
    void shouldUseConfiguredCertificate(@VersionSelected SecureSocketConnection connection) {
        var certificatesSeen = connection.getServerCertificatesSeen();

        assertThat(certificatesSeen).containsExactly(this.certificate);
    }
}
