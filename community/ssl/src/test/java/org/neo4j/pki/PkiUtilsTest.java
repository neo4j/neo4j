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
package org.neo4j.pki;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.netty.handler.ssl.util.SelfSignedCertificate;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Path;
import java.security.KeyException;
import java.security.PrivateKey;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.ssl.SslResourceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.ssl.SelfSignedCertificateFactory;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class PkiUtilsTest {
    @Inject
    private TestDirectory testDirectory;

    @Inject
    private FileSystemAbstraction fileSystem;

    @Test
    void shouldCreateASelfSignedCertificate() throws Exception {
        // Given
        var sslFactory = new SelfSignedCertificateFactory();
        var cPath = testDirectory.homePath().resolve("certificate");
        var pkPath = testDirectory.homePath().resolve("key");

        // When
        sslFactory.createSelfSignedCertificate(fileSystem, cPath, pkPath, "myhost");

        // Then
        // Attempt to load certificate
        var certificates = PkiUtils.loadCertificates(fileSystem, cPath);
        assertThat(certificates.length).isGreaterThan(0);

        // Attempt to load private key
        PrivateKey pk = PkiUtils.loadPrivateKey(fileSystem, pkPath, null);
        assertThat(pk).isNotNull();
    }

    @Test
    void shouldLoadPEMCertificates() throws Throwable {
        // Given
        var cert = new SelfSignedCertificate("example.com");

        var pemCertificate = cert.certificate().toPath();

        // When
        var certificates = PkiUtils.loadCertificates(fileSystem, pemCertificate);

        // Then
        assertThat(certificates.length).isEqualTo(1);
    }

    @Test
    void shouldLoadPEMPrivateKey() throws Throwable {
        // Given
        var cert = new SelfSignedCertificate("example.com");

        var privateKey = cert.privateKey().toPath();

        // When
        var pk = PkiUtils.loadPrivateKey(fileSystem, privateKey, null);

        // Then
        assertNotNull(pk);
    }

    @Test
    void shouldReadEncryptedPrivateKey() throws Exception {
        Path keyFile = testDirectory.file("private.key");
        URL resource = SslResourceBuilder.class.getResource("test-certificates/encrypted/private.key");
        copy(requireNonNull(resource), keyFile);

        PrivateKey pk = PkiUtils.loadPrivateKey(fileSystem, keyFile, "neo4j");
        assertThat(pk.getAlgorithm()).isEqualTo("RSA");
    }

    @Test
    void shouldThrowOnMissingPassphraseForEncryptedPrivateKey() throws Exception {
        Path keyFile = testDirectory.file("private.key");
        URL resource = SslResourceBuilder.class.getResource("test-certificates/encrypted/private.key");
        copy(requireNonNull(resource), keyFile);

        assertThrows(KeyException.class, () -> PkiUtils.loadPrivateKey(fileSystem, keyFile, null));
    }

    private void copy(URL in, Path outFile) throws IOException {
        try (InputStream is = in.openStream();
                OutputStream os = testDirectory.getFileSystem().openAsOutputStream(outFile, false)) {
            is.transferTo(os);
        }
    }
}
