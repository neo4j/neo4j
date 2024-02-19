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
package org.neo4j.ssl;

import static java.nio.file.Files.createDirectories;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.Config.newBuilder;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.ssl.SslPolicyScope.BOLT;
import static org.neo4j.configuration.ssl.SslPolicyScope.TESTING;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import javax.net.ssl.SSLException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.SslSystemInternalSettings;
import org.neo4j.configuration.ssl.SslPolicyConfig;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.ssl.config.SslPolicyLoader;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.ssl.SelfSignedCertificateFactory;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class SslPolicyLoaderTest {
    private static final String REVOCATION_ERROR_MSG = "Could not load CRL";
    private static final String TRUSTED_CERTS_ERROR_MSG = "Failed to create trust manager";

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private FileSystemAbstraction fileSystem;

    private Path home;
    private Path publicCertificateFile;
    private Path privateKeyFile;
    private Path trustedDir;
    private Path revokedDir;
    private Path baseDir;

    @BeforeEach
    void setup() throws Exception {
        home = testDirectory.directory("home");
        baseDir = home.resolve("certificates/default");
        publicCertificateFile = baseDir.resolve("public.crt");
        privateKeyFile = baseDir.resolve("private.key");

        new SelfSignedCertificateFactory()
                .createSelfSignedCertificate(fileSystem, publicCertificateFile, privateKeyFile, "localhost");

        trustedDir = makeDir(baseDir, "trusted");
        FileUtils.copyFile(publicCertificateFile, trustedDir.resolve("public.crt"));

        revokedDir = makeDir(baseDir, "revoked");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldLoadBaseCryptographicObjects(boolean ignoreDotfiles) throws Exception {
        // when
        SslPolicyLoader sslPolicyLoader = createSslPolicyLoader(ignoreDotfiles);

        // then
        SslPolicy sslPolicy = sslPolicyLoader.getPolicy(TESTING);
        assertPolicyValid(sslPolicy);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldComplainIfUnparseableFilesPresentInTrusted(boolean ignoreDotfiles) throws IOException {
        // given
        writeJunkToFile(trustedDir, "foo.txt");
        Files.createFile(revokedDir.resolve("empty.crt"));

        // then
        shouldThrowCertificateExceptionCreatingSslPolicy(TRUSTED_CERTS_ERROR_MSG, Exception.class, ignoreDotfiles);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldComplainIfDirectoriesPresentInTrusted(boolean ignoreDotfiles) throws IOException {
        // given
        makeDir(trustedDir, "foo");

        // then
        shouldThrowCertificateExceptionCreatingSslPolicy(TRUSTED_CERTS_ERROR_MSG, Exception.class, ignoreDotfiles);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldComplainIfUnparseableFilesPresentInRevoked(boolean ignoreDotfiles) throws IOException {
        // given
        writeJunkToFile(revokedDir, "foo.txt");

        // then
        shouldThrowCertificateExceptionCreatingSslPolicy(REVOCATION_ERROR_MSG, Exception.class, ignoreDotfiles);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldComplainIfDirectoriesPresentInRevoked(boolean ignoreDotfiles) throws IOException {
        // given
        makeDir(revokedDir, "foo");

        // then
        shouldThrowCertificateExceptionCreatingSslPolicy(REVOCATION_ERROR_MSG, Exception.class, ignoreDotfiles);
    }

    private void shouldThrowCertificateExceptionCreatingSslPolicy(
            String expectedMessage, Class<? extends Exception> expectedCause, boolean ignoreDotfiles) {
        SslPolicyConfig policyConfig = SslPolicyConfig.forScope(TESTING);

        Config config = newBuilder()
                .set(neo4j_home, home.toAbsolutePath())
                .set(SslSystemInternalSettings.ignore_dotfiles, ignoreDotfiles)
                .set(policyConfig.enabled, Boolean.TRUE)
                .set(policyConfig.base_directory, Path.of("certificates/default"))
                .build();

        // when
        Exception exception = assertThrows(
                Exception.class, () -> SslPolicyLoader.create(fileSystem, config, NullLogProvider.getInstance()));
        assertThat(exception).hasMessageContaining(expectedMessage).isInstanceOf(expectedCause);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void correctBehaviourIfDotfilesPresent(boolean ignoreDotfiles) throws IOException {
        // given
        writeJunkToFile(baseDir, ".README");
        writeJunkToFile(trustedDir, ".README");
        writeJunkToFile(revokedDir, ".README");

        // when
        SslPolicyLoader sslPolicyLoader;
        if (!ignoreDotfiles) {
            assertThrows(Exception.class, () -> createSslPolicyLoader(ignoreDotfiles));
            return;
        } else {
            sslPolicyLoader = createSslPolicyLoader(ignoreDotfiles);
        }

        SslPolicy sslPolicy = sslPolicyLoader.getPolicy(TESTING);

        // then
        assertPolicyValid(sslPolicy);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldNotComplainIfDotdirsPresent(boolean ignoreDotfiles) throws IOException {
        // given
        makeDir(baseDir, "..data");
        makeDir(trustedDir, "..data");
        makeDir(revokedDir, "..data");

        // when
        SslPolicyLoader sslPolicyLoader;
        if (!ignoreDotfiles) {
            Exception exception = assertThrows(Exception.class, () -> createSslPolicyLoader(ignoreDotfiles));
            assertThat(exception.getMessage()).contains("Failed to create trust manager");
            return;
        } else {
            sslPolicyLoader = createSslPolicyLoader(ignoreDotfiles);
        }

        // then
        SslPolicy sslPolicy = sslPolicyLoader.getPolicy(TESTING);
        assertPolicyValid(sslPolicy);
    }

    @Test
    void shouldComplainIfMissingPrivateKey() throws IOException {
        shouldComplainIfMissingFile(privateKeyFile, "Failed to load private key");
    }

    @Test
    void shouldComplainIfMissingPublicCertificate() throws IOException {
        shouldComplainIfMissingFile(publicCertificateFile, "Failed to load public certificate chain");
    }

    private void shouldComplainIfMissingFile(Path file, String expectedErrorMessage) throws IOException {
        // given
        Files.delete(file);

        SslPolicyConfig policyConfig = SslPolicyConfig.forScope(TESTING);

        Config config = newBuilder()
                .set(neo4j_home, home.toAbsolutePath())
                .set(policyConfig.enabled, Boolean.TRUE)
                .set(policyConfig.base_directory, Path.of("certificates/default"))
                .build();

        // when
        Exception exception = assertThrows(
                Exception.class, () -> SslPolicyLoader.create(fileSystem, config, NullLogProvider.getInstance()));
        assertThat(exception.getMessage()).contains(expectedErrorMessage);
        assertThat(exception.getCause()).isInstanceOf(NoSuchFileException.class);
    }

    @Test
    void shouldThrowIfPolicyNameDoesNotExist() {
        // given
        SslPolicyConfig policyConfig = SslPolicyConfig.forScope(TESTING);

        Config config = newBuilder()
                .set(neo4j_home, home.toAbsolutePath())
                .set(policyConfig.base_directory, Path.of("certificates/default"))
                .build();

        SslPolicyLoader sslPolicyLoader = SslPolicyLoader.create(fileSystem, config, NullLogProvider.getInstance());

        // when
        assertThrows(IllegalArgumentException.class, () -> sslPolicyLoader.getPolicy(BOLT));
    }

    @Test
    void shouldReturnNullPolicyIfNullRequested() {
        // given
        SslPolicyLoader sslPolicyLoader =
                SslPolicyLoader.create(fileSystem, Config.defaults(), NullLogProvider.getInstance());

        // when
        SslPolicy sslPolicy = sslPolicyLoader.getPolicy(null);

        // then
        assertNull(sslPolicy);
    }

    @Test
    void shouldLogWarningIfHostnameValidationDisabledByDefault() {

        var logProvider = new AssertableLogProvider();
        SslPolicyConfig policyConfig = SslPolicyConfig.forScope(TESTING);
        Config config = newBuilder()
                .set(neo4j_home, home.toAbsolutePath())
                .set(SslSystemInternalSettings.ignore_dotfiles, false)
                .set(policyConfig.enabled, Boolean.TRUE)
                .set(policyConfig.base_directory, Path.of("certificates/default"))
                .build();

        SslPolicyLoader sslPolicyLoader = SslPolicyLoader.create(fileSystem, config, logProvider);
        LogAssertions.assertThat(logProvider)
                .containsMessageWithArguments(
                        "SSL Hostname verification is disabled by default. Consider explicitly setting %s",
                        policyConfig.verify_hostname.name());
    }

    private static Path makeDir(Path parent, String child) throws IOException {
        Path dir = parent.resolve(child);
        createDirectories(dir);
        return dir;
    }

    private static void writeJunkToFile(Path parent, String child) throws IOException {
        Path file = parent.resolve(child);
        Files.write(file, "junk data".getBytes());
    }

    private SslPolicyLoader createSslPolicyLoader(boolean ignoreDotfiles) {
        SslPolicyConfig policyConfig = SslPolicyConfig.forScope(TESTING);

        Config config = newBuilder()
                .set(neo4j_home, home.toAbsolutePath())
                .set(SslSystemInternalSettings.ignore_dotfiles, ignoreDotfiles)
                .set(policyConfig.enabled, Boolean.TRUE)
                .set(policyConfig.base_directory, Path.of("certificates/default"))
                .build();

        return SslPolicyLoader.create(fileSystem, config, NullLogProvider.getInstance());
    }

    private static void assertPolicyValid(SslPolicy sslPolicy) throws SSLException {
        assertNotNull(sslPolicy);
        assertNotNull(sslPolicy.privateKey());
        assertNotNull(sslPolicy.certificateChain());
        assertNotNull(sslPolicy.nettyClientContext());
        assertNotNull(sslPolicy.nettyServerContext());
    }
}
