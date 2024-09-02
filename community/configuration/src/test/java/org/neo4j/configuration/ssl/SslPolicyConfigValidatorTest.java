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
package org.neo4j.configuration.ssl;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.strict_config_validation;
import static org.neo4j.configuration.ssl.SslPolicyScope.TESTING;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.string.SecureString;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class SslPolicyConfigValidatorTest {
    @Inject
    private TestDirectory testDirectory;

    @Test
    void shouldFindPolicyDefaults() {
        // given
        SslPolicyConfig policyConfig = SslPolicyConfig.forScope(SslPolicyScope.TESTING);

        Path homeDir = testDirectory.directory("home");
        Config config = Config.newBuilder()
                .set(GraphDatabaseSettings.neo4j_home, homeDir.toAbsolutePath())
                .set(policyConfig.base_directory, Path.of("certificates/testing"))
                .build();

        // derived defaults
        Path privateKey = homeDir.resolve("certificates/testing/private.key");
        Path publicCertificate = homeDir.resolve("certificates/testing/public.crt");
        Path trustedDir = homeDir.resolve("certificates/testing/trusted");
        Path revokedDir = homeDir.resolve("certificates/testing/revoked");

        // when
        Path privateKeyFromConfig = config.get(policyConfig.private_key);
        Path publicCertificateFromConfig = config.get(policyConfig.public_certificate);
        Path trustedDirFromConfig = config.get(policyConfig.trusted_dir);
        Path revokedDirFromConfig = config.get(policyConfig.revoked_dir);
        SecureString privateKeyPassword = config.get(policyConfig.private_key_password);
        boolean trustAll = config.get(policyConfig.trust_all);
        List<String> tlsVersions = config.get(policyConfig.tls_versions);
        List<String> ciphers = config.get(policyConfig.ciphers);
        ClientAuth clientAuth = config.get(policyConfig.client_auth);

        // then
        assertEquals(privateKey, privateKeyFromConfig);
        assertEquals(publicCertificate, publicCertificateFromConfig);
        assertEquals(trustedDir, trustedDirFromConfig);
        assertEquals(revokedDir, revokedDirFromConfig);
        assertNull(privateKeyPassword);
        assertFalse(trustAll);
        assertEquals(List.of("TLSv1.2", "TLSv1.3"), tlsVersions);
        assertNull(ciphers);
        assertEquals(ClientAuth.REQUIRE, clientAuth);
    }

    @Test
    void shouldFindPolicyOverrides() {
        // given
        Config.Builder builder = Config.newBuilder();

        SslPolicyConfig policyConfig = SslPolicyConfig.forScope(SslPolicyScope.TESTING);

        Path homeDir = testDirectory.directory("home");

        builder.set(GraphDatabaseSettings.neo4j_home, homeDir.toAbsolutePath());
        builder.set(policyConfig.base_directory, Path.of("certificates/testing"));

        Path privateKey = testDirectory.directory("path/to/my.key");
        Path publicCertificate = testDirectory.directory("path/to/my.crt");
        Path trustedDir = testDirectory.directory("some/other/path/to/trusted");
        Path revokedDir = testDirectory.directory("some/other/path/to/revoked");

        builder.set(policyConfig.private_key, privateKey.toAbsolutePath());
        builder.set(policyConfig.public_certificate, publicCertificate.toAbsolutePath());
        builder.set(policyConfig.trusted_dir, trustedDir.toAbsolutePath());
        builder.set(policyConfig.revoked_dir, revokedDir.toAbsolutePath());

        builder.set(policyConfig.trust_all, true);

        builder.set(policyConfig.private_key_password, new SecureString("setecastronomy"));
        builder.set(policyConfig.tls_versions, List.of("TLSv1.1", "TLSv1.2"));
        builder.set(
                policyConfig.ciphers,
                List.of("TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384", "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384"));
        builder.set(policyConfig.client_auth, ClientAuth.OPTIONAL);

        Config config = builder.build();

        // when
        Path privateKeyFromConfig = config.get(policyConfig.private_key);
        Path publicCertificateFromConfig = config.get(policyConfig.public_certificate);
        Path trustedDirFromConfig = config.get(policyConfig.trusted_dir);
        Path revokedDirFromConfig = config.get(policyConfig.revoked_dir);

        SecureString privateKeyPassword = config.get(policyConfig.private_key_password);
        boolean trustAll = config.get(policyConfig.trust_all);
        List<String> tlsVersions = config.get(policyConfig.tls_versions);
        List<String> ciphers = config.get(policyConfig.ciphers);
        ClientAuth clientAuth = config.get(policyConfig.client_auth);

        // then
        assertEquals(privateKey, privateKeyFromConfig);
        assertEquals(publicCertificate, publicCertificateFromConfig);
        assertEquals(trustedDir, trustedDirFromConfig);
        assertEquals(revokedDir, revokedDirFromConfig);

        assertTrue(trustAll);
        assertEquals("setecastronomy", privateKeyPassword.getString());
        assertEquals(asList("TLSv1.1", "TLSv1.2"), tlsVersions);
        assertEquals(
                asList("TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384", "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384"), ciphers);
        assertEquals(ClientAuth.OPTIONAL, clientAuth);
    }

    @Test
    void shouldAcceptAllValidPemPolicyKeys() {
        SslPolicyConfig sslPolicy = SslPolicyConfig.forScope(TESTING);
        var builder = Config.newBuilder()
                .set(sslPolicy.base_directory, Path.of("xyz"))
                .set(sslPolicy.revoked_dir, Path.of("xyz"))
                .set(sslPolicy.trust_all, false)
                .set(sslPolicy.client_auth, ClientAuth.NONE)
                .set(sslPolicy.tls_versions, List.of("xyz"))
                .set(sslPolicy.ciphers, List.of("xyz"))
                .set(sslPolicy.verify_hostname, true)
                .set(sslPolicy.private_key, Path.of("xyz"))
                .set(sslPolicy.public_certificate, Path.of("xyz"))
                .set(sslPolicy.trusted_dir, Path.of("xyz"))
                .set(sslPolicy.private_key_password, new SecureString("xyz"));

        assertDoesNotThrow(builder::build);
    }

    @Test
    void shouldThrowOnUnknownPolicySetting() throws IOException {
        // given
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(
                confFile, Arrays.asList("dbms.ssl.policy.testing.trust_all=xyz", "dbms.ssl.policy.testing.color=blue"));

        // when
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> Config.newBuilder().fromFile(confFile).build());

        assertThat(exception.getMessage()).contains("Error evaluating value for setting");
    }

    @Test
    void shouldThrowOnDirectPolicySetting() throws IOException {
        // given
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(
                confFile,
                Arrays.asList("dbms.ssl.policy.base_directory.trust_all=xyz", "dbms.ssl.policy.base_directory=path"));

        Config.Builder builder =
                Config.newBuilder().set(strict_config_validation, true).fromFile(confFile);
        // when
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, builder::build);

        assertThat(exception.getMessage()).contains("No declared setting with name: dbms.ssl.policy.");
    }

    @Test
    void shouldIgnoreUnknownNonPolicySettings() throws IOException {
        // given
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(
                confFile,
                Arrays.asList("dbms.ssl.unknown=xyz", "dbms.ssl.something=xyz", "dbms.unrelated.totally=xyz"));

        // when
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> Config.newBuilder()
                .set(strict_config_validation, true)
                .fromFile(confFile)
                .build());

        assertThat(exception.getMessage()).contains("Unrecognized setting");
    }
}
