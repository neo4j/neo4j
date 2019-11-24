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
package org.neo4j.configuration.ssl;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.string.SecureString;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.strict_config_validation;
import static org.neo4j.configuration.ssl.SslPolicyScope.TESTING;
import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;

@TestDirectoryExtension
class SslPolicyConfigValidatorTest
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void shouldFindPolicyDefaults()
    {
        // given
        SslPolicyConfig policyConfig = SslPolicyConfig.forScope( SslPolicyScope.TESTING );

        File homeDir = testDirectory.directory( "home" );
        Config config = Config.newBuilder()
                .set( GraphDatabaseSettings.neo4j_home, homeDir.toPath().toAbsolutePath() )
                .set( policyConfig.base_directory, Path.of( "certificates/testing" ) )
                .build();

        // derived defaults
        File privateKey = new File( homeDir, "certificates/testing/private.key" );
        File publicCertificate = new File( homeDir, "certificates/testing/public.crt" );
        File trustedDir = new File( homeDir, "certificates/testing/trusted" );
        File revokedDir = new File( homeDir, "certificates/testing/revoked" );

        // when
        File privateKeyFromConfig = config.get( policyConfig.private_key ).toFile();
        File publicCertificateFromConfig = config.get( policyConfig.public_certificate ).toFile();
        File trustedDirFromConfig = config.get( policyConfig.trusted_dir ).toFile();
        File revokedDirFromConfig = config.get( policyConfig.revoked_dir ).toFile();
        SecureString privateKeyPassword = config.get( policyConfig.private_key_password );
        boolean trustAll = config.get( policyConfig.trust_all );
        List<String> tlsVersions = config.get( policyConfig.tls_versions );
        List<String> ciphers = config.get( policyConfig.ciphers );
        ClientAuth clientAuth = config.get( policyConfig.client_auth );

        // then
        assertEquals( privateKey, privateKeyFromConfig );
        assertEquals( publicCertificate, publicCertificateFromConfig );
        assertEquals( trustedDir, trustedDirFromConfig );
        assertEquals( revokedDir, revokedDirFromConfig );
        assertNull( privateKeyPassword );
        assertFalse( trustAll );
        assertEquals( singletonList( "TLSv1.2" ), tlsVersions );
        assertNull( ciphers );
        assertEquals( ClientAuth.REQUIRE, clientAuth );
    }

    @Test
    void shouldFindPolicyOverrides()
    {
        // given
        Config.Builder builder = Config.newBuilder();

        SslPolicyConfig policyConfig = SslPolicyConfig.forScope( SslPolicyScope.TESTING );

        File homeDir = testDirectory.directory( "home" );

        builder.set( GraphDatabaseSettings.neo4j_home, homeDir.toPath().toAbsolutePath() );
        builder.set( policyConfig.base_directory, Path.of( "certificates/testing" ) );

        File privateKey = testDirectory.directory( "/path/to/my.key" );
        File publicCertificate = testDirectory.directory( "/path/to/my.crt" );
        File trustedDir = testDirectory.directory( "/some/other/path/to/trusted" );
        File revokedDir = testDirectory.directory( "/some/other/path/to/revoked" );

        builder.set( policyConfig.private_key, privateKey.toPath().toAbsolutePath() );
        builder.set( policyConfig.public_certificate, publicCertificate.toPath().toAbsolutePath() );
        builder.set( policyConfig.trusted_dir, trustedDir.toPath().toAbsolutePath() );
        builder.set( policyConfig.revoked_dir, revokedDir.toPath().toAbsolutePath() );

        builder.set( policyConfig.trust_all, true );

        builder.set( policyConfig.private_key_password, new SecureString( "setecastronomy" ) );
        builder.set( policyConfig.tls_versions, List.of( "TLSv1.1", "TLSv1.2" ) );
        builder.set( policyConfig.ciphers, List.of( "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384", "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384" ) );
        builder.set( policyConfig.client_auth, ClientAuth.OPTIONAL );

        Config config = builder.build();

        // when
        File privateKeyFromConfig = config.get( policyConfig.private_key ).toFile();
        File publicCertificateFromConfig = config.get( policyConfig.public_certificate ).toFile();
        File trustedDirFromConfig = config.get( policyConfig.trusted_dir ).toFile();
        File revokedDirFromConfig = config.get( policyConfig.revoked_dir ).toFile();

        SecureString privateKeyPassword = config.get( policyConfig.private_key_password );
        boolean trustAll = config.get( policyConfig.trust_all );
        List<String> tlsVersions = config.get( policyConfig.tls_versions );
        List<String> ciphers = config.get( policyConfig.ciphers );
        ClientAuth clientAuth = config.get( policyConfig.client_auth );

        // then
        assertEquals( privateKey, privateKeyFromConfig );
        assertEquals( publicCertificate, publicCertificateFromConfig );
        assertEquals( trustedDir, trustedDirFromConfig );
        assertEquals( revokedDir, revokedDirFromConfig );

        assertTrue( trustAll );
        assertEquals( "setecastronomy", privateKeyPassword.getString() );
        assertEquals( asList( "TLSv1.1", "TLSv1.2" ), tlsVersions );
        assertEquals( asList( "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384", "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384" ), ciphers );
        assertEquals( ClientAuth.OPTIONAL, clientAuth );
    }

    @Test
    void shouldAcceptAllValidPemPolicyKeys()
    {
        SslPolicyConfig sslPolicy = SslPolicyConfig.forScope( TESTING );
        var builder = Config.newBuilder()
                .set( sslPolicy.base_directory, Path.of( "xyz" ) )
                .set( sslPolicy.revoked_dir, Path.of( "xyz" ) )
                .set( sslPolicy.trust_all, false )
                .set( sslPolicy.client_auth, ClientAuth.NONE )
                .set( sslPolicy.tls_versions, List.of( "xyz" ) )
                .set( sslPolicy.ciphers, List.of( "xyz" ) )
                .set( sslPolicy.verify_hostname, true )

                .set( sslPolicy.private_key, Path.of("xyz" ) )
                .set( sslPolicy.public_certificate, Path.of("xyz" ) )
                .set( sslPolicy.trusted_dir, Path.of("xyz" ) )
                .set( sslPolicy.private_key_password, new SecureString( "xyz" ) );

        assertDoesNotThrow( builder::build );
    }

    @Test
    void shouldThrowOnUnknownPolicySetting() throws IOException
    {
        // given
        File confFile = testDirectory.createFile( "test.conf" );
        Files.write( confFile.toPath(), Arrays.asList(
                "dbms.ssl.policy.testing.trust_all=xyz",
                "dbms.ssl.policy.testing.color=blue"
        ) );

        // when
        IllegalArgumentException exception = assertThrows( IllegalArgumentException.class, () -> Config.newBuilder().fromFile( confFile ).build() );

        assertThat( exception.getMessage() ).contains( "Error evaluating value for setting" );
    }

    @Test
    void shouldThrowOnDirectPolicySetting() throws IOException
    {
        // given
        File confFile = testDirectory.createFile( "test.conf" );
        Files.write( confFile.toPath(), Arrays.asList(
                "dbms.ssl.policy.base_directory.trust_all=xyz",
                "dbms.ssl.policy.base_directory=path"
        ) );

        Config.Builder builder = Config.newBuilder().set( strict_config_validation, true ).fromFile( confFile );
        // when
        IllegalArgumentException exception = assertThrows( IllegalArgumentException.class, builder::build );

        assertThat( exception.getMessage() ).contains( "No declared setting with name: dbms.ssl.policy." );
    }

    @Test
    void shouldIgnoreUnknownNonPolicySettings() throws IOException
    {
        // given
        File confFile = testDirectory.createFile( "test.conf" );
        Files.write( confFile.toPath(), Arrays.asList(
                "dbms.ssl.unknown=xyz",
                "dbms.ssl.something=xyz",
                "dbms.unrelated.totally=xyz"
        ) );

        // when
        IllegalArgumentException exception =
                assertThrows( IllegalArgumentException.class, () -> Config.newBuilder().set( strict_config_validation, true ).fromFile( confFile ).build() );

        assertThat( exception.getMessage() ).contains( "Unrecognized setting" );
    }

    private static Map<String,String> params( String... params )
    {
        return unmodifiableMap( stringMap( params ) );
    }
}
