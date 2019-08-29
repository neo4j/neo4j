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
import org.neo4j.string.SecureString;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.Collections.unmodifiableMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.strict_config_validation;
import static org.neo4j.configuration.ssl.SslPolicyScope.TESTING;
import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;

@TestDirectoryExtension
class SslPolicyConfigValidatorTest
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void shouldAcceptAllValidPemPolicyKeys()
    {
        PemSslPolicyConfig sslPolicy = PemSslPolicyConfig.forScope( TESTING );
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
    void shouldAcceptAllValidJksPolicyKeys()
    {
        JksSslPolicyConfig sslPolicy = JksSslPolicyConfig.forScope( TESTING );

        var builder = Config.newBuilder()
                .set( sslPolicy.base_directory, Path.of( "xyz" ) )
                .set( sslPolicy.revoked_dir, Path.of( "xyz" ) )
                .set( sslPolicy.trust_all, true )
                .set( sslPolicy.client_auth, ClientAuth.NONE )
                .set( sslPolicy.tls_versions, List.of( "xyz" ) )
                .set( sslPolicy.ciphers, List.of( "xyz" ) )
                .set( sslPolicy.verify_hostname, true )
                .set( sslPolicy.keystore, Path.of( "abc" ) )
                .set( sslPolicy.keystore_pass, new SecureString( "abc" ) )
                .set( sslPolicy.entry_alias, "abc" )
                .set( sslPolicy.entry_pass, new SecureString( "abc" ) );

        assertDoesNotThrow( builder::build );
    }

    @Test()
    void shouldThrowIfCombinationOfPemAndJks() throws IOException
    {
        // given
        File confFile = testDirectory.createFile( "test.conf" );
        Files.write( confFile.toPath(), Arrays.asList(
                "dbms.ssl.policy.pem.testing.base_directory=xyz",
                "dbms.ssl.policy.pem.testing.allow_key_generation=xyz",
                "dbms.ssl.policy.pem.testing.trust_all=xyz",
                "dbms.ssl.policy.pem.testing.keystore=xyz",
                "dbms.ssl.policy.pem.testing.private_key_password=xyz",
                "dbms.ssl.policy.pem.testing.public_certificate=xyz",
                "dbms.ssl.policy.pem.testing.client_auth=xyz",
                "dbms.ssl.policy.pem.testing.tls_versions=xyz",
                "dbms.ssl.policy.pem.testing.ciphers=xyz"
        ) );
        // when
        IllegalArgumentException exception = assertThrows( IllegalArgumentException.class, () -> Config.newBuilder().fromFile( confFile ).build() );
        assertThat( exception.getMessage(), containsString( "Error evaluating value for setting" ) );
    }

    @Test
    void shouldThrowOnUnknownPolicySetting() throws IOException
    {
        // given
        File confFile = testDirectory.createFile( "test.conf" );
        Files.write( confFile.toPath(), Arrays.asList(
                "dbms.ssl.policy.pem.testing.trust_all=xyz",
                "dbms.ssl.policy.pem.testing.color=blue"
        ) );

        // when
        IllegalArgumentException exception = assertThrows( IllegalArgumentException.class, () -> Config.newBuilder().fromFile( confFile ).build() );

        assertThat( exception.getMessage(), containsString( "Error evaluating value for setting" ) );
    }

    @Test
    void shouldThrowOnDirectPolicySetting() throws IOException
    {
        // given
        File confFile = testDirectory.createFile( "test.conf" );
        Files.write( confFile.toPath(), Arrays.asList(
                "dbms.ssl.policy.pem.base_directory.trust_all=xyz",
                "dbms.ssl.policy.pem.base_directory=path"
        ) );

        Config.Builder builder = Config.newBuilder().set( strict_config_validation, true ).fromFile( confFile );
        // when
        IllegalArgumentException exception = assertThrows( IllegalArgumentException.class, builder::build );

        assertThat( exception.getMessage(), containsString( "No declared setting with name: dbms.ssl.policy.pem." ) );
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

        assertThat( exception.getMessage(), containsString( "Unrecognized setting" ) );
    }

    private static Map<String,String> params( String... params )
    {
        return unmodifiableMap( stringMap( params ) );
    }
}
