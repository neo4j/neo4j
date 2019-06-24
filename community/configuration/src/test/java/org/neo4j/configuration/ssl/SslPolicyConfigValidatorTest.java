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

import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;

import static java.util.Collections.unmodifiableMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;

class SslPolicyConfigValidatorTest
{
    @Test
    void shouldAcceptAllValidPemPolicyKeys()
    {
        PemSslPolicyConfig sslPolicy = PemSslPolicyConfig.group( "default" );
        Map<String,String> originalParams = params(
                sslPolicy.base_directory.name(), "xyz",
                sslPolicy.revoked_dir.name(), "xyz",
                sslPolicy.trust_all.name(), FALSE,
                sslPolicy.client_auth.name(), "NONE",
                sslPolicy.tls_versions.name(), "xyz",
                sslPolicy.ciphers.name(), "xyz",
                sslPolicy.verify_hostname.name(), TRUE,

                sslPolicy.allow_key_generation.name(), TRUE,
                sslPolicy.private_key.name(), "xyz",
                sslPolicy.private_key_password.name(), "xyz",
                sslPolicy.public_certificate.name(), "xyz",
                sslPolicy.trusted_dir.name(), "xyz"
        );

        assertDoesNotThrow( () -> Config.defaults( originalParams ) );
    }

    @Test
    void shouldAcceptAllValidJksPolicyKeys()
    {
        JksSslPolicyConfig sslPolicy = JksSslPolicyConfig.group( "default" );

        Map<String,String> originalParams = params(
                sslPolicy.base_directory.name(), "xyz",
                sslPolicy.revoked_dir.name(), "xyz",
                sslPolicy.trust_all.name(), TRUE,
                sslPolicy.client_auth.name(), "none",
                sslPolicy.tls_versions.name(), "xyz",
                sslPolicy.ciphers.name(), "xyz",
                sslPolicy.verify_hostname.name(), TRUE,

                sslPolicy.keystore.name(), "abc",
                sslPolicy.keystore_pass.name(), "abc",
                sslPolicy.entry_alias.name(), "abc",
                sslPolicy.entry_pass.name(), "abc"
        );
        assertDoesNotThrow( () -> Config.defaults( originalParams ) );

    }

    @Test()
    void shouldThrowIfCombinationOfPemAndJks()
    {
        // given
        Map<String,String> originalParams = params(
                "dbms.ssl.policy.pem.default.base_directory", "xyz",
                "dbms.ssl.policy.pem.default.allow_key_generation", "xyz",
                "dbms.ssl.policy.pem.default.trust_all", "xyz",
                "dbms.ssl.policy.pem.default.keystore", "xyz",
                "dbms.ssl.policy.pem.default.private_key_password", "xyz",
                "dbms.ssl.policy.pem.default.public_certificate", "xyz",
                "dbms.ssl.policy.pem.default.client_auth", "xyz",
                "dbms.ssl.policy.pem.default.tls_versions", "xyz",
                "dbms.ssl.policy.pem.default.ciphers", "xyz"
        );

        // when
        IllegalArgumentException exception = assertThrows( IllegalArgumentException.class, () -> Config.defaults( originalParams ) );
        assertThat( exception.getMessage(), containsString( "Error evaluate setting" ) );
    }

    @Test
    void shouldThrowOnUnknownPolicySetting()
    {
        // given
        Map<String,String> originalParams = params(
                "dbms.ssl.policy.pem.default.trust_all", "xyz",
                "dbms.ssl.policy.pem.default.color", "blue" );

        // when
        IllegalArgumentException exception = assertThrows( IllegalArgumentException.class, () -> Config.defaults( originalParams ) );
        assertThat( exception.getMessage(), containsString( "Error evaluate setting" ) );
    }

    @Test
    void shouldThrowOnDirectPolicySetting()
    {
        // given
        Map<String,String> originalParams = params(
                "dbms.ssl.policy.pem.base_directory.trust_all", "xyz",
                "dbms.ssl.policy.pem.base_directory", "path" );

        // when
        IllegalArgumentException exception = assertThrows( IllegalArgumentException.class, () -> Config.defaults( originalParams ) );
        assertThat( exception.getMessage(), containsString( "Error evaluate setting" ) );
    }

    @Test
    void shouldIgnoreUnknownNonPolicySettings()
    {
        // given
        Map<String,String> originalParams = params(
                GraphDatabaseSettings.strict_config_validation.name(), TRUE,
                "dbms.ssl.unknown", "xyz",
                "dbms.ssl.something", "xyz",
                "dbms.unrelated.totally", "xyz"
        );

        // when
        IllegalArgumentException exception = assertThrows( IllegalArgumentException.class, () -> Config.defaults( originalParams ) );
        assertThat( exception.getMessage(), containsString( "Unrecognized setting" ) );
    }

    private static Map<String,String> params( String... params )
    {
        return unmodifiableMap( stringMap( params ) );
    }
}
