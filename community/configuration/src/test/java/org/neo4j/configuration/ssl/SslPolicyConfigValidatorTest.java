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
import java.util.function.Consumer;

import org.neo4j.graphdb.config.InvalidSettingException;

import static java.util.Collections.unmodifiableMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

class SslPolicyConfigValidatorTest
{
    private final Consumer<String> warnings = warning -> {};

    @Test
    void shouldAcceptAllValidPemPolicyKeys()
    {
        // given
        SslPolicyConfigValidator validator = new SslPolicyConfigValidator();
        Map<String,String> originalParams = params(
                "dbms.ssl.policy.default.format", "pem",
                "dbms.ssl.policy.default.base_directory", "xyz",
                "dbms.ssl.policy.default.revoked_dir", "xyz",
                "dbms.ssl.policy.default.trust_all", "xyz",
                "dbms.ssl.policy.default.client_auth", "xyz",
                "dbms.ssl.policy.default.tls_versions", "xyz",
                "dbms.ssl.policy.default.ciphers", "xyz",
                "dbms.ssl.policy.default.verify_hostname", "true",

                "dbms.ssl.policy.default.allow_key_generation", "xyz",
                "dbms.ssl.policy.default.private_key", "xyz",
                "dbms.ssl.policy.default.private_key_password", "xyz",
                "dbms.ssl.policy.default.public_certificate", "xyz",
                "dbms.ssl.policy.default.trusted_dir", "xyz"
        );

        // when
        Map<String,String> validatedParams = validator.validate( originalParams, warnings );

        // then
        assertEquals( originalParams, validatedParams );
    }

    @Test
    void shouldAcceptAllValidJksPolicyKeys()
    {
        // given
        SslPolicyConfigValidator validator = new SslPolicyConfigValidator();
        Map<String,String> originalParams = params(
                "dbms.ssl.policy.default.format", "jks",
                "dbms.ssl.policy.default.base_directory", "xyz",
                "dbms.ssl.policy.default.revoked_dir", "xyz",
                "dbms.ssl.policy.default.trust_all", "xyz",
                "dbms.ssl.policy.default.client_auth", "xyz",
                "dbms.ssl.policy.default.tls_versions", "xyz",
                "dbms.ssl.policy.default.ciphers", "xyz",
                "dbms.ssl.policy.default.verify_hostname", "true",

                "dbms.ssl.policy.default.keystore", "abc",
                "dbms.ssl.policy.default.keystore_pass", "abc",
                "dbms.ssl.policy.default.entry_alias", "abc",
                "dbms.ssl.policy.default.entry_pass", "abc"
        );

        // when
        Map<String,String> validatedParams = validator.validate( originalParams, warnings );

        // then
        assertEquals( originalParams, validatedParams );
    }

    @Test()
    void shouldThrowIfCombinationOfPemAndJks()
    {
        // given
        SslPolicyConfigValidator validator = new SslPolicyConfigValidator();
        Map<String,String> originalParams = params(
                "dbms.ssl.policy.default.format", "pem",
                "dbms.ssl.policy.default.base_directory", "xyz",
                "dbms.ssl.policy.default.allow_key_generation", "xyz",
                "dbms.ssl.policy.default.trust_all", "xyz",
                "dbms.ssl.policy.default.keystore", "xyz",
                "dbms.ssl.policy.default.private_key_password", "xyz",
                "dbms.ssl.policy.default.public_certificate", "xyz",
                "dbms.ssl.policy.default.client_auth", "xyz",
                "dbms.ssl.policy.default.tls_versions", "xyz",
                "dbms.ssl.policy.default.ciphers", "xyz"
        );

        // when
        InvalidSettingException exception = assertThrows( InvalidSettingException.class, () -> validator.validate( originalParams, warnings ) );
        assertThat( exception.getMessage(), containsString( "Invalid setting name" ) );
    }

    @Test
    void shouldThrowOnUnknownPolicySetting()
    {
        // given
        SslPolicyConfigValidator validator = new SslPolicyConfigValidator();
        Map<String,String> originalParams = params(
                "dbms.ssl.policy.default.format", "pem",
                "dbms.ssl.policy.default.color", "blue" );

        // when
        InvalidSettingException exception = assertThrows( InvalidSettingException.class, () -> validator.validate( originalParams, warnings ) );
        assertThat( exception.getMessage(), containsString( "Invalid setting name" ) );
    }

    @Test
    void shouldThrowOnDirectPolicySetting()
    {
        // given
        SslPolicyConfigValidator validator = new SslPolicyConfigValidator();
        Map<String,String> originalParams = params(
                "dbms.ssl.policy.base_directory.format", "pem",
                "dbms.ssl.policy.base_directory", "path" );

        // when
        InvalidSettingException exception = assertThrows( InvalidSettingException.class, () -> validator.validate( originalParams, warnings ) );
        assertThat( exception.getMessage(), containsString( "Invalid setting name" ) );
    }

    @Test
    void shouldIgnoreUnknownNonPolicySettings()
    {
        // given
        SslPolicyConfigValidator validator = new SslPolicyConfigValidator();
        Map<String,String> originalParams = params(
                "dbms.ssl.unknown", "xyz",
                "dbms.ssl.something", "xyz",
                "dbms.unrelated.totally", "xyz"
        );

        // when
        Map<String,String> validatedParams = validator.validate( originalParams, warnings );

        // then
        assertTrue( validatedParams.isEmpty() );
    }

    @Test
    void shouldComplainWhenMissingMandatoryBaseDirectory()
    {
        // given
        SslPolicyConfigValidator validator = new SslPolicyConfigValidator();
        Map<String,String> originalParams = params(
                "dbms.ssl.policy.default.format", "pem",
                "dbms.ssl.policy.default.private_key", "private.key",
                "dbms.ssl.policy.default.public_certificate", "public.crt"
        );

        // when
        InvalidSettingException exception = assertThrows( InvalidSettingException.class, () -> validator.validate( originalParams, warnings ) );
        assertThat( exception.getMessage(), containsString( "Missing mandatory setting" ) );
    }

    @Test
    void shouldThrowIfUnknownFormat()
    {
        // given
        SslPolicyConfigValidator validator = new SslPolicyConfigValidator();
        Map<String,String> originalParams = params(
                "dbms.ssl.policy.default.base_directory", "xyz",
                "dbms.ssl.policy.default.format", "woot"
        );

        // when
        InvalidSettingException exception = assertThrows( InvalidSettingException.class, () -> validator.validate( originalParams, warnings ) );
        assertThat( exception.getMessage(), containsString( "Unrecognised format" ) );
    }

    @Test
    void shouldThrowIfMissingFormat()
    {
        // given
        SslPolicyConfigValidator validator = new SslPolicyConfigValidator();
        Map<String,String> originalParams = params(
                "dbms.ssl.policy.default.base_directory", "xyz"
        );

        // when
        InvalidSettingException exception = assertThrows( InvalidSettingException.class, () -> validator.validate( originalParams, warnings ) );
        assertThat( exception.getMessage(), containsString( "Missing format" ) );
    }

    @Test
    void shouldAllowCaseInsensitiveFormat()
    {
        // given
        SslPolicyConfigValidator validator = new SslPolicyConfigValidator();
        Map<String,String> originalParams = params(
                "dbms.ssl.policy.default.format", "pKcS12",
                "dbms.ssl.policy.default.base_directory", "xyz",
                "dbms.ssl.policy.default.revoked_dir", "xyz",
                "dbms.ssl.policy.default.trust_all", "xyz",
                "dbms.ssl.policy.default.client_auth", "xyz",
                "dbms.ssl.policy.default.tls_versions", "xyz",
                "dbms.ssl.policy.default.ciphers", "xyz",
                "dbms.ssl.policy.default.verify_hostname", "true",

                "dbms.ssl.policy.default.keystore", "abc",
                "dbms.ssl.policy.default.keystore_pass", "abc",
                "dbms.ssl.policy.default.entry_alias", "abc",
                "dbms.ssl.policy.default.entry_pass", "abc"
        );

        // when
        Map<String,String> validated = validator.validate( originalParams, warnings );

        // then
        assertEquals( validated, originalParams );
    }

    private static Map<String,String> params( String... params )
    {
        return unmodifiableMap( stringMap( params ) );
    }
}
