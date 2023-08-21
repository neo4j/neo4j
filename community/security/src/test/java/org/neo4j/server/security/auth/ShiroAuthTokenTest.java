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
package org.neo4j.server.security.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.api.security.AuthToken.BASIC_SCHEME;
import static org.neo4j.kernel.api.security.AuthToken.CREDENTIALS;
import static org.neo4j.kernel.api.security.AuthToken.PRINCIPAL;
import static org.neo4j.kernel.api.security.AuthToken.REALM_KEY;
import static org.neo4j.kernel.api.security.AuthToken.SCHEME_KEY;
import static org.neo4j.server.security.auth.SecurityTestUtils.password;
import static org.neo4j.test.AuthTokenUtil.matches;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;

class ShiroAuthTokenTest {
    private static final String USERNAME = "myuser";
    private static final String PASSWORD = "mypw123";

    @Test
    void shouldSupportBasicAuthToken() throws Exception {
        ShiroAuthToken token = new ShiroAuthToken(AuthToken.newBasicAuthToken(USERNAME, PASSWORD));
        testBasicAuthToken(token, USERNAME, PASSWORD, BASIC_SCHEME);
        var expected = map(PRINCIPAL, USERNAME, CREDENTIALS, PASSWORD, SCHEME_KEY, BASIC_SCHEME);
        assertThat(token.getAuthTokenMap())
                .as("Token map should have only expected values")
                .matches(map -> matches(expected, map));
        testTokenSupportsRealm(token, true, "unknown", "native", "ldap");
    }

    @Test
    void shouldSupportBasicAuthTokenWithEmptyRealm() throws Exception {
        ShiroAuthToken token = new ShiroAuthToken(AuthToken.newBasicAuthToken(USERNAME, PASSWORD, ""));
        testBasicAuthToken(token, USERNAME, PASSWORD, BASIC_SCHEME);
        var expected = map(PRINCIPAL, USERNAME, CREDENTIALS, PASSWORD, SCHEME_KEY, BASIC_SCHEME, REALM_KEY, "");
        assertThat(token.getAuthTokenMap())
                .as("Token map should have only expected values")
                .matches(map -> matches(expected, map));
        testTokenSupportsRealm(token, true, "unknown", "native", "ldap");
    }

    @Test
    void shouldSupportBasicAuthTokenWithNullRealm() throws Exception {
        ShiroAuthToken token = new ShiroAuthToken(AuthToken.newBasicAuthToken(USERNAME, PASSWORD, null));
        testBasicAuthToken(token, USERNAME, PASSWORD, BASIC_SCHEME);
        var expected = map(PRINCIPAL, USERNAME, CREDENTIALS, PASSWORD, SCHEME_KEY, BASIC_SCHEME, REALM_KEY, null);
        assertThat(token.getAuthTokenMap())
                .as("Token map should have only expected values")
                .matches(map -> matches(expected, map));
        testTokenSupportsRealm(token, true, "unknown", "native", "ldap");
    }

    @Test
    void shouldSupportBasicAuthTokenWithWildcardRealm() throws Exception {
        ShiroAuthToken token = new ShiroAuthToken(AuthToken.newBasicAuthToken(USERNAME, PASSWORD, "*"));
        testBasicAuthToken(token, USERNAME, PASSWORD, BASIC_SCHEME);
        var expected = map(PRINCIPAL, USERNAME, CREDENTIALS, PASSWORD, SCHEME_KEY, BASIC_SCHEME, REALM_KEY, "*");
        assertThat(token.getAuthTokenMap())
                .as("Token map should have only expected values")
                .matches(map -> matches(expected, map));
        testTokenSupportsRealm(token, true, "unknown", "native", "ldap");
    }

    @Test
    void shouldSupportBasicAuthTokenWithSpecificRealm() throws Exception {
        String realm = "ldap";
        ShiroAuthToken token = new ShiroAuthToken(AuthToken.newBasicAuthToken(USERNAME, PASSWORD, realm));
        testBasicAuthToken(token, USERNAME, PASSWORD, BASIC_SCHEME);
        var expected = map(PRINCIPAL, USERNAME, CREDENTIALS, PASSWORD, SCHEME_KEY, BASIC_SCHEME, REALM_KEY, "ldap");
        assertThat(token.getAuthTokenMap())
                .as("Token map should have only expected values")
                .matches(map -> matches(expected, map));
        testTokenSupportsRealm(token, true, realm);
        testTokenSupportsRealm(token, false, "unknown", "native");
    }

    @Test
    void shouldSupportCustomAuthTokenWithSpecificRealm() throws Exception {
        String realm = "ldap";
        ShiroAuthToken token =
                new ShiroAuthToken(AuthToken.newCustomAuthToken(USERNAME, PASSWORD, realm, BASIC_SCHEME));
        testBasicAuthToken(token, USERNAME, PASSWORD, BASIC_SCHEME);
        var expected = map(PRINCIPAL, USERNAME, CREDENTIALS, PASSWORD, SCHEME_KEY, BASIC_SCHEME, REALM_KEY, "ldap");
        assertThat(token.getAuthTokenMap())
                .as("Token map should have only expected values")
                .matches(map -> matches(expected, map));
        testTokenSupportsRealm(token, true, realm);
        testTokenSupportsRealm(token, false, "unknown", "native");
    }

    @Test
    void shouldSupportCustomAuthTokenWithSpecificRealmAndParameters() throws Exception {
        String realm = "ldap";
        Map<String, Object> params = map("a", "A", "b", "B");
        ShiroAuthToken token =
                new ShiroAuthToken(AuthToken.newCustomAuthToken(USERNAME, PASSWORD, realm, BASIC_SCHEME, params));
        testBasicAuthToken(token, USERNAME, PASSWORD, BASIC_SCHEME);
        var expected = map(
                PRINCIPAL,
                USERNAME,
                CREDENTIALS,
                PASSWORD,
                SCHEME_KEY,
                BASIC_SCHEME,
                REALM_KEY,
                "ldap",
                "parameters",
                params);
        assertThat(token.getAuthTokenMap())
                .as("Token map should have only expected values")
                .matches(map -> matches(expected, map));
        testTokenSupportsRealm(token, true, realm);
        testTokenSupportsRealm(token, false, "unknown", "native");
    }

    @Test
    void shouldHaveStringRepresentationWithNullRealm() throws Exception {
        ShiroAuthToken token = new ShiroAuthToken(AuthToken.newBasicAuthToken(USERNAME, PASSWORD, null));
        testBasicAuthToken(token, USERNAME, PASSWORD, BASIC_SCHEME);

        String stringRepresentation = token.toString();
        assertThat(stringRepresentation).contains("realm='null'");
    }

    private static void testTokenSupportsRealm(ShiroAuthToken token, boolean supports, String... realms) {
        for (String realm : realms) {
            assertThat(token.supportsRealm(realm))
                    .as("Token should support '" + realm + "' realm")
                    .isEqualTo(supports);
        }
    }

    private static void testBasicAuthToken(ShiroAuthToken token, String username, String password, String scheme)
            throws InvalidAuthTokenException {
        assertThat(token.getScheme()).as("Token should have basic scheme").isEqualTo(scheme);
        assertThat(token.getPrincipal()).as("Token have correct principal").isEqualTo(username);
        assertThat(token.getCredentials()).as("Token have correct credentials").isEqualTo(password(password));
    }
}
