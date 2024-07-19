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
package org.neo4j.bolt.security.basic;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo.EMBEDDED_CONNECTION;
import static org.neo4j.server.security.auth.SecurityTestUtils.credentialFor;
import static org.neo4j.server.security.auth.SecurityTestUtils.password;

import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.security.Authentication;
import org.neo4j.bolt.security.AuthenticationResult;
import org.neo4j.bolt.security.error.AuthenticationException;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.security.CommunitySecurityLog;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.SecureHasher;
import org.neo4j.server.security.auth.RateLimitedAuthenticationStrategy;
import org.neo4j.server.security.systemgraph.BasicSystemGraphRealm;
import org.neo4j.server.security.systemgraph.SecurityGraphHelper;
import org.neo4j.time.Clocks;

class BasicAuthenticationTest {

    private Authentication authentication;

    @Test
    void shouldNotDoAnythingOnSuccess() throws Exception {
        // When
        AuthenticationResult result = authentication.authenticate(
                map("scheme", "basic", "principal", "mike", "credentials", password("secret2")), EMBEDDED_CONNECTION);

        // Then
        assertThat(result.getLoginContext().subject().executingUser()).isEqualTo("mike");
    }

    @Test
    void shouldThrowAndLogOnFailure() {
        var e = assertThrows(
                AuthenticationException.class,
                () -> authentication.authenticate(
                        map("scheme", "basic", "principal", "bob", "credentials", password("banana")),
                        EMBEDDED_CONNECTION));
        assertEquals(Status.Security.Unauthorized, e.status());
        assertEquals("The client is unauthorized due to authentication failure.", e.getMessage());
    }

    @Test
    void shouldIndicateThatCredentialsExpired() throws Exception {
        // When
        AuthenticationResult result = authentication.authenticate(
                map("scheme", "basic", "principal", "bob", "credentials", password("secret")), EMBEDDED_CONNECTION);

        // Then
        assertTrue(result.credentialsExpired());
    }

    @Test
    void shouldFailWhenTooManyAttempts() throws Exception {
        // Given
        int maxFailedAttempts = ThreadLocalRandom.current().nextInt(1, 10);
        Authentication auth = createAuthentication(maxFailedAttempts);

        for (int i = 0; i < maxFailedAttempts; ++i) {
            try {
                auth.authenticate(
                        map("scheme", "basic", "principal", "bob", "credentials", password("gelato")),
                        EMBEDDED_CONNECTION);
            } catch (AuthenticationException e) {
                assertThat(e.status()).isEqualTo(Status.Security.Unauthorized);
            }
        }

        var e = assertThrows(
                AuthenticationException.class,
                () -> auth.authenticate(
                        map("scheme", "basic", "principal", "bob", "credentials", password("gelato")),
                        EMBEDDED_CONNECTION));
        assertEquals(Status.Security.AuthenticationRateLimit, e.status());
        assertEquals(
                "The client has provided incorrect authentication details too many times in a row.", e.getMessage());
    }

    @Test
    void shouldClearCredentialsAfterUse() throws Exception {
        // When
        byte[] password = password("secret2");

        authentication.authenticate(
                map("scheme", "basic", "principal", "mike", "credentials", password), EMBEDDED_CONNECTION);

        // Then
        assertThat(password).containsOnly(0);
    }

    @Test
    void shouldThrowWithNoScheme() {
        var e = assertThrows(
                AuthenticationException.class,
                () -> authentication.authenticate(
                        map("principal", "bob", "credentials", password("secret")), EMBEDDED_CONNECTION));
        assertEquals(Status.Security.Unauthorized, e.status());
    }

    @Test
    void shouldFailOnInvalidAuthToken() {
        var e = assertThrows(
                AuthenticationException.class,
                () -> authentication.authenticate(
                        map("this", "does", "not", "matter", "for", "test"), EMBEDDED_CONNECTION));
        assertEquals(Status.Security.Unauthorized, e.status());
    }

    @Test
    void shouldFailOnMalformedToken() {
        var e = assertThrows(
                AuthenticationException.class,
                () -> authentication.authenticate(
                        map("scheme", "basic", "principal", singletonList("bob"), "credentials", password("secret")),
                        EMBEDDED_CONNECTION));
        assertEquals(Status.Security.Unauthorized, e.status());
        assertEquals(
                "Unsupported authentication token, the value associated with the key `principal` "
                        + "must be a String but was: SingletonList",
                e.getMessage());
    }

    @BeforeEach
    void setup() throws Throwable {
        authentication = createAuthentication(3);
    }

    private static Authentication createAuthentication(int maxFailedAttempts) {
        Config config = Config.defaults(GraphDatabaseSettings.auth_max_failed_attempts, maxFailedAttempts);
        SecurityGraphHelper realmHelper =
                spy(new SecurityGraphHelper(null, new SecureHasher(), CommunitySecurityLog.NULL_LOG));
        BasicSystemGraphRealm realm = new BasicSystemGraphRealm(
                realmHelper, new RateLimitedAuthenticationStrategy(Clocks.systemClock(), config));
        Authentication authentication = new BasicAuthentication(realm);
        doReturn(new User("bob", null, credentialFor("secret"), true, false))
                .when(realmHelper)
                .getUserByName("bob");
        doReturn(new User("mike", null, credentialFor("secret2"), false, false))
                .when(realmHelper)
                .getUserByName("mike");

        return authentication;
    }
}
