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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo.EMBEDDED_CONNECTION;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.FAILURE;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.PASSWORD_CHANGE_REQUIRED;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.SUCCESS;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.TOO_MANY_ATTEMPTS;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;
import static org.neo4j.server.security.auth.SecurityTestUtils.credentialFor;
import static org.neo4j.server.security.auth.SecurityTestUtils.password;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.CommunitySecurityLog;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.SecureHasher;
import org.neo4j.server.security.systemgraph.BasicSystemGraphRealm;
import org.neo4j.server.security.systemgraph.SecurityGraphHelper;

public class BasicSystemGraphRealmTest {
    private AuthenticationStrategy authStrategy;
    private SecurityGraphHelper realmHelper;
    private BasicSystemGraphRealm realm;

    @BeforeEach
    void setUp() {
        authStrategy = mock(AuthenticationStrategy.class);
        realmHelper = spy(new SecurityGraphHelper(null, new SecureHasher(), CommunitySecurityLog.NULL_LOG));
        realm = new BasicSystemGraphRealm(realmHelper, authStrategy);
    }

    @Test
    void shouldFindAndAuthenticateUserSuccessfully() throws Throwable {
        // Given
        User user = new User("jake", null, credentialFor("abc123"), false, false);
        doReturn(user).when(realmHelper).getUserByName("jake");

        // When
        setMockAuthenticationStrategyResult(user, "abc123", SUCCESS);

        // Then
        assertLoginGivesResult("jake", "abc123", SUCCESS);
    }

    @Test
    void shouldFindAndAuthenticateUserAndReturnAuthStrategyResult() throws Throwable {
        // Given
        User user = new User("jake", null, credentialFor("abc123"), false, false);
        doReturn(user).when(realmHelper).getUserByName("jake");

        // When
        setMockAuthenticationStrategyResult(user, "abc123", TOO_MANY_ATTEMPTS);

        // Then
        assertLoginGivesResult("jake", "abc123", TOO_MANY_ATTEMPTS);
    }

    @Test
    void shouldFindAndAuthenticateUserAndReturnPasswordChangeIfRequired() throws Throwable {
        // Given
        User user = new User("jake", null, credentialFor("abc123"), true, false);
        doReturn(user).when(realmHelper).getUserByName("jake");

        // When
        setMockAuthenticationStrategyResult(user, "abc123", SUCCESS);

        // Then
        assertLoginGivesResult("jake", "abc123", PASSWORD_CHANGE_REQUIRED);
    }

    @Test
    void shouldFailAuthenticationIfUserIsNotFound() throws Throwable {
        // Given
        doReturn(null).when(realmHelper).getUserByName("unknown");

        // Then
        assertLoginGivesResult("unknown", "abc123", FAILURE);
    }

    @Test
    void shouldClearPasswordOnLogin() throws Throwable {
        // Given
        when(authStrategy.authenticate(any(), any())).thenReturn(AuthenticationResult.SUCCESS);

        User user = new User("jake", null, credentialFor("abc123"), true, false);
        doReturn(user).when(realmHelper).getUserByName("jake");

        byte[] password = password("abc123");
        Map<String, Object> authToken = AuthToken.newBasicAuthToken("jake", password);

        // When
        realm.login(authToken, EMBEDDED_CONNECTION);

        // Then
        assertThat(password).isEqualTo(clearedPasswordWithSameLengthAs("abc123"));
        assertThat(authToken.get(AuthToken.CREDENTIALS)).isEqualTo(clearedPasswordWithSameLengthAs("abc123"));
    }

    @Test
    void shouldClearPasswordOnInvalidAuthToken() {
        // Given
        byte[] password = password("abc123");
        Map<String, Object> authToken = AuthToken.newBasicAuthToken("jake", password);
        authToken.put(AuthToken.SCHEME_KEY, null); // Null is not a valid scheme

        // When
        try {
            realm.login(authToken, EMBEDDED_CONNECTION);
            fail("exception expected");
        } catch (InvalidAuthTokenException e) {
            // expected
        }
        assertThat(password).isEqualTo(clearedPasswordWithSameLengthAs("abc123"));
        assertThat(authToken.get(AuthToken.CREDENTIALS)).isEqualTo(clearedPasswordWithSameLengthAs("abc123"));
    }

    @Test
    void shouldFailWhenAuthTokenIsInvalid() {
        assertThatThrownBy(() -> realm.login(
                        map(AuthToken.SCHEME_KEY, "supercool", AuthToken.PRINCIPAL, "neo4j"), EMBEDDED_CONNECTION))
                .isInstanceOf(InvalidAuthTokenException.class)
                .hasMessage("Unsupported authentication token, scheme 'supercool' is not supported.");

        assertThatThrownBy(() -> realm.login(map(AuthToken.SCHEME_KEY, "none"), EMBEDDED_CONNECTION))
                .isInstanceOf(InvalidAuthTokenException.class)
                .hasMessage("Unsupported authentication token, scheme 'none' is only allowed when auth is disabled.");

        assertThatThrownBy(() -> realm.login(map("key", "value"), EMBEDDED_CONNECTION))
                .isInstanceOf(InvalidAuthTokenException.class)
                .hasMessage("Unsupported authentication token, missing key `scheme`");

        assertThatThrownBy(() -> realm.login(
                        map(AuthToken.SCHEME_KEY, "basic", AuthToken.PRINCIPAL, "neo4j"), EMBEDDED_CONNECTION))
                .isInstanceOf(InvalidAuthTokenException.class)
                .hasMessage("Unsupported authentication token, missing key `credentials`");

        assertThatThrownBy(() -> realm.login(
                        map(AuthToken.SCHEME_KEY, "basic", AuthToken.CREDENTIALS, "very-secret"), EMBEDDED_CONNECTION))
                .isInstanceOf(InvalidAuthTokenException.class)
                .hasMessage("Unsupported authentication token, missing key `principal`");
    }

    private void assertLoginGivesResult(String username, String password, AuthenticationResult expectedResult)
            throws InvalidAuthTokenException {
        LoginContext securityContext = realm.login(authToken(username, password), EMBEDDED_CONNECTION);
        assertThat(securityContext.subject().getAuthenticationResult()).isEqualTo(expectedResult);
    }

    private void setMockAuthenticationStrategyResult(User user, String password, AuthenticationResult result) {
        when(authStrategy.authenticate(user, password(password))).thenReturn(result);
    }

    public static byte[] clearedPasswordWithSameLengthAs(String passwordString) {
        byte[] password = passwordString.getBytes(StandardCharsets.UTF_8);
        Arrays.fill(password, (byte) 0);
        return password;
    }
}
