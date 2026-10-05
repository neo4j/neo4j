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
package org.neo4j.server.queryapi.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Base64;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.HttpHeaders;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.server.queryapi.exception.InvalidAuthenticationHeaderException;
import org.neo4j.server.rest.dbms.AuthorizationHeaders;

class AuthTokenFactoryTest {

    @Nested
    class FromHttpServletRequestTest {

        @Test
        void shouldReturnAuthTokenNoneWhenHeaderIsMissing() {
            var httpRequest = mock(HttpServletRequest.class);

            var authToken = AuthTokenFactory.fromHttpServletRequest(httpRequest);

            assertEquals(AuthTokens.none(), authToken);
        }

        @Test
        void shouldReturnAuthTokenNoneWhenHeaderIsEmpty() {
            var httpRequest = mock(HttpServletRequest.class);
            when(httpRequest.getHeader(HttpHeaders.AUTHORIZATION)).thenReturn("");

            var authToken = AuthTokenFactory.fromHttpServletRequest(httpRequest);

            assertEquals(AuthTokens.none(), authToken);
        }

        @Test
        void shouldReturnAuthTokenBasicWhenWellFormatedBasicAuthIsPresent() {
            var username = "gregory";
            var password = "qwerty123456";
            var header = basicAuthHeader(username, password);

            var httpRequest = mock(HttpServletRequest.class);
            when(httpRequest.getHeader(HttpHeaders.AUTHORIZATION)).thenReturn(header);

            var authToken = AuthTokenFactory.fromHttpServletRequest(httpRequest);

            assertEquals(AuthTokens.basic(username, password), authToken);
        }

        @Test
        void shouldReturnAuthTokenBasicWithRealmWhenRealmHeaderIsPresent() {
            var username = "gregory";
            var password = "qwerty123456";
            var realm = "native";
            var header = basicAuthHeader(username, password);

            var httpRequest = mock(HttpServletRequest.class);
            when(httpRequest.getHeader(HttpHeaders.AUTHORIZATION)).thenReturn(header);
            when(httpRequest.getHeader("Neo4j-Realm")).thenReturn(realm);

            var authToken = AuthTokenFactory.fromHttpServletRequest(httpRequest);

            assertEquals(AuthTokens.basic(username, password, realm), authToken);
        }

        @Test
        void shouldReturnAuthTokenBearerWhenWellFormatedBearerAuthIsPresent() {
            var token = "some-bearer-token";
            var header = "Bearer " + token;

            var httpRequest = mock(HttpServletRequest.class);
            when(httpRequest.getHeader(HttpHeaders.AUTHORIZATION)).thenReturn(header);

            var authToken = AuthTokenFactory.fromHttpServletRequest(httpRequest);

            assertEquals(AuthTokens.custom("", token, null, AuthTokenFactory.BEARER_SCHEME), authToken);
        }

        @Test
        void shouldReturnAuthTokenBearerWithRealmWhenRealmHeaderIsPresent() {
            var token = "some-bearer-token";
            var realm = "oidc";
            var header = "Bearer " + token;

            var httpRequest = mock(HttpServletRequest.class);
            when(httpRequest.getHeader(HttpHeaders.AUTHORIZATION)).thenReturn(header);
            when(httpRequest.getHeader("Neo4j-Realm")).thenReturn(realm);

            var authToken = AuthTokenFactory.fromHttpServletRequest(httpRequest);

            assertEquals(AuthTokens.custom("", token, realm, AuthTokenFactory.BEARER_SCHEME), authToken);
        }

        @Test
        void shouldThrowWhenAuthorizationSchemeIsUnsupported() {
            var httpRequest = mock(HttpServletRequest.class);
            when(httpRequest.getHeader(HttpHeaders.AUTHORIZATION)).thenReturn("Digest token");

            assertThrows(
                    InvalidAuthenticationHeaderException.class,
                    () -> AuthTokenFactory.fromHttpServletRequest(httpRequest));
        }

        @Test
        void shouldThrowWhenBasicAuthTokenIsBlank() {
            var httpRequest = mock(HttpServletRequest.class);
            when(httpRequest.getHeader(HttpHeaders.AUTHORIZATION)).thenReturn("Basic ");

            assertThrows(
                    InvalidAuthenticationHeaderException.class,
                    () -> AuthTokenFactory.fromHttpServletRequest(httpRequest));
        }

        @Test
        void shouldThrowWhenBasicAuthTokenIsNotBase64Encoded() {
            var httpRequest = mock(HttpServletRequest.class);
            when(httpRequest.getHeader(HttpHeaders.AUTHORIZATION)).thenReturn("Basic not-base64");

            assertThrows(
                    InvalidAuthenticationHeaderException.class,
                    () -> AuthTokenFactory.fromHttpServletRequest(httpRequest));
        }

        @Test
        void shouldThrowWhenBasicAuthTokenDoesNotContainPasswordSeparator() {
            var header = AuthorizationHeaders.Scheme.BASIC + " "
                    + Base64.getEncoder().encodeToString("gregory".getBytes());

            var httpRequest = mock(HttpServletRequest.class);
            when(httpRequest.getHeader(HttpHeaders.AUTHORIZATION)).thenReturn(header);

            assertThrows(
                    InvalidAuthenticationHeaderException.class,
                    () -> AuthTokenFactory.fromHttpServletRequest(httpRequest));
        }

        @Test
        void shouldThrowWhenBearerAuthHeaderContainsTooManyParts() {
            var httpRequest = mock(HttpServletRequest.class);
            when(httpRequest.getHeader(HttpHeaders.AUTHORIZATION)).thenReturn("Bearer token extra");

            assertThrows(
                    InvalidAuthenticationHeaderException.class,
                    () -> AuthTokenFactory.fromHttpServletRequest(httpRequest));
        }

        @Test
        void shouldThrowWhenBearerAuthHeaderContainsTooLittleParts() {
            var httpRequest = mock(HttpServletRequest.class);
            when(httpRequest.getHeader(HttpHeaders.AUTHORIZATION)).thenReturn("Bearer");

            assertThrows(
                    InvalidAuthenticationHeaderException.class,
                    () -> AuthTokenFactory.fromHttpServletRequest(httpRequest));
        }

        public static String basicAuthHeader(String username, String password) {
            var usernamePassword = username + ':' + password;
            return AuthorizationHeaders.Scheme.BASIC + " "
                    + Base64.getEncoder().encodeToString(usernamePassword.getBytes());
        }
    }
}
