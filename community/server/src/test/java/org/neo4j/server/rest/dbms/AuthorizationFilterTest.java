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
package org.neo4j.server.rest.dbms;

import static java.nio.charset.StandardCharsets.UTF_8;
import static javax.servlet.http.HttpServletRequest.BASIC_AUTH;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.logging.AssertableLogProvider.Level.WARN;
import static org.neo4j.logging.LogAssertions.assertThat;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Pattern;
import javax.servlet.FilterChain;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.HttpHeaders;
import org.apache.commons.codec.binary.Base64;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.server.security.auth.BasicLoginContext;
import org.neo4j.server.security.systemgraph.BasicSystemGraphRealm;
import org.neo4j.test.AuthTokenUtil;

class AuthorizationFilterTest {
    private final BasicSystemGraphRealm authManager = mock(BasicSystemGraphRealm.class);
    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    private final HttpServletRequest servletRequest = mock(HttpServletRequest.class);
    private final HttpServletResponse servletResponse = mock(HttpServletResponse.class);
    private final FilterChain filterChain = mock(FilterChain.class);

    @BeforeEach
    void setUp() throws Exception {
        when(servletResponse.getOutputStream()).thenReturn(new ServletOutputStream() {
            @Override
            public void write(int b) {
                outputStream.write(b);
            }

            @Override
            public boolean isReady() {
                return true;
            }

            @Override
            public void setWriteListener(WriteListener writeListener) {
                throw new UnsupportedOperationException();
            }
        });
    }

    @Test
    void shouldAllowOptionsRequests() throws Exception {
        // Given
        final AuthorizationEnabledFilter filter = newFilter();
        when(servletRequest.getMethod()).thenReturn("OPTIONS");

        // When
        filter.doFilter(servletRequest, servletResponse, filterChain);

        // Then
        verify(filterChain).doFilter(same(servletRequest), same(servletResponse));
    }

    @Test
    void shouldWhitelistMatchingUris() throws Exception {
        // Given
        final AuthorizationEnabledFilter filter = newFilter("/", "/browser.*");
        when(servletRequest.getMethod()).thenReturn("GET");
        when(servletRequest.getContextPath()).thenReturn("/", "/browser/index.html");

        // When
        filter.doFilter(servletRequest, servletResponse, filterChain);
        filter.doFilter(servletRequest, servletResponse, filterChain);

        // Then
        verify(filterChain, times(2)).doFilter(same(servletRequest), same(servletResponse));
    }

    @Test
    void shouldRequireAuthorizationForNonWhitelistedUris() throws Exception {
        // Given
        final AuthorizationEnabledFilter filter = newFilter("/", "/browser.*");
        when(servletRequest.getMethod()).thenReturn("GET");
        when(servletRequest.getContextPath()).thenReturn("/db/data");

        // When
        filter.doFilter(servletRequest, servletResponse, filterChain);

        // Then
        verifyNoMoreInteractions(filterChain);
        verify(servletResponse).setStatus(401);
        verify(servletResponse).addHeader(HttpHeaders.WWW_AUTHENTICATE, "Basic realm=\"Neo4j\"");
        verify(servletResponse).addHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=UTF-8");
        assertThat(outputStream.toString(UTF_8)).contains("\"code\":\"Neo" + ".ClientError.Security.Unauthorized\"");
        assertThat(outputStream.toString(UTF_8)).contains("\"message\":\"No authentication header supplied.\"");
    }

    @Test
    void shouldRequireValidAuthorizationHeader() throws Exception {
        // Given
        final AuthorizationEnabledFilter filter = newFilter();
        when(servletRequest.getMethod()).thenReturn("GET");
        when(servletRequest.getContextPath()).thenReturn("/db/data");
        when(servletRequest.getHeader(HttpHeaders.AUTHORIZATION)).thenReturn("NOT A VALID VALUE");

        // When
        filter.doFilter(servletRequest, servletResponse, filterChain);

        // Then
        verifyNoMoreInteractions(filterChain);
        verify(servletResponse).setStatus(400);
        verify(servletResponse).addHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=UTF-8");
        assertThat(outputStream.toString(UTF_8)).contains("\"code\":\"Neo.ClientError.Request.InvalidFormat\"");
        assertThat(outputStream.toString(UTF_8)).contains("\"message\":\"Invalid authentication header.\"");
    }

    @Test
    void shouldNotAuthorizeInvalidCredentials() throws Exception {
        // Given
        final AuthorizationEnabledFilter filter = newFilter();
        String credentials = Base64.encodeBase64String("foo:bar".getBytes(UTF_8));
        BasicLoginContext loginContext = mock(BasicLoginContext.class);
        AuthSubject authSubject = mock(AuthSubject.class);
        when(servletRequest.getRemoteAddr()).thenReturn("client");
        when(servletRequest.getRemotePort()).thenReturn(1337);
        when(servletRequest.getServerName()).thenReturn("server");
        when(servletRequest.getServerPort()).thenReturn(42);
        when(servletRequest.getMethod()).thenReturn("GET");
        when(servletRequest.getContextPath()).thenReturn("/db/data");
        when(servletRequest.getHeader(HttpHeaders.AUTHORIZATION)).thenReturn("BASIC " + credentials);
        when(servletRequest.getRemoteAddr()).thenReturn("remote_ip_address");
        when(authManager.login(argThat(new AuthTokenMatcher(authToken("foo", "bar"))), any()))
                .thenReturn(loginContext);
        when(loginContext.subject()).thenReturn(authSubject);
        when(authSubject.getAuthenticationResult()).thenReturn(AuthenticationResult.FAILURE);

        // When
        filter.doFilter(servletRequest, servletResponse, filterChain);

        // Then
        verifyNoMoreInteractions(filterChain);
        assertThat(logProvider)
                .forClass(AuthorizationEnabledFilter.class)
                .forLevel(WARN)
                .containsMessages("Failed authentication attempt for '%s' from %s", "foo", "remote_ip_address");
        verify(servletResponse).setStatus(401);
        verify(servletResponse).addHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=UTF-8");
        assertThat(outputStream.toString(UTF_8)).contains("\"code\":\"Neo.ClientError.Security.Unauthorized\"");
        assertThat(outputStream.toString(UTF_8)).contains("\"message\":\"Invalid username or password.\"");
    }

    @Test
    void shouldAuthorizeWhenPasswordChangeRequired() throws Exception {
        // Given
        final AuthorizationEnabledFilter filter = newFilter();
        String credentials = Base64.encodeBase64String("foo:bar".getBytes(UTF_8));
        BasicLoginContext loginContext = mock(BasicLoginContext.class);
        AuthSubject authSubject = mock(AuthSubject.class);
        when(servletRequest.getRemoteAddr()).thenReturn("client");
        when(servletRequest.getRemotePort()).thenReturn(1337);
        when(servletRequest.getServerName()).thenReturn("server");
        when(servletRequest.getServerPort()).thenReturn(42);
        when(servletRequest.getMethod()).thenReturn("GET");
        when(servletRequest.getContextPath()).thenReturn("/db/data");
        when(servletRequest.getRequestURL()).thenReturn(new StringBuffer("http://bar.baz:7474/db/data/"));
        when(servletRequest.getRequestURI()).thenReturn("/db/data/");
        when(servletRequest.getHeader(HttpHeaders.AUTHORIZATION)).thenReturn("BASIC " + credentials);
        when(authManager.login(argThat(new AuthTokenMatcher(authToken("foo", "bar"))), any()))
                .thenReturn(loginContext);
        when(loginContext.subject()).thenReturn(authSubject);
        when(authSubject.getAuthenticationResult()).thenReturn(AuthenticationResult.PASSWORD_CHANGE_REQUIRED);

        // When
        filter.doFilter(servletRequest, servletResponse, filterChain);

        // Then
        verify(filterChain)
                .doFilter(
                        eq(new AuthorizedRequestWrapper(BASIC_AUTH, "foo", servletRequest, AUTH_DISABLED)),
                        same(servletResponse));
    }

    @Test
    void shouldNotAuthorizeWhenTooManyAttemptsMade() throws Exception {
        // Given
        final AuthorizationEnabledFilter filter = newFilter();
        String credentials = Base64.encodeBase64String("foo:bar".getBytes(UTF_8));
        BasicLoginContext loginContext = mock(BasicLoginContext.class);
        AuthSubject authSubject = mock(AuthSubject.class);
        when(servletRequest.getRemoteAddr()).thenReturn("client");
        when(servletRequest.getRemotePort()).thenReturn(1337);
        when(servletRequest.getServerName()).thenReturn("server");
        when(servletRequest.getServerPort()).thenReturn(42);
        when(servletRequest.getMethod()).thenReturn("GET");
        when(servletRequest.getContextPath()).thenReturn("/db/data");
        when(servletRequest.getHeader(HttpHeaders.AUTHORIZATION)).thenReturn("BASIC " + credentials);
        when(authManager.login(argThat(new AuthTokenMatcher(authToken("foo", "bar"))), any()))
                .thenReturn(loginContext);
        when(loginContext.subject()).thenReturn(authSubject);
        when(authSubject.getAuthenticationResult()).thenReturn(AuthenticationResult.TOO_MANY_ATTEMPTS);

        // When
        filter.doFilter(servletRequest, servletResponse, filterChain);

        // Then
        verifyNoMoreInteractions(filterChain);
        verify(servletResponse).setStatus(429);
        verify(servletResponse).addHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=UTF-8");
        assertThat(outputStream.toString(UTF_8))
                .contains("\"code\":\"Neo.ClientError.Security.AuthenticationRateLimit\"");
        assertThat(outputStream.toString(UTF_8))
                .contains("\"message\":\"Too many failed authentication requests. "
                        + "Please wait 5 seconds and try again.\"");
    }

    @Test
    void shouldAuthorizeWhenValidCredentialsSupplied() throws Exception {
        // Given
        final AuthorizationEnabledFilter filter = newFilter();
        String credentials = Base64.encodeBase64String("foo:bar".getBytes(UTF_8));
        BasicLoginContext loginContext = mock(BasicLoginContext.class);
        AuthSubject authSubject = mock(AuthSubject.class);
        when(servletRequest.getRemoteAddr()).thenReturn("client");
        when(servletRequest.getRemotePort()).thenReturn(1337);
        when(servletRequest.getServerName()).thenReturn("server");
        when(servletRequest.getServerPort()).thenReturn(42);
        when(servletRequest.getMethod()).thenReturn("GET");
        when(servletRequest.getContextPath()).thenReturn("/db/data");
        when(servletRequest.getHeader(HttpHeaders.AUTHORIZATION)).thenReturn("BASIC " + credentials);
        when(authManager.login(argThat(new AuthTokenMatcher(authToken("foo", "bar"))), any()))
                .thenReturn(loginContext);
        when(loginContext.subject()).thenReturn(authSubject);
        when(authSubject.getAuthenticationResult()).thenReturn(AuthenticationResult.SUCCESS);

        // When
        filter.doFilter(servletRequest, servletResponse, filterChain);

        // Then
        verify(filterChain)
                .doFilter(
                        eq(new AuthorizedRequestWrapper(BASIC_AUTH, "foo", servletRequest, AUTH_DISABLED)),
                        same(servletResponse));
    }

    @Test
    void shouldIncludeCrippledAuthHeaderIfBrowserIsTheOneCalling() throws Throwable {
        // Given
        final AuthorizationEnabledFilter filter = newFilter("/", "/browser.*");
        when(servletRequest.getMethod()).thenReturn("GET");
        when(servletRequest.getContextPath()).thenReturn("/db/data");
        when(servletRequest.getHeader("X-Ajax-Browser-Auth")).thenReturn("true");

        // When
        filter.doFilter(servletRequest, servletResponse, filterChain);

        // Then
        verifyNoMoreInteractions(filterChain);
        verify(servletResponse).setStatus(401);
        verify(servletResponse).addHeader(HttpHeaders.WWW_AUTHENTICATE, "None");
        verify(servletResponse).addHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=UTF-8");
        assertThat(outputStream.toString(UTF_8)).contains("\"code\":\"Neo.ClientError.Security.Unauthorized\"");
        assertThat(outputStream.toString(UTF_8)).contains("\"message\":\"No authentication header supplied.\"");
    }

    private AuthorizationEnabledFilter newFilter(String... uriWhitelist) {
        var uriWhitelistPatterns =
                Arrays.stream(uriWhitelist).map(Pattern::compile).toList();

        return new AuthorizationEnabledFilter(() -> authManager, logProvider, uriWhitelistPatterns);
    }

    private record AuthTokenMatcher(Map<String, Object> expectedMap) implements ArgumentMatcher<Map<String, Object>> {
        @Override
        public boolean matches(Map<String, Object> map) {
            return AuthTokenUtil.matches(expectedMap, map);
        }
    }
}
