/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.server.rest.dbms;

import org.apache.commons.codec.binary.Base64;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.kernel.impl.util.Charsets;
import org.neo4j.server.security.auth.AuthManager;
import org.neo4j.server.security.auth.AuthenticationResult;

import javax.servlet.FilterChain;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.HttpHeaders;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.regex.Pattern;

import static javax.servlet.http.HttpServletRequest.BASIC_AUTH;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.*;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class AuthorizationFilterTest
{
    private final AuthManager authManager = mock( AuthManager.class );
    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    private final HttpServletRequest servletRequest = mock( HttpServletRequest.class );
    private final HttpServletResponse servletResponse = mock( HttpServletResponse.class );
    private final FilterChain filterChain = mock( FilterChain.class );

    @Before
    public void setUp() throws Exception
    {
        when( servletResponse.getOutputStream() ).thenReturn( new ServletOutputStream()
        {
            @Override
            public void write( int b ) throws IOException
            {
                outputStream.write( b );
            }

            @Override
            public boolean isReady()
            {
                return true;
            }

            @Override
            public void setWriteListener( WriteListener writeListener )
            {
                throw new UnsupportedOperationException();
            }
        } );
    }

    @Test
    public void shouldAllowOptionsRequests() throws Exception
    {
        // Given
        final AuthorizationFilter filter = new AuthorizationFilter( authManager, logProvider );
        when( servletRequest.getMethod() ).thenReturn( "OPTIONS" );

        // When
        filter.doFilter( servletRequest, servletResponse, filterChain );

        // Then
        verify( filterChain ).doFilter( same( servletRequest ), same( servletResponse ) );
    }

    @Test
    public void shouldWhitelistMatchingUris() throws Exception
    {
        // Given
        final AuthorizationFilter filter = new AuthorizationFilter( authManager, logProvider,
                Pattern.compile( "/" ), Pattern.compile( "/webadmin.*" ), Pattern.compile( "/browser.*" ) );
        when( servletRequest.getMethod() ).thenReturn( "GET" );
        when( servletRequest.getContextPath() ).thenReturn( "/", "/webadmin/index.html", "/browser/index.html" );

        // When
        filter.doFilter( servletRequest, servletResponse, filterChain );
        filter.doFilter( servletRequest, servletResponse, filterChain );
        filter.doFilter( servletRequest, servletResponse, filterChain );

        // Then
        verify( filterChain, times( 3 ) ).doFilter( same( servletRequest ), same( servletResponse ) );
    }

    @Test
    public void shouldRequireAuthorizationForNonWhitelistedUris() throws Exception
    {
        // Given
        final AuthorizationFilter filter = new AuthorizationFilter( authManager, logProvider, Pattern.compile( "/" ), Pattern.compile( "/browser.*" ) );
        when( servletRequest.getMethod() ).thenReturn( "GET" );
        when( servletRequest.getContextPath() ).thenReturn( "/db/data" );

        // When
        filter.doFilter( servletRequest, servletResponse, filterChain );

        // Then
        verifyNoMoreInteractions( filterChain );
        verify( servletResponse ).setStatus( 401 );
        verify( servletResponse ).addHeader( HttpHeaders.WWW_AUTHENTICATE, "None" );
        verify( servletResponse ).addHeader( HttpHeaders.CONTENT_TYPE, "application/json; charset=UTF-8" );
        assertThat( outputStream.toString( Charsets.UTF_8.name() ), containsString( "\"code\" : \"Neo.ClientError.Security.AuthorizationFailed\"" ) );
        assertThat( outputStream.toString( Charsets.UTF_8.name() ), containsString( "\"message\" : \"No authorization header supplied.\"" ) );
    }

    @Test
    public void shouldRequireValidAuthorizationHeader() throws Exception
    {
        // Given
        final AuthorizationFilter filter = new AuthorizationFilter( authManager, logProvider );
        when( servletRequest.getMethod() ).thenReturn( "GET" );
        when( servletRequest.getContextPath() ).thenReturn( "/db/data" );
        when( servletRequest.getHeader( HttpHeaders.AUTHORIZATION ) ).thenReturn( "NOT A VALID VALUE" );

        // When
        filter.doFilter( servletRequest, servletResponse, filterChain );

        // Then
        verifyNoMoreInteractions( filterChain );
        verify( servletResponse ).setStatus( 400 );
        verify( servletResponse ).addHeader( HttpHeaders.CONTENT_TYPE, "application/json; charset=UTF-8" );
        assertThat( outputStream.toString( Charsets.UTF_8.name() ), containsString( "\"code\" : \"Neo.ClientError.Request.InvalidFormat\"" ) );
        assertThat( outputStream.toString( Charsets.UTF_8.name() ), containsString( "\"message\" : \"Invalid Authorization header.\"" ) );
    }

    @Test
    public void shouldNotAuthorizeInvalidCredentials() throws Exception
    {
        // Given
        final AuthorizationFilter filter = new AuthorizationFilter( authManager, logProvider );
        String credentials = Base64.encodeBase64String( "foo:bar".getBytes( Charsets.UTF_8 ) );
        when( servletRequest.getMethod() ).thenReturn( "GET" );
        when( servletRequest.getContextPath() ).thenReturn( "/db/data" );
        when( servletRequest.getHeader( HttpHeaders.AUTHORIZATION ) ).thenReturn( "BASIC " + credentials );
        when( servletRequest.getRemoteAddr() ).thenReturn( "remote_ip_address" );
        when( authManager.authenticate( "foo", "bar" ) ).thenReturn( AuthenticationResult.FAILURE );

        // When
        filter.doFilter( servletRequest, servletResponse, filterChain );

        // Then
        verifyNoMoreInteractions( filterChain );
        logProvider.assertExactly(
                inLog( AuthorizationFilter.class ).warn( "Failed authentication attempt for '%s' from %s", "foo", "remote_ip_address" )
        );
        verify( servletResponse ).setStatus( 401 );
        verify( servletResponse ).addHeader( HttpHeaders.CONTENT_TYPE, "application/json; charset=UTF-8" );
        assertThat( outputStream.toString( Charsets.UTF_8.name() ), containsString( "\"code\" : \"Neo.ClientError.Security.AuthorizationFailed\"" ) );
        assertThat( outputStream.toString( Charsets.UTF_8.name() ), containsString( "\"message\" : \"Invalid username or password.\"" ) );
    }

    @Test
    public void shouldAuthorizeWhenPasswordChangeRequiredForWhitelistedPath() throws Exception
    {
        // Given
        final AuthorizationFilter filter = new AuthorizationFilter( authManager, logProvider );
        String credentials = Base64.encodeBase64String( "foo:bar".getBytes( Charsets.UTF_8 ) );
        when( servletRequest.getMethod() ).thenReturn( "GET" );
        when( servletRequest.getContextPath() ).thenReturn( "/user/foo" );
        when( servletRequest.getHeader( HttpHeaders.AUTHORIZATION ) ).thenReturn( "BASIC " + credentials );
        when( authManager.authenticate( "foo", "bar" ) ).thenReturn( AuthenticationResult.PASSWORD_CHANGE_REQUIRED );

        // When
        filter.doFilter( servletRequest, servletResponse, filterChain );

        // Then
        verify( filterChain ).doFilter( eq( new AuthorizedRequestWrapper( BASIC_AUTH, "foo", servletRequest ) ), same( servletResponse ) );
    }

    @Test
    public void shouldNotAuthorizeWhenPasswordChangeRequired() throws Exception
    {
        // Given
        final AuthorizationFilter filter = new AuthorizationFilter( authManager, logProvider );
        String credentials = Base64.encodeBase64String( "foo:bar".getBytes( Charsets.UTF_8 ) );
        when( servletRequest.getMethod() ).thenReturn( "GET" );
        when( servletRequest.getContextPath() ).thenReturn( "/db/data" );
        when( servletRequest.getRequestURL() ).thenReturn( new StringBuffer( "http://bar.baz:7474/db/data/" ) );
        when( servletRequest.getRequestURI() ).thenReturn( "/db/data/" );
        when( servletRequest.getHeader( HttpHeaders.AUTHORIZATION ) ).thenReturn( "BASIC " + credentials );
        when( authManager.authenticate( "foo", "bar" ) ).thenReturn( AuthenticationResult.PASSWORD_CHANGE_REQUIRED );

        // When
        filter.doFilter( servletRequest, servletResponse, filterChain );

        // Then
        verifyNoMoreInteractions( filterChain );
        verify( servletResponse ).setStatus( 403 );
        verify( servletResponse ).addHeader( HttpHeaders.CONTENT_TYPE, "application/json; charset=UTF-8" );
        assertThat( outputStream.toString( Charsets.UTF_8.name() ), containsString( "\"password_change\" : \"http://bar.baz:7474/user/foo/password\"" ) );
        assertThat( outputStream.toString( Charsets.UTF_8.name() ), containsString( "\"code\" : \"Neo.ClientError.Security.AuthorizationFailed\"" ) );
        assertThat( outputStream.toString( Charsets.UTF_8.name() ), containsString( "\"message\" : \"User is required to change their password.\"" ) );
    }

    @Test
    public void shouldNotAuthorizeWhenTooManyAttemptsMade() throws Exception
    {
        // Given
        final AuthorizationFilter filter = new AuthorizationFilter( authManager, logProvider );
        String credentials = Base64.encodeBase64String( "foo:bar".getBytes( Charsets.UTF_8 ) );
        when( servletRequest.getMethod() ).thenReturn( "GET" );
        when( servletRequest.getContextPath() ).thenReturn( "/db/data" );
        when( servletRequest.getHeader( HttpHeaders.AUTHORIZATION ) ).thenReturn( "BASIC " + credentials );
        when( authManager.authenticate( "foo", "bar" ) ).thenReturn( AuthenticationResult.TOO_MANY_ATTEMPTS );

        // When
        filter.doFilter( servletRequest, servletResponse, filterChain );

        // Then
        verifyNoMoreInteractions( filterChain );
        verify( servletResponse ).setStatus( 429 );
        verify( servletResponse ).addHeader( HttpHeaders.CONTENT_TYPE, "application/json; charset=UTF-8" );
        assertThat( outputStream.toString( Charsets.UTF_8.name() ), containsString( "\"code\" : \"Neo.ClientError.Security.AuthenticationRateLimit\"" ) );
        assertThat( outputStream.toString( Charsets.UTF_8.name() ), containsString( "\"message\" : \"Too many failed authentication requests. Please wait 5 seconds and try again.\"" ) );
    }

    @Test
    public void shouldAuthorizeWhenValidCredentialsSupplied() throws Exception
    {
        // Given
        final AuthorizationFilter filter = new AuthorizationFilter( authManager, logProvider );
        String credentials = Base64.encodeBase64String( "foo:bar".getBytes( Charsets.UTF_8 ) );
        when( servletRequest.getMethod() ).thenReturn( "GET" );
        when( servletRequest.getContextPath() ).thenReturn( "/db/data" );
        when( servletRequest.getHeader( HttpHeaders.AUTHORIZATION ) ).thenReturn( "BASIC " + credentials );
        when( authManager.authenticate( "foo", "bar" ) ).thenReturn( AuthenticationResult.SUCCESS );

        // When
        filter.doFilter( servletRequest, servletResponse, filterChain );

        // Then
        verify( filterChain ).doFilter( eq( new AuthorizedRequestWrapper( BASIC_AUTH, "foo", servletRequest ) ), same( servletResponse ) );
    }
}
