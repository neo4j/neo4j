/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.UriBuilder;

import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphdb.security.AuthProviderFailedException;
import org.neo4j.graphdb.security.AuthProviderTimeoutException;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.web.XForwardUtil;

import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static javax.servlet.http.HttpServletRequest.BASIC_AUTH;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.api.security.AuthToken.newBasicAuthToken;
import static org.neo4j.server.web.XForwardUtil.X_FORWARD_HOST_HEADER_KEY;
import static org.neo4j.server.web.XForwardUtil.X_FORWARD_PROTO_HEADER_KEY;

public class AuthorizationEnabledFilter extends AuthorizationFilter
{
    private static final Pattern PASSWORD_CHANGE_WHITELIST = Pattern.compile( "/user/.*" );

    private final Supplier<AuthManager> authManagerSupplier;
    private final Log log;
    private final Pattern[] uriWhitelist;

    public AuthorizationEnabledFilter( Supplier<AuthManager> authManager, LogProvider logProvider, Pattern... uriWhitelist )
    {
        this.authManagerSupplier = authManager;
        this.log = logProvider.getLog( getClass() );
        this.uriWhitelist = uriWhitelist;
    }

    @Override
    public void doFilter( ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain )
            throws IOException, ServletException
    {
        validateRequestType( servletRequest );
        validateResponseType( servletResponse );

        final HttpServletRequest request = (HttpServletRequest) servletRequest;
        final HttpServletResponse response = (HttpServletResponse) servletResponse;

        final String path = request.getContextPath() + ( request.getPathInfo() == null ? "" : request.getPathInfo() );

        if ( request.getMethod().equals( "OPTIONS" ) || whitelisted( path ) )
        {
            // NOTE: If starting transactions with access mode on whitelisted uris should be possible we need to
            //       wrap servletRequest in an AuthorizedRequestWarpper here
            filterChain.doFilter( servletRequest, servletResponse );
            return;
        }

        final String header = request.getHeader( HttpHeaders.AUTHORIZATION );
        if ( header == null )
        {
            requestAuthentication( request, noHeader ).accept( response );
            return;
        }

        final String[] usernameAndPassword = extractCredential( header );
        if ( usernameAndPassword == null )
        {
            badHeader.accept( response );
            return;
        }

        final String username = usernameAndPassword[0];
        final String password = usernameAndPassword[1];

        try
        {
            SecurityContext securityContext = authenticate( username, password );
            switch ( securityContext.subject().getAuthenticationResult() )
            {
            case PASSWORD_CHANGE_REQUIRED:
                if ( !PASSWORD_CHANGE_WHITELIST.matcher( path ).matches() )
                {
                    passwordChangeRequired( username, baseURL( request ) ).accept( response );
                    return;
                }
                // fall through
            case SUCCESS:
                try
                {
                    filterChain.doFilter( new AuthorizedRequestWrapper( BASIC_AUTH, username, request, securityContext ),
                            servletResponse );
                }
                catch ( AuthorizationViolationException e )
                {
                    unauthorizedAccess( e.getMessage() ).accept( response );
                }
                return;
            case TOO_MANY_ATTEMPTS:
                tooManyAttempts.accept( response );
                return;
            default:
                log.warn( "Failed authentication attempt for '%s' from %s", username, request.getRemoteAddr() );
                requestAuthentication( request, invalidCredential ).accept( response );
            }
        }
        catch ( InvalidAuthTokenException e )
        {
            requestAuthentication( request, invalidAuthToken( e.getMessage() ) ).accept( response );
        }
        catch ( AuthProviderTimeoutException e )
        {
            authProviderTimeout.accept( response );
        }
        catch ( AuthProviderFailedException e )
        {
            authProviderFailed.accept( response );
        }
    }

    private SecurityContext authenticate( String username, String password ) throws InvalidAuthTokenException
    {
        AuthManager authManager = authManagerSupplier.get();
        Map<String,Object> authToken = newBasicAuthToken( username, password );
        return authManager.login( authToken );
    }

    private static final ThrowingConsumer<HttpServletResponse, IOException> noHeader =
            error(  401,
                    map( "errors", singletonList( map(
                            "code", Status.Security.Unauthorized.code().serialize(),
                            "message", "No authentication header supplied." ) ) ) );

    private static final ThrowingConsumer<HttpServletResponse, IOException> badHeader =
            error(  400,
                    map( "errors", singletonList( map(
                            "code", Status.Request.InvalidFormat.code().serialize(),
                            "message", "Invalid authentication header." ) ) ) );

    private static final ThrowingConsumer<HttpServletResponse, IOException> invalidCredential =
            error(  401,
                    map( "errors", singletonList( map(
                            "code", Status.Security.Unauthorized.code().serialize(),
                            "message", "Invalid username or password." ) ) ) );

    private static final ThrowingConsumer<HttpServletResponse, IOException> tooManyAttempts =
            error(  429,
                    map( "errors", singletonList( map(
                            "code", Status.Security.AuthenticationRateLimit.code().serialize(),
                            "message", "Too many failed authentication requests. Please wait 5 seconds and try again." ) ) ) );

    private static final ThrowingConsumer<HttpServletResponse, IOException> authProviderFailed =
            error(  502,
                    map( "errors", singletonList( map(
                            "code", Status.Security.AuthProviderFailed.code().serialize(),
                            "message", "An auth provider request failed." ) ) ) );

    private static final ThrowingConsumer<HttpServletResponse, IOException> authProviderTimeout =
            error(  504,
                    map( "errors", singletonList( map(
                            "code", Status.Security.AuthProviderTimeout.code().serialize(),
                            "message", "An auth provider request timed out." ) ) ) );

    private static ThrowingConsumer<HttpServletResponse, IOException> invalidAuthToken( final String message )
    {
        return error( 401,
                map( "errors", singletonList( map(
                        "code", Status.Security.Unauthorized.code().serialize(),
                        "message", message ) ) ) );
    }

    private static ThrowingConsumer<HttpServletResponse, IOException> passwordChangeRequired( final String username, final String baseURL )
    {
        URI path = UriBuilder.fromUri( baseURL ).path( format( "/user/%s/password", username ) ).build();
        return error( 403,
                map( "errors", singletonList( map(
                        "code", Status.Security.Forbidden.code().serialize(),
                        "message", "User is required to change their password." ) ), "password_change", path.toString() ) );
    }

    /**
     * In order to avoid browsers popping up an auth box when using the Neo4j Browser, it sends us a special header.
     * When we get that special header, we send a crippled authentication challenge back that the browser does not
     * understand, which lets the Neo4j Browser handle auth on its own.
     *
     * Otherwise, we send a regular basic auth challenge. This method adds the appropriate header depending on the
     * inbound request.
     */
    private static ThrowingConsumer<HttpServletResponse, IOException> requestAuthentication(
            HttpServletRequest req, ThrowingConsumer<HttpServletResponse, IOException> responseGen )
    {
        if ( "true".equals( req.getHeader( "X-Ajax-Browser-Auth" ) ) )
        {
            return res ->
            {
                responseGen.accept( res );
                res.addHeader( HttpHeaders.WWW_AUTHENTICATE, "None" );
            };
        }
        else
        {
            return res ->
            {
                responseGen.accept( res );
                res.addHeader( HttpHeaders.WWW_AUTHENTICATE, "Basic realm=\"Neo4j\"" );
            };
        }
    }

    private String baseURL( HttpServletRequest request )
    {
        StringBuffer url = request.getRequestURL();
        String baseURL = url.substring( 0, url.length() - request.getRequestURI().length() ) + "/";

        return XForwardUtil.externalUri(
                baseURL,
                request.getHeader( X_FORWARD_HOST_HEADER_KEY ),
                request.getHeader( X_FORWARD_PROTO_HEADER_KEY ) );
    }

    private boolean whitelisted( String path )
    {
        for ( Pattern pattern : uriWhitelist )
        {
            if ( pattern.matcher( path ).matches() )
            {
                return true;
            }
        }
        return false;
    }

    private String[] extractCredential( String header )
    {
        if ( header == null )
        {
            return null;
        }
        else
        {
            return AuthorizationHeaders.decode( header );
        }
    }
}
