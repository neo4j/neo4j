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

import java.io.IOException;
import java.net.URI;
import java.util.regex.Pattern;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.UriBuilder;

import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.Charsets;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.security.auth.AuthManager;
import org.neo4j.server.web.XForwardUtil;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static javax.servlet.http.HttpServletRequest.BASIC_AUTH;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.server.web.XForwardUtil.X_FORWARD_HOST_HEADER_KEY;
import static org.neo4j.server.web.XForwardUtil.X_FORWARD_PROTO_HEADER_KEY;

public class AuthorizationFilter implements Filter
{
    private static final Pattern PASSWORD_CHANGE_WHITELIST = Pattern.compile( "/user/.*" );

    private final AuthManager authManager;
    private final Log log;
    private final Pattern[] uriWhitelist;

    public AuthorizationFilter( AuthManager authManager, LogProvider logProvider, Pattern... uriWhitelist )
    {
        this.authManager = authManager;
        this.log = logProvider.getLog( getClass() );
        this.uriWhitelist = uriWhitelist;
    }

    @Override
    public void init( FilterConfig filterConfig ) throws ServletException
    {
    }

    @Override
    public void doFilter( ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain ) throws IOException, ServletException
    {
        validateRequestType( servletRequest );
        validateResponseType( servletResponse );

        final HttpServletRequest request = (HttpServletRequest) servletRequest;
        final HttpServletResponse response = (HttpServletResponse) servletResponse;

        final String path = request.getContextPath() + ( request.getPathInfo() == null ? "" : request.getPathInfo() );

        if ( request.getMethod().equals( "OPTIONS" ) || whitelisted( path ) )
        {
            filterChain.doFilter( servletRequest, servletResponse );
            return;
        }

        final String header = request.getHeader( HttpHeaders.AUTHORIZATION );
        if ( header == null )
        {
            noHeader().writeResponse( response );
            return;
        }

        final String[] usernameAndPassword = extractCredential( header );
        if ( usernameAndPassword == null )
        {
            badHeader().writeResponse( response );
            return;
        }

        final String username = usernameAndPassword[0];
        final String password = usernameAndPassword[1];

        switch ( authManager.authenticate( username, password ) )
        {
            case PASSWORD_CHANGE_REQUIRED:
                if ( !PASSWORD_CHANGE_WHITELIST.matcher( path ).matches() )
                {
                    passwordChangeRequired( username, baseURL( request ) ).writeResponse( response );
                    return;
                }
                // fall through
            case SUCCESS:
                filterChain.doFilter( new AuthorizedRequestWrapper( BASIC_AUTH, username, request ), servletResponse );
                return;
            case TOO_MANY_ATTEMPTS:
                tooManyAttemptes().writeResponse( response );
                return;
            default:
                log.warn( "Failed authentication attempt for '%s' from %s", username, request.getRemoteAddr() );
                invalidCredential().writeResponse( response );
                return;
        }
    }

    // This is a pretty annoying duplication of work, where we're manually re-implementing the JSON serialization
    // layer. Because we don't have access to jersey at this level, we can't use our regular serialization. This,
    // obviously, implies a larger architectural issue, which is left as a future exercise.
    private static abstract class ErrorResponse
    {
        private final int statusCode;

        private ErrorResponse( int statusCode )
        {
            this.statusCode = statusCode;
        }

        void addHeaders( HttpServletResponse response )
        {
        }

        abstract Object body();

        void writeResponse( HttpServletResponse response ) throws IOException
        {
            response.setStatus( statusCode );
            response.addHeader( HttpHeaders.CONTENT_TYPE, "application/json; charset=UTF-8" );
            addHeaders( response );
            response.getOutputStream().write( JsonHelper.createJsonFrom( body() ).getBytes( Charsets.UTF_8 ) );
        }
    }

    private static final ErrorResponse NO_HEADER = new ErrorResponse( 401 )
    {
        @Override
        void addHeaders( HttpServletResponse response )
        {
            response.addHeader( HttpHeaders.WWW_AUTHENTICATE, "None" );
        }

        @Override
        Object body()
        {
            return map( "errors", asList( map(
                    "code", Status.Security.AuthorizationFailed.code().serialize(),
                    "message", "No authorization header supplied." ) ) );
        }
    };

    private static ErrorResponse noHeader()
    {
        return NO_HEADER;
    }

    private static final ErrorResponse BAD_HEADER = new ErrorResponse( 400 )
    {
        @Override
        Object body()
        {
            return map( "errors", asList( map(
                    "code", Status.Request.InvalidFormat.code().serialize(),
                    "message", "Invalid Authorization header." ) ) );
        }
    };

    private static ErrorResponse badHeader()
    {
        return BAD_HEADER;
    }

    private static final ErrorResponse INVALID_CREDENTIAL = new ErrorResponse( 401 )
    {
        @Override
        void addHeaders( HttpServletResponse response )
        {
            response.addHeader( HttpHeaders.WWW_AUTHENTICATE, "None" );
        }

        @Override
        Object body()
        {
            return map( "errors", asList( map(
                    "code", Status.Security.AuthorizationFailed.code().serialize(),
                    "message", "Invalid username or password." ) ) );
        }
    };

    private static ErrorResponse invalidCredential()
    {
        return INVALID_CREDENTIAL;
    }

    private static final ErrorResponse TOO_MANY_ATTEMPTS = new ErrorResponse( 429 )
    {
        @Override
        Object body()
        {
            return map( "errors", asList( map(
                    "code", Status.Security.AuthenticationRateLimit.code().serialize(),
                    "message", "Too many failed authentication requests. Please wait 5 seconds and try again." ) ) );
        }
    };

    private static ErrorResponse tooManyAttemptes()
    {
        return TOO_MANY_ATTEMPTS;
    }

    private static ErrorResponse passwordChangeRequired( final String username, final String baseURL )
    {
        return new ErrorResponse( 403 )
        {
            @Override
            Object body()
            {
                URI path = UriBuilder.fromUri( baseURL ).path( format( "/user/%s/password", username ) ).build();
                return map( "errors", asList( map(
                        "code", Status.Security.AuthorizationFailed.code().serialize(),
                        "message", "User is required to change their password."
                ) ), "password_change", path.toString() );
            }
        };
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

    @Override
    public void destroy()
    {
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
        } else
        {
            return AuthorizationHeaders.decode( header );
        }
    }

    private void validateRequestType( ServletRequest request ) throws ServletException
    {
        if ( !( request instanceof HttpServletRequest ) )
        {
            throw new ServletException( format( "Expected HttpServletRequest, received [%s]", request.getClass()
                    .getCanonicalName() ) );
        }
    }

    private void validateResponseType( ServletResponse response ) throws ServletException
    {
        if ( !( response instanceof HttpServletResponse ) )
        {
            throw new ServletException( format( "Expected HttpServletResponse, received [%s]",
                    response.getClass()
                            .getCanonicalName() ) );
        }
    }
}
