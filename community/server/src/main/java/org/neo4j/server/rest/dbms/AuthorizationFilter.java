/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.HttpHeaders;

import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.Charsets;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.security.UriPathWildcardMatcher;
import org.neo4j.server.security.auth.SecurityCentral;

import static java.util.Arrays.asList;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.server.rest.dbms.AuthenticationService.AUTHENTICATION_PATH;

public class AuthorizationFilter implements Filter
{
    enum ErrorType
    {
        // This is a pretty annoying duplication of work, where we're manually re-implementing the JSON serialization
        // layer. Because we don't have access to jersey at this level, we can't use our regular serialization. This,
        // obviously, implies a larger architectural issue, which is left as a future exercise.

        NO_HEADER(401, true )
        {
            @Override
            Object body(String authURL)
            {
                return map("errors", asList(map(
                                "code", Status.Security.AuthorizationFailed.code().serialize(),
                                "message", "No authorization token supplied.")),
                           "authentication", authURL);
            }
        },
        INVALID_TOKEN(401, true )
        {
            @Override
            Object body(String authURL)
            {
                return map("errors", asList(map(
                                "code", Status.Security.AuthorizationFailed.code().serialize(),
                                "message", "Invalid authorization token supplied.")),
                           "authentication", authURL);
            }
        },
        BAD_HEADER(400, false)
        {
            @Override
            Object body(String authURL)
            {
                return map("errors", asList(map(
                        "code", Status.Request.InvalidFormat.code().serialize(),
                        "message", "Invalid Authorization header.")));
            }
        };

        private final int statusCode;
        private final boolean includeWWWAuthenticateHeader;

        private ErrorType( int statusCode, boolean includeWWWAuthenticateHeader )
        {
            this.statusCode = statusCode;
            this.includeWWWAuthenticateHeader = includeWWWAuthenticateHeader;
        }

        synchronized void reply( HttpServletResponse response, HttpServletRequest req ) throws IOException
        {
            response.setStatus( statusCode );
            if(includeWWWAuthenticateHeader)
            {
                response.addHeader( HttpHeaders.WWW_AUTHENTICATE, "None" );
            }

            String authUrl = req.getScheme() + "://" + req.getHeader( HttpHeaders.HOST ) + AUTHENTICATION_PATH;

            response.getOutputStream().write( JsonHelper.createJsonFrom( body(authUrl) ).getBytes( Charsets.UTF_8 ) );
        }

        abstract Object body(String authenticateURL);
    }

    private final UriPathWildcardMatcher[] whitelist = new UriPathWildcardMatcher[]
    {
        new UriPathWildcardMatcher("/authentication"),
        new UriPathWildcardMatcher("/browser*"),
        new UriPathWildcardMatcher("/webadmin*"),
        new UriPathWildcardMatcher("/user/*/authorization_token"),
        new UriPathWildcardMatcher("/user/*/password"),
        new UriPathWildcardMatcher("/"),
    };

    private final SecurityCentral security;

    public AuthorizationFilter( SecurityCentral security )
    {
        this.security = security;
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

        HttpServletRequest request = (HttpServletRequest) servletRequest;
        HttpServletResponse response = (HttpServletResponse) servletResponse;

        if(authorized(request) || whitelisted(request))
        {
            filterChain.doFilter( servletRequest, servletResponse );
        }
        else
        {
            errorType(request).reply( response, request );
        }
    }

    @Override
    public void destroy()
    {

    }

    private boolean whitelisted( HttpServletRequest request )
    {
        String path = request.getContextPath() + (request.getPathInfo() == null ? "" : request.getPathInfo());
        for ( UriPathWildcardMatcher pattern : whitelist )
        {
            if(pattern.matches( path ))
            {
                return true;
            }
        }
        return false;
    }

    private boolean authorized( HttpServletRequest request )
    {
        String token = extractToken( request );
        return token != null && security.userForToken( token ).privileges().APIAccess();
    }

    private ErrorType errorType( HttpServletRequest request )
    {
        String token = extractToken( request );
        if(token == null)
        {
            return ErrorType.NO_HEADER;
        }
        else if(token.length() == 0)
        {
            return ErrorType.BAD_HEADER;
        }
        return ErrorType.INVALID_TOKEN;
    }

    private String extractToken( HttpServletRequest request )
    {
        String value = request.getHeader( HttpHeaders.AUTHORIZATION );
        if(value == null)
        {
            return null;
        }
        else
        {
            return AuthenticateHeaders.extractToken( value );
        }
    }

    private void validateRequestType( ServletRequest request ) throws ServletException
    {
        if ( !(request instanceof HttpServletRequest) )
        {
            throw new ServletException( String.format( "Expected HttpServletRequest, received [%s]", request.getClass()
                    .getCanonicalName() ) );
        }
    }

    private void validateResponseType( ServletResponse response ) throws ServletException
    {
        if ( !(response instanceof HttpServletResponse) )
        {
            throw new ServletException( String.format( "Expected HttpServletResponse, received [%s]",
                    response.getClass()
                            .getCanonicalName() ) );
        }
    }
}
