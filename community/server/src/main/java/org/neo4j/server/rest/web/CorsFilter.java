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
package org.neo4j.server.rest.web;

import java.io.IOException;
import java.util.Enumeration;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.web.HttpMethod;

import static org.neo4j.server.web.HttpHeaderUtils.isValidHttpHeaderName;

/**
 * This filter adds the header "Access-Control-Allow-Origin : *" to all
 * responses that goes through it. This allows modern browsers to do cross-site
 * requests to us via javascript.
 */
public class CorsFilter implements Filter
{
    public static final String ACCESS_CONTROL_ALLOW_ORIGIN = "Access-Control-Allow-Origin";
    public static final String ACCESS_CONTROL_ALLOW_METHODS = "Access-Control-Allow-Methods";
    public static final String ACCESS_CONTROL_ALLOW_HEADERS = "Access-Control-Allow-Headers";
    public static final String ACCESS_CONTROL_REQUEST_METHOD = "Access-Control-Request-Method";
    public static final String ACCESS_CONTROL_REQUEST_HEADERS = "Access-Control-Request-Headers";

    private final Log log;

    public CorsFilter( LogProvider logProvider )
    {
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void init( FilterConfig filterConfig ) throws ServletException
    {
    }

    @Override
    public void doFilter( ServletRequest servletRequest, ServletResponse servletResponse, FilterChain chain )
            throws IOException, ServletException
    {
        HttpServletRequest request = (HttpServletRequest) servletRequest;
        HttpServletResponse response = (HttpServletResponse) servletResponse;

        response.setHeader( ACCESS_CONTROL_ALLOW_ORIGIN, "*" );

        Enumeration<String> requestMethodEnumeration = request.getHeaders( ACCESS_CONTROL_REQUEST_METHOD );
        if ( requestMethodEnumeration != null )
        {
            while ( requestMethodEnumeration.hasMoreElements() )
            {
                String requestMethod = requestMethodEnumeration.nextElement();
                addAllowedMethodIfValid( requestMethod, response );
            }
        }

        Enumeration<String> requestHeaderEnumeration = request.getHeaders( ACCESS_CONTROL_REQUEST_HEADERS );
        if ( requestHeaderEnumeration != null )
        {
            while ( requestHeaderEnumeration.hasMoreElements() )
            {
                String requestHeader = requestHeaderEnumeration.nextElement();
                addAllowedHeaderIfValid( requestHeader, response );
            }
        }

        chain.doFilter( request, response );
    }

    @Override
    public void destroy()
    {
    }

    private void addAllowedMethodIfValid( String methodName, HttpServletResponse response )
    {
        HttpMethod method = HttpMethod.valueOfOrNull( methodName );
        if ( method != null )
        {
            response.addHeader( ACCESS_CONTROL_ALLOW_METHODS, methodName );
        }
        else
        {
            log.warn( "Unknown HTTP method specified in " + ACCESS_CONTROL_REQUEST_METHOD + " '" + methodName + "'. " +
                      "It will be ignored and not attached to the " + ACCESS_CONTROL_ALLOW_METHODS + " response header" );
        }
    }

    private void addAllowedHeaderIfValid( String headerName, HttpServletResponse response )
    {
        if ( isValidHttpHeaderName( headerName ) )
        {
            response.addHeader( ACCESS_CONTROL_ALLOW_HEADERS, headerName );
        }
        else
        {
            log.warn( "Invalid HTTP header specified in " + ACCESS_CONTROL_REQUEST_HEADERS + " '" + headerName + "'. " +
                      "It will be ignored and not attached to the " + ACCESS_CONTROL_ALLOW_HEADERS + " response header" );
        }
    }
}
