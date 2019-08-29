/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import java.nio.charset.StandardCharsets;
import javax.servlet.Filter;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.HttpHeaders;

import org.neo4j.function.ThrowingConsumer;
import org.neo4j.server.rest.domain.JsonHelper;

import static java.lang.String.format;

public abstract class AbstractFilter implements Filter
{
    protected static ThrowingConsumer<HttpServletResponse, IOException> error( int code, Object body )
    {
        return response ->
        {
            response.setStatus( code );
            response.addHeader( HttpHeaders.CONTENT_TYPE, "application/json; charset=UTF-8" );
            response.getOutputStream().write( JsonHelper.createJsonFrom( body ).getBytes( StandardCharsets.UTF_8 ) );
        };
    }

    @Override
    public void init( FilterConfig filterConfig )
    {
    }

    @Override
    public void destroy()
    {
    }

    protected HttpServletRequest validateRequestType( ServletRequest request ) throws ServletException
    {
        if ( !(request instanceof HttpServletRequest) )
        {
            throw new ServletException( format( "Expected HttpServletRequest, received [%s]", request.getClass().getCanonicalName() ) );
        }
        return (HttpServletRequest) request;
    }

    protected HttpServletResponse validateResponseType( ServletResponse response ) throws ServletException
    {
        if ( !(response instanceof HttpServletResponse) )
        {
            throw new ServletException( format( "Expected HttpServletResponse, received [%s]", response.getClass().getCanonicalName() ) );
        }
        return (HttpServletResponse) response;
    }
}
