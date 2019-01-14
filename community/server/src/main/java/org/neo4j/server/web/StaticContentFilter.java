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
package org.neo4j.server.web;

import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class StaticContentFilter implements Filter
{
    @Override
    public void init( FilterConfig filterConfig )
    {
    }

    @Override
    public void doFilter( ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain )
            throws IOException, ServletException
    {
        HttpServletRequest request = (HttpServletRequest) servletRequest;
        HttpServletResponse response = (HttpServletResponse) servletResponse;
        if ( request.getServletPath() != null && request.getServletPath().endsWith( ".html" ) )
        {
            response.addHeader( "Cache-Control", "private, no-cache, no-store, proxy-revalidate, no-transform" );
            response.addHeader( "Pragma", "no-cache" );
            response.addHeader( "Content-Security-Policy", "frame-ancestors 'none'" );
            response.addHeader( "X-Frame-Options", "DENY" );
            response.addHeader( "X-Content-Type-Options", "nosniff" );
            response.addHeader( "X-XSS-Protection", "1; mode=block" );
        }
        filterChain.doFilter( servletRequest, servletResponse);
    }

    @Override
    public void destroy()
    {
    }
}
