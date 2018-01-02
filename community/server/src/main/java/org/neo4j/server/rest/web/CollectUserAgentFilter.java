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
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.neo4j.concurrent.RecentK;

/**
 * Collects user agent information and publishes it to a tracker of most recently seen user agents.
 */
public class CollectUserAgentFilter implements Filter
{
    private final RecentK<String> output;

    public CollectUserAgentFilter( RecentK<String> output )
    {
        this.output = output;
    }

    @Override
    public void init( FilterConfig filterConfig ) throws ServletException
    {

    }

    @Override
    public void doFilter( ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain )
            throws IOException, ServletException
    {
        try
        {
            final HttpServletRequest request = (HttpServletRequest) servletRequest;
            String ua = request.getHeader( "User-Agent" );
            if ( ua != null && !ua.isEmpty() )
            {
                output.add( ua.split( " " )[0] );
            }
        }
        catch ( RuntimeException e )
        {
            // We're fine with that
        }

        filterChain.doFilter( servletRequest, servletResponse );
    }

    @Override
    public void destroy()
    {

    }
}
