/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.server.statistic;

import org.mortbay.jetty.Response;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.ServletResponseWrapper;
import java.io.IOException;

import static java.lang.System.nanoTime;

/**
 * @author tbaum
 * @since 19.05.11 16:47
 */
public class StatisticFilter implements Filter
{

    private final StatisticCollector collector;

    public StatisticFilter( final StatisticCollector collector )
    {
        this.collector = collector;
    }

    @Override
    public void init( FilterConfig filterConfig ) throws ServletException
    {
    }

    @Override
    public void doFilter( ServletRequest request, ServletResponse response,
                          FilterChain chain ) throws IOException, ServletException
    {
        final Long start = nanoTime();
        try
        {
            chain.doFilter( request, response );
        } finally
        {
            collector.update( ( nanoTime() - start ) / 1000000.0, getResponseSize( response ) );
        }
    }

    private long getResponseSize( final ServletResponse response )
    {
        if ( response instanceof ServletResponseWrapper )
        {
            ServletResponseWrapper wrapper = (ServletResponseWrapper) response;
            return getResponseSize( wrapper.getResponse() );
        }
        if ( response instanceof Response )
        {
            return ( (Response) response ).getContentCount();
        }
        return 0;
    }

    @Override
    public void destroy()
    {
    }
}
