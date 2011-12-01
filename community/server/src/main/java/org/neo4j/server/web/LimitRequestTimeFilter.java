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
package org.neo4j.server.web;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

/**
 * @author tbaum
 * @since 01.12.11
 */
public class LimitRequestTimeFilter implements Filter
{
    private static final Log LOG = LogFactory.getLog( LimitRequestTimeFilter.class );
    private final int timeLimit;
    private final Timer timer = new Timer();

    public LimitRequestTimeFilter( int timeLimit )
    {
        this.timeLimit = timeLimit;
    }

    @Override
    public void init( final FilterConfig filterConfig ) throws ServletException
    {
    }

    public void doFilter( final ServletRequest req, final ServletResponse res, final FilterChain chain )
            throws ServletException, IOException
    {
        HttpServletRequest request = (HttpServletRequest) req;

        int timeLimit = getTimeLimit( request );
        if ( timeLimit <= 0 )
        {
            chain.doFilter( req, res );
        } else
        {
            TimerTask timer = createTimer( timeLimit );
            try
            {
                chain.doFilter( req, res );
            } finally
            {
                timer.cancel();
            }
        }
    }

    public void destroy()
    {
    }

    private TimerTask createTimer( int timeLimit )
    {
        final Thread current = Thread.currentThread();
        TimerTask timerTask = new TimerTask()
        {
            @Override
            public void run()
            {
                LOG.warn( "request canceld" );
                current.interrupt();
            }
        };
        timer.schedule( timerTask, timeLimit );
        return timerTask;
    }

    private int getTimeLimit( HttpServletRequest request )
    {
        String header = request.getHeader( "max-execution-time" );
        if ( header != null )
        {
            int maxHeader = Integer.parseInt( header );
            if ( timeLimit < 0 || ( maxHeader > 0 && maxHeader < timeLimit ) )
            {
                return maxHeader;
            }
        }
        return timeLimit;
    }
}
