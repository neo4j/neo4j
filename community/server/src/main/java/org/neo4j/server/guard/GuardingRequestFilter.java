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
package org.neo4j.server.guard;

import org.neo4j.kernel.guard.Guard;
import org.neo4j.kernel.guard.GuardException;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Timer;

import static javax.servlet.http.HttpServletResponse.SC_REQUEST_TIMEOUT;

public class GuardingRequestFilter implements Filter
{

    private final Guard guard;
    private final long timeout; // in milliSeconds
    private final Timer timer = new Timer();

    public GuardingRequestFilter( final Guard guard, final long timeout )
    {
        this.guard = guard;
        this.timeout = timeout;
    }

    @Override
    public void init( FilterConfig filterConfig ) throws ServletException
    {
    }

    public void doFilter( ServletRequest req, ServletResponse res, FilterChain chain ) throws ServletException, IOException
    {
        if ( req instanceof HttpServletRequest && res instanceof HttpServletResponse )
        {
            HttpServletRequest request = (HttpServletRequest) req;
            HttpServletResponse response = (HttpServletResponse) res;

            long timeLimit = getTimeLimit( request );
            if ( timeLimit <= 0 )
            {
                chain.doFilter( req, res );
            } else
            {
                guard.startTimeout( timeLimit );

                try
                {
                    chain.doFilter( req, res );
                } catch ( GuardException e )
                {
                    response.setStatus( SC_REQUEST_TIMEOUT );
                } finally
                {
                    guard.stop();
                }
            }
        } else
        {
            chain.doFilter( req, res );
        }
    }

    public void destroy()
    {
        timer.cancel();
    }

    private long getTimeLimit( HttpServletRequest request )
    {
        long timeLimit = timeout;
        String headerValue = request.getHeader( "max-execution-time" );
        if ( headerValue != null )
        {
            long maxHeader = Long.parseLong( headerValue );
            if ( timeLimit < 0 || (maxHeader > 0 && maxHeader < timeLimit ) )
            {
                return maxHeader;
            }
        }
        return timeLimit;
    }
}
