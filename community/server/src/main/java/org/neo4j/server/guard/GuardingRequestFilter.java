/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static java.lang.System.currentTimeMillis;
import static javax.servlet.http.HttpServletResponse.SC_REQUEST_TIMEOUT;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class GuardingRequestFilter implements Filter {

    private final Guard guard;

    public GuardingRequestFilter(final Guard guard) {
        this.guard = guard;
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }

    public void doFilter(ServletRequest req, ServletResponse res, FilterChain chain) throws ServletException, IOException {
        if (req instanceof HttpServletRequest && res instanceof HttpServletResponse) {
            HttpServletRequest request = (HttpServletRequest) req;
            HttpServletResponse response = (HttpServletResponse) res;

            int timeLimit = getTimeLimit(request);
            if (timeLimit <= 0) {
                guard.clear();
                chain.doFilter(req, res);
            } else {
                guard.start(currentTimeMillis() + timeLimit);
                try {
                    chain.doFilter(req, res);
                } catch (GuardException e) {
                    response.setStatus(SC_REQUEST_TIMEOUT);
                } finally {
                    guard.stop();
                }
            }
        } else {
            chain.doFilter(req, res);
        }
    }

    public void destroy() {
    }

    private int getTimeLimit(HttpServletRequest request) {
        int timeLimit = guard.getTimeLimit();
        String headerValue = request.getHeader("max-execution-time");
        if (headerValue != null) {
            int maxHeader = Integer.parseInt(headerValue);
            if (timeLimit < 0 || (maxHeader > 0 && maxHeader < timeLimit)) {
                return maxHeader;
            }
        }
        return timeLimit;
    }
}
