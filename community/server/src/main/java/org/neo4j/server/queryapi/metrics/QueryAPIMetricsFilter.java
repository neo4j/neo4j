/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.server.queryapi.metrics;

import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.http.HttpVersion;
import org.neo4j.server.queryapi.QueryResource;

public class QueryAPIMetricsFilter implements Filter {

    private final QueryAPIMetricsMonitor monitor;

    public QueryAPIMetricsFilter(QueryAPIMetricsMonitor monitor) {
        this.monitor = monitor;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        var execStartTime = System.currentTimeMillis();
        chain.doFilter(request, response);
        var totalExecutionTime = System.currentTimeMillis() - execStartTime;
        var httpServletRequest = (HttpServletRequest) request;
        var httpServletResponse = (HttpServletResponse) response;

        if (httpServletRequest.getRequestURI().endsWith(QueryResource.API_PATH_FRAGMENT)) {
            monitor.totalRequests();
            monitor.requestTimeTaken(totalExecutionTime);
            meterRequest(httpServletRequest);
            meterResponse(httpServletResponse);
        }
    }

    private void meterRequest(HttpServletRequest httpServletRequest) {
        var contentType = httpServletRequest.getContentType();

        monitor.requestContentType(contentType);
        monitor.httpVersion(HttpVersion.fromString(httpServletRequest.getProtocol()));
    }

    private void meterResponse(HttpServletResponse response) {
        if (response == null) {
            return;
        }
        monitor.responseStatusCode(response.getStatus());
        var contentType = response.getContentType();

        if (contentType == null) {
            return;
        }
        monitor.responseContentType(contentType);
    }
}
