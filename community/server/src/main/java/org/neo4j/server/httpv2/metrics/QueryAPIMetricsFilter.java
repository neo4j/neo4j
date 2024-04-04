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
package org.neo4j.server.httpv2.metrics;

import static org.neo4j.server.httpv2.response.TypedJsonDriverResultWriter.TYPED_JSON_MIME_TYPE_VALUE;

import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import org.neo4j.server.httpv2.QueryResource;

public class QueryAPIMetricsFilter implements Filter {

    private final QueryAPIMetricsMonitor monitor;
    private final String pathSpec;

    public QueryAPIMetricsFilter(QueryAPIMetricsMonitor monitor, String pathSpec) {
        this.monitor = monitor;
        this.pathSpec = pathSpec;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        chain.doFilter(request, response);

        var httpServletRequest = (HttpServletRequest) request;
        var httpServletResponse = (HttpServletResponse) response;

        if (httpServletRequest.getRequestURI().endsWith(QueryResource.API_PATH_FRAGMENT)) {
            monitor.totalRequests();
            meterStatusCode(httpServletResponse);
            meterRequestedContentType(httpServletRequest);
            meterReturnedContentType(httpServletResponse);
        }
    }

    private void meterRequestedContentType(HttpServletRequest httpServletRequest) {
        var contentType = httpServletRequest.getContentType();
        if (contentType.contains(TYPED_JSON_MIME_TYPE_VALUE)) {
            monitor.applicationVndNeo4jQueryRequests();
        } else if (contentType.contains(MediaType.APPLICATION_JSON)) {
            monitor.applicationJsonRequests();
        }
    }

    private void meterReturnedContentType(HttpServletResponse httpServletResponse) {
        var contentType = httpServletResponse.getContentType();
        if (contentType.contains(TYPED_JSON_MIME_TYPE_VALUE)) {
            monitor.applicationVndNeo4jQueryResponses();
        } else if (contentType.contains(MediaType.APPLICATION_JSON)) {
            monitor.applicationJsonResponses();
        }
    }

    private void meterStatusCode(HttpServletResponse response) {
        if (response != null) {
            switch (response.getStatus() / 100) {
                case 2 -> monitor.successStatus();
                case 4 -> monitor.badRequestStatus();
                case 5 -> monitor.serverErrorStatus();
                default -> {}
            }
        }
    }
}
