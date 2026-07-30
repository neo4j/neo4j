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
import java.util.regex.Pattern;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.http.HttpVersion;

public class QueryAPIMetricsFilter implements Filter {

    private final QueryAPIMetricsMonitor monitor;
    private final Pattern rootPattern;
    private final Pattern continuePattern;
    private final Pattern commitPattern;

    public QueryAPIMetricsFilter(QueryAPIMetricsMonitor monitor, long transactionIdLength) {
        this.monitor = monitor;
        this.rootPattern = Pattern.compile(".*/query/v2.*");
        this.continuePattern = Pattern.compile(".*/query/v2/tx/.{%d}".formatted(transactionIdLength));
        this.commitPattern = Pattern.compile(".*/query/v2/tx/.{%d}/commit".formatted(transactionIdLength));
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        var execStartTime = System.currentTimeMillis();
        chain.doFilter(request, response);
        var totalExecutionTime = System.currentTimeMillis() - execStartTime;
        var httpServletRequest = (HttpServletRequest) request;
        var httpServletResponse = (HttpServletResponse) response;

        if (httpServletRequest.getRequestURI().matches(rootPattern.pattern())) {
            monitor.totalRequests();
            monitor.requestTimeTaken(totalExecutionTime);
            meterEndpoint(httpServletRequest.getRequestURI(), httpServletRequest.getMethod());
            meterRequest(httpServletRequest);
            meterResponse(httpServletResponse);
        }
    }

    private void meterRequest(HttpServletRequest httpServletRequest) {
        var contentType = httpServletRequest.getContentType();

        monitor.requestContentType(contentType);
        monitor.httpVersion(HttpVersion.fromString(httpServletRequest.getProtocol()));
    }

    private void meterEndpoint(String uri, String httpVerb) {
        if (uri.endsWith("/query/v2")) {
            monitor.autoCommitRequest();
        } else if (uri.endsWith("/query/v2/tx")) {
            monitor.beginRequest();
        } else if (uri.matches(continuePattern.pattern())) {
            if (httpVerb.equals("POST")) {
                monitor.continueRequest();
            } else {
                monitor.rollbackRequest();
            }
        } else if (uri.matches(commitPattern.pattern())) {
            monitor.commitRequest();
        }
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
