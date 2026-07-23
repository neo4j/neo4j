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
package org.neo4j.server.web;

import static org.eclipse.jetty.server.Request.getTimeStamp;
import static org.eclipse.jetty.server.Response.getContentBytesWritten;

import java.util.function.Function;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.Response;

/**
 * Adapter for Jetty's RequestLog interface to use Neo4j's WebServerRequestLog interface.
 * <p/>
 * This adapter hides Jetty's implementation details and provides a clean interface for logging web server requests.
 */
public class JettyWebServerServerLogAdapter implements RequestLog {
    private final WebServerRequestLog adapted;

    public JettyWebServerServerLogAdapter(WebServerRequestLog adapted) {
        this.adapted = adapted;
    }

    @Override
    public void log(Request request, Response response) {
        this.adapted.log(getWebServerRequestLogInfo(request, response));
    }

    private static WebServerRequestLogInfo getWebServerRequestLogInfo(Request request, Response response) {
        return new WebServerRequestLogInfo(
                swallowExceptions(request, Request::getRemoteAddr),
                swallowExceptions(request, Request::getId),
                findRequestURI(request),
                response.getStatus(),
                getContentBytesWritten(response),
                swallowExceptions(request, r -> r.getHeaders().get("Referer")),
                swallowExceptions(request, r -> r.getHeaders().get("User-Agent")),
                request != null ? getTimeStamp(request) : -1);
    }

    private static <T> T swallowExceptions(Request outerRequest, Function<Request, T> function) {
        try {
            return outerRequest == null ? null : function.apply(outerRequest);
        } catch (Throwable t) {
            return null;
        }
    }

    private static String findRequestURI(Request request) {
        var requestURI = swallowExceptions(request, Request::getHttpURI);

        if (requestURI != null) {
            return requestURI.asString();
        }

        return "";
    }
}
