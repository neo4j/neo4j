/*
 * Copyright (c) "Neo4j"
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

import static org.apache.commons.lang3.StringUtils.defaultString;
import static org.neo4j.logging.log4j.LoggerTarget.HTTP_LOGGER;

import java.util.function.Function;
import jakarta.servlet.http.HttpServletRequest;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.util.concurrent.AsyncEvents;

public class RotatingRequestLog extends AbstractLifeCycle implements RequestLog, AsyncEvents.Monitor {
    private final InternalLog log;

    public RotatingRequestLog(InternalLogProvider logProvider) {
        log = logProvider.getLog(HTTP_LOGGER);
    }

    @Override
    public void log(Request request, Response response) {
        // Trying to replicate this logback pattern:
        // %h %l %user [%t{dd/MMM/yyyy:HH:mm:ss Z}] "%r" %s %b "%i{Referer}" "%i{User-Agent}" %D
        String remoteHost = swallowExceptions(request, HttpServletRequest::getRemoteHost);
        String user = swallowExceptions(request, HttpServletRequest::getRemoteUser);
        String requestURL = findRequestURI(request);
        int statusCode = response.getStatus();
        long length = response.getContentLength();
        String referer = swallowExceptions(request, r -> r.getHeader("Referer"));
        String userAgent = swallowExceptions(request, r -> r.getHeader("User-Agent"));
        long requestTimeStamp = request != null ? request.getTimeStamp() : -1;
        long now = System.currentTimeMillis();
        long serviceTime = requestTimeStamp < 0 ? -1 : now - requestTimeStamp;

        log.info(
                "%s - %s [%tc] \"%s\" %s %s \"%s\" \"%s\" %s",
                defaultString(remoteHost),
                defaultString(user),
                now,
                defaultString(requestURL),
                statusCode,
                length,
                defaultString(referer),
                defaultString(userAgent),
                serviceTime);
    }

    private static <T> T swallowExceptions(HttpServletRequest outerRequest, Function<HttpServletRequest, T> function) {
        try {
            return outerRequest == null ? null : function.apply(outerRequest);
        } catch (Throwable t) {
            return null;
        }
    }

    @Override
    public void eventCount(long count) {}

    private static String findRequestURI(Request request) {
        var requestURI = swallowExceptions(request, HttpServletRequest::getRequestURI);
        var queryString = swallowExceptions(request, HttpServletRequest::getQueryString);

        if (queryString == null || queryString.isBlank()) {
            return requestURI;
        }
        return requestURI + "?" + queryString;
    }
}
