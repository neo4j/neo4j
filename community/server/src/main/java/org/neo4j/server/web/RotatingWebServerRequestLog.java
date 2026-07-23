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

import static org.apache.commons.lang3.StringUtils.defaultString;
import static org.neo4j.logging.log4j.LoggerTarget.HTTP_LOGGER;

import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;

public class RotatingWebServerRequestLog extends AbstractLifeCycle implements WebServerRequestLog {
    private final InternalLog log;

    public RotatingWebServerRequestLog(InternalLogProvider logProvider) {
        log = logProvider.getLog(HTTP_LOGGER);
    }

    @Override
    public void log(WebServerRequestLogInfo info) {
        // Trying to replicate this logback pattern:
        // %h %l %user [%t{dd/MMM/yyyy:HH:mm:ss Z}] "%r" %s %b "%i{Referer}" "%i{User-Agent}" %D
        var now = System.currentTimeMillis();
        var serviceTime = info.requestTimeStamp() < 0 ? -1 : now - info.requestTimeStamp();

        log.info(
                "%s - %s [%tc] \"%s\" %s %s \"%s\" \"%s\" %s",
                defaultString(info.remoteHost()),
                defaultString(info.user()),
                now,
                defaultString(info.requestURL()),
                info.statusCode(),
                info.contentBytesWritten(),
                defaultString(info.referer()),
                defaultString(info.userAgent()),
                serviceTime);
    }
}
