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

/**
 * Information about a web server request.
 * <p/>
 * This record holds information about a web server request, including the remote host, user, request URL, status code,
 * content bytes written, referer, user agent, and request timestamp.
 * The record provides a clean and immutable way to store and access information about a web server request.
 * This is tailored to the needs of Neo4j's web server request logging.
 */
public record WebServerRequestLogInfo(
        String remoteHost,
        String user,
        String requestURL,
        int statusCode,
        long contentBytesWritten,
        String referer,
        String userAgent,
        long requestTimeStamp) {}
