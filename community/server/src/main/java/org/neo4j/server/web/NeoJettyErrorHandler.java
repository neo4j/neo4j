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

import static org.neo4j.server.queryapi.response.HttpErrorResponse.singleError;

import java.io.IOException;
import java.io.Writer;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.server.http.cypher.format.DefaultJsonFactory;
import org.neo4j.server.queryapi.response.format.QueryAPICodec;
import org.neo4j.server.queryapi.response.format.View;

public class NeoJettyErrorHandler extends ErrorHandler {

    @Override
    protected void generateAcceptableResponse(
            Request baseRequest,
            HttpServletRequest request,
            HttpServletResponse response,
            int code,
            String message,
            String contentType)
            throws IOException {
        // Overriding to generate HTTP API V2's error format
        // todo should be filter out other endpoints here?
        response.addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);

        var jsonGenerator = DefaultJsonFactory.INSTANCE
                .get()
                .copy()
                .setCodec(new QueryAPICodec(View.PLAIN_JSON))
                .createGenerator(response.getOutputStream());
        jsonGenerator.writeObject(singleError(Status.Request.Invalid.code().serialize(), message));
    }

    @Override
    protected void handleErrorPage(HttpServletRequest request, Writer writer, int code, String message) {
        writeErrorPage(request, writer, code, message, false);
    }

    @Override
    protected void writeErrorPage(
            HttpServletRequest request, Writer writer, int code, String message, boolean showStacks) {

        // we don't want any Jetty output

    }

    @Override
    protected void writeErrorPageHead(HttpServletRequest request, Writer writer, int code, String message) {
        // we don't want any Jetty output

    }

    @Override
    protected void writeErrorPageBody(
            HttpServletRequest request, Writer writer, int code, String message, boolean showStacks) {
        // we don't want any Jetty output

    }

    @Override
    protected void writeErrorPageMessage(
            HttpServletRequest request, Writer writer, int code, String message, String uri) {
        // we don't want any Jetty output

    }

    @Override
    protected void writeErrorPageStacks(HttpServletRequest request, Writer writer) {
        // we don't want any stack output

    }
}
