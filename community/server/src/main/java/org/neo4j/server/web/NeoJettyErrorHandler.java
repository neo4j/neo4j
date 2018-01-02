/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.web;

import java.io.IOException;
import java.io.Writer;

import javax.servlet.http.HttpServletRequest;

import org.eclipse.jetty.server.handler.ErrorHandler;

public class NeoJettyErrorHandler extends ErrorHandler
{

    @Override
    protected void handleErrorPage(HttpServletRequest request, Writer writer, int code,
                                   String message) throws IOException
    {
        writeErrorPage(request, writer, code, message, false);
    }

    @Override
    protected void writeErrorPage(HttpServletRequest request, Writer writer, int code, String message,
                                  boolean showStacks) throws IOException {

        // we don't want any Jetty output

    }

    @Override
    protected void writeErrorPageHead(HttpServletRequest request, Writer writer, int code,
                                      String message) throws IOException {
        // we don't want any Jetty output

    }

    @Override
    protected void writeErrorPageBody(HttpServletRequest request, Writer writer, int code,
                                      String message, boolean showStacks) throws IOException {
        // we don't want any Jetty output

    }

    @Override
    protected void writeErrorPageMessage(HttpServletRequest request, Writer writer, int code,
                                         String message, String uri) throws IOException {
        // we don't want any Jetty output

    }

    @Override
    protected void writeErrorPageStacks(HttpServletRequest request, Writer writer)
            throws IOException {
        // we don't want any stack output

    }
}
