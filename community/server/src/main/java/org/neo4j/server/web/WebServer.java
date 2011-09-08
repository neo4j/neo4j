/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.mortbay.jetty.Server;
import org.neo4j.server.NeoServer;

public interface WebServer
{
    void init();

    void setNeoServer( NeoServer server );

    void setPort( int portNo );

    void setAddress( String addr );

    void start();

    void stop();

    void setMaxThreads( int maxThreads );

    void addJAXRSPackages( List<String> packageNames, String serverMountPoint );

    void addStaticContent( String contentLocation, String serverMountPoint );

    void invokeDirectly( String targetUri, HttpServletRequest request, HttpServletResponse response )
            throws IOException, ServletException;

    Server getJetty();
}
