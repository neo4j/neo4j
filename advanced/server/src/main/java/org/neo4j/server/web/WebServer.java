/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.web;

import java.util.List;

import javax.servlet.Servlet;

import org.neo4j.server.NeoServer;

public interface WebServer {
    public void setNeoServer(NeoServer server);
    public void setPort(int portNo);
    public void start();
    public void stop();
    public void setMaxThreads(int maxThreads);
    public void addJAXRSPackages(List<String> packageNames, String serverMountPoint);
    public void addStaticContent(String contentLocation, String serverMountPoint);

    void addServlet( Servlet unmanagedServlet, String serverMountPoint );
}
