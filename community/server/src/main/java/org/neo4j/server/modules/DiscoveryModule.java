/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.server.modules;

import java.util.List;

import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.server.rest.discovery.DiscoveryService;
import org.neo4j.server.web.WebServer;

import static org.neo4j.server.JAXRSHelper.listFrom;

public class DiscoveryModule implements ServerModule
{
    private static final String ROOT_PATH = "/";

    private final WebServer webServer;
    private final ConsoleLogger log;

    public DiscoveryModule( WebServer webServer, Logging logging )
    {
        this.webServer = webServer;
        this.log = logging.getConsoleLog( getClass() );
    }

    @Override
    public void start()
    {
        webServer.addJAXRSClasses( getClassNames(), ROOT_PATH, null );
        log.log( "Mounted discovery module at [%s]", ROOT_PATH );
    }

    private List<String> getClassNames()
    {
        return listFrom( DiscoveryService.class.getName() );
    }

    @Override
    public void stop()
    {
        webServer.removeJAXRSClasses( getClassNames(), ROOT_PATH );
    }
}
