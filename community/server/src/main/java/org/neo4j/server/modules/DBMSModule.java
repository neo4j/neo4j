/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.server.modules;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.server.rest.dbms.UserService;
import org.neo4j.server.rest.discovery.DiscoveryService;
import org.neo4j.server.web.WebServer;

/**
 * Mounts the DBMS REST API.
 */
public class DBMSModule implements ServerModule
{
    private static final String ROOT_PATH = "/";

    private final WebServer webServer;
    private final Config config;

    public DBMSModule( WebServer webServer, Config config )
    {
        this.webServer = webServer;
        this.config = config;
    }

    @Override
    public void start()
    {
        webServer.addJAXRSClasses( getClassNames(), ROOT_PATH, null );
    }

    private List<String> getClassNames()
    {
        List<String> toReturn = new ArrayList<>( 2 );

        toReturn.add( DiscoveryService.class.getName() );
        if ( config.get( GraphDatabaseSettings.auth_enabled ) )
        {
            toReturn.add( UserService.class.getName() );
        }

        return toReturn;
    }

    @Override
    public void stop()
    {
        webServer.removeJAXRSClasses( getClassNames(), ROOT_PATH );
    }
}
