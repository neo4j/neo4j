/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.server.webadmin;

import org.neo4j.rest.WebServer;
import org.neo4j.server.NeoServer;

/**
 * Launcher for the Grizzly server that handles the admin interface. This code
 * based on {@link WebServer} in the neo4j REST interface.
 * 
 * @author Jacob Hansson <jacob@voltvoodoo.com>
 * 
 */
public enum AdminServer
{
    INSTANCE;

    public void startServer()
    {
        throw new RuntimeException( "Don't use this!" );
    }

    public void stopServer()
    {
        startServer();
    }

    public String getStaticPath()
    {
        throw new RuntimeException( "Don't use this!" );
    }

    public static int DEFAULT_PORT()
    {
        return NeoServer.DEFAULT_WEBSERVER_PORT;
    }

    public static String DEFAULT_STATIC_PATH()
    {
        throw new RuntimeException( "Don't use this!" );
    }

    public void startServer( int adminPort, String webRoot )
    {
        startServer( );
    }

    public static Object getLocalhostBaseUri( int adminPort )
    {
        throw new RuntimeException( "Don't use this!" );
    }

    public String getBaseUri()
    {
        throw new RuntimeException( "Don't use this!" );
    }
}
