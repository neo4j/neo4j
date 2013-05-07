/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.test.server;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import org.neo4j.server.NeoServer;
import org.neo4j.server.helpers.ServerHelper;

public class SharedServerTestBase
{
    private static boolean useExternal = Boolean.valueOf( System.getProperty( "neo-server.external", "false" ) );
    private static String externalURL = System.getProperty( "neo-server.external.url", "http://localhost:7474" );

    protected static NeoServer server()
    {
        return server;
    }

    protected final void cleanDatabase()
    {
        if ( useExternal )
        {
            // TODO
        }
        else
        {
            ServerHelper.cleanTheDatabase( server );
        }
    }

    private static NeoServer server;
    private static String serverUrl;

    public static String getServerURL()
    {
        return serverUrl;
    }

    @BeforeClass
    public static void allocateServer() throws IOException
    {
        if ( useExternal )
        {
            serverUrl = externalURL;
        }
        else
        {
            server = ServerHolder.allocate();
            serverUrl = server.baseUri().toString();
        }
    }

    @AfterClass
    public static void releaseServer()
    {
        if ( !useExternal )
        {
            try
            {
                ServerHolder.release( server );
            }
            finally
            {
                server = null;
            }
        }
    }
}
