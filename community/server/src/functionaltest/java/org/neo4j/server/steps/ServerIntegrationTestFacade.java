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
package org.neo4j.server.steps;

import java.io.IOException;

import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.helpers.ServerBuilder;

public class ServerIntegrationTestFacade
{

    private static final String USE_EXTERNAL_SERVER_KEY = "testWithExternalServer";
    private static final String EXTERNAL_SERVER_KEY = "externalTestServerUrl";
    private static final String DEFAULT_EXTERNAL_SERVER_URL = "http://localhost:7474/";

    private NeoServerWithEmbeddedWebServer server;

    public String getServerUrl()
    {
        if ( isUsingExternalServer() )
        {
            return getExternalServerUrl();
        }
        else
        {
            return server.baseUri()
                    .toString();
        }
    }

    public void ensureServerIsRunning() throws IOException
    {
        if ( !isUsingExternalServer() )
        {
            server = ServerBuilder.server()
                    .build();
            server.start();
        }
    }

    public void cleanup()
    {
        if ( server != null )
        {
            server.stop();
            server = null;
        }
    }

    public boolean isUsingExternalServer()
    {
        return System.getProperty( USE_EXTERNAL_SERVER_KEY, "false" )
                .equals( "true" );
    }

    private String getExternalServerUrl()
    {
        return System.getProperty( EXTERNAL_SERVER_KEY, DEFAULT_EXTERNAL_SERVER_URL );
    }

}
