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
package org.neo4j.server.rest.security;

import org.junit.After;

import java.io.IOException;
import java.util.Base64;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.helpers.CommunityServerBuilder;
import org.neo4j.test.server.ExclusiveServerTestBase;

public class CommunityServerTestBase extends ExclusiveServerTestBase
{
    protected CommunityNeoServer server;

    @After
    public void cleanup()
    {
        if ( server != null )
        {
            server.stop();
        }
    }

    protected void startServer( boolean authEnabled ) throws IOException
    {
        server = CommunityServerBuilder.serverOnRandomPorts()
                .withProperty( GraphDatabaseSettings.auth_enabled.name(), Boolean.toString( authEnabled ) )
                .build();
        server.start();
    }

    protected void startServer( boolean authEnabled, String accessControlAllowOrigin ) throws IOException
    {
        server = CommunityServerBuilder.serverOnRandomPorts()
                .withProperty( GraphDatabaseSettings.auth_enabled.name(), Boolean.toString( authEnabled ) )
                .withProperty( ServerSettings.http_access_control_allow_origin.name(), accessControlAllowOrigin )
                .build();
        server.start();
    }

    protected String basicAuthHeader( String username, String password )
    {
        String usernamePassword = username + ':' + password;
        return "Basic " + Base64.getEncoder().encodeToString( usernamePassword.getBytes() );
    }

    protected String dataURL()
    {
        return server.baseUri().resolve( "db/data/" ).toString();
    }

    protected String userURL( String username )
    {
        return server.baseUri().resolve( "user/" + username ).toString();
    }

    protected String passwordURL( String username )
    {
        return server.baseUri().resolve( "user/" + username + "/password" ).toString();
    }

    protected String txCommitURL()
    {
        return server.baseUri().resolve( "db/data/transaction/commit" ).toString();
    }
}
