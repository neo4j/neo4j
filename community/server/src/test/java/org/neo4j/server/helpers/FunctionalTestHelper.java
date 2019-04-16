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
package org.neo4j.server.helpers;

import java.net.URI;

import org.neo4j.server.NeoServer;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;
import org.neo4j.server.rest.domain.GraphDbHelper;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

public final class FunctionalTestHelper
{
    private final NeoServer server;
    private final GraphDbHelper helper;

    private RestRequest request;

    public FunctionalTestHelper( NeoServer server )
    {
        if ( server.getDatabase() == null )
        {
            throw new RuntimeException( "Server must be started before using " + getClass().getName() );
        }
        this.helper = new GraphDbHelper( server.getDatabase() );
        this.server = server;
        this.request = new RestRequest( server.baseUri().resolve( "db/data/" ) );
    }

    public GraphDbHelper getGraphDbHelper()
    {
        return helper;
    }

    public String dataUri()
    {
        return server.baseUri().toString() + "db/data/";
    }

    public String managementUri()
    {
        return server.baseUri()
                .toString() + "db/manage";
    }

    public String extensionUri()
    {
        return dataUri() + "ext";
    }

    public JaxRsResponse get( String path )
    {
        return request.get( path );
    }

    public URI baseUri()
    {
        return server.baseUri();
    }

    public String cypherURL()
    {
        return dataUri() + "transaction/commit";
    }

    public String simpleCypherRequestBody()
    {
        return "{\"statements\": [{\"statement\": \"CREATE (n:MyLabel) RETURN n\"}]}";
    }

    public void verifyCypherResponse( String responseBody )
    {
        // if at least one node is returned, there will be "node" in the metadata part od the the row
        assertThat( responseBody, containsString( "node" ) );
    }
}
