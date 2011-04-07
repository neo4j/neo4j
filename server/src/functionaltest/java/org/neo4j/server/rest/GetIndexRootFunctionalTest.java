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
package org.neo4j.server.rest;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.ServerBuilder;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;

public class GetIndexRootFunctionalTest
{
    
    private NeoServerWithEmbeddedWebServer server;
    private FunctionalTestHelper functionalTestHelper;
    
    
    @Before
    public void setupServer() throws IOException {
        server = ServerBuilder.server().withRandomDatabaseDir().withPassingStartupHealthcheck().build();
        server.start();
        functionalTestHelper = new FunctionalTestHelper(server);
    }
    
    @After
    public void stopServer() {
        server.stop();
        server = null;
    }

    /**
     * /db/data/index is not itself a resource
     *
     */
    @Test
    public void shouldRespondWith404ForNonResourceIndexPath() throws Exception
    {
        ClientResponse response = Client.create().resource(functionalTestHelper.indexUri() ).get( ClientResponse.class );
        assertEquals( 404, response.getStatus() );
    }

    /**
     * /db/data/index/node should be a resource with no content
     * @throws Exception
     */
    @Test
    public void shouldRespondWithNodeIndexes() throws Exception
    {
        ClientResponse response = Client.create().resource(functionalTestHelper.nodeIndexUri() ).get( ClientResponse.class );
        assertEquals( 204, response.getStatus() );
    }

    /**
     * /db/data/index/relationship should be a resource with no content
     * @throws Exception
     */
    @Test
    public void shouldRespondWithRelationshipIndexes() throws Exception
    {
        ClientResponse response = Client.create().resource(functionalTestHelper.relationshipIndexUri() ).get( ClientResponse.class );
        assertEquals( 204, response.getStatus() );
    }

    // TODO More tests...
}
