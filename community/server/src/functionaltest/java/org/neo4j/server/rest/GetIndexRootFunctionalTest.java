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

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.helpers.ServerHelper;

public class GetIndexRootFunctionalTest
{

    private static NeoServerWithEmbeddedWebServer server;
    private static FunctionalTestHelper functionalTestHelper;

    @BeforeClass
    public static void setupServer() throws IOException
    {
        server = ServerHelper.createServer();
        functionalTestHelper = new FunctionalTestHelper( server );
    }

    @Before
    public void cleanTheDatabase()
    {
        ServerHelper.cleanTheDatabase( server );
    }

    @AfterClass
    public static void stopServer()
    {
        server.stop();
    }

    /**
     * /db/data/index is not itself a resource
     * 
     */
    @Test
    public void shouldRespondWith404ForNonResourceIndexPath() throws Exception
    {
        JaxRsResponse response = RestRequest.req().get(functionalTestHelper.indexUri());
        assertEquals( 404, response.getStatus() );
        response.close();
    }

    /**
     * /db/data/index/node should be a resource with no content
     * 
     * @throws Exception
     */
    @Test
    public void shouldRespondWithNodeIndexes() throws Exception {
        JaxRsResponse response = RestRequest.req().get(functionalTestHelper.nodeIndexUri());
        assertEquals(204, response.getStatus());
        response.close();
    }

    /**
     * /db/data/index/relationship should be a resource with no content
     * 
     * @throws Exception
     */
    @Test
    public void shouldRespondWithRelationshipIndexes() throws Exception {
        JaxRsResponse response = RestRequest.req().get(functionalTestHelper.relationshipIndexUri());
        assertEquals(204, response.getStatus());
        response.close();
    }

    // TODO More tests...
}
