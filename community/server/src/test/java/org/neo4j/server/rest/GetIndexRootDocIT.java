/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.rest.domain.JsonParseException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.server.rest.domain.JsonHelper.jsonToMap;

public class GetIndexRootDocIT extends AbstractRestFunctionalTestBase
{
    private static FunctionalTestHelper functionalTestHelper;

    @BeforeClass
    public static void setupServer() throws IOException
    {
        functionalTestHelper = new FunctionalTestHelper( server() );
    }

    /**
     * /db/data/index is not itself a resource
     */
    @Test
    public void shouldRespondWith404ForNonResourceIndexPath() throws Exception
    {
        JaxRsResponse response = RestRequest.req().get( functionalTestHelper.indexUri() );
        assertEquals( 404, response.getStatus() );
        response.close();
    }

    /**
     * /db/data/index/node should be a resource with no content
     *
     * @throws Exception
     */
    @Test
    public void shouldRespondWithNodeIndexes() throws Exception
    {
        JaxRsResponse response = RestRequest.req().get( functionalTestHelper.nodeIndexUri() );
        assertResponseContainsNoIndexesOtherThanAutoIndexes( response );
        response.close();
    }

    private void assertResponseContainsNoIndexesOtherThanAutoIndexes( JaxRsResponse response ) throws JsonParseException
    {
        switch ( response.getStatus() )
        {
        case 204:
            return; // OK no auto indices
        case 200:
            assertEquals( 0, functionalTestHelper.removeAnyAutoIndex( jsonToMap( response.getEntity() ) ).size() );
            break;
        default:
            fail( "Invalid response code " + response.getStatus() );
        }
    }

    /**
     * /db/data/index/relationship should be a resource with no content
     *
     * @throws Exception
     */
    @Test
    public void shouldRespondWithRelationshipIndexes() throws Exception
    {
        JaxRsResponse response = RestRequest.req().get( functionalTestHelper.relationshipIndexUri() );
        assertResponseContainsNoIndexesOtherThanAutoIndexes( response );
        response.close();
    }

    // TODO More tests...
}
