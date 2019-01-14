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
package org.neo4j.server.rest;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.rest.domain.GraphDbHelper;

import static org.junit.Assert.assertEquals;

public class RemoveRelationshipIT extends AbstractRestFunctionalTestBase
{
    private static FunctionalTestHelper functionalTestHelper;
    private static GraphDbHelper helper;

    @BeforeClass
    public static void setupServer()
    {
        functionalTestHelper = new FunctionalTestHelper( server() );
        helper = functionalTestHelper.getGraphDbHelper();
    }

    @Test
    public void shouldGet204WhenRemovingAValidRelationship() throws Exception
    {
        long relationshipId = helper.createRelationship( "KNOWS" );

        JaxRsResponse response = sendDeleteRequest( new URI( functionalTestHelper.relationshipUri( relationshipId ) ) );

        assertEquals( 204, response.getStatus() );
        response.close();
    }

    @Test
    public void shouldGet404WhenRemovingAnInvalidRelationship() throws Exception
    {
        long relationshipId = helper.createRelationship( "KNOWS" );

        JaxRsResponse response = sendDeleteRequest(new URI(
                functionalTestHelper.relationshipUri( (relationshipId + 1) * 9999 ) ) );

        assertEquals( 404, response.getStatus() );
        response.close();
    }

    private JaxRsResponse sendDeleteRequest( URI requestUri )
    {
       return RestRequest.req().delete(requestUri);
    }
}
