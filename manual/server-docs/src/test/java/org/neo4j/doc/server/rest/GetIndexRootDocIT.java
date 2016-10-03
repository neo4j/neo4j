/*
 * Licensed to Neo Technology under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Neo Technology licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.neo4j.doc.server.rest;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.rest.domain.JsonParseException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.server.rest.domain.JsonHelper.jsonToMap;

public class GetIndexRootDocIT extends org.neo4j.doc.server.rest.AbstractRestFunctionalTestBase
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
        org.neo4j.doc.server.rest.JaxRsResponse response = RestRequest.req().get( functionalTestHelper.indexUri() );
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
        org.neo4j.doc.server.rest.JaxRsResponse response = RestRequest.req().get( functionalTestHelper.nodeIndexUri() );
        assertResponseContainsNoIndexesOtherThanAutoIndexes( response );
        response.close();
    }

    private void assertResponseContainsNoIndexesOtherThanAutoIndexes( org.neo4j.doc.server.rest.JaxRsResponse response ) throws JsonParseException
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
        org.neo4j.doc.server.rest.JaxRsResponse response = RestRequest.req().get( functionalTestHelper.relationshipIndexUri() );
        assertResponseContainsNoIndexesOtherThanAutoIndexes( response );
        response.close();
    }

    // TODO More tests...
}
