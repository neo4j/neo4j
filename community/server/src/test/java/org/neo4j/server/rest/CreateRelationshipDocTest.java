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

import com.sun.jersey.api.client.ClientResponse.Status;
import org.junit.Test;

import java.util.Map;
import javax.ws.rs.core.MediaType;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.repr.RelationshipRepresentationTest;
import org.neo4j.test.GraphDescription.Graph;
import org.neo4j.test.TestData.Title;

import static org.junit.Assert.assertTrue;

public class CreateRelationshipDocTest extends AbstractRestFunctionalDocTestBase
{
    @Test
    @Graph( "Joe knows Sara" )
    @Documented( "Upon successful creation of a relationship, the new relationship is returned." )
    @Title( "Create a relationship with properties" )
    public void create_a_relationship_with_properties() throws Exception
    {
        String jsonString = "{\"to\" : \""
                            + getDataUri()
                            + "node/"
                            + getNode( "Sara" ).getId()
                            + "\", \"type\" : \"LOVES\", \"data\" : {\"foo\" : \"bar\"}}";
        Node i = getNode( "Joe" );
        gen.get().description( startGraph( "Add relationship with properties before" ) );
        gen.get().expectedStatus(
                Status.CREATED.getStatusCode() ).payload( jsonString ).post(
                getNodeUri( i ) + "/relationships" );
        try ( Transaction tx = graphdb().beginTx() )
        {
            assertTrue( i.hasRelationship( DynamicRelationshipType.withName( "LOVES" ) ) );
        }
    }

    @Test
    @Documented( "Upon successful creation of a relationship, the new relationship is returned." )
    @Title( "Create relationship" )
    @Graph( "Joe knows Sara" )
    public void create_relationship() throws Exception
    {
        String jsonString = "{\"to\" : \""
                            + getDataUri()
                            + "node/"
                            + getNode( "Sara" ).getId()
                            + "\", \"type\" : \"LOVES\"}";
        Node i = getNode( "Joe" );
        String entity = gen.get().expectedStatus(
                Status.CREATED.getStatusCode() ).payload( jsonString )
                .description( startGraph( "create relationship" ) )
                .post( getNodeUri( i ) + "/relationships" ).entity();
        try ( Transaction tx = graphdb().beginTx() )
        {
            assertTrue( i.hasRelationship( DynamicRelationshipType.withName( "LOVES" ) ) );
        }
        assertProperRelationshipRepresentation( JsonHelper.jsonToMap( entity ) );
    }

    @Test
    @Graph( "Joe knows Sara" )
    public void shouldRespondWith404WhenStartNodeDoesNotExist()
    {
        String jsonString = "{\"to\" : \""
                            + getDataUri()
                            + "node/"
                            + getNode( "Joe" )
                            + "\", \"type\" : \"LOVES\", \"data\" : {\"foo\" : \"bar\"}}";
        gen.get().expectedStatus(
                Status.NOT_FOUND.getStatusCode() ).expectedType( MediaType.TEXT_HTML_TYPE ).payload( jsonString ).post(
                getDataUri() + "/node/12345/relationships" ).entity();
            }

    @Test
    @Graph( "Joe knows Sara" )
    public void creating_a_relationship_to_a_nonexisting_end_node()
    {
        String jsonString = "{\"to\" : \""
                            + getDataUri()
                            + "node/"
                            + "999999\", \"type\" : \"LOVES\", \"data\" : {\"foo\" : \"bar\"}}";
        gen.get().expectedStatus(
                Status.BAD_REQUEST.getStatusCode() ).payload( jsonString ).post(
                        getNodeUri( getNode( "Joe" ) ) + "/relationships" ).entity();
     }

    @Test
    @Graph( "Joe knows Sara" )
    public void creating_a_loop_relationship()
            throws Exception
    {
        
        Node joe = getNode( "Joe" );
        String jsonString = "{\"to\" : \"" + getNodeUri( joe )
                            + "\", \"type\" : \"LOVES\"}";
        String entity = gen.get().expectedStatus(
                Status.CREATED.getStatusCode() ).payload( jsonString ).post(
                        getNodeUri( getNode( "Joe" ) ) + "/relationships" ).entity();
        assertProperRelationshipRepresentation( JsonHelper.jsonToMap( entity ) );
    }

    @Test
    @Graph( "Joe knows Sara" )
    public void providing_bad_JSON()
    {
        String jsonString = "{\"to\" : \""
                            + getNodeUri( data.get().get( "Joe" ) )
                            + "\", \"type\" : \"LOVES\", \"data\" : {\"foo\" : **BAD JSON HERE*** \"bar\"}}";
        gen.get().expectedStatus(
                Status.BAD_REQUEST.getStatusCode() ).payload( jsonString ).post(
                        getNodeUri( getNode( "Joe" ) ) + "/relationships" ).entity();
    }

    private void assertProperRelationshipRepresentation(
            Map<String, Object> relrep )
    {
        RelationshipRepresentationTest.verifySerialisation( relrep );
    }
}
