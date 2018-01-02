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

import com.sun.jersey.api.client.ClientResponse;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import javax.ws.rs.core.Response.Status;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.rest.repr.StreamingFormat;
import org.neo4j.test.GraphDescription;
import org.neo4j.test.GraphDescription.Graph;
import org.neo4j.test.GraphDescription.NODE;
import org.neo4j.test.GraphDescription.PROP;
import org.neo4j.test.GraphDescription.REL;
import org.neo4j.test.TestData.Title;

import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.Neo4jMatchers.hasProperty;
import static org.neo4j.graphdb.Neo4jMatchers.inTx;

public class RelationshipDocIT extends AbstractRestFunctionalDocTestBase
{
    private static FunctionalTestHelper functionalTestHelper;

    @BeforeClass
    public static void setupServer() throws IOException
    {
        functionalTestHelper = new FunctionalTestHelper( server() );
    }

    @Test
    @Title("Remove properties from a relationship")
    @Graph(nodes = {@NODE(name = "Romeo", setNameProperty = true),
            @NODE(name = "Juliet", setNameProperty = true)}, relationships = {@REL(start = "Romeo", end = "Juliet",
            type = "LOVES", properties = {@PROP(key = "cost", value = "high", type = GraphDescription.PropType
            .STRING)})})
    public void shouldReturn204WhenPropertiesAreRemovedFromRelationship()
    {
        Relationship loves = getFirstRelationshipFromRomeoNode();
        gen().description( startGraph( "remove properties from a relationship" ) )
                .expectedStatus( Status.NO_CONTENT.getStatusCode() )
                .delete( functionalTestHelper.relationshipPropertiesUri( loves.getId() ) ).entity();
    }

    @Test
    @Graph("I know you")
    public void get_Relationship_by_ID() throws JsonParseException
    {
        Node node = data.get().get( "I" );
        Relationship relationship;
        try ( Transaction transaction = node.getGraphDatabase().beginTx() )
        {
            relationship = node.getSingleRelationship(
                    DynamicRelationshipType.withName( "know" ),
                    Direction.OUTGOING );
        }
        String response = gen().expectedStatus(
                com.sun.jersey.api.client.ClientResponse.Status.OK.getStatusCode() ).get(
                getRelationshipUri( relationship ) ).entity();
        assertTrue( JsonHelper.jsonToMap( response ).containsKey( "start" ) );
    }

    @Test
    @Title("Remove property from a relationship")
    @Documented( "See the example request below." )
    @Graph(nodes = {@NODE(name = "Romeo", setNameProperty = true),
            @NODE(name = "Juliet", setNameProperty = true)}, relationships = {@REL(start = "Romeo", end = "Juliet",
            type = "LOVES", properties = {@PROP(key = "cost", value = "high", type = GraphDescription.PropType
            .STRING)})})
    public void shouldReturn204WhenPropertyIsRemovedFromRelationship()
    {
        data.get();
        Relationship loves = getFirstRelationshipFromRomeoNode();
        gen().description(
                startGraph( "Remove property from a relationship1" ) );
        gen().expectedStatus( Status.NO_CONTENT.getStatusCode() ).delete(
                getPropertiesUri( loves ) + "/cost" ).entity();

    }

    @Test
    @Title("Remove non-existent property from a relationship")
    @Documented( "Attempting to remove a property that doesn't exist results in an error." )
    @Graph(nodes = {@NODE(name = "Romeo", setNameProperty = true),
            @NODE(name = "Juliet", setNameProperty = true)}, relationships = {@REL(start = "Romeo", end = "Juliet",
            type = "LOVES", properties = {@PROP(key = "cost", value = "high", type = GraphDescription.PropType
            .STRING)})})
    public void shouldReturn404WhenPropertyWhichDoesNotExistRemovedFromRelationship()
    {
        data.get();
        Relationship loves = getFirstRelationshipFromRomeoNode();
        gen().description( startGraph( "remove non-existent property from relationship" ) ).noGraph()
                .expectedStatus( Status.NOT_FOUND.getStatusCode() )
                .delete( getPropertiesUri( loves ) + "/non-existent" ).entity();
    }

    @Test
    @Graph(nodes = {@NODE(name = "Romeo", setNameProperty = true),
            @NODE(name = "Juliet", setNameProperty = true)}, relationships = {@REL(start = "Romeo", end = "Juliet",
            type = "LOVES", properties = {@PROP(key = "cost", value = "high", type = GraphDescription.PropType
            .STRING)})})
    public void shouldReturn404WhenPropertyWhichDoesNotExistRemovedFromRelationshipStreaming()
    {
        data.get();
        Relationship loves = getFirstRelationshipFromRomeoNode();
        gen().withHeader( StreamingFormat.STREAM_HEADER, "true" ).expectedStatus( Status.NOT_FOUND.getStatusCode
                () ).delete(
                getPropertiesUri( loves ) + "/non-existent" ).entity();
    }

    @Test
    @Graph( "I know you" )
    @Title( "Remove properties from a non-existing relationship" )
    @Documented( "Attempting to remove all properties from a relationship which doesn't exist results in an error." )
    public void shouldReturn404WhenPropertiesRemovedFromARelationshipWhichDoesNotExist()
    {
        data.get();
        gen().noGraph().expectedStatus( Status.NOT_FOUND.getStatusCode() )
                .delete( functionalTestHelper.relationshipPropertiesUri( 1234L ) )
                .entity();
    }

    @Test
    @Graph( "I know you" )
    @Title( "Remove property from a non-existing relationship" )
    @Documented( "Attempting to remove a property from a relationship which doesn't exist results in an error." )
    public void shouldReturn404WhenPropertyRemovedFromARelationshipWhichDoesNotExist()
    {
        data.get();
        gen().noGraph().expectedStatus( Status.NOT_FOUND.getStatusCode() )
                .delete(
                        functionalTestHelper.relationshipPropertiesUri( 1234L )
                                + "/cost" )
                .entity();

    }

    @Test
    @Graph(nodes = {@NODE(name = "Romeo", setNameProperty = true),
            @NODE(name = "Juliet", setNameProperty = true)}, relationships = {@REL(start = "Romeo", end = "Juliet",
            type = "LOVES", properties = {@PROP(key = "cost", value = "high", type = GraphDescription.PropType
            .STRING)})})
    @Title("Delete relationship")
    public void removeRelationship()
    {
        data.get();
        Relationship loves = getFirstRelationshipFromRomeoNode();
        gen().description( startGraph( "Delete relationship1" ) );
        gen().expectedStatus( Status.NO_CONTENT.getStatusCode() ).delete(
                getRelationshipUri( loves ) ).entity();

    }

    @Test
    @Graph(nodes = {@NODE(name = "Romeo", setNameProperty = true),
            @NODE(name = "Juliet", setNameProperty = true)}, relationships = {@REL(start = "Romeo", end = "Juliet",
            type = "LOVES", properties = {@PROP(key = "cost", value = "high", type = GraphDescription.PropType
            .STRING)})})
    public void get_single_property_on_a_relationship() throws Exception
    {
        Relationship loves = getFirstRelationshipFromRomeoNode();
        String response = gen().expectedStatus( ClientResponse.Status.OK ).get( getRelPropURI( loves,
                "cost" ) ).entity();
        assertTrue( response.contains( "high" ) );
    }

    private String getRelPropURI( Relationship loves, String propertyKey )
    {
        return getRelationshipUri( loves ) + "/properties/" + propertyKey;
    }

    @Test
    @Graph(nodes = {@NODE(name = "Romeo", setNameProperty = true),
            @NODE(name = "Juliet", setNameProperty = true)}, relationships = {@REL(start = "Romeo", end = "Juliet",
            type = "LOVES", properties = {@PROP(key = "cost", value = "high", type = GraphDescription.PropType
            .STRING)})})
    public void set_single_property_on_a_relationship() throws Exception
    {
        Relationship loves = getFirstRelationshipFromRomeoNode();
        assertThat( loves, inTx( graphdb(), hasProperty( "cost" ).withValue( "high" ) ) );
        gen().description( startGraph( "Set relationship property1" ) );
        gen().expectedStatus( ClientResponse.Status.NO_CONTENT ).payload( "\"deadly\"" ).put( getRelPropURI( loves,
                "cost" ) ).entity();
        assertThat( loves, inTx( graphdb(), hasProperty( "cost" ).withValue( "deadly" ) ) );
    }

    @Test
    @Graph(nodes = {@NODE(name = "Romeo", setNameProperty = true),
            @NODE(name = "Juliet", setNameProperty = true)}, relationships = {@REL(start = "Romeo", end = "Juliet",
            type = "LOVES", properties = {@PROP(key = "cost", value = "high", type = GraphDescription.PropType
            .STRING), @PROP(key = "since", value = "1day", type = GraphDescription.PropType.STRING)})})
    public void set_all_properties_on_a_relationship() throws Exception
    {
        Relationship loves = getFirstRelationshipFromRomeoNode();
        assertThat( loves, inTx( graphdb(), hasProperty( "cost" ).withValue( "high" ) ) );
        gen().description( startGraph( "Set relationship property1" ) );
        gen().expectedStatus( ClientResponse.Status.NO_CONTENT ).payload( JsonHelper.createJsonFrom( MapUtil.map(
                "happy", false ) ) ).put( getRelPropsURI( loves ) ).entity();
        assertThat( loves, inTx( graphdb(), hasProperty( "happy" ).withValue( false ) ) );
        assertThat( loves, inTx( graphdb(), not( hasProperty( "cost" ) ) ) );
    }

    @Test
    @Graph(nodes = {@NODE(name = "Romeo", setNameProperty = true),
            @NODE(name = "Juliet", setNameProperty = true)}, relationships = {@REL(start = "Romeo", end = "Juliet",
            type = "LOVES", properties = {@PROP(key = "cost", value = "high", type = GraphDescription.PropType
            .STRING), @PROP(key = "since", value = "1day", type = GraphDescription.PropType.STRING)})})
    public void get_all_properties_on_a_relationship() throws Exception
    {
        Relationship loves = getFirstRelationshipFromRomeoNode();
        String response = gen().expectedStatus( ClientResponse.Status.OK ).get( getRelPropsURI( loves ) ).entity();
        assertTrue( response.contains( "high" ) );
    }

    private Relationship getFirstRelationshipFromRomeoNode()
    {
        Node romeo = getNode( "Romeo" );

        try ( Transaction transaction = romeo.getGraphDatabase().beginTx() )
        {
            return romeo.getRelationships().iterator().next();
        }
    }

    private String getRelPropsURI( Relationship rel )
    {
        return getRelationshipUri( rel ) + "/properties";
    }
}
