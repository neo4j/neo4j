/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import javax.ws.rs.core.Response.Status;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.rest.repr.formats.StreamingJsonFormat;
import org.neo4j.test.GraphDescription;
import org.neo4j.test.GraphDescription.Graph;
import org.neo4j.test.GraphDescription.NODE;
import org.neo4j.test.GraphDescription.PROP;
import org.neo4j.test.GraphDescription.REL;
import org.neo4j.test.TestData.Title;

import com.sun.jersey.api.client.ClientResponse;

public class RelationshipDocIT extends AbstractRestFunctionalTestBase
{
    private static FunctionalTestHelper functionalTestHelper;

    @BeforeClass
    public static void setupServer() throws IOException
    {
        functionalTestHelper = new FunctionalTestHelper( server() );
    }

    @Test
    @Graph( nodes = { @NODE( name = "Romeo", setNameProperty = true ),
            @NODE( name = "Juliet", setNameProperty = true ) }, relationships = { @REL( start = "Romeo", end = "Juliet", type = "LOVES", properties = { @PROP( key = "cost", value = "high", type = GraphDescription.PropType.STRING ) } ) } )
    @Title( "Remove properties from a relationship" )
    public void shouldReturn204WhenPropertiesAreRemovedFromRelationship()
    {
        Relationship loves = getNode( "Romeo" ).getRelationships().iterator().next();
        gen().expectedStatus( Status.NO_CONTENT.getStatusCode() ).delete(
                        functionalTestHelper.relationshipPropertiesUri( loves.getId() ) )
                .entity();
    }

    @Test
    @Graph( "I know you" )
    public void get_Relationship_by_ID() throws JsonParseException
    {
        String response = gen().expectedStatus(
                com.sun.jersey.api.client.ClientResponse.Status.OK.getStatusCode() ).get(
                getRelationshipUri( data.get().get( "I" ).getSingleRelationship(
                        DynamicRelationshipType.withName( "know" ),
                        Direction.OUTGOING ) ) ).entity();
        assertTrue(JsonHelper.jsonToMap( response ).containsKey( "start" ));
    }

    /**
     * See the example request below.
     */
    @Test
    @Documented
    @Graph( nodes = { @NODE( name = "Romeo", setNameProperty = true ),
            @NODE( name = "Juliet", setNameProperty = true ) }, relationships = { @REL( start = "Romeo", end = "Juliet", type = "LOVES", properties = { @PROP( key = "cost", value = "high", type = GraphDescription.PropType.STRING ) } ) } )
    @Title( "Remove property from a relationship" )
    public void shouldReturn204WhenPropertyIsRemovedFromRelationship()
    {
        data.get();
        Relationship loves = getNode( "Romeo" ).getRelationships().iterator().next();
        gen().description(
                startGraph( "Remove property from a relationship1" ) );
        gen().expectedStatus( Status.NO_CONTENT.getStatusCode() ).delete(
                getPropertiesUri( loves ) + "/cost" ).entity();

    }

    /**
     * Attempting to remove a property that doesn't exist results in
     * an error.
     */
    @Test
    @Documented
    @Title( "Remove non-existent property from a relationship" )
    @Graph( nodes = { @NODE( name = "Romeo", setNameProperty = true ),
            @NODE( name = "Juliet", setNameProperty = true ) }, relationships = { @REL( start = "Romeo", end = "Juliet", type = "LOVES", properties = { @PROP( key = "cost", value = "high", type = GraphDescription.PropType.STRING ) } ) } )
    public void shouldReturn404WhenPropertyWhichDoesNotExistRemovedFromRelationship()
    {
        data.get();
        Relationship loves = getNode( "Romeo" ).getRelationships().iterator().next();
        gen().expectedStatus( Status.NOT_FOUND.getStatusCode() ).delete(
                getPropertiesUri( loves ) + "/non-existent" ).entity();
    }
    @Test
    @Graph( nodes = { @NODE( name = "Romeo", setNameProperty = true ),
            @NODE( name = "Juliet", setNameProperty = true ) }, relationships = { @REL( start = "Romeo", end = "Juliet", type = "LOVES", properties = { @PROP( key = "cost", value = "high", type = GraphDescription.PropType.STRING ) } ) } )
    public void shouldReturn404WhenPropertyWhichDoesNotExistRemovedFromRelationshipStreaming()
    {
        data.get();
        Relationship loves = IteratorUtil.first(getNode("Romeo").getRelationships());
        gen().withHeader(StreamingJsonFormat.STREAM_HEADER,"true").expectedStatus(Status.NOT_FOUND.getStatusCode()).delete(
                getPropertiesUri( loves ) + "/non-existent" ).entity();
    }

    /**
     * Attempting to remove all properties from a relationship which doesn't
     * exist results in an error.
     */
    @Test
    @Graph( "I know you" )
    @Documented
    @Title( "Remove properties from a non-existing relationship" )
    public void shouldReturn404WhenPropertiesRemovedFromARelationshipWhichDoesNotExist()
    {
        data.get();
        gen().expectedStatus( Status.NOT_FOUND.getStatusCode() )
                .delete( functionalTestHelper.relationshipPropertiesUri( 1234L ) )
                .entity();
    }

    /**
     * Attempting to remove a property from a relationship which doesn't exist
     * results in an error.
     */
    @Test
    @Graph( "I know you" )
    @Documented
    @Title( "Remove property from a non-existing relationship" )
    public void shouldReturn404WhenPropertyRemovedFromARelationshipWhichDoesNotExist()
    {
        data.get();
        gen().expectedStatus( Status.NOT_FOUND.getStatusCode() )
                .delete(
                        functionalTestHelper.relationshipPropertiesUri( 1234L )
                                + "/cost" )
                .entity();

    }

    @Test
    @Graph( nodes = { @NODE( name = "Romeo", setNameProperty = true ),
            @NODE( name = "Juliet", setNameProperty = true ) }, relationships = { @REL( start = "Romeo", end = "Juliet", type = "LOVES", properties = { @PROP( key = "cost", value = "high", type = GraphDescription.PropType.STRING ) } ) } )
    @Title( "Delete relationship" )
    public void removeRelationship()
    {
        data.get();
        Relationship loves = getNode( "Romeo" ).getRelationships().iterator().next();
        gen().description( startGraph( "Delete relationship1" ) );
        gen().expectedStatus( Status.NO_CONTENT.getStatusCode() ).delete(
                getRelationshipUri( loves ) ).entity();

    }
    
    @Test
    @Graph( nodes = { @NODE( name = "Romeo", setNameProperty = true ),
            @NODE( name = "Juliet", setNameProperty = true ) }, relationships = { @REL( start = "Romeo", end = "Juliet", type = "LOVES", properties = { @PROP( key = "cost", value = "high", type = GraphDescription.PropType.STRING ) } ) } )
    public void get_single_property_on_a_relationship() throws Exception {
        Relationship loves = getNode( "Romeo" ).getRelationships().iterator().next();
        String response = gen().expectedStatus( ClientResponse.Status.OK ).get(getRelPropURI(loves, "cost")).entity();
        assertTrue(response.contains( "high" ));
    }
    
    private String getRelPropURI( Relationship loves, String propertyKey )
    {
        return getRelationshipUri( loves )+"/properties/"+propertyKey;
    }
    @Test
    @Graph( nodes = { @NODE( name = "Romeo", setNameProperty = true ),
            @NODE( name = "Juliet", setNameProperty = true ) }, relationships = { @REL( start = "Romeo", end = "Juliet", type = "LOVES", properties = { @PROP( key = "cost", value = "high", type = GraphDescription.PropType.STRING ) } ) } )
    public void set_single_property_on_a_relationship() throws Exception {
        Relationship loves = getNode( "Romeo" ).getRelationships().iterator().next();
        assertTrue(loves.getProperty( "cost" ).equals( "high" ));
        gen().description( startGraph( "Set relationship property1" ) );
        gen().expectedStatus( ClientResponse.Status.NO_CONTENT ).payload("\"deadly\"").put(getRelPropURI(loves, "cost")).entity();
        assertTrue(loves.getProperty( "cost" ).equals( "deadly" ));
    }
    
    @Test
    @Graph( nodes = { @NODE( name = "Romeo", setNameProperty = true ),
            @NODE( name = "Juliet", setNameProperty = true ) }, relationships = { @REL( start = "Romeo", end = "Juliet", type = "LOVES", properties = { @PROP( key = "cost", value = "high", type = GraphDescription.PropType.STRING ),@PROP( key = "since", value = "1day", type = GraphDescription.PropType.STRING ) } ) } )
    public void set_all_properties_on_a_relationship() throws Exception {
        Relationship loves = getNode( "Romeo" ).getRelationships().iterator().next();
        assertTrue(loves.getProperty( "cost" ).equals( "high" ));
        gen().description( startGraph( "Set relationship property1" ) );
        gen().expectedStatus( ClientResponse.Status.NO_CONTENT ).payload(JsonHelper.createJsonFrom( MapUtil.map( "happy",false ) )).put(getRelPropsURI(loves)).entity();
        assertTrue(((Boolean)loves.getProperty( "happy")) ==false);
        assertFalse(loves.hasProperty( "cost" ));
    }
    @Test
    @Graph( nodes = { @NODE( name = "Romeo", setNameProperty = true ),
            @NODE( name = "Juliet", setNameProperty = true ) }, relationships = { @REL( start = "Romeo", end = "Juliet", type = "LOVES", properties = { @PROP( key = "cost", value = "high", type = GraphDescription.PropType.STRING ),@PROP( key = "since", value = "1day", type = GraphDescription.PropType.STRING ) } ) } )
    public void get_all_properties_on_a_relationship() throws Exception {
        Relationship loves = getNode( "Romeo" ).getRelationships().iterator().next();
        String response = gen().expectedStatus( ClientResponse.Status.OK ).get(getRelPropsURI(loves)).entity();
        assertTrue(response.contains( "high" ));
    }
    private String getRelPropsURI( Relationship rel )
    {
        return getRelationshipUri( rel) + "/properties";
    }
}
