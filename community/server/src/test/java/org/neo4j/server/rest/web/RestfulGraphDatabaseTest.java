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
package org.neo4j.server.rest.web;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.helpers.FakeClock;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.server.configuration.ConfigWrappingConfiguration;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.WrappedDatabase;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.rest.domain.TraverserReturnType;
import org.neo4j.server.rest.paging.LeaseManager;
import org.neo4j.server.rest.repr.BadInputException;
import org.neo4j.server.rest.repr.RelationshipRepresentationTest;
import org.neo4j.server.rest.repr.formats.JsonFormat;
import org.neo4j.server.rest.web.DatabaseActions.RelationshipDirection;
import org.neo4j.server.rest.web.RestfulGraphDatabase.AmpersandSeparatedCollection;
import org.neo4j.server.web.ServerInternalSettings;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.server.EntityOutputFormat;

import static java.lang.Long.parseLong;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.api.exceptions.Status.Request;
import static org.neo4j.kernel.api.exceptions.Status.Request.InvalidFormat;
import static org.neo4j.kernel.api.exceptions.Status.Schema;
import static org.neo4j.kernel.api.exceptions.Status.Statement;

public class RestfulGraphDatabaseTest
{
    private static final String NODE_AUTO_INDEX = "node_auto_index";
    private static final String RELATIONSHIP_AUTO_INDEX = "relationship_auto_index";
    private static final String BASE_URI = "http://neo4j.org/";
    private static final String NODE_SUBPATH = "node/";
    private static RestfulGraphDatabase service;
    private static Database database;
    private static GraphDbHelper helper;
    private static EntityOutputFormat output;
    private static LeaseManager leaseManager;
    private static GraphDatabaseAPI graph;

    @BeforeClass
    public static void doBefore() throws IOException
    {
        graph = (GraphDatabaseAPI)new TestGraphDatabaseFactory().newImpermanentDatabase();
        database = new WrappedDatabase(graph);
        helper = new GraphDbHelper( database );
        output = new EntityOutputFormat( new JsonFormat(), URI.create( BASE_URI ), null );
        leaseManager = new LeaseManager( new FakeClock() );

        Config config = new Config();
        config.registerSettingsClasses( asList( ServerSettings.class, ServerInternalSettings.class,
                GraphDatabaseSettings.class ));

        service = new RestfulGraphDatabase( new JsonFormat(), output,
                new DatabaseActions( leaseManager, true, database.getGraph() ), new ConfigWrappingConfiguration(
                config ) );
        service = new TransactionWrappingRestfulGraphDatabase( graph, service );
    }

    @Before
    public void deleteAllIndexes() throws JsonParseException
    {
        for ( String name : helper.getNodeIndexes() )
        {
            if ( NODE_AUTO_INDEX.equals( name ) )
            {
                stopAutoIndexAllPropertiesAndDisableAutoIndex( "node" );
            }
            else
            {
                service.deleteNodeIndex( name );
            }
        }
        for ( String name : helper.getRelationshipIndexes() )
        {
            if ( RELATIONSHIP_AUTO_INDEX.equals( name ) )
            {
                stopAutoIndexAllPropertiesAndDisableAutoIndex( "relationship" );
            }
            else
            {
                service.deleteRelationshipIndex( name );
            }
        }
    }

    protected void stopAutoIndexAllPropertiesAndDisableAutoIndex( String type )
            throws JsonParseException
    {
        Response response = service.getAutoIndexedProperties( type );
        List<String> properties = entityAsList( response );
        for ( String property : properties )
        {
            service.stopAutoIndexingProperty( type, property );
        }
        service.setAutoIndexerEnabled( type, "false" );
    }

    @AfterClass
    public static void shutdownDatabase() throws Throwable
    {
        graph.shutdown();
    }

    private static String entityAsString( Response response )
    {
        byte[] bytes = (byte[]) response.getEntity();
        try
        {
            return new String( bytes, "UTF-8" );
        }
        catch ( UnsupportedEncodingException e )
        {
            throw new RuntimeException( "Could not decode UTF-8", e );
        }
    }

    @SuppressWarnings("unchecked")
    private static List<String> entityAsList( Response response )
            throws JsonParseException
    {
        String entity = entityAsString( response );
        return (List<String>) JsonHelper.readJson( entity );
    }

    @Test
    public void shouldFailGracefullyWhenViolatingConstraintOnPropertyUpdate() throws Exception
    {
        Response response = service.createPropertyUniquenessConstraint( "Person", "{\"property_keys\":[\"name\"]}" );
        assertEquals( 200, response.getStatus() );

        createPerson( "Fred" );
        String wilma = createPerson( "Wilma" );

        Response setAllNodePropertiesResponse = service.setAllNodeProperties( parseLong( wilma ), "{\"name\":\"Fred\"}" );
        assertEquals( 409, setAllNodePropertiesResponse.getStatus() );
        assertEquals( Schema.ConstraintViolation.code().serialize(), singleErrorCode( setAllNodePropertiesResponse ) );

        Response singleNodePropertyResponse = service.setNodeProperty( parseLong( wilma ), "name", "\"Fred\"" );
        assertEquals( 409, singleNodePropertyResponse.getStatus() );
        assertEquals( Schema.ConstraintViolation.code().serialize(), singleErrorCode( singleNodePropertyResponse ) );
    }

    private String createPerson( final String name ) throws JsonParseException
    {
        Response response = service.createNode( "{\"name\" : \"" + name + "\"}" );
        assertEquals( 201, response.getStatus() );
        String self = (String) JsonHelper.jsonToMap( entityAsString( response ) ).get( "self" );
        String nodeId = self.substring( self.indexOf( NODE_SUBPATH ) + NODE_SUBPATH.length() );
        response = service.addNodeLabel( parseLong( nodeId ), "\"Person\"" );
        assertEquals( 204, response.getStatus() );
        return nodeId;
    }

    @Test
    public void shouldRespondWith201LocationHeaderAndNodeRepresentationInJSONWhenEmptyNodeCreated() throws Exception
    {
        Response response = service.createNode( null );

        assertEquals( 201, response.getStatus() );
        assertNotNull( response.getMetadata()
                .get( "Location" )
                .get( 0 ) );

        checkContentTypeCharsetUtf8( response );
        String json = entityAsString( response );

        Map<String, Object> map = JsonHelper.jsonToMap( json );

        assertNotNull( map );

        assertTrue( map.containsKey( "self" ) );
    }

    @Test
    public void shouldRespondWith201LocationHeaderAndNodeRepresentationInJSONWhenPopulatedNodeCreated()
            throws Exception
    {
        Response response = service.createNode( "{\"foo\" : \"bar\"}" );

        assertEquals( 201, response.getStatus() );
        assertNotNull( response.getMetadata()
                .get( "Location" )
                .get( 0 ) );

        checkContentTypeCharsetUtf8(response);
        String json = entityAsString( response );

        Map<String, Object> map = JsonHelper.jsonToMap( json );

        assertNotNull( map );

        assertTrue( map.containsKey( "self" ) );

        @SuppressWarnings("unchecked") Map<String, Object> data = (Map<String, Object>) map.get( "data" );

        assertEquals( "bar", data.get( "foo" ) );
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldRespondWith201LocationHeaderAndNodeRepresentationInJSONWhenPopulatedNodeCreatedWithArrays()
            throws Exception
    {
        Response response = service.createNode( "{\"foo\" : [\"bar\", \"baz\"] }" );

        assertEquals( 201, response.getStatus() );
        assertNotNull( response.getMetadata()
                .get( "Location" )
                .get( 0 ) );
        String json = entityAsString( response );

        Map<String, Object> map = JsonHelper.jsonToMap( json );

        assertNotNull( map );

        Map<String, Object> data = (Map<String, Object>) map.get( "data" );

        List<String> foo = (List<String>) data.get( "foo" );
        assertNotNull( foo );
        assertEquals( 2, foo.size() );
    }

    @Test
    public void shouldRespondWith400WhenNodeCreatedWithUnsupportedPropertyData() throws Exception
    {
        Response response = service.createNode( "{\"foo\" : {\"bar\" : \"baz\"}}" );

        assertEquals( 400, response.getStatus() );
        assertEquals( Statement.InvalidArguments.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith400WhenNodeCreatedWithInvalidJSON() throws Exception
    {
        Response response = service.createNode( "this:::isNot::JSON}" );

        assertEquals( 400, response.getStatus() );
        assertEquals( InvalidFormat.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith200AndNodeRepresentationInJSONWhenNodeRequested() throws Exception
    {
        Response response = service.getNode( helper.createNode() );
        assertEquals( 200, response.getStatus() );
        String json = entityAsString( response );
        Map<String, Object> map = JsonHelper.jsonToMap( json );
        assertNotNull( map );
        assertTrue( map.containsKey( "self" ) );
    }

    @Test
    public void shouldRespondWith404WhenRequestedNodeDoesNotExist() throws Exception
    {
        Response response = service.getNode( 9000000000000L );
        assertEquals( 404, response.getStatus() );
        assertEquals( Statement.EntityNotFound.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith204AfterSettingPropertiesOnExistingNode() throws Exception
    {
        Response response = service.setAllNodeProperties( helper.createNode(),
                "{\"foo\" : \"bar\", \"a-boolean\": true, \"boolean-array\": [true, false, false]}" );
        assertEquals( 204, response.getStatus() );
    }

    @Test
    public void shouldRespondWith404WhenSettingPropertiesOnNodeThatDoesNotExist() throws Exception
    {
        Response response = service.setAllNodeProperties( 9000000000000L, "{\"foo\" : \"bar\"}" );
        assertEquals( 404, response.getStatus() );
        assertEquals( Statement.EntityNotFound.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith400WhenTransferringCorruptJsonPayload() throws Exception
    {
        Response response = service.setAllNodeProperties( helper.createNode(),
                "{\"foo\" : bad-json-here \"bar\"}" );
        assertEquals( 400, response.getStatus() );
        assertEquals( Request.InvalidFormat.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith400WhenTransferringIncompatibleJsonPayload() throws Exception
    {
        Response response = service.setAllNodeProperties( helper.createNode(),
                "{\"foo\" : {\"bar\" : \"baz\"}}" );
        assertEquals( 400, response.getStatus() );
        assertEquals( Statement.InvalidArguments.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith200ForGetNodeProperties() throws Exception
    {
        long nodeId = helper.createNode();
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put( "foo", "bar" );
        helper.setNodeProperties( nodeId, properties );
        Response response = service.getAllNodeProperties( nodeId );
        assertEquals( 200, response.getStatus() );

        checkContentTypeCharsetUtf8( response );
    }

    @Test
    public void shouldGetPropertiesForGetNodeProperties() throws Exception
    {
        long nodeId = helper.createNode();
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put( "foo", "bar" );
        properties.put( "number", 15 );
        properties.put( "double", 15.7 );
        helper.setNodeProperties( nodeId, properties );
        Response response = service.getAllNodeProperties( nodeId );
        String jsonBody = entityAsString( response );
        Map<String, Object> readProperties = JsonHelper.jsonToMap( jsonBody );
        assertEquals( properties, readProperties );
    }

    @Test
    public void shouldRespondWith204OnSuccessfulDelete()
    {
        long id = helper.createNode();

        Response response = service.deleteNode( id );

        assertEquals( 204, response.getStatus() );
    }

    @Test
    public void shouldRespondWith409IfNodeCannotBeDeleted() throws Exception
    {
        long id = helper.createNode();
        helper.createRelationship( "LOVES", id, helper.createNode() );

        Response response = service.deleteNode( id );

        assertEquals( 409, response.getStatus() );
        assertEquals( Schema.ConstraintViolation.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith404IfNodeToBeDeletedDoesNotExist() throws Exception
    {
        long nonExistentId = 999999;
        Response response = service.deleteNode( nonExistentId );

        assertEquals( 404, response.getStatus() );
        assertEquals( Statement.EntityNotFound.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith204ForSetNodeProperty()
    {
        long nodeId = helper.createNode();
        String key = "foo";
        String json = "\"bar\"";
        Response response = service.setNodeProperty( nodeId, key, json );
        assertEquals( 204, response.getStatus() );
    }

    @Test
    public void shouldSetRightValueForSetNodeProperty()
    {
        long nodeId = helper.createNode();
        String key = "foo";
        Object value = "bar";
        String json = "\"" + value + "\"";
        service.setNodeProperty( nodeId, key, json );
        Map<String, Object> readProperties = helper.getNodeProperties( nodeId );
        assertEquals( Collections.singletonMap( key, value ), readProperties );
    }

    @Test
    public void shouldRespondWith404ForSetNodePropertyOnNonExistingNode() throws Exception
    {
        String key = "foo";
        String json = "\"bar\"";
        Response response = service.setNodeProperty( 999999, key, json );
        assertEquals( 404, response.getStatus() );
        assertEquals( Statement.EntityNotFound.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith400ForSetNodePropertyWithInvalidJson() throws Exception
    {
        String key = "foo";
        String json = "{invalid json";
        Response response = service.setNodeProperty( 999999, key, json );
        assertEquals( 400, response.getStatus() );
        assertEquals( Request.InvalidFormat.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith404ForGetNonExistentNodeProperty() throws Exception
    {
        long nodeId = helper.createNode();
        Response response = service.getNodeProperty( nodeId, "foo" );
        assertEquals( 404, response.getStatus() );
        assertEquals( Statement.NoSuchProperty.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith404ForGetNodePropertyOnNonExistentNode() throws Exception
    {
        long nodeId = 999999;
        Response response = service.getNodeProperty( nodeId, "foo" );
        assertEquals( 404, response.getStatus() );
        assertEquals( Statement.EntityNotFound.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith200ForGetNodeProperty()
    {
        long nodeId = helper.createNode();
        String key = "foo";
        Object value = "bar";
        helper.setNodeProperties( nodeId, Collections.singletonMap( key, value ) );
        Response response = service.getNodeProperty( nodeId, "foo" );
        assertEquals( 200, response.getStatus() );

        checkContentTypeCharsetUtf8( response );
    }

    @Test
    public void shouldReturnCorrectValueForGetNodeProperty() throws Exception
    {
        long nodeId = helper.createNode();
        String key = "foo";
        Object value = "bar";
        helper.setNodeProperties( nodeId, Collections.singletonMap( key, value ) );
        Response response = service.getNodeProperty( nodeId, "foo" );
        assertEquals( JsonHelper.createJsonFrom( value ), entityAsString( response ) );
    }

    @Test
    public void shouldRespondWith201AndLocationWhenRelationshipIsCreatedWithoutProperties()

    {
        long startNode = helper.createNode();
        long endNode = helper.createNode();
        Response response = service.createRelationship( startNode, "{\"to\" : \"" + BASE_URI + endNode
                                                                   + "\", \"type\" : \"LOVES\"}" );
        assertEquals( 201, response.getStatus() );
        assertNotNull( response.getMetadata()
                .get( "Location" )
                .get( 0 ) );
    }

    @Test
    public void shouldRespondWith201AndLocationWhenRelationshipIsCreatedWithProperties()

    {
        long startNode = helper.createNode();
        long endNode = helper.createNode();
        Response response = service.createRelationship( startNode,
                "{\"to\" : \"" + BASE_URI + endNode + "\", \"type\" : \"LOVES\", " +
                        "\"properties\" : {\"foo\" : \"bar\"}}" );
        assertEquals( 201, response.getStatus() );
        assertNotNull( response.getMetadata()
                .get( "Location" )
                .get( 0 ) );
    }

    @Test
    public void shouldReturnRelationshipRepresentationWhenCreatingRelationship() throws Exception
    {
        long startNode = helper.createNode();
        long endNode = helper.createNode();
        Response response = service.createRelationship( startNode,
                "{\"to\" : \"" + BASE_URI + endNode + "\", \"type\" : \"LOVES\", \"data\" : {\"foo\" : \"bar\"}}" );
        Map<String, Object> map = JsonHelper.jsonToMap( entityAsString( response ) );
        assertNotNull( map );
        assertTrue( map.containsKey( "self" ) );

        checkContentTypeCharsetUtf8( response );

        @SuppressWarnings("unchecked") Map<String, Object> data = (Map<String, Object>) map.get( "data" );

        assertEquals( "bar", data.get( "foo" ) );
    }

    @Test
    public void shouldRespondWith404WhenTryingToCreateRelationshipFromNonExistentNode() throws Exception
    {
        long nodeId = helper.createNode();
        Response response = service.createRelationship( nodeId + 1000, "{\"to\" : \"" + BASE_URI + nodeId
                + "\", \"type\" : \"LOVES\"}" );
        assertEquals( 404, response.getStatus() );
        assertEquals( Statement.EntityNotFound.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith400WhenTryingToCreateRelationshipToNonExistentNode() throws Exception
    {
        long nodeId = helper.createNode();
        Response response = service.createRelationship( nodeId, "{\"to\" : \"" + BASE_URI + (nodeId + 1000)
                + "\", \"type\" : \"LOVES\"}" );
        assertEquals( 400, response.getStatus() );
        assertEquals( Statement.EntityNotFound.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith201WhenTryingToCreateRelationshipBackToSelf()
    {
        long nodeId = helper.createNode();
        Response response = service.createRelationship( nodeId, "{\"to\" : \"" + BASE_URI + nodeId
                + "\", \"type\" : \"LOVES\"}" );
        assertEquals( 201, response.getStatus() );
    }

    @Test
    public void shouldRespondWith400WhenTryingToCreateRelationshipWithBadJson() throws Exception
    {
        long startNode = helper.createNode();
        long endNode = helper.createNode();
        Response response = service.createRelationship( startNode, "{\"to\" : \"" + BASE_URI + endNode
                + "\", \"type\" ***and junk*** : \"LOVES\"}" );
        assertEquals( 400, response.getStatus() );
        assertEquals( Request.InvalidFormat.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith400WhenTryingToCreateRelationshipWithUnsupportedProperties() throws Exception

    {
        long startNode = helper.createNode();
        long endNode = helper.createNode();
        Response response = service.createRelationship( startNode,
                "{\"to\" : \"" + BASE_URI + endNode
                        + "\", \"type\" : \"LOVES\", \"data\" : {\"foo\" : {\"bar\" : \"baz\"}}}" );
        assertEquals( 400, response.getStatus() );
        assertEquals( Statement.InvalidArguments.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith204ForRemoveNodeProperties()
    {
        long nodeId = helper.createNode();
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put( "foo", "bar" );
        properties.put( "number", 15 );
        helper.setNodeProperties( nodeId, properties );
        Response response = service.deleteAllNodeProperties( nodeId );
        assertEquals( 204, response.getStatus() );
    }

    @Test
    public void shouldBeAbleToRemoveNodeProperties()
    {
        long nodeId = helper.createNode();
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put( "foo", "bar" );
        properties.put( "number", 15 );
        helper.setNodeProperties( nodeId, properties );
        service.deleteAllNodeProperties( nodeId );
        assertEquals( true, helper.getNodeProperties( nodeId )
                .isEmpty() );
    }

    @Test
    public void shouldRespondWith404ForRemoveNodePropertiesForNonExistingNode() throws Exception
    {
        long nodeId = 999999;
        Response response = service.deleteAllNodeProperties( nodeId );
        assertEquals( 404, response.getStatus() );
        assertEquals( Statement.EntityNotFound.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldBeAbleToRemoveNodeProperty()
    {
        long nodeId = helper.createNode();
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put( "foo", "bar" );
        properties.put( "number", 15 );
        helper.setNodeProperties( nodeId, properties );
        service.deleteNodeProperty( nodeId, "foo" );
        assertEquals( Collections.singletonMap( "number", (Object) 15 ),
                helper.getNodeProperties( nodeId ) );
    }

    @Test
    public void shouldGet404WhenRemovingNonExistingProperty() throws Exception
    {
        long nodeId = helper.createNode();
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put( "foo", "bar" );
        properties.put( "number", 15 );
        helper.setNodeProperties( nodeId, properties );
        Response response = service.deleteNodeProperty( nodeId, "baz" );
        assertEquals( 404, response.getStatus() );
        assertEquals( Statement.NoSuchProperty.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldGet404WhenRemovingPropertyFromNonExistingNode() throws Exception
    {
        long nodeId = 999999;
        Response response = service.deleteNodeProperty( nodeId, "foo" );
        assertEquals( 404, response.getStatus() );
        assertEquals( Statement.EntityNotFound.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldGet200WhenRetrievingARelationshipFromANode()
    {
        long relationshipId = helper.createRelationship( "BEATS" );
        Response response = service.getRelationship( relationshipId );
        assertEquals( 200, response.getStatus() );

        checkContentTypeCharsetUtf8( response );
    }

    @Test
    public void shouldGet404WhenRetrievingRelationshipThatDoesNotExist() throws Exception
    {
        Response response = service.getRelationship( 999999 );
        assertEquals( 404, response.getStatus() );
        assertEquals( Statement.EntityNotFound.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith200AndDataForGetRelationshipProperties() throws Exception
    {
        long relationshipId = helper.createRelationship( "knows" );
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put( "foo", "bar" );
        helper.setRelationshipProperties( relationshipId, properties );
        Response response = service.getAllRelationshipProperties( relationshipId );
        assertEquals( 200, response.getStatus() );

        checkContentTypeCharsetUtf8( response );

        Map<String, Object> readProperties = JsonHelper.jsonToMap( entityAsString( response ) );
        assertEquals( properties, readProperties );
    }

    @Test
    public void shouldGet200WhenSuccessfullyRetrievedPropertyOnRelationship()
            throws Exception
    {

        long relationshipId = helper.createRelationship( "knows" );
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put( "some-key", "some-value" );
        helper.setRelationshipProperties( relationshipId, properties );

        Response response = service.getRelationshipProperty( relationshipId, "some-key" );

        assertEquals( 200, response.getStatus() );
        assertEquals( "some-value", JsonHelper.readJson( entityAsString( response ) ) );

        checkContentTypeCharsetUtf8( response );
    }

    @Test
    public void shouldGet404WhenCannotResolveAPropertyOnRelationship() throws Exception
    {
        long relationshipId = helper.createRelationship( "knows" );
        Response response = service.getRelationshipProperty( relationshipId, "some-key" );
        assertEquals( 404, response.getStatus() );
        assertEquals( Statement.NoSuchProperty.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldGet204WhenRemovingARelationship()
    {
        long relationshipId = helper.createRelationship( "KNOWS" );

        Response response = service.deleteRelationship( relationshipId );
        assertEquals( 204, response.getStatus() );
    }

    @Test
    public void shouldGet404WhenRemovingNonExistentRelationship() throws Exception
    {
        long relationshipId = helper.createRelationship( "KNOWS" );

        Response response = service.deleteRelationship( relationshipId + 1000 );
        assertEquals( 404, response.getStatus() );
        assertEquals( Statement.EntityNotFound.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith200AndListOfRelationshipRepresentationsWhenGettingRelationshipsForANode()
            throws Exception
    {
        long nodeId = helper.createNode();
        helper.createRelationship( "LIKES", nodeId, helper.createNode() );
        helper.createRelationship( "LIKES", helper.createNode(), nodeId );
        helper.createRelationship( "HATES", nodeId, helper.createNode() );

        Response response = service.getNodeRelationships( nodeId, RelationshipDirection.all,
                new AmpersandSeparatedCollection( "" ) );
        assertEquals( 200, response.getStatus() );

        checkContentTypeCharsetUtf8(response);

        verifyRelReps( 3, entityAsString( response ) );

        response = service.getNodeRelationships( nodeId, RelationshipDirection.in,
                new AmpersandSeparatedCollection( "" ) );
        assertEquals( 200, response.getStatus() );
        verifyRelReps( 1, entityAsString( response ) );

        response = service.getNodeRelationships( nodeId, RelationshipDirection.out, new AmpersandSeparatedCollection(
                "" ) );
        assertEquals( 200, response.getStatus() );
        verifyRelReps( 2, entityAsString( response ) );

        response = service.getNodeRelationships( nodeId, RelationshipDirection.out, new AmpersandSeparatedCollection(
                "LIKES&HATES" ) );
        assertEquals( 200, response.getStatus() );
        verifyRelReps( 2, entityAsString( response ) );

        response = service.getNodeRelationships( nodeId, RelationshipDirection.all, new AmpersandSeparatedCollection(
                "LIKES" ) );
        assertEquals( 200, response.getStatus() );
        verifyRelReps( 2, entityAsString( response ) );
    }

    @Test
    public void shouldNotReturnDuplicatesIfSameTypeSpecifiedMoreThanOnce() throws Exception
    {
        long nodeId = helper.createNode();
        helper.createRelationship( "LIKES", nodeId, helper.createNode() );
        Response response = service.getNodeRelationships( nodeId, RelationshipDirection.all,
                new AmpersandSeparatedCollection( "LIKES&LIKES" ) );
        Collection<?> array = (Collection<?>) JsonHelper.readJson( entityAsString( response ) );
        assertEquals( 1, array.size() );
    }

    private void verifyRelReps( int expectedSize, String entity ) throws JsonParseException
    {
        List<Map<String, Object>> relreps = JsonHelper.jsonToList( entity );
        assertEquals( expectedSize, relreps.size() );
        for ( Map<String, Object> relrep : relreps )
        {
            RelationshipRepresentationTest.verifySerialisation( relrep );
        }
    }

    @Test
    public void
    shouldRespondWith200AndEmptyListOfRelationshipRepresentationsWhenGettingRelationshipsForANodeWithoutRelationships()
            throws Exception
    {
        long nodeId = helper.createNode();

        Response response = service.getNodeRelationships( nodeId, RelationshipDirection.all,
                new AmpersandSeparatedCollection( "" ) );
        assertEquals( 200, response.getStatus() );
        verifyRelReps( 0, entityAsString( response ) );

        checkContentTypeCharsetUtf8( response );
    }

    @Test
    public void shouldRespondWith404WhenGettingIncomingRelationshipsForNonExistingNode() throws Exception

    {
        Response response = service.getNodeRelationships( 999999, RelationshipDirection.all,
                new AmpersandSeparatedCollection( "" ) );
        assertEquals( 404, response.getStatus() );
        assertEquals( Statement.EntityNotFound.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith204AndSetCorrectDataWhenSettingRelationshipProperties()

    {
        long relationshipId = helper.createRelationship( "KNOWS" );
        String json = "{\"name\": \"Mattias\", \"age\": 30}";
        Response response = service.setAllRelationshipProperties( relationshipId, json );
        assertEquals( 204, response.getStatus() );
        Map<String, Object> setProperties = new HashMap<String, Object>();
        setProperties.put( "name", "Mattias" );
        setProperties.put( "age", 30 );
        assertEquals( setProperties, helper.getRelationshipProperties( relationshipId ) );
    }

    @Test
    public void shouldRespondWith400WhenSettingRelationshipPropertiesWithBadJson() throws Exception
    {
        long relationshipId = helper.createRelationship( "KNOWS" );
        String json = "{\"name: \"Mattias\", \"age\": 30}";
        Response response = service.setAllRelationshipProperties( relationshipId, json );
        assertEquals( 400, response.getStatus() );
        assertEquals( Request.InvalidFormat.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith404WhenSettingRelationshipPropertiesOnNonExistingRelationship() throws Exception

    {
        long relationshipId = 99999999;
        String json = "{\"name\": \"Mattias\", \"age\": 30}";
        Response response = service.setAllRelationshipProperties( relationshipId, json );
        assertEquals( 404, response.getStatus() );
        assertEquals( Statement.EntityNotFound.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith204AndSetCorrectDataWhenSettingRelationshipProperty()
    {
        long relationshipId = helper.createRelationship( "KNOWS" );
        String key = "name";
        Object value = "Mattias";
        String json = "\"" + value + "\"";
        Response response = service.setRelationshipProperty( relationshipId, key, json );
        assertEquals( 204, response.getStatus() );
        assertEquals( value, helper.getRelationshipProperties( relationshipId )
                .get( "name" ) );
    }

    @Test
    public void shouldRespondWith400WhenSettingRelationshipPropertyWithBadJson() throws Exception
    {
        long relationshipId = helper.createRelationship( "KNOWS" );
        String json = "}Mattias";
        Response response = service.setRelationshipProperty( relationshipId, "name", json );
        assertEquals( 400, response.getStatus() );
        assertEquals( Request.InvalidFormat.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith404WhenSettingRelationshipPropertyOnNonExistingRelationship() throws Exception

    {
        long relationshipId = 99999999;
        String json = "\"Mattias\"";
        Response response = service.setRelationshipProperty( relationshipId, "name", json );
        assertEquals( 404, response.getStatus() );
        assertEquals( Statement.EntityNotFound.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith204WhenSuccessfullyRemovedRelationshipProperties()
    {
        long relationshipId = helper.createRelationship( "KNOWS" );
        helper.setRelationshipProperties( relationshipId, Collections.singletonMap( "foo", (Object) "bar" ) );

        Response response = service.deleteAllRelationshipProperties( relationshipId );
        assertEquals( 204, response.getStatus() );
    }

    @Test
    public void shouldRespondWith204WhenSuccessfullyRemovedRelationshipPropertiesWhichAreEmpty()

    {
        long relationshipId = helper.createRelationship( "KNOWS" );

        Response response = service.deleteAllRelationshipProperties( relationshipId );
        assertEquals( 204, response.getStatus() );
    }

    @Test
    public void shouldRespondWith404WhenNoRelationshipFromWhichToRemoveProperties() throws Exception
    {
        long relationshipId = helper.createRelationship( "KNOWS" );

        Response response = service.deleteAllRelationshipProperties( relationshipId + 1000 );
        assertEquals( 404, response.getStatus() );
        assertEquals( Statement.EntityNotFound.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith204WhenRemovingRelationshipProperty()
    {
        long relationshipId = helper.createRelationship( "KNOWS" );
        helper.setRelationshipProperties( relationshipId, Collections.singletonMap( "foo", (Object) "bar" ) );

        Response response = service.deleteRelationshipProperty( relationshipId, "foo" );

        assertEquals( 204, response.getStatus() );
    }

    @Test
    public void shouldRespondWith404WhenRemovingRelationshipPropertyWhichDoesNotExist() throws Exception
    {
        long relationshipId = helper.createRelationship( "KNOWS" );
        Response response = service.deleteRelationshipProperty( relationshipId, "foo" );
        assertEquals( 404, response.getStatus() );
        assertEquals( Statement.NoSuchProperty.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith404WhenNoRelationshipFromWhichToRemoveProperty() throws Exception
    {
        long relationshipId = helper.createRelationship( "KNOWS" );

        Response response = service.deleteRelationshipProperty( relationshipId * 1000, "some-key" );
        assertEquals( 404, response.getStatus() );
        assertEquals( Statement.EntityNotFound.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWithNoIndexOrOnlyNodeAutoIndex()
            throws JsonParseException
    {
        Response isEnabled = service.isAutoIndexerEnabled( "node" );
        assertEquals( "false", entityAsString( isEnabled ) );
        Response response = service.getNodeIndexRoot();
        if ( response.getStatus() == 200 )
        {
            Set<String> indexes = output.getResultAsMap()
                    .keySet();
            assertEquals( 1, indexes.size() );
            assertTrue( indexes.iterator()
                    .next()
                    .equals( NODE_AUTO_INDEX ) );
        }
        else
        {
            assertEquals( 204, response.getStatus() );
        }
    }

    @Test
    public void shouldRespondWithAvailableIndexNodeRoots() throws BadInputException
    {
        int numberOfAutoIndexesWhichCouldNotBeDeletedAtTestSetup = helper.getNodeIndexes().length;
        String indexName = "someNodes";
        helper.createNodeIndex( indexName );
        Response response = service.getNodeIndexRoot();
        assertEquals( 200, response.getStatus() );

        try ( Transaction transaction = graph.beginTx() )
        {
            Map<String, Object> resultAsMap = output.getResultAsMap();
            assertThat( resultAsMap.size(), is( numberOfAutoIndexesWhichCouldNotBeDeletedAtTestSetup + 1 ) );
            assertThat( resultAsMap, hasKey( indexName ) );
        }
    }

    @Test
    public void shouldRespondWithNoContentWhenNoRelationshipIndexesExist()
    {
        Response response = service.getRelationshipIndexRoot();
        assertEquals( 204, response.getStatus() );
    }

    @Test
    public void shouldRespondWithAvailableIndexRelationshipRoots() throws BadInputException
    {
        String indexName = "someRelationships";
        helper.createRelationshipIndex( indexName );
        Response response = service.getRelationshipIndexRoot();
        assertEquals( 200, response.getStatus() );

        try ( Transaction transaction = graph.beginTx() )
        {
            Map<String, Object> resultAsMap = output.getResultAsMap();
            assertThat( resultAsMap.size(), is( 1 ) );
            assertThat( resultAsMap, hasKey( indexName ) );
        }
    }

    @Test
    public void shouldBeAbleToGetRoot() throws JsonParseException
    {
        Response response = service.getRoot();
        assertEquals( 200, response.getStatus() );
        String entity = entityAsString( response );
        Map<String, Object> map = JsonHelper.jsonToMap( entity );
        assertNotNull( map.get( "node" ) );
        //this can be null
//        assertNotNull( map.get( "reference_node" ) );
        assertNotNull( map.get( "neo4j_version" ) );
        assertNotNull( map.get( "node_index" ) );
        assertNotNull( map.get( "extensions_info" ) );
        assertNotNull( map.get( "relationship_index" ) );
        assertNotNull( map.get( "batch" ) );

        checkContentTypeCharsetUtf8( response );
    }

    @Test
    public void shouldBeAbleToGetRootWhenNoReferenceNodePresent() throws Exception
    {
        helper.deleteNode( 0l );

        Response response = service.getRoot();
        assertEquals( 200, response.getStatus() );
        String entity = entityAsString( response );
        Map<String, Object> map = JsonHelper.jsonToMap( entity );
        assertNotNull( map.get( "node" ) );

        assertNotNull( map.get( "node_index" ) );
        assertNotNull( map.get( "extensions_info" ) );
        assertNotNull( map.get( "relationship_index" ) );

        assertNull( map.get( "reference_node" ) );

        checkContentTypeCharsetUtf8(response);
    }

    @Test
    public void shouldBeAbleToIndexNode()
    {
        Response response = service.createNode( null );
        URI nodeUri = (URI) response.getMetadata()
                .getFirst( "Location" );

        Map<String, String> postBody = new HashMap<String, String>();
        postBody.put( "key", "mykey" );
        postBody.put( "value", "my/key" );
        postBody.put( "uri", nodeUri.toString() );

        response = service.addToNodeIndex( "node", null, null, JsonHelper.createJsonFrom( postBody ) );

        assertEquals( 201, response.getStatus() );
        assertNotNull( response.getMetadata()
                .getFirst( "Location" ) );
    }

    @Test
    public void shouldNotBeAbleToIndexANodePropertyThatsTooLarge()
    {
        Response response = service.createNode( null );
        URI nodeUri = (URI) response.getMetadata()
                .getFirst( "Location" );

        Map<String, String> postBody = new HashMap<>();
        postBody.put( "key", "mykey" );

        char[] alphabet = "abcdefghijklmnopqrstuvwxyz".toCharArray();

        String largePropertyValue = "";
        Random random = new Random();
        for ( int i = 0; i < 30_000; i++ )
        {
            largePropertyValue += alphabet[random.nextInt( alphabet.length )];
        }

        postBody.put( "value", largePropertyValue );
        postBody.put( "uri", nodeUri.toString() );

        response = service.addToNodeIndex( "node", null, null, JsonHelper.createJsonFrom( postBody ) );

        assertEquals( 413, response.getStatus() );
    }


    @Test
    public void shouldBeAbleToIndexNodeUniquely()
    {
        Map<String, String> postBody = new HashMap<String, String>();
        postBody.put( "key", "mykey" );
        postBody.put( "value", "my/key" );

        Response response = service.addToNodeIndex( "unique-nodes", "", "",
                JsonHelper.createJsonFrom( postBody ) );

        assertEquals( 201, response.getStatus() );
        assertNotNull( response.getMetadata().getFirst( "Location" ) );

        response = service.addToNodeIndex( "unique-nodes", "", "",
                JsonHelper.createJsonFrom( postBody ) );

        assertEquals( 200, response.getStatus() );
    }

    @Test
    public void shouldNotBeAbleToIndexNodeUniquelyWithBothUriAndPropertiesInPayload() throws Exception
    {
        URI node = (URI) service.createNode( null ).getMetadata().getFirst( "Location" );
        Map<String, Object> postBody = new HashMap<String, Object>();
        postBody.put( "key", "mykey" );
        postBody.put( "value", "my/key" );
        postBody.put( "uri", node.toString() );
        postBody.put( "properties", new HashMap<String,Object>() );

        Response response = service.addToNodeIndex( "unique-nodes", "", "",
                JsonHelper.createJsonFrom( postBody ) );
        assertEquals( 400, response.getStatus() );
        assertEquals( Statement.InvalidArguments.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void uniquelyIndexedNodeGetsTheSpecifiedKeyAndValueAsPropertiesIfNoPropertiesAreSpecified() throws Exception
    {
        final String key = "somekey", value = "somevalue";

        Map<String, Object> postBody = new HashMap<String, Object>();
        postBody.put( "key", key );
        postBody.put( "value", value );

        Response response = service.addToNodeIndex( "unique-nodes", "", "",
                JsonHelper.createJsonFrom( postBody ) );
        assertEquals( 201, response.getStatus() );
        Object node = response.getMetadata().getFirst( "Location" );
        assertNotNull( node );
        String uri = node.toString();
        Map<String, Object> properties = helper.getNodeProperties( parseLong( uri.substring( uri.lastIndexOf(
                '/' ) + 1 ) ) );
        assertEquals( 1, properties.size() );
        assertEquals( value, properties.get( key ) );
    }

    @Test
    public void specifiedPropertiesOverrideKeyAndValueForUniquelyIndexedNodes() throws Exception
    {
        final String key = "a_key", value = "a value";

        Map<String, Object> postBody = new HashMap<String, Object>();
        postBody.put( "key", key );
        postBody.put( "value", value );
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put( "name", "Jürgen" );
        properties.put( "age", "42" );
        properties.put( "occupation", "crazy" );
        postBody.put( "properties", properties );

        Response response = service.addToNodeIndex( "unique-nodes", "", "",
                JsonHelper.createJsonFrom( postBody ) );
        assertEquals( 201, response.getStatus() );
        Object node = response.getMetadata().getFirst( "Location" );
        assertNotNull( node );
        String uri = node.toString();
        assertEquals( properties, helper.getNodeProperties( parseLong( uri.substring( uri.lastIndexOf( '/' ) + 1
        ) ) ) );
    }

    @Test
    public void shouldNotBeAbleToCreateAnIndexWithEmptyName() throws Exception
    {
        URI node = (URI) service.createNode( null ).getMetadata().getFirst( "Location" );

        Map<String, String> createRel = new HashMap<String, String>();
        createRel.put( "to", node.toString() );
        createRel.put( "type", "knows" );
        URI rel = (URI) service.createRelationship( helper.createNode(), JsonHelper.createJsonFrom( createRel
        ) ).getMetadata().getFirst( "Location" );

        Map<String, String> indexPostBody = new HashMap<String, String>();
        indexPostBody.put( "key", "mykey" );
        indexPostBody.put( "value", "myvalue" );

        indexPostBody.put( "uri", node.toString() );
        Response response = service.addToNodeIndex( "", "", "", JsonHelper.createJsonFrom( indexPostBody ) );
        assertEquals( "http bad request when trying to create an index with empty name", 400, response.getStatus() );

        indexPostBody.put( "uri", rel.toString() );
        response = service.addToRelationshipIndex( "", "", "", JsonHelper.createJsonFrom( indexPostBody ) );
        assertEquals( "http bad request when trying to create an index with empty name", 400, response.getStatus() );

        Map<String, String> basicIndexCreation = new HashMap<String, String>();
        basicIndexCreation.put( "name", "" );

        response = service.jsonCreateNodeIndex( JsonHelper.createJsonFrom( basicIndexCreation ) );
        assertEquals( "http bad request when trying to create an index with empty name", 400, response.getStatus() );

        response = service.jsonCreateRelationshipIndex( JsonHelper.createJsonFrom( basicIndexCreation ) );
        assertEquals( "http bad request when trying to create an index with empty name", 400, response.getStatus() );
    }

    @Test
    public void shouldNotBeAbleToIndexNodeUniquelyWithRequiredParameterMissing() throws Exception
    {
        service.createNode( null ).getMetadata().getFirst( "Location" );
        Map<String, Object> body = new HashMap<String, Object>();
        body.put( "key", "mykey" );
        body.put( "value", "my/key" );
        for ( String key : body.keySet() )
        {
            Map<String, Object> postBody = new HashMap<String, Object>( body );
            postBody.remove( key );
            Response response = service.addToNodeIndex( "unique-nodes", "", "",
                    JsonHelper.createJsonFrom( postBody ) );

            assertEquals( "unexpected response code with \"" + key + "\" missing.", 400, response.getStatus() );
        }
    }

    @Test
    public void shouldBeAbleToIndexRelationshipUniquely() throws Exception
    {
        URI start = (URI) service.createNode( null ).getMetadata().getFirst( "Location" );
        URI end = (URI) service.createNode( null ).getMetadata().getFirst( "Location" );
        Map<String, String> postBody = new HashMap<String, String>();
        postBody.put( "key", "mykey" );
        postBody.put( "value", "my/key" );
        postBody.put( "start", start.toString() );
        postBody.put( "end", end.toString() );
        postBody.put( "type", "knows" );
        for ( int i = 0; i < 2; i++ )
        {
            Response response = service.addToNodeIndex( "unique-relationships", "", "",
                    JsonHelper.createJsonFrom( postBody ) );

            assertEquals( 201 - i, response.getStatus() );
            if ( i == 0 )
            {
                assertNotNull( response.getMetadata().getFirst( "Location" ) );
            }
        }
    }

    @Test
    public void uniquelyIndexedRelationshipGetsTheSpecifiedKeyAndValueAsPropertiesIfNoPropertiesAreSpecified() throws
            Exception
    {
        final String key = "somekey", value = "somevalue";
        URI start = (URI) service.createNode( null ).getMetadata().getFirst( "Location" );
        URI end = (URI) service.createNode( null ).getMetadata().getFirst( "Location" );

        Map<String, Object> postBody = new HashMap<String, Object>();
        postBody.put( "key", key );
        postBody.put( "value", value );
        postBody.put( "start", start.toString() );
        postBody.put( "end", end.toString() );
        postBody.put( "type", "knows" );

        Response response = service.addToRelationshipIndex( "unique-relationships", "", "",
                JsonHelper.createJsonFrom( postBody ) );
        assertEquals( 201, response.getStatus() );
        Object rel = response.getMetadata().getFirst( "Location" );
        assertNotNull( rel );
        String uri = rel.toString();
        Map<String, Object> properties = helper.getRelationshipProperties( parseLong( uri.substring( uri
                .lastIndexOf( '/' ) + 1 ) ) );
        assertEquals( 1, properties.size() );
        assertEquals( value, properties.get( key ) );
    }

    @Test
    public void specifiedPropertiesOverrideKeyAndValueForUniquelyIndexedRelationships() throws Exception
    {
        final String key = "a_key", value = "a value";
        URI start = (URI) service.createNode( null ).getMetadata().getFirst( "Location" );
        URI end = (URI) service.createNode( null ).getMetadata().getFirst( "Location" );

        Map<String, Object> postBody = new HashMap<String, Object>();
        postBody.put( "key", key );
        postBody.put( "value", value );
        postBody.put( "start", start.toString() );
        postBody.put( "end", end.toString() );
        postBody.put( "type", "knows" );
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put( "name", "Jürgen" );
        properties.put( "age", "42" );
        properties.put( "occupation", "crazy" );
        postBody.put( "properties", properties );

        Response response = service.addToRelationshipIndex( "unique-relationships", "", "",
                JsonHelper.createJsonFrom( postBody ) );
        assertEquals( 201, response.getStatus() );
        Object rel = response.getMetadata().getFirst( "Location" );
        assertNotNull( rel );
        String uri = rel.toString();
        assertEquals( properties, helper.getRelationshipProperties( parseLong( uri.substring( uri.lastIndexOf(
                '/' ) + 1 ) ) ) );
    }

    @Test
    public void shouldNotBeAbleToIndexRelationshipUniquelyWithBothUriAndCreationalDataInPayload() throws Exception
    {
        URI start = (URI) service.createNode( null ).getMetadata().getFirst( "Location" );
        URI end = (URI) service.createNode( null ).getMetadata().getFirst( "Location" );
        String path = start.getPath();
        URI rel = (URI) service.createRelationship( parseLong( path.substring( path.lastIndexOf( '/' ) + 1 ) ),
                "{\"to\":\"" + end + "\",\"type\":\"knows\"}" ).getMetadata()
                .getFirst( "Location" );
        Map<String, Object> unwanted = new HashMap<String, Object>();
        unwanted.put( "properties", new HashMap() );
        unwanted.put( "start", start.toString() );
        unwanted.put( "end", end.toString() );
        unwanted.put( "type", "friend" );
        for ( Map.Entry<String, Object> bad : unwanted.entrySet() )
        {
            Map<String, Object> postBody = new HashMap<String, Object>();
            postBody.put( "key", "mykey" );
            postBody.put( "value", "my/key" );
            postBody.put( "uri", rel.toString() );
            postBody.put( bad.getKey(), bad.getValue() );

            Response response = service.addToRelationshipIndex( "unique-relationships", "", "",
                    JsonHelper.createJsonFrom( postBody ) );
            assertEquals( "unexpected response code with \"" + bad.getKey() + "\".", 400, response.getStatus() );
        }
    }

    @Test
    public void shouldNotBeAbleToIndexRelationshipUniquelyWithRequiredParameterMissing() throws Exception
    {
        URI start = (URI) service.createNode( null ).getMetadata().getFirst( "Location" );
        URI end = (URI) service.createNode( null ).getMetadata().getFirst( "Location" );
        Map<String, Object> body = new HashMap<String, Object>();
        body.put( "key", "mykey" );
        body.put( "value", "my/key" );
        body.put( "start", start.toString() );
        body.put( "end", end.toString() );
        body.put( "type", "knows" );
        for ( String key : body.keySet() )
        {
            Map<String, Object> postBody = new HashMap<String, Object>( body );
            postBody.remove( key );
            Response response = service.addToRelationshipIndex( "unique-relationships", "", "",
                    JsonHelper.createJsonFrom( postBody ) );

            assertEquals( "unexpected response code with \"" + key + "\" missing.", 400, response.getStatus() );
        }
    }

    @Test
    public void shouldBeAbleToRemoveNodeIndex()
    {
        String indexName = "myFancyIndex";

        int numberOfAutoIndexesWhichCouldNotBeDeletedAtTestSetup = helper.getNodeIndexes().length;

        helper.createNodeIndex( indexName );
        helper.createNodeIndex( "another one" );

        assertEquals( numberOfAutoIndexesWhichCouldNotBeDeletedAtTestSetup + 2, helper.getNodeIndexes().length );

        Response response = service.deleteNodeIndex( indexName );

        assertEquals( 204, response.getStatus() );
        assertEquals( numberOfAutoIndexesWhichCouldNotBeDeletedAtTestSetup + 1, helper.getNodeIndexes().length );
    }

    @Test
    public void shouldBeAbleToRemoveRelationshipIndex()
    {
        String indexName = "myFancyIndex";

        assertEquals( 0, helper.getRelationshipIndexes().length );

        helper.createRelationshipIndex( indexName );

        assertEquals( 1, helper.getRelationshipIndexes().length );

        Response response = service.deleteRelationshipIndex( indexName );

        assertEquals( 204, response.getStatus() );
        assertEquals( 0, helper.getRelationshipIndexes().length );
    }

    @Test
    public void shouldBeAbleToGetNodeRepresentationFromIndexUri() throws Exception
    {
        String key = "key_get_noderep";
        String value = "value";
        long nodeId = helper.createNode();
        String indexName = "all-the-best-nodes";
        helper.addNodeToIndex( indexName, key, value, nodeId );
        Response response = service.getNodeFromIndexUri( indexName, key, value, nodeId );
        assertEquals( 200, response.getStatus() );

        checkContentTypeCharsetUtf8( response );
        assertNull( response.getMetadata()
                .get( "Location" ) );
        Map<String, Object> map = JsonHelper.jsonToMap( entityAsString( response ) );
        assertNotNull( map );
        assertTrue( map.containsKey( "self" ) );
    }

    private void checkContentTypeCharsetUtf8(Response response)
    {
        assertTrue( response.getMetadata()
                .getFirst( HttpHeaders.CONTENT_TYPE ).toString().contains( "UTF-8" ));
    }

    @Test
    public void shouldBeAbleToGetRelationshipRepresentationFromIndexUri() throws Exception
    {
        String key = "key_get_noderep";
        String value = "value";
        long startNodeId = helper.createNode();
        long endNodeId = helper.createNode();
        String relationshipType = "knows";
        long relationshipId = helper.createRelationship( relationshipType, startNodeId, endNodeId );

        String indexName = "all-the-best-relationships";
        helper.addRelationshipToIndex( indexName, key, value, relationshipId );
        Response response = service.getRelationshipFromIndexUri( indexName, key, value, relationshipId );
        assertEquals( 200, response.getStatus() );
        checkContentTypeCharsetUtf8(response);

        assertNull( response.getMetadata()
                .get( "Location" ) );
        Map<String, Object> map = JsonHelper.jsonToMap( entityAsString( response ) );
        assertNotNull( map );
        assertTrue( map.containsKey( "self" ) );
    }

    @Test
    public void shouldBeAbleToGetListOfNodeRepresentationsFromIndexLookup() throws Exception
    {
        ModelBuilder.DomainModel matrixers = ModelBuilder.generateMatrix( service );

        Map.Entry<String, String> indexedKeyValue = matrixers.indexedNodeKeyValues.entrySet()
                .iterator()
                .next();
        Response response = service.getIndexedNodes( matrixers.nodeIndexName, indexedKeyValue.getKey(),
                indexedKeyValue.getValue() );
        assertEquals( Status.OK.getStatusCode(), response.getStatus() );
        Collection<?> items = (Collection<?>) JsonHelper.readJson( entityAsString( response ) );
        int counter = 0;
        for ( Object item : items )
        {
            Map<?, ?> map = (Map<?, ?>) item;
            Map<?, ?> properties = (Map<?, ?>) map.get( "data" );
            assertNotNull( map.get( "self" ) );
            String indexedUri = (String) map.get( "indexed" );
            assertEquals( matrixers.indexedNodeUriToEntityMap.get( new URI( indexedUri ) ).properties.get( "name" ),
                    properties.get( "name" ) );
            counter++;
        }
        assertEquals( 2, counter );
    }

    @Test
    public void shouldBeAbleToGetListOfNodeRepresentationsFromIndexQuery() throws Exception
    {
        ModelBuilder.DomainModel matrixers = ModelBuilder.generateMatrix( service );

        Map.Entry<String, String> indexedKeyValue = matrixers.indexedNodeKeyValues.entrySet()
                .iterator()
                .next();
        // query for the first letter with which the nodes were indexed.
        Response response = service.getIndexedNodesByQuery( matrixers.nodeIndexName, indexedKeyValue.getKey() + ":"
                                                                                     +
                                                                                     indexedKeyValue.getValue().substring( 0, 1 ) +
                                                                                     "*",
                "" /*default ordering*/ );
        assertEquals( Status.OK.getStatusCode(), response.getStatus() );
        Collection<?> items = (Collection<?>) JsonHelper.readJson( entityAsString( response ) );
        int counter = 0;
        for ( Object item : items )
        {
            Map<?, ?> map = (Map<?, ?>) item;
            Map<?, ?> properties = (Map<?, ?>) map.get( "data" );
            String indexedUri = (String) map.get( "indexed" ); // unlike exact
            // match, a query
            // can not return
            // a sensible
            // index uri for
            // the result
            assertNull( indexedUri );
            String selfUri = (String) map.get( "self" );
            assertNotNull( selfUri );
            assertEquals( matrixers.nodeUriToEntityMap.get( new URI( selfUri ) ).properties.get( "name" ),
                    properties.get( "name" ) );
            counter++;
        }
        assertThat( counter, is( greaterThanOrEqualTo( 2 ) ) );
    }

    @Test
    public void shouldBeAbleToGetListOfNodeRepresentationsFromIndexQueryWithDefaultKey() throws Exception
    {
        ModelBuilder.DomainModel matrixers = ModelBuilder.generateMatrix( service );

        Map.Entry<String, String> indexedKeyValue = matrixers.indexedNodeKeyValues.entrySet()
                .iterator()
                .next();
        // query for the first letter with which the nodes were indexed.
        Response response = service.getIndexedNodesByQuery( matrixers.nodeIndexName, indexedKeyValue.getKey(),
                indexedKeyValue.getValue().substring( 0, 1 ) + "*", "" /*default ordering*/ );
        assertEquals( Status.OK.getStatusCode(), response.getStatus() );
        Collection<?> items = (Collection<?>) JsonHelper.readJson( entityAsString( response ) );
        int counter = 0;
        for ( Object item : items )
        {
            Map<?, ?> map = (Map<?, ?>) item;
            Map<?, ?> properties = (Map<?, ?>) map.get( "data" );
            String indexedUri = (String) map.get( "indexed" ); // unlike exact
            // match, a query
            // can not return
            // a sensible
            // index uri for
            // the result
            assertNull( indexedUri );
            String selfUri = (String) map.get( "self" );
            assertNotNull( selfUri );
            assertEquals( matrixers.nodeUriToEntityMap.get( new URI( selfUri ) ).properties.get( "name" ),
                    properties.get( "name" ) );
            counter++;
        }
        assertThat( counter, is( greaterThanOrEqualTo( 2 ) ) );
    }

    @Test
    public void shouldBeAbleToGetListOfRelationshipRepresentationsFromIndexLookup() throws Exception
    {
        String key = "key_get";
        String value = "value";

        long startNodeId = helper.createNode();
        long endNodeId = helper.createNode();

        String relationshipType1 = "KNOWS";
        long relationshipId1 = helper.createRelationship( relationshipType1, startNodeId, endNodeId );
        String relationshipType2 = "PLAYS-NICE-WITH";
        long relationshipId2 = helper.createRelationship( relationshipType2, startNodeId, endNodeId );

        String indexName = "matrixal-relationships";
        helper.createRelationshipIndex( indexName );
        helper.addRelationshipToIndex( indexName, key, value, relationshipId1 );
        helper.addRelationshipToIndex( indexName, key, value, relationshipId2 );

        Response response = service.getIndexedRelationships( indexName, key, value );
        assertEquals( Status.OK.getStatusCode(), response.getStatus() );
        Collection<?> items = (Collection<?>) JsonHelper.readJson( entityAsString( response ) );
        int counter = 0;
        for ( Object item : items )
        {
            Map<?, ?> map = (Map<?, ?>) item;
            assertNotNull( map.get( "self" ) );
            String indexedUri = (String) map.get( "indexed" );
            assertThat( indexedUri, containsString( key ) );
            assertThat( indexedUri, containsString( value ) );
            assertTrue( indexedUri.endsWith( Long.toString( relationshipId1 ) )
                        || indexedUri.endsWith( Long.toString( relationshipId2 ) ) );
            counter++;
        }
        assertEquals( 2, counter );
    }

    @Test
    public void shouldBeAbleToGetListOfRelationshipRepresentationsFromIndexQuery() throws Exception
    {
        String key = "key_get";
        String value = "value";

        long startNodeId = helper.createNode();
        long endNodeId = helper.createNode();

        String relationshipType1 = "KNOWS";
        long relationshipId1 = helper.createRelationship( relationshipType1, startNodeId, endNodeId );
        String relationshipType2 = "PLAYS-NICE-WITH";
        long relationshipId2 = helper.createRelationship( relationshipType2, startNodeId, endNodeId );

        String indexName = "matrixal-relationships";
        helper.createRelationshipIndex( indexName );
        helper.addRelationshipToIndex( indexName, key, value, relationshipId1 );
        helper.addRelationshipToIndex( indexName, key, value, relationshipId2 );

        Response response = service.getIndexedRelationshipsByQuery( indexName,
                key + ":" + value.substring( 0, 1 ) + "*", "" /*default ordering*/ );
        assertEquals( Status.OK.getStatusCode(), response.getStatus() );
        Collection<?> items = (Collection<?>) JsonHelper.readJson( entityAsString( response ) );
        int counter = 0;
        for ( Object item : items )
        {
            Map<?, ?> map = (Map<?, ?>) item;
            String indexedUri = (String) map.get( "indexed" );
            assertNull( indexedUri ); // queries can not return a sensible index
            // uri
            String selfUri = (String) map.get( "self" );
            assertNotNull( selfUri );
            assertTrue( selfUri.endsWith( Long.toString( relationshipId1 ) )
                    || selfUri.endsWith( Long.toString( relationshipId2 ) ) );
            counter++;
        }
        assertThat( counter, is( greaterThanOrEqualTo( 2 ) ) );
    }

    @Test
    public void shouldBeAbleToGetListOfRelationshipRepresentationsFromIndexQueryWithDefaultKey() throws Exception
    {
        String key = "key_get";
        String value = "value";

        long startNodeId = helper.createNode();
        long endNodeId = helper.createNode();

        String relationshipType1 = "KNOWS";
        long relationshipId1 = helper.createRelationship( relationshipType1, startNodeId, endNodeId );
        String relationshipType2 = "PLAYS-NICE-WITH";
        long relationshipId2 = helper.createRelationship( relationshipType2, startNodeId, endNodeId );

        String indexName = "matrixal-relationships";
        helper.createRelationshipIndex( indexName );
        helper.addRelationshipToIndex( indexName, key, value, relationshipId1 );
        helper.addRelationshipToIndex( indexName, key, value, relationshipId2 );

        Response response = service.getIndexedRelationshipsByQuery( indexName,
                key, value.substring( 0, 1 ) + "*", "" /*default ordering*/ );
        assertEquals( Status.OK.getStatusCode(), response.getStatus() );
        Collection<?> items = (Collection<?>) JsonHelper.readJson( entityAsString( response ) );
        int counter = 0;
        for ( Object item : items )
        {
            Map<?, ?> map = (Map<?, ?>) item;
            String indexedUri = (String) map.get( "indexed" );
            assertNull( indexedUri ); // queries can not return a sensible index
            // uri
            String selfUri = (String) map.get( "self" );
            assertNotNull( selfUri );
            assertTrue( selfUri.endsWith( Long.toString( relationshipId1 ) )
                    || selfUri.endsWith( Long.toString( relationshipId2 ) ) );
            counter++;
        }
        assertThat( counter, is( greaterThanOrEqualTo( 2 ) ) );
    }

    @Test
    public void shouldGet200AndEmptyListWhenNothingFoundInIndexLookup() throws Exception
    {
        String indexName = "nothing-in-this-index";
        helper.createNodeIndex( indexName );
        Response response = service.getIndexedNodes( indexName, "fooo", "baaar" );
        assertEquals( Status.OK.getStatusCode(), response.getStatus() );

        checkContentTypeCharsetUtf8( response );

        String entity = entityAsString( response );
        Object parsedJson = JsonHelper.readJson( entity );
        assertTrue( parsedJson instanceof Collection<?> );
        assertTrue( ((Collection<?>) parsedJson).isEmpty() );
    }

    @Test
    public void shouldBeAbleToRemoveNodeFromIndex()
    {
        long nodeId = helper.createNode();
        String key = "key_remove";
        String value = "value";
        helper.addNodeToIndex( "node", key, value, nodeId );
        assertEquals( 1, helper.getIndexedNodes( "node", key, value )
                .size() );
        Response response = service.deleteFromNodeIndex( "node", key, value, nodeId );
        assertEquals( Status.NO_CONTENT.getStatusCode(), response.getStatus() );
        assertEquals( 0, helper.getIndexedNodes( "node", key, value )
                .size() );
    }

    @Test
    public void shouldBeAbleToRemoveRelationshipFromIndex()
    {
        long startNodeId = helper.createNode();
        long endNodeId = helper.createNode();
        String relationshipType = "related-to";
        long relationshipId = helper.createRelationship( relationshipType, startNodeId, endNodeId );
        String key = "key_remove";
        String value = "value";
        String indexName = "relationships";
        helper.addRelationshipToIndex( indexName, key, value, relationshipId );
        assertEquals( 1, helper.getIndexedRelationships( indexName, key, value )
                .size() );
        Response response = service.deleteFromRelationshipIndex( indexName, key, value, relationshipId );
        assertEquals( Status.NO_CONTENT.getStatusCode(), response.getStatus() );
        assertEquals( 0, helper.getIndexedRelationships( indexName, key, value )
                .size() );
    }

    @Test
    public void shouldGet404IfRemovingNonExistentNodeIndexing()
    {
        Response response = service.deleteFromNodeIndex( "nodes", "bogus", "bogus", 999999 );
        assertEquals( Status.NOT_FOUND.getStatusCode(), response.getStatus() );
//        assertEquals( Statement..code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldGet404IfRemovingNonExistentRelationshipIndexing()
    {
        Response response = service.deleteFromRelationshipIndex( "relationships", "bogus", "bogus", 999999 );
        assertEquals( Status.NOT_FOUND.getStatusCode(), response.getStatus() );
    }

    @Test
    public void shouldGet404WhenTraversingFromNonExistentNode()
    {
        Response response = service.traverse( 9999999, TraverserReturnType.node, "{}" );
        assertEquals( Status.NOT_FOUND.getStatusCode(), response.getStatus() );
    }

    @Test
    public void shouldGet200WhenNoHitsReturnedFromTraverse()
    {
        long startNode = helper.createNode();

        try ( Transaction transaction = graph.beginTx() )
        {
            Response response = service.traverse( startNode, TraverserReturnType.node, "" );
            assertEquals( Status.OK.getStatusCode(), response.getStatus() );
            assertThat( output.getResultAsList().size(), is( 0 ) );
        }
    }

    @Test
    public void shouldGetSomeHitsWhenTraversingWithDefaultDescription()
    {
        long startNode = helper.createNode();
        long child1_l1 = helper.createNode();
        helper.createRelationship( "knows", startNode, child1_l1 );
        long child2_l1 = helper.createNode();
        helper.createRelationship( "knows", startNode, child2_l1 );
        long child1_l2 = helper.createNode();
        helper.createRelationship( "knows", child2_l1, child1_l2 );
        Response response = service.traverse( startNode, TraverserReturnType.node, "" );
        String entity = entityAsString( response );
        assertTrue( entity.contains( "/node/" + child1_l1 ) );
        assertTrue( entity.contains( "/node/" + child2_l1 ) );
        assertFalse( entity.contains( "/node/" + child1_l2 ) );

        checkContentTypeCharsetUtf8(response);
    }

    @Test
    public void shouldBeAbleToDescribeTraverser()
    {
        long startNode = helper.createNode( MapUtil.map( "name", "Mattias" ) );
        long node1 = helper.createNode( MapUtil.map( "name", "Emil" ) );
        long node2 = helper.createNode( MapUtil.map( "name", "Johan" ) );
        long node3 = helper.createNode( MapUtil.map( "name", "Tobias" ) );
        helper.createRelationship( "knows", startNode, node1 );
        helper.createRelationship( "knows", startNode, node2 );
        helper.createRelationship( "knows", node1, node3 );
        String description = "{"
                + "\"prune_evaluator\":{\"language\":\"builtin\",\"name\":\"none\"},"
                + "\"return_filter\":{\"language\":\"javascript\",\"body\":\"position.endNode().getProperty('name')" +
                ".toLowerCase().contains('t');\"},"
                + "\"order\":\"depth_first\","
                + "\"relationships\":{\"type\":\"knows\",\"direction\":\"all\"}" + "}";
        Response response = service.traverse( startNode, TraverserReturnType.node, description );
        assertEquals( Status.OK.getStatusCode(), response.getStatus() );
        String entity = entityAsString( response );
        assertTrue( entity.contains( NODE_SUBPATH + startNode ) );
        assertFalse( entity.contains( NODE_SUBPATH + node1 ) );
        assertFalse( entity.contains( NODE_SUBPATH + node2 ) );
        assertTrue( entity.contains( NODE_SUBPATH + node3 ) );
    }

    @Test
    public void shouldBeAbleToGetOtherResultTypesWhenTraversing()
    {
        long startNode = helper.createNode( MapUtil.map( "name", "Mattias" ) );
        long node1 = helper.createNode( MapUtil.map( "name", "Emil" ) );
        long node2 = helper.createNode( MapUtil.map( "name", "Johan" ) );
        long node3 = helper.createNode( MapUtil.map( "name", "Tobias" ) );
        long rel1 = helper.createRelationship( "knows", startNode, node1 );
        long rel2 = helper.createRelationship( "knows", startNode, node2 );
        long rel3 = helper.createRelationship( "knows", node1, node3 );

        Response response = service.traverse( startNode, TraverserReturnType.relationship, "" );
        assertEquals( Status.OK.getStatusCode(), response.getStatus() );
        String entity = entityAsString( response );
        assertTrue( entity.contains( "/relationship/" + rel1 ) );
        assertTrue( entity.contains( "/relationship/" + rel2 ) );
        assertFalse( entity.contains( "/relationship/" + rel3 ) );

        response = service.traverse( startNode, TraverserReturnType.path, "" );
        assertEquals( Status.OK.getStatusCode(), response.getStatus() );
        entity = entityAsString( response );
        assertTrue( entity.contains( "nodes" ) );
        assertTrue( entity.contains( "relationships" ) );
        assertTrue( entity.contains( "length" ) );

        response = service.traverse( startNode, TraverserReturnType.fullpath, "" );
        assertEquals( Status.OK.getStatusCode(), response.getStatus() );
        entity = entityAsString( response );
        assertTrue( entity.contains( "nodes" ) );
        assertTrue( entity.contains( "data" ) );
        assertTrue( entity.contains( "type" ) );
        assertTrue( entity.contains( "self" ) );
        assertTrue( entity.contains( "outgoing_relationships" ) );
        assertTrue( entity.contains( "incoming_relationships" ) );
        assertTrue( entity.contains( "relationships" ) );
        assertTrue( entity.contains( "length" ) );
    }

    private static String markWithUnicodeMarker( String string )
    {
        return String.valueOf( (char) 0xfeff ) + string;
    }

    @Test
    public void shouldBeAbleToFindSinglePathBetweenTwoNodes() throws Exception
    {
        long n1 = helper.createNode();
        long n2 = helper.createNode();
        helper.createRelationship( "knows", n1, n2 );
        Map<String, Object> config = MapUtil.map( "max depth", 3, "algorithm", "shortestPath", "to",
                Long.toString( n2 ), "relationships", MapUtil.map( "type", "knows", "direction", "out" ) );
        String payload = JsonHelper.createJsonFrom( config );

        Response response = service.singlePath( n1, payload );

        assertThat( response.getStatus(), is( 200 ) );
        try ( Transaction transaction = graph.beginTx() )
        {
            Map<String, Object> resultAsMap = output.getResultAsMap();
            assertThat( (Integer) resultAsMap.get( "length" ), is( 1 ) );
        }
    }

    @Test
    public void shouldBeAbleToFindSinglePathBetweenTwoNodesEvenWhenAskingForAllPaths() throws Exception
    {
        long n1 = helper.createNode();
        long n2 = helper.createNode();
        helper.createRelationship( "knows", n1, n2 );
        Map<String, Object> config = MapUtil.map( "max depth", 3, "algorithm", "shortestPath", "to",
                Long.toString( n2 ), "relationships", MapUtil.map( "type", "knows", "direction", "out" ) );
        String payload = JsonHelper.createJsonFrom( config );

        Response response = service.allPaths( n1, payload );

        assertThat( response.getStatus(), is( 200 ) );
        try ( Transaction transaction = graph.beginTx() )
        {
            List<Object> resultAsList = output.getResultAsList();
            assertThat( resultAsList.size(), is( 1 ) );
        }
    }

    @Test
    public void shouldBeAbleToParseJsonEvenWithUnicodeMarkerAtTheStart() throws Exception
    {
        Response response = service.createNode( markWithUnicodeMarker( "{\"name\":\"Mattias\"}" ) );
        assertEquals( Status.CREATED.getStatusCode(), response.getStatus() );
        String nodeLocation = response.getMetadata()
                .getFirst( HttpHeaders.LOCATION )
                .toString();

        long node = helper.createNode();
        assertEquals( Status.NO_CONTENT.getStatusCode(),
                service.setNodeProperty( node, "foo", markWithUnicodeMarker( "\"bar\"" ) )
                        .getStatus() );
        assertEquals( Status.NO_CONTENT.getStatusCode(),
                service.setNodeProperty( node, "foo", markWithUnicodeMarker( "" + 10 ) )
                        .getStatus() );
        assertEquals( Status.NO_CONTENT.getStatusCode(),
                service.setAllNodeProperties( node, markWithUnicodeMarker( "{\"name\":\"Something\"," +
                        "\"number\":10}" ) )
                        .getStatus() );

        assertEquals(
                Status.CREATED.getStatusCode(),
                service.createRelationship( node,
                        markWithUnicodeMarker( "{\"to\":\"" + nodeLocation + "\",\"type\":\"knows\"}" ) )
                        .getStatus() );

        long relationship = helper.createRelationship( "knows" );
        assertEquals( Status.NO_CONTENT.getStatusCode(),
                service.setRelationshipProperty( relationship, "foo", markWithUnicodeMarker( "\"bar\"" ) )
                        .getStatus() );
        assertEquals( Status.NO_CONTENT.getStatusCode(),
                service.setRelationshipProperty( relationship, "foo", markWithUnicodeMarker( "" + 10 ) )
                        .getStatus() );
        assertEquals(
                Status.NO_CONTENT.getStatusCode(),
                service.setAllRelationshipProperties( relationship,
                        markWithUnicodeMarker( "{\"name\":\"Something\",\"number\":10}" ) )
                        .getStatus() );

        assertEquals(
                Status.CREATED.getStatusCode(),
                service.addToNodeIndex( "node", null, null,
                        markWithUnicodeMarker( "{\"key\":\"foo\", \"value\":\"bar\", \"uri\": \"" + nodeLocation
                                + "\"}" ) )
                        .getStatus() );

        assertEquals( Status.OK.getStatusCode(),
                service.traverse( node, TraverserReturnType.node, markWithUnicodeMarker( "{\"max depth\":2}" ) )
                        .getStatus() );
    }

    @Test
    public void shouldAdvertiseUriForQueringAllRelationsInTheDatabase()
    {
        Response response = service.getRoot();
        assertThat( new String( (byte[]) response.getEntity() ),
                containsString( "\"relationship_types\" : \"http://neo4j.org/relationship/types\"" ) );
    }

    @Test
    public void nodeAutoIndexerEnabling()
    {
        testAutoIndexEnableForType( "node" );
    }

    @Test
    public void relationshipAutoIndexerEnabling()
    {
        testAutoIndexEnableForType( "relationship" );
    }

    @Test
    public void addRemoveAutoindexPropertiesOnNodes() throws JsonParseException
    {
        addRemoveAutoindexProperties( "node" );
    }

    @Test
    public void addRemoveAutoindexPropertiesOnRelationships() throws JsonParseException
    {
        addRemoveAutoindexProperties( "relationship" );
    }

    @Test
    public void nodeAutoindexingSupposedToWork() throws JsonParseException
    {
        String type = "node";
        Response response = service.startAutoIndexingProperty( type, "myAutoIndexedProperty" );
        assertEquals( 204, response.getStatus() );

        response = service.setAutoIndexerEnabled( type, "true" );
        assertEquals( 204, response.getStatus() );

        service.createNode( "{\"myAutoIndexedProperty\" : \"value\"}" );

        try ( Transaction transaction = graph.beginTx() )
        {
            IndexHits<Node> indexResult = database.getGraph().index().getNodeAutoIndexer().getAutoIndex().get(
                    "myAutoIndexedProperty", "value" );
            assertEquals( 1, indexResult.size() );
        }
    }

    @Test
    public void shouldReturnAllLabelsPresentInTheDatabase() throws JsonParseException
    {
        // given
        helper.createNode( DynamicLabel.label( "ALIVE" ) );
        long nodeId = helper.createNode( DynamicLabel.label( "DEAD" ) );
        helper.deleteNode( nodeId );

        // when
        Response response = service.getAllLabels( false );

        // then
        assertEquals( 200, response.getStatus() );

        List<String> labels = entityAsList( response );
        assertThat( labels, hasItem( "DEAD" ) );
    }

    @Test
    public void shouldReturnAllLabelsInUseInTheDatabase() throws JsonParseException
    {
        // given
        helper.createNode( DynamicLabel.label( "ALIVE" ) );
        long nodeId = helper.createNode( DynamicLabel.label( "DEAD" ) );
        helper.deleteNode( nodeId );

        // when
        Response response = service.getAllLabels( true );

        // then
        assertEquals( 200, response.getStatus() );

        List<String> labels = entityAsList( response );
        assertThat( labels, not( hasItem( "DEAD" ) ) );
    }

    @SuppressWarnings("unchecked")
    private void addRemoveAutoindexProperties( String type ) throws JsonParseException
    {
        Response response = service.getAutoIndexedProperties( type );
        assertEquals( 200, response.getStatus() );
        String entity = entityAsString( response );
        List<String> properties = (List<String>) JsonHelper.readJson( entity );
        assertEquals( 0, properties.size() );

        response = service.startAutoIndexingProperty( type, "myAutoIndexedProperty1" );
        assertEquals( 204, response.getStatus() );

        response = service.startAutoIndexingProperty( type, "myAutoIndexedProperty2" );
        assertEquals( 204, response.getStatus() );


        response = service.getAutoIndexedProperties( type );
        assertEquals( 200, response.getStatus() );
        entity = entityAsString( response );
        properties = (List<String>) JsonHelper.readJson( entity );
        assertEquals( 2, properties.size() );
        assertTrue( properties.contains( "myAutoIndexedProperty1" ) );
        assertTrue( properties.contains( "myAutoIndexedProperty2" ) );

        response = service.stopAutoIndexingProperty( type, "myAutoIndexedProperty2" );
        assertEquals( 204, response.getStatus() );

        response = service.getAutoIndexedProperties( type );
        assertEquals( 200, response.getStatus() );
        entity = entityAsString( response );
        properties = (List<String>) JsonHelper.readJson( entity );
        assertEquals( 1, properties.size() );
        assertTrue( properties.contains( "myAutoIndexedProperty1" ) );
    }
    private void testAutoIndexEnableForType( String type )
    {
        Response response = service.isAutoIndexerEnabled( type );
        assertEquals( 200, response.getStatus() );
        assertFalse( Boolean.parseBoolean( entityAsString( response ) ) );

        response = service.setAutoIndexerEnabled( type, "true" );
        assertEquals( 204, response.getStatus() );

        response = service.isAutoIndexerEnabled( type );
        assertEquals( 200, response.getStatus() );
        assertTrue( Boolean.parseBoolean( entityAsString( response ) ) );

        response = service.setAutoIndexerEnabled( type, "false" );
        assertEquals( 204, response.getStatus() );

        response = service.isAutoIndexerEnabled( type );
        assertEquals( 200, response.getStatus() );
        assertFalse( Boolean.parseBoolean( entityAsString( response ) ) );
    }

    @SuppressWarnings("unchecked")
    private String singleErrorCode( Response response ) throws JsonParseException
    {
        String json = entityAsString( response );
        Map<String, Object> map = JsonHelper.jsonToMap( json );
        List<Object> errors = (List<Object>) map.get( "errors" );
        assertEquals( 1, errors.size() );
        Map<String, String> error = (Map<String, String>) errors.get( 0 );
        return error.get( "code" );
    }
}
