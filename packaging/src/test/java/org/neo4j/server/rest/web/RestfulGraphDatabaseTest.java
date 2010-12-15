/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.server.rest.web;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.core.Response.Status;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.server.ServerTestUtils;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.DatabaseBlockedException;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.RelationshipRepresentationTest;
import org.neo4j.server.rest.domain.StorageActions.TraverserReturnType;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.formats.JsonFormat;
import org.neo4j.server.rest.web.DatabaseActions.RelationshipDirection;
import org.neo4j.server.rest.web.RestfulGraphDatabase.AmpersandSeparatedCollection;

@Ignore( "not enabled yet" )
public class RestfulGraphDatabaseTest {
    private static final String BASE_URI = "http://neo4j.org/";
    private RestfulGraphDatabase service;
    private Database database;
    private GraphDbHelper helper;

    @Before
    public void doBefore() throws IOException {
        database = new Database(ServerTestUtils.createTempDir().getAbsolutePath());
        helper = new GraphDbHelper(database);
        service = new RestfulGraphDatabase( uriInfo(), database, new JsonFormat(),
                new OutputFormat( new JsonFormat(), URI.create( BASE_URI ), null ) );
    }

    @After
    public void shutdownDatabase()
    {
        this.database.shutdown();
    }

    private UriInfo uriInfo() {
        UriInfo mockUriInfo = mock(UriInfo.class);
        try {
            when(mockUriInfo.getBaseUri()).thenReturn(new URI(BASE_URI));
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        return mockUriInfo;
    }

    private static String entityAsString(Response response) {
        byte[] bytes = (byte[]) response.getEntity();
        try {
            return new String(bytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Could not decode UTF-8", e);
        }
    }

    @Test
    public void shouldRespondWith201LocationHeaderAndNodeRepresentationInJSONWhenEmptyNodeCreated() throws Exception {
        Response response = service.createNode( null );

        assertEquals(201, response.getStatus());
        assertNotNull(response.getMetadata().get("Location").get(0));
        assertEquals(response.getMetadata().getFirst(HttpHeaders.CONTENT_ENCODING), "UTF-8");
        String json = entityAsString(response);

        Map<String, Object> map = JsonHelper.jsonToMap(json);

        assertNotNull(map);

        assertTrue(map.containsKey("self"));
    }

    @Test
    public void shouldRespondWith201LocationHeaderAndNodeRepresentationInJSONWhenPopulatedNodeCreated() throws Exception {
        Response response = service.createNode( "{\"foo\" : \"bar\"}" );

        assertEquals(201, response.getStatus());
        assertNotNull(response.getMetadata().get("Location").get(0));
        assertEquals(response.getMetadata().getFirst(HttpHeaders.CONTENT_ENCODING), "UTF-8");
        String json = entityAsString(response);

        Map<String, Object> map = JsonHelper.jsonToMap(json);

        assertNotNull(map);

        assertTrue(map.containsKey("self"));

        @SuppressWarnings("unchecked")
        Map<String, Object> data = (Map<String, Object>) map.get("data");

        assertEquals("bar", data.get("foo"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldRespondWith201LocationHeaderAndNodeRepresentationInJSONWhenPopulatedNodeCreatedWithArrays() throws Exception {
        Response response = service.createNode( "{\"foo\" : [\"bar\", \"baz\"] }" );

        assertEquals(201, response.getStatus());
        assertNotNull(response.getMetadata().get("Location").get(0));
        String json = entityAsString(response);

        Map<String, Object> map = JsonHelper.jsonToMap(json);

        assertNotNull(map);

        Map<String, Object> data = (Map<String, Object>) map.get("data");

        List<String> foo = (List<String>) data.get("foo");
        assertNotNull(foo);
        assertEquals(2, foo.size());
    }

    @Test
    public void shouldRespondWith400WhenNodeCreatedWithUnsupportedPropertyData() throws DatabaseBlockedException {
        Response response = service.createNode( "{\"foo\" : {\"bar\" : \"baz\"}}" );

        assertEquals(400, response.getStatus());
    }

    @Test
    public void shouldRespondWith400WhenNodeCreatedWithInvalidJSON() throws DatabaseBlockedException {
        Response response = service.createNode( "this:::isNot::JSON}" );

        assertEquals(400, response.getStatus());
    }

    @Test
    public void shouldRespondWith200AndNodeRepresentationInJSONWhenNodeRequested() throws Exception {
        Response response = service.getNode( helper.createNode() );
        assertEquals(200, response.getStatus());
        String json = entityAsString(response);
        Map<String, Object> map = JsonHelper.jsonToMap(json);
        assertNotNull(map);
        assertTrue(map.containsKey("self"));
    }

    @Test
    public void shouldRespondWith404WhenRequestedNodeDoesNotExist() throws Exception {
        Response response = service.getNode( 9000000000000L );
        assertEquals(404, response.getStatus());
    }

    @Test
    public void shouldRespondWith204AfterSettingPropertiesOnExistingNode() throws Exception {
        Response response = service.setAllNodeProperties( helper.createNode(),
                "{\"foo\" : \"bar\", \"a-boolean\": true, \"boolean-array\": [true, false, false]}");
        assertEquals(204, response.getStatus());
    }

    @Test
    public void shouldRespondWith404WhenSettingPropertiesOnNodeThatDoesNotExist() throws Exception {
        Response response = service.setAllNodeProperties( 9000000000000L, "{\"foo\" : \"bar\"}" );
        assertEquals(404, response.getStatus());
    }

    @Test
    public void shouldRespondWith400WhenTransferringCorruptJsonPayload() throws Exception {
        Response response = service.setAllNodeProperties( helper.createNode(),
                "{\"foo\" : bad-json-here \"bar\"}" );
        assertEquals(400, response.getStatus());
    }

    @Test
    public void shouldRespondWith400WhenTransferringIncompatibleJsonPayload() throws Exception {
        Response response = service.setAllNodeProperties( helper.createNode(),
                "{\"foo\" : {\"bar\" : \"baz\"}}" );
        assertEquals(400, response.getStatus());
    }

    @Test
    public void shouldRespondWith200ForGetNodeProperties() throws Exception {
        long nodeId = helper.createNode();
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("foo", "bar");
        helper.setNodeProperties(nodeId, properties);
        Response response = service.getAllNodeProperties( nodeId );
        assertEquals(200, response.getStatus());
        assertEquals(response.getMetadata().getFirst(HttpHeaders.CONTENT_ENCODING), "UTF-8");
    }

    @Test
    public void shouldRespondWith204ForGetNoNodeProperties() throws Exception {
        long nodeId = helper.createNode();
        Response response = service.getAllNodeProperties( nodeId );
        assertEquals(204, response.getStatus());
    }

    @Test
    public void shouldGetPropertiesForGetNodeProperties() throws Exception {
        long nodeId = helper.createNode();
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("foo", "bar");
        properties.put("number", 15);
        properties.put("double", 15.7);
        helper.setNodeProperties(nodeId, properties);
        Response response = service.getAllNodeProperties( nodeId );
        String jsonBody = entityAsString(response);
        Map<String, Object> readProperties = JsonHelper.jsonToMap(jsonBody);
        assertEquals(properties, readProperties);
    }

    @Test
    public void shouldRespondWith204OnSuccessfulDelete() throws DatabaseBlockedException {
        long id = helper.createNode();

        Response response = service.deleteNode( id );

        assertEquals(204, response.getStatus());
    }

    @Test
    public void shouldRespondWith409IfNodeCannotBeDeleted() throws DatabaseBlockedException {
        long id = helper.createNode();
        helper.createRelationship("LOVES", id, helper.createNode());

        Response response = service.deleteNode( id );

        assertEquals(409, response.getStatus());
    }

    @Test
    public void shouldRespondWith404IfNodeToBeDeletedDoesNotExist() throws DatabaseBlockedException {
        long nonExistentId = 999999;
        Response response = service.deleteNode( nonExistentId );

        assertEquals(404, response.getStatus());
    }

    @Test
    public void shouldRespondWith204ForSetNodeProperty() throws DatabaseBlockedException {
        long nodeId = helper.createNode();
        String key = "foo";
        String json = "\"bar\"";
        Response response = service.setNodeProperty( nodeId, key, json );
        assertEquals(204, response.getStatus());
    }

    @Test
    public void shouldSetRightValueForSetNodeProperty() throws DatabaseBlockedException {
        long nodeId = helper.createNode();
        String key = "foo";
        String value = "bar";
        String json = "\"" + value + "\"";
        service.setNodeProperty( nodeId, key, json );
        Map<String, Object> readProperties = helper.getNodeProperties(nodeId);
        assertEquals(Collections.singletonMap(key, value), readProperties);
    }

    @Test
    public void shouldRespondWith404ForSetNodePropertyOnNonExistingNode() throws DatabaseBlockedException {
        String key = "foo";
        String json = "\"bar\"";
        Response response = service.setNodeProperty( 999999, key, json );
        assertEquals(404, response.getStatus());
    }

    @Test
    public void shouldRespondWith400ForSetNodePropertyWithInvalidJson() throws DatabaseBlockedException {
        String key = "foo";
        String json = "{invalid json";
        Response response = service.setNodeProperty( 999999, key, json );
        assertEquals(400, response.getStatus());
    }

    @Test
    public void shouldRespondWith404ForGetNonExistentNodeProperty() throws DatabaseBlockedException {
        long nodeId = helper.createNode();
        Response response = service.getNodeProperty( nodeId, "foo" );
        assertEquals(404, response.getStatus());
    }

    @Test
    public void shouldRespondWith404ForGetNodePropertyOnNonExistentNode() throws DatabaseBlockedException {
        long nodeId = 999999;
        Response response = service.getNodeProperty( nodeId, "foo" );
        assertEquals(404, response.getStatus());
    }

    @Test
    public void shouldRespondWith200ForGetNodeProperty() throws DatabaseBlockedException {
        long nodeId = helper.createNode();
        String key = "foo";
        Object value = "bar";
        helper.setNodeProperties(nodeId, Collections.singletonMap(key, value));
        Response response = service.getNodeProperty( nodeId, "foo" );
        assertEquals(200, response.getStatus());
        assertEquals(response.getMetadata().getFirst(HttpHeaders.CONTENT_ENCODING), "UTF-8");
    }

    @Test
    public void shouldReturnCorrectValueForGetNodeProperty() throws Exception {
        long nodeId = helper.createNode();
        String key = "foo";
        Object value = "bar";
        helper.setNodeProperties(nodeId, Collections.singletonMap(key, value));
        Response response = service.getNodeProperty( nodeId, "foo" );
        assertEquals(JsonHelper.createJsonFrom(value), entityAsString(response));
    }

    @Test
    public void shouldRespondWith201AndLocationWhenRelationshipIsCreatedWithoutProperties() throws DatabaseBlockedException {
        long startNode = helper.createNode();
        long endNode = helper.createNode();
        Response response = service.createRelationship( startNode, "{\"to\" : \"" + BASE_URI
                                                                   + endNode
                + "\", \"type\" : \"LOVES\"}");
        assertEquals(201, response.getStatus());
        assertNotNull(response.getMetadata().get("Location").get(0));
    }

    @Test
    public void shouldRespondWith201AndLocationWhenRelationshipIsCreatedWithProperties() throws DatabaseBlockedException {
        long startNode = helper.createNode();
        long endNode = helper.createNode();
        Response response = service.createRelationship( startNode,
                "{\"to\" : \"" + BASE_URI + endNode
                + "\", \"type\" : \"LOVES\", \"properties\" : {\"foo\" : \"bar\"}}");
        assertEquals(201, response.getStatus());
        assertNotNull(response.getMetadata().get("Location").get(0));
    }

    @Test
    public void shouldReturnRelationshipRepresentationWhenCreatingRelationship() throws Exception {
        long startNode = helper.createNode();
        long endNode = helper.createNode();
        Response response = service.createRelationship( startNode,
                "{\"to\" : \"" + BASE_URI + endNode
                + "\", \"type\" : \"LOVES\", \"data\" : {\"foo\" : \"bar\"}}");
        Map<String, Object> map = JsonHelper.jsonToMap(entityAsString(response));
        assertNotNull(map);
        assertTrue(map.containsKey("self"));
        assertEquals(response.getMetadata().getFirst(HttpHeaders.CONTENT_ENCODING), "UTF-8");

        @SuppressWarnings("unchecked")
        Map<String, Object> data = (Map<String, Object>) map.get("data");

        assertEquals("bar", data.get("foo"));
    }

    @Test
    public void shouldRespondWith404WhenTryingToCreateRelationshipFromNonExistentNode() throws DatabaseBlockedException {
        long nodeId = helper.createNode();
        Response response = service.createRelationship( nodeId * 1000,
                "{\"to\" : \"" + BASE_URI + nodeId
                + "\", \"type\" : \"LOVES\"}");
        assertEquals(404, response.getStatus());
    }

    @Test
    public void shouldRespondWith400WhenTryingToCreateRelationshipToNonExistentNode() throws DatabaseBlockedException {
        long nodeId = helper.createNode();
        Response response = service.createRelationship( nodeId, "{\"to\" : \"" + BASE_URI
                                                                + ( nodeId * 1000 )
                + "\", \"type\" : \"LOVES\"}");
        assertEquals(400, response.getStatus());
    }

    @Test
    public void shouldRespondWith400WhenTryingToCreateRelationshipToStartNode() throws DatabaseBlockedException {
        long nodeId = helper.createNode();
        Response response = service.createRelationship( nodeId, "{\"to\" : \"" + BASE_URI + nodeId
                + "\", \"type\" : \"LOVES\"}");
        assertEquals(400, response.getStatus());
    }

    @Test
    public void shouldRespondWith400WhenTryingToCreateRelationshipWithBadJson() throws DatabaseBlockedException {
        long startNode = helper.createNode();
        long endNode = helper.createNode();
        Response response = service.createRelationship( startNode,
                "{\"to\" : \"" + BASE_URI + endNode
                + "\", \"type\" ***and junk*** : \"LOVES\"}");
        assertEquals(400, response.getStatus());
    }

    @Test
    public void shouldRespondWith400WhenTryingToCreateRelationshipWithUnsupportedProperties() throws DatabaseBlockedException {
        long startNode = helper.createNode();
        long endNode = helper.createNode();
        Response response = service.createRelationship( startNode,
                "{\"to\" : \"" + BASE_URI + endNode
                + "\", \"type\" : \"LOVES\", \"data\" : {\"foo\" : {\"bar\" : \"baz\"}}}");
        assertEquals(400, response.getStatus());
    }

    @Test
    public void shouldRespondWith204ForRemoveNodeProperties() throws DatabaseBlockedException {
        long nodeId = helper.createNode();
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("foo", "bar");
        properties.put("number", 15);
        helper.setNodeProperties(nodeId, properties);
        Response response = service.deleteAllNodeProperties( nodeId );
        assertEquals(204, response.getStatus());
    }

    @Test
    public void shouldBeAbleToRemoveNodeProperties() throws DatabaseBlockedException {
        long nodeId = helper.createNode();
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("foo", "bar");
        properties.put("number", 15);
        helper.setNodeProperties(nodeId, properties);
        service.deleteAllNodeProperties( nodeId );
        assertEquals(true, helper.getNodeProperties(nodeId).isEmpty());
    }

    @Test
    public void shouldRespondWith404ForRemoveNodePropertiesForNonExistingNode() throws DatabaseBlockedException {
        long nodeId = 999999;
        Response response = service.deleteAllNodeProperties( nodeId );
        assertEquals(404, response.getStatus());
    }

    @Test
    public void shouldBeAbleToRemoveNodeProperty() throws DatabaseBlockedException {
        long nodeId = helper.createNode();
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("foo", "bar");
        properties.put("number", 15);
        helper.setNodeProperties(nodeId, properties);
        service.deleteNodeProperty( nodeId, "foo" );
        assertEquals(Collections.singletonMap("number", (Object) new Integer(15)), helper.getNodeProperties(nodeId));
    }

    @Test
    public void shouldGet404WhenRemovingNonExistingProperty() throws DatabaseBlockedException {
        long nodeId = helper.createNode();
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("foo", "bar");
        properties.put("number", 15);
        helper.setNodeProperties(nodeId, properties);
        Response response = service.deleteNodeProperty( nodeId, "baz" );
        assertEquals(404, response.getStatus());
    }

    @Test
    public void shouldGet404WhenRemovingPropertyFromNonExistingNode() throws DatabaseBlockedException {
        long nodeId = 999999;
        Response response = service.deleteNodeProperty( nodeId, "foo" );
        assertEquals(404, response.getStatus());
    }

    @Test
    public void shouldGet200WhenRetrievingARelationshipFromANode() throws DatabaseBlockedException {
        long relationshipId = helper.createRelationship("BEATS");
        Response response = service.getRelationship( relationshipId );
        assertEquals(200, response.getStatus());
        assertEquals(response.getMetadata().getFirst(HttpHeaders.CONTENT_ENCODING), "UTF-8");
    }

    @Test
    public void shouldGet404WhenRetrievingRelationshipThatDoesNotExist() throws DatabaseBlockedException {
        Response response = service.getRelationship( 999999 );
        assertEquals(404, response.getStatus());
    }

    @Test
    public void shouldRespondWith200AndDataForGetRelationshipProperties() throws Exception {
        long relationshipId = helper.createRelationship("knows");
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("foo", "bar");
        helper.setRelationshipProperties(relationshipId, properties);
        Response response = service.getAllRelationshipProperties( relationshipId );
        assertEquals(200, response.getStatus());
        assertEquals(response.getMetadata().getFirst(HttpHeaders.CONTENT_ENCODING), "UTF-8");
        Map<String, Object> readProperties = JsonHelper.jsonToMap(entityAsString(response));
        assertEquals(properties, readProperties);
    }

    @Test
    public void shouldRespondWith204ForGetNoRelationshipProperties() throws DatabaseBlockedException {
        long relationshipId = helper.createRelationship("knows");
        Response response = service.getAllRelationshipProperties( relationshipId );
        assertEquals(204, response.getStatus());
    }

    @Test
    public void shouldGet200WhenSuccessfullyRetrievedPropertyOnRelationship() throws DatabaseBlockedException {

        long relationshipId = helper.createRelationship("knows");
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("some-key", "some-value");
        helper.setRelationshipProperties(relationshipId, properties);

        Response response = service.getRelationshipProperty( relationshipId, "some-key" );

        assertEquals(200, response.getStatus());
        assertEquals("some-value", JsonHelper.jsonToSingleValue(entityAsString(response)));
        assertEquals(response.getMetadata().getFirst(HttpHeaders.CONTENT_ENCODING), "UTF-8");
    }

    @Test
    public void shouldGet404WhenCannotResolveAPropertyOnRelationship() throws DatabaseBlockedException {
        long relationshipId = helper.createRelationship("knows");
        Response response = service.getRelationshipProperty( relationshipId, "some-key" );
        assertEquals(404, response.getStatus());
    }

    @Test
    public void shouldGet204WhenRemovingARelationship() throws DatabaseBlockedException {
        long relationshipId = helper.createRelationship("KNOWS");

        Response response = service.deleteRelationship( relationshipId );
        assertEquals(204, response.getStatus());
    }

    @Test
    public void shouldGet404WhenRemovingNonExistentRelationship() throws DatabaseBlockedException {
        long relationshipId = helper.createRelationship("KNOWS");

        Response response = service.deleteRelationship( relationshipId + 1 * 1000 );
        assertEquals(404, response.getStatus());
    }

    @Test
    public void shouldRespondWith200AndListOfRelationshipRepresentationsWhenGettingRelationshipsForANode() throws DatabaseBlockedException {
        long nodeId = helper.createNode();
        helper.createRelationship("LIKES", nodeId, helper.createNode());
        helper.createRelationship("LIKES", helper.createNode(), nodeId);
        helper.createRelationship("HATES", nodeId, helper.createNode());

        Response response = service.getNodeRelationships( nodeId, RelationshipDirection.all,
                new AmpersandSeparatedCollection( "" ) );
        assertEquals(200, response.getStatus());
        assertEquals(response.getMetadata().getFirst(HttpHeaders.CONTENT_ENCODING), "UTF-8");
        verifyRelReps(3, entityAsString(response));

        response = service.getNodeRelationships( nodeId, RelationshipDirection.in,
                new AmpersandSeparatedCollection( "" ) );
        assertEquals(200, response.getStatus());
        verifyRelReps(1, entityAsString(response));

        response = service.getNodeRelationships( nodeId, RelationshipDirection.out,
                new AmpersandSeparatedCollection( "" ) );
        assertEquals(200, response.getStatus());
        verifyRelReps(2, entityAsString(response));

        response = service.getNodeRelationships( nodeId, RelationshipDirection.out,
                new AmpersandSeparatedCollection( "LIKES&HATES" ) );
        assertEquals(200, response.getStatus());
        verifyRelReps(2, entityAsString(response));

        response = service.getNodeRelationships( nodeId, RelationshipDirection.all,
                new AmpersandSeparatedCollection( "LIKES" ) );
        assertEquals(200, response.getStatus());
        verifyRelReps(2, entityAsString(response));
    }

    @Test
    public void shouldNotReturnDuplicatesIfSameTypeSpecifiedMoreThanOnce() throws DatabaseBlockedException {
        long nodeId = helper.createNode();
        helper.createRelationship("LIKES", nodeId, helper.createNode());
        Response response = service.getNodeRelationships( nodeId, RelationshipDirection.all,
                new AmpersandSeparatedCollection( "LIKES&LIKES" ) );
        Collection<?> array = (Collection<?>) JsonHelper.jsonToSingleValue(entityAsString(response));
        assertEquals(1, array.size());
    }

    private void verifyRelReps(int expectedSize, String entity) {
        List<Map<String, Object>> relreps = JsonHelper.jsonToListOfRelationshipRepresentations(entity);
        assertEquals(expectedSize, relreps.size());
        for (Map<String, Object> relrep : relreps) {
            RelationshipRepresentationTest.verifySerialisation(relrep);
        }
    }

    @Test
    public void shouldRespondWith200AndEmptyListOfRelationshipRepresentationsWhenGettingRelationshipsForANodeWithoutRelationships()
            throws DatabaseBlockedException {
        long nodeId = helper.createNode();

        Response response = service.getNodeRelationships( nodeId, RelationshipDirection.all,
                new AmpersandSeparatedCollection( "" ) );
        assertEquals(200, response.getStatus());
        verifyRelReps(0, entityAsString(response));
        assertEquals(response.getMetadata().getFirst(HttpHeaders.CONTENT_ENCODING), "UTF-8");
    }

    @Test
    public void shouldRespondWith404WhenGettingIncomingRelationshipsForNonExistingNode() throws DatabaseBlockedException {
        Response response = service.getNodeRelationships( 999999, RelationshipDirection.all,
                new AmpersandSeparatedCollection( "" ) );
        assertEquals(404, response.getStatus());
    }

    @Test
    public void shouldRespondWith204AndSetCorrectDataWhenSettingRelationshipProperties() throws DatabaseBlockedException {
        long relationshipId = helper.createRelationship("KNOWS");
        String json = "{\"name\": \"Mattias\", \"age\": 30}";
        Response response = service.setAllRelationshipProperties( relationshipId, json );
        assertEquals(204, response.getStatus());
        Map<String, Object> setProperties = new HashMap<String, Object>();
        setProperties.put("name", "Mattias");
        setProperties.put("age", 30);
        assertEquals(setProperties, helper.getRelationshipProperties(relationshipId));
    }

    @Test
    public void shouldRespondWith400WhenSettingRelationshipPropertiesWithBadJson() throws DatabaseBlockedException {
        long relationshipId = helper.createRelationship("KNOWS");
        String json = "{\"name: \"Mattias\", \"age\": 30}";
        Response response = service.setAllRelationshipProperties( relationshipId, json );
        assertEquals(400, response.getStatus());
    }

    @Test
    public void shouldRespondWith404WhenSettingRelationshipPropertiesOnNonExistingRelationship() throws DatabaseBlockedException {
        long relationshipId = 99999999;
        String json = "{\"name\": \"Mattias\", \"age\": 30}";
        Response response = service.setAllRelationshipProperties( relationshipId, json );
        assertEquals(404, response.getStatus());
    }

    @Test
    public void shouldRespondWith204AndSetCorrectDataWhenSettingRelationshipProperty() throws DatabaseBlockedException {
        long relationshipId = helper.createRelationship("KNOWS");
        String key = "name";
        Object value = "Mattias";
        String json = "\"" + value + "\"";
        Response response = service.setRelationshipProperty( relationshipId, key, json );
        assertEquals(204, response.getStatus());
        assertEquals(value, helper.getRelationshipProperties(relationshipId).get("name"));
    }

    @Test
    public void shouldRespondWith400WhenSettingRelationshipPropertyWithBadJson() throws DatabaseBlockedException {
        long relationshipId = helper.createRelationship("KNOWS");
        String json = "}Mattias";
        Response response = service.setRelationshipProperty( relationshipId, "name", json );
        assertEquals(400, response.getStatus());
    }

    @Test
    public void shouldRespondWith404WhenSettingRelationshipPropertyOnNonExistingRelationship() throws DatabaseBlockedException {
        long relationshipId = 99999999;
        String json = "\"Mattias\"";
        Response response = service.setRelationshipProperty( relationshipId, "name", json );
        assertEquals(404, response.getStatus());
    }

    @Test
    public void shouldRespondWith204WhenSuccessfullyRemovedRelationshipProperties() throws DatabaseBlockedException {
        long relationshipId = helper.createRelationship("KNOWS");
        helper.setRelationshipProperties(relationshipId, Collections.singletonMap("foo", (Object) "bar"));

        Response response = service.deleteAllRelationshipProperties( relationshipId );
        assertEquals(204, response.getStatus());
    }

    @Test
    public void shouldRespondWith204WhenSuccessfullyRemovedRelationshipPropertiesWhichAreEmpty() throws DatabaseBlockedException {
        long relationshipId = helper.createRelationship("KNOWS");

        Response response = service.deleteAllRelationshipProperties( relationshipId );
        assertEquals(204, response.getStatus());
    }

    @Test
    public void shouldRespondWith404WhenNoRelationshipFromWhichToRemoveProperties() throws DatabaseBlockedException {
        long relationshipId = helper.createRelationship("KNOWS");

        Response response = service.deleteAllRelationshipProperties( relationshipId + 1 * 1000 );
        assertEquals(404, response.getStatus());
    }

    @Test
    public void shouldRespondWith204WhenRemovingRelationshipProperty() throws DatabaseBlockedException {
        long relationshipId = helper.createRelationship("KNOWS");
        helper.setRelationshipProperties(relationshipId, Collections.singletonMap("foo", (Object) "bar"));

        Response response = service.deleteRelationshipProperty( relationshipId, "foo" );

        assertEquals(204, response.getStatus());
    }

    @Test
    public void shouldRespondWith404WhenRemovingRelationshipPropertyWhichDoesNotExist() throws DatabaseBlockedException {
        long relationshipId = helper.createRelationship("KNOWS");
        Response response = service.deleteRelationshipProperty( relationshipId, "foo" );
        assertEquals(404, response.getStatus());
    }

    @Test
    public void shouldRespondWith404WhenNoRelationshipFromWhichToRemoveProperty() throws DatabaseBlockedException {
        long relationshipId = helper.createRelationship("KNOWS");

        Response response = service.deleteRelationshipProperty( relationshipId * 1000, "some-key" );
        assertEquals(404, response.getStatus());
    }

    @Test
    public void shouldRespondWithAvailableIndexRoots() {
        Response response = service.getIndexRoot();
        assertEquals(200, response.getStatus());
        String entity = entityAsString(response);
        Map<String, Object> map = JsonHelper.jsonToMap(entity);
        assertNotNull(map.get("node"));
        assertFalse(((Collection<?>) map.get("node")).isEmpty());
        assertEquals(response.getMetadata().getFirst(HttpHeaders.CONTENT_ENCODING), "UTF-8");
    }

    @Test
    public void shouldBeAbleToGetRoot() {
        Response response = service.getRoot();
        assertEquals(200, response.getStatus());
        String entity = entityAsString(response);
        Map<String, Object> map = JsonHelper.jsonToMap(entity);
        assertNotNull(map.get("node"));
        assertNotNull(map.get("reference_node"));
        assertNotNull(map.get("index"));
        assertEquals(response.getMetadata().getFirst(HttpHeaders.CONTENT_ENCODING), "UTF-8");
    }

    @Test
    public void shouldBeAbleToIndexNode() throws DatabaseBlockedException {
        Response response = service.createNode( null );
        URI nodeUri = (URI) response.getMetadata().getFirst("Location");

        String key = "key";
        String value = "value";
        response = service.addToIndex( "node", key, value,
                JsonHelper.createJsonFrom( nodeUri.toString() ) );
        assertEquals(201, response.getStatus());
        assertNotNull(response.getMetadata().getFirst("Location"));
    }

    @Test
    public void shouldBeAbleToGetNodeRepresentationFromIndexUri() throws DatabaseBlockedException {
        String key = "key_get_noderep";
        String value = "value";
        long nodeId = helper.createNode();
        Response response = service.addToIndex( "node", key, value,
                JsonHelper.createJsonFrom( BASE_URI + "node/"
                + nodeId));
        response = service.getObjectFromIndexUri( "node", key, value, nodeId );
        assertEquals(200, response.getStatus());
        assertEquals(response.getMetadata().getFirst(HttpHeaders.CONTENT_ENCODING), "UTF-8");
        assertNull(response.getMetadata().get("Location"));
        Map<String, Object> map = JsonHelper.jsonToMap(entityAsString(response));
        assertNotNull(map);
        assertTrue(map.containsKey("self"));
    }

    @Test
    public void shouldBeAbleToGetListOfNodeRepresentationsFromIndexLookup() throws DatabaseBlockedException {
        String key = "key_get";
        String value = "value";

        String name1 = "Thomas Anderson";
        String name2 = "Agent Smith";
        URI location1 = (URI) service.createNode( "{\"name\":\"" + name1 + "\"}" ).getMetadata().getFirst(
                "Location" );
        URI location2 = (URI) service.createNode( "{\"name\":\"" + name2 + "\"}" ).getMetadata().getFirst(
                "Location" );
        URI indexLocation1 = (URI) service.addToIndex( "node", key, value,
                JsonHelper.createJsonFrom( location1.toString() ) ).getMetadata().getFirst(
                "Location");
        URI indexLocation2 = (URI) service.addToIndex( "node", key, value,
                JsonHelper.createJsonFrom( location2.toString() ) ).getMetadata().getFirst(
                "Location");
        Map<String, String> uriToName = new HashMap<String, String>();
        uriToName.put(indexLocation1.toString(), name1);
        uriToName.put(indexLocation2.toString(), name2);

        Response response = service.getIndexedObjects( "node", key, value );
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        Collection<?> items = (Collection<?>) JsonHelper.jsonToSingleValue(entityAsString(response));
        int counter = 0;
        for (Object item : items) {
            Map<?, ?> map = (Map<?, ?>) item;
            Map<?, ?> properties = (Map<?, ?>) map.get("data");
            assertNotNull(map.get("self"));
            String indexedUri = (String) map.get("indexed");
            assertEquals(uriToName.get(indexedUri), properties.get("name"));
            counter++;
        }
        assertEquals(2, counter);
    }

    @Test
    public void shouldGet200AndEmptyListWhenNothingFoundInIndexLookup() throws DatabaseBlockedException {
        Response response = service.getIndexedObjects( "node", "fooo", "baaar" );
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        assertEquals(response.getMetadata().getFirst(HttpHeaders.CONTENT_ENCODING), "UTF-8");
        String entity = entityAsString(response);
        Object parsedJson = JsonHelper.jsonToSingleValue(entity);
        assertTrue(parsedJson instanceof Collection<?>);
        assertTrue(((Collection<?>) parsedJson).isEmpty());
    }

    @Test
    public void shouldBeAbleToRemoveNodeFromIndex() throws DatabaseBlockedException {
        long nodeId = helper.createNode();
        String key = "key_remove";
        String value = "value";
        helper.addNodeToIndex("node", key, value, nodeId);
        assertEquals(1, helper.getIndexedNodes("node", key, value).size());
        Response response = service.deleteFromIndex( "node", key, value, nodeId );
        assertEquals(Status.NO_CONTENT.getStatusCode(), response.getStatus());
        assertEquals(0, helper.getIndexedNodes("node", key, value).size());
    }

    @Test
    public void shouldGet404IfRemovingNonExistentIndexing() throws DatabaseBlockedException {
        Response response = service.deleteFromIndex( "node", "bogus", "bogus", 999999 );
        assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
    }

    @Test
    public void shouldGet404WhentTraversingFromNonExistentNode() throws DatabaseBlockedException {
        Response response = service.traverse( 9999999, TraverserReturnType.node, "{}" );
        assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
    }

    @Test
    public void shouldGet200WhenNoHitsReturnedFromTraverse() throws DatabaseBlockedException {
        long startNode = helper.createNode();
        Response response = service.traverse( startNode, TraverserReturnType.node, "" );
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        Object parsedJson = JsonHelper.jsonToSingleValue(entityAsString(response));
        assertTrue(parsedJson instanceof Collection<?>);
        assertTrue(((Collection<?>) parsedJson).isEmpty());
        assertEquals(response.getMetadata().getFirst(HttpHeaders.CONTENT_ENCODING), "UTF-8");
    }

    @Test
    public void shouldGetSomeHitsWhenTraversingWithDefaultDescription() throws DatabaseBlockedException {
        long startNode = helper.createNode();
        long child1_l1 = helper.createNode();
        helper.createRelationship("knows", startNode, child1_l1);
        long child2_l1 = helper.createNode();
        helper.createRelationship("knows", startNode, child2_l1);
        long child1_l2 = helper.createNode();
        helper.createRelationship("knows", child2_l1, child1_l2);
        Response response = service.traverse( startNode, TraverserReturnType.node, "" );
        String entity = entityAsString(response);
        assertTrue(entity.contains("/node/" + child1_l1));
        assertTrue(entity.contains("/node/" + child2_l1));
        assertFalse(entity.contains("/node/" + child1_l2));
        assertEquals(response.getMetadata().getFirst(HttpHeaders.CONTENT_ENCODING), "UTF-8");
    }

    @Test
    public void shouldBeAbleToDescribeTraverser() throws DatabaseBlockedException {
        long startNode = helper.createNode(MapUtil.map("name", "Mattias"));
        long node1 = helper.createNode(MapUtil.map("name", "Emil"));
        long node2 = helper.createNode(MapUtil.map("name", "Johan"));
        long node3 = helper.createNode(MapUtil.map("name", "Tobias"));
        helper.createRelationship("knows", startNode, node1);
        helper.createRelationship("knows", startNode, node2);
        helper.createRelationship("knows", node1, node3);
        String description = "{" + "\"prune evaluator\":{\"language\":\"builtin\",\"name\":\"none\"},"
                + "\"return filter\":{\"language\":\"javascript\",\"body\":\"position.endNode().getProperty('name').toLowerCase().contains('t');\"},"
                + "\"order\":\"depth first\"," + "\"relationships\":{\"type\":\"knows\",\"direction\":\"all\"}" + "}";
        Response response = service.traverse( startNode, TraverserReturnType.node, description );
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        String entity = entityAsString(response);
        assertTrue(entity.contains("node/" + startNode));
        assertFalse(entity.contains("node/" + node1));
        assertFalse(entity.contains("node/" + node2));
        assertTrue(entity.contains("node/" + node3));
    }

    @Test
    public void shouldBeAbleToGetOtherResultTypesWhenTraversing() throws DatabaseBlockedException {
        long startNode = helper.createNode(MapUtil.map("name", "Mattias"));
        long node1 = helper.createNode(MapUtil.map("name", "Emil"));
        long node2 = helper.createNode(MapUtil.map("name", "Johan"));
        long node3 = helper.createNode(MapUtil.map("name", "Tobias"));
        long rel1 = helper.createRelationship("knows", startNode, node1);
        long rel2 = helper.createRelationship("knows", startNode, node2);
        long rel3 = helper.createRelationship("knows", node1, node3);

        Response response = service.traverse( startNode, TraverserReturnType.relationship, "" );
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        String entity = entityAsString(response);
        assertTrue(entity.contains("/relationship/" + rel1));
        assertTrue(entity.contains("/relationship/" + rel2));
        assertFalse(entity.contains("/relationship/" + rel3));

        response = service.traverse( startNode, TraverserReturnType.path, "" );
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        entity = entityAsString(response);
        assertTrue(entity.contains("nodes"));
        assertTrue(entity.contains("relationships"));
        assertTrue(entity.contains("length"));
    }

    private static String markWithUnicodeMarker(String string) {
        return String.valueOf((char) 0xfeff) + string;
    }

    @Test
    public void shouldBeAbleToParseJsonEvenWithUnicodeMarkerAtTheStart() throws DatabaseBlockedException {
        Response response = service.createNode( markWithUnicodeMarker( "{\"name\":\"Mattias\"}" ) );
        assertEquals(Status.CREATED.getStatusCode(), response.getStatus());
        String nodeLocation = response.getMetadata().getFirst(HttpHeaders.LOCATION).toString();

        long node = helper.createNode();
        assertEquals( Status.NO_CONTENT.getStatusCode(), service.setNodeProperty( node, "foo",
                markWithUnicodeMarker( "\"bar\"" ) ).getStatus() );
        assertEquals( Status.NO_CONTENT.getStatusCode(), service.setNodeProperty( node, "foo",
                markWithUnicodeMarker( "" + 10 ) ).getStatus() );
        assertEquals( Status.NO_CONTENT.getStatusCode(), service.setAllNodeProperties( node,
                markWithUnicodeMarker( "{\"name\":\"Something\",\"number\":10}" ) )
                .getStatus());

        assertEquals( Status.CREATED.getStatusCode(),
                service.createRelationship(
                        node,
                markWithUnicodeMarker("{\"to\":\"" + nodeLocation + "\",\"type\":\"knows\"}")).getStatus());

        long relationship = helper.createRelationship("knows");
        assertEquals( Status.NO_CONTENT.getStatusCode(), service.setRelationshipProperty(
                relationship, "foo", markWithUnicodeMarker( "\"bar\"" ) ).getStatus() );
        assertEquals( Status.NO_CONTENT.getStatusCode(), service.setRelationshipProperty(
                relationship, "foo", markWithUnicodeMarker( "" + 10 ) ).getStatus() );
        assertEquals(
                Status.NO_CONTENT.getStatusCode(),
                service.setAllRelationshipProperties( relationship,
                markWithUnicodeMarker("{\"name\":\"Something\",\"number\":10}")).getStatus());

        assertEquals( Status.CREATED.getStatusCode(), service.addToIndex( "node", "foo", "bar",
                markWithUnicodeMarker(JsonHelper.createJsonFrom(nodeLocation))).getStatus());

        assertEquals( Status.OK.getStatusCode(), service.traverse( node, TraverserReturnType.node,
                markWithUnicodeMarker( "{\"max depth\":2}" ) ).getStatus() );
    }
}
