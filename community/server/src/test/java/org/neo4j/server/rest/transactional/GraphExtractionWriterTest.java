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
package org.neo4j.server.rest.transactional;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.Property;

import static java.util.Arrays.asList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.neo4j.test.Property.property;
import static org.neo4j.test.mocking.GraphMock.node;
import static org.neo4j.test.mocking.GraphMock.path;
import static org.neo4j.test.mocking.GraphMock.relationship;
import static org.neo4j.test.mocking.Link.link;
import static org.neo4j.test.mocking.Properties.properties;

public class GraphExtractionWriterTest
{
    private final Node n1 = node( 17, properties( property( "name", "n1" ) ), "Foo" );
    private final Node n2 = node( 666, properties( property( "name", "n2" ) ) );
    private final Node n3 = node( 42, properties( property( "name", "n3" ) ), "Foo", "Bar" );
    private final Relationship r1 = relationship( 7, n1, "ONE", n2, property( "name", "r1" ) );
    private final Relationship r2 = relationship( 8, n1, "TWO", n3, property( "name", "r2" ) );

    @Test
    public void shouldExtractNodesFromRow() throws Exception
    {
        // given
        Map<String, Object> row = new HashMap<>();
        row.put( "n1", n1 );
        row.put( "n2", n2 );
        row.put( "n3", n3 );
        row.put( "other.thing", "hello" );
        row.put( "some.junk", 0x0099cc );

        // when
        JsonNode result = write( row );

        // then
        assertNodes( result );
        assertEquals( "there should be no relationships", 0, result.get( "graph" ).get( "relationships" ).size() );
    }

    @Test
    public void shouldExtractRelationshipsFromRowAndNodesFromRelationships() throws Exception
    {
        // given
        Map<String, Object> row = new HashMap<>();
        row.put( "r1", r1 );
        row.put( "r2", r2 );

        // when
        JsonNode result = write( row );

        // then
        assertNodes( result );
        assertRelationships( result );
    }

    @Test
    public void shouldExtractPathFromRowAndExtractNodesAndRelationshipsFromPath() throws Exception
    {
        // given
        Map<String, Object> row = new HashMap<>();
        row.put( "p", path( n2, link( r1, n1 ), link( r2, n3 ) ) );

        // when
        JsonNode result = write( row );

        // then
        assertNodes( result );
        assertRelationships( result );
    }

    @Test
    public void shouldExtractGraphFromMapInTheRow() throws Exception
    {
        // given
        Map<String, Object> row = new HashMap<>(), map = new HashMap<>();
        row.put( "map", map );
        map.put( "r1", r1 );
        map.put( "r2", r2 );

        // when
        JsonNode result = write( row );

        // then
        assertNodes( result );
        assertRelationships( result );
    }

    @Test
    public void shouldExtractGraphFromListInTheRow() throws Exception
    {
        // given
        Map<String, Object> row = new HashMap<>();
        List<Object> list = new ArrayList<>();
        row.put( "list", list );
        list.add( r1 );
        list.add( r2 );

        // when
        JsonNode result = write( row );

        // then
        assertNodes( result );
        assertRelationships( result );
    }

    @Test
    public void shouldExtractGraphFromListInMapInTheRow() throws Exception
    {
        // given
        Map<String, Object> row = new HashMap<>(), map = new HashMap<>();
        List<Object> list = new ArrayList<>();
        map.put( "list", list );
        row.put( "map", map );
        list.add( r1 );
        list.add( r2 );

        // when
        JsonNode result = write( row );

        // then
        assertNodes( result );
        assertRelationships( result );
    }

    @Test
    public void shouldExtractGraphFromMapInListInTheRow() throws Exception
    {
        // given
        Map<String, Object> row = new HashMap<>(), map = new HashMap<>();
        List<Object> list = new ArrayList<>();
        list.add( map );
        row.put( "list", list );
        map.put( "r1", r1 );
        map.put( "r2", r2 );

        // when
        JsonNode result = write( row );

        // then
        assertNodes( result );
        assertRelationships( result );
    }

    // The code under test

    private JsonFactory jsonFactory = new JsonFactory();

    private JsonNode write( Map<String, Object> row ) throws IOException, JsonParseException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        JsonGenerator json = jsonFactory.createJsonGenerator( out );
        json.writeStartObject();
        try
        {
            new GraphExtractionWriter().write( json, row.keySet(), new MapRow( row ) );
        }
        finally
        {
            json.writeEndObject();
            json.flush();
        }
        return JsonHelper.jsonNode( out.toString( "UTF-8" ) );
    }

    // The expected format of the result

    private void assertNodes( JsonNode result )
    {
        JsonNode nodes = result.get( "graph" ).get( "nodes" );
        assertEquals( "there should be 3 nodes", 3, nodes.size() );
        assertNode( "17", nodes, asList( "Foo" ), property( "name", "n1" ) );
        assertNode( "666", nodes, Arrays.<String>asList(), property( "name", "n2" ) );
        assertNode( "42", nodes, asList( "Foo", "Bar" ), property( "name", "n3" ) );
    }

    private void assertRelationships( JsonNode result )
    {
        JsonNode relationships = result.get( "graph" ).get( "relationships" );
        assertEquals( "there should be 2 relationships", 2, relationships.size() );
        assertRelationship( "7", relationships, "17", "ONE", "666", property( "name", "r1" ) );
        assertRelationship( "8", relationships, "17", "TWO", "42", property( "name", "r2" ) );
    }

    // Helpers

    private static void assertNode( String id, JsonNode nodes, List<String> labels, Property... properties )
    {
        JsonNode node = get( nodes, id );
        assertListEquals( "Node[" + id + "].labels", labels, node.get( "labels" ) );
        JsonNode props = node.get( "properties" );
        assertEquals( "length( Node[" + id + "].properties )", properties.length, props.size() );
        for ( Property property : properties )
        {
            assertJsonEquals( "Node[" + id + "].properties[" + property.key() + "]",
                              property.value(), props.get( property.key() ) );
        }
    }

    private static void assertRelationship( String id, JsonNode relationships, String startNodeId, String type,
                                            String endNodeId, Property... properties )
    {
        JsonNode relationship = get( relationships, id );
        assertEquals( "Relationship[" + id + "].labels", type, relationship.get( "type" ).getTextValue() );
        assertEquals( "Relationship[" + id + "].startNode", startNodeId,
                      relationship.get( "startNode" ).getTextValue() );
        assertEquals( "Relationship[" + id + "].endNode", endNodeId, relationship.get( "endNode" ).getTextValue() );
        JsonNode props = relationship.get( "properties" );
        assertEquals( "length( Relationship[" + id + "].properties )", properties.length, props.size() );
        for ( Property property : properties )
        {
            assertJsonEquals( "Relationship[" + id + "].properties[" + property.key() + "]",
                              property.value(), props.get( property.key() ) );
        }
    }

    private static void assertJsonEquals( String message, Object expected, JsonNode actual )
    {
        if ( expected == null )
        {
            assertTrue( message, actual == null || actual.isNull() );
        }
        else if ( expected instanceof String )
        {
            assertEquals( message, expected, actual.getTextValue() );
        }
        else if ( expected instanceof Number )
        {
            assertEquals( message, expected, actual.getNumberValue() );
        }
        else
        {
            fail( message + " - unexpected type - " + expected );
        }
    }

    private static void assertListEquals( String what, List<String> expected, JsonNode jsonNode )
    {
        assertTrue( what + " - should be a list", jsonNode.isArray() );
        List<String> actual = new ArrayList<>( jsonNode.size() );
        for ( JsonNode node : jsonNode )
        {
            actual.add( node.getTextValue() );
        }
        assertEquals( what, expected, actual );
    }

    private static JsonNode get( Iterable<JsonNode> jsonNodes, String id )
    {
        for ( JsonNode jsonNode : jsonNodes )
        {
            if ( id.equals( jsonNode.get( "id" ).getTextValue() ) )
            {
                return jsonNode;
            }
        }
        return null;
    }
}
