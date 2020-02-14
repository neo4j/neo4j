/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.server.http.cypher.integration;

import com.fasterxml.jackson.databind.JsonNode;
import org.assertj.core.api.Condition;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.server.HTTP;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.internal.helpers.collection.Iterators.iterator;

/**
 * Matchers and assertion methods for the transactional endpoint.
 */
public class TransactionConditions
{
    private TransactionConditions()
    {
    }

    static Condition<String> validRFCTimestamp()
    {
        return new Condition<>( value ->
        {
            try
            {
                ZonedDateTime.parse( value, DateTimeFormatter.RFC_1123_DATE_TIME );
                return true;
            }
            catch ( Exception e )
            {
                return false;
            }
        }, "Valid RFC1134 timestamp." );
    }

    public static Consumer<HTTP.Response> containsNoErrors()
    {
        return hasErrors();
    }

    public static Consumer<HTTP.Response> hasErrors( final Status... expectedErrors )
    {
        return response ->
        {
            try
            {
                Iterator<JsonNode> errors = response.get( "errors" ).iterator();
                Iterator<Status> expected = iterator( expectedErrors );

                while ( expected.hasNext() )
                {
                    assertTrue( errors.hasNext() );
                    assertThat( errors.next().get( "code" ).asText() ).isEqualTo( expected.next().code().serialize() );
                }
                if ( errors.hasNext() )
                {
                    JsonNode error = errors.next();
                    fail( "Expected no more errors, but got " + error.get( "code" ) + " - '" + error.get( "message" ) + "'." );
                }
            }
            catch ( JsonParseException e )
            {
                assertNull( e );
            }
        };
    }

    static JsonNode getJsonNodeWithName( HTTP.Response response, String name ) throws JsonParseException
    {
        return response.get( "results" ).get( 0 ).get( "data" ).get( 0 ).get( name );
    }

    public static Consumer<HTTP.Response> rowContainsDeletedEntities( final int nodes, final int rels )
    {
        return response ->
        {
            try
            {
                Iterator<JsonNode> meta = getJsonNodeWithName( response, "meta" ).iterator();

                int nodeCounter = 0;
                int relCounter = 0;
                for ( int i = 0; i < nodes + rels; ++i )
                {
                    assertTrue( meta.hasNext() );
                    JsonNode node = meta.next();
                    assertThat( node.get( "deleted" ).asBoolean() ).isEqualTo( Boolean.TRUE );
                    String type = node.get( "type" ).asText();
                    switch ( type )
                    {
                    case "node":
                        ++nodeCounter;
                        break;
                    case "relationship":
                        ++relCounter;
                        break;
                    default:
                        fail( "Unexpected type: " + type );
                        break;
                    }
                }
                assertEquals( nodes, nodeCounter );
                assertEquals( rels, relCounter );
                while ( meta.hasNext() )
                {
                    JsonNode node = meta.next();
                    assertThat( node.get( "deleted" ).asBoolean() ).isEqualTo( Boolean.FALSE );
                }
            }
            catch ( JsonParseException e )
            {
                assertNull( e );
            }
        };
    }

    public static Consumer<HTTP.Response> rowContainsDeletedEntitiesInPath( final int nodes, final int rels )
    {
        return response ->
        {
            try
            {
                Iterator<JsonNode> meta = getJsonNodeWithName( response, "meta" ).iterator();

                int nodeCounter = 0;
                int relCounter = 0;
                assertTrue( "Expected to find a JSON node, but there was none", meta.hasNext() );
                JsonNode node = meta.next();
                assertTrue( "Expected the node to be a list (for a path)", node.isArray() );
                for ( JsonNode inner : node )
                {
                    String type = inner.get( "type" ).asText();
                    switch ( type )
                    {
                    case "node":
                        if ( inner.get( "deleted" ).asBoolean() )
                        {
                            ++nodeCounter;
                        }
                        break;
                    case "relationship":
                        if ( inner.get( "deleted" ).asBoolean() )
                        {
                            ++relCounter;
                        }
                        break;
                    default:
                        fail( "Unexpected type: " + type );
                        break;
                    }
                }
                assertEquals( nodes, nodeCounter );
                assertEquals( rels, relCounter );
            }
            catch ( JsonParseException e )
            {
                assertNull( e );
            }
        };
    }

    public static Consumer<HTTP.Response> rowContainsMetaNodesAtIndex( int... indexes )
    {
        return response -> assertElementAtMetaIndex( response, indexes, "node" );
    }

    public static Consumer<HTTP.Response> rowContainsMetaRelsAtIndex( int... indexes )
    {
        return response -> assertElementAtMetaIndex( response, indexes, "relationship" );
    }

    private static void assertElementAtMetaIndex( HTTP.Response response, int[] indexes, String element )
    {
        try
        {
            Iterator<JsonNode> meta = getJsonNodeWithName( response, "meta" ).iterator();

            int i = 0;
            for ( int metaIndex = 0; meta.hasNext() && i < indexes.length; metaIndex++ )
            {
                JsonNode node = meta.next();
                if ( !node.isNull() )
                {
                    String type = node.get( "type" ).asText();
                    if ( type.equals( element ) )
                    {
                        assertEquals( "Expected " + element + " to be at indexes " + Arrays.toString( indexes ) +
                                      ", but found it at " + metaIndex, indexes[i], metaIndex );
                        ++i;
                    }
                    else
                    {
                        assertNotEquals( "Expected " + element + " at index " + metaIndex + ", but found " + type,
                                indexes[i],
                                metaIndex );
                    }
                }
            }
            assertEquals( indexes.length, i );
        }
        catch ( JsonParseException e )
        {
            assertNull( e );
        }
    }

    public static Consumer<HTTP.Response> rowContainsAMetaListAtIndex( int index )
    {
        return response ->
        {
            try
            {
                Iterator<JsonNode> meta = getJsonNodeWithName( response, "meta" ).iterator();

                for ( int metaIndex = 0; meta.hasNext(); metaIndex++ )
                {
                    JsonNode node = meta.next();
                    if ( metaIndex == index )
                    {
                        assertTrue( node.isArray() );
                    }
                }
            }
            catch ( JsonParseException e )
            {
                assertNull( e );
            }
        };
    }

    public static Consumer<HTTP.Response> restContainsDeletedEntities( final int amount )
    {
        return response ->
        {
            try
            {
                Iterator<JsonNode> entities = getJsonNodeWithName( response, "rest" ).iterator();

                for ( int i = 0; i < amount; ++i )
                {
                    assertTrue( entities.hasNext() );
                    JsonNode node = entities.next();
                    assertThat( node.get( "metadata" ).get( "deleted" ).asBoolean() ).isEqualTo( Boolean.TRUE );
                }
                if ( entities.hasNext() )
                {
                    fail( "Expected no more entities" );
                }
            }
            catch ( JsonParseException e )
            {
               assertNull( e );
            }
        };
    }

    public static Consumer<HTTP.Response> graphContainsDeletedNodes( final int amount )
    {
        return response ->
        {
            try
            {
                Iterator<JsonNode> nodes = getJsonNodeWithName( response, "graph" ).get( "nodes" ).iterator();
                int deleted = 0;
                while ( nodes.hasNext() )
                {
                    JsonNode node = nodes.next();
                    if ( node.get( "deleted" ) != null )
                    {
                        assertTrue( node.get( "deleted" ).asBoolean() );
                        deleted++;
                    }
                }
                assertEquals( format( "Expected to see %d deleted elements but %d was encountered.", amount, deleted ), amount, deleted );
            }
            catch ( JsonParseException e )
            {
                assertNull( e );
            }
        };
    }

    public static Consumer<HTTP.Response> graphContainsNoDeletedEntities()
    {
        return response ->
        {
            try
            {
                for ( JsonNode node : getJsonNodeWithName( response, "graph" ).get( "nodes" ) )
                {
                    assertNull( node.get( "deleted" ) );
                }
                for ( JsonNode node : getJsonNodeWithName( response, "graph" ).get( "relationships" ) )
                {
                    assertNull( node.get( "deleted" ) );
                }
            }
            catch ( JsonParseException e )
            {
                assertNull( e );
            }
        };
    }

    public static Consumer<HTTP.Response> rowContainsNoDeletedEntities()
    {
        return response ->
        {
            try
            {
                for ( JsonNode node : getJsonNodeWithName( response, "meta" ) )
                {
                    assertFalse( node.get( "deleted" ).asBoolean() );
                }
            }
            catch ( JsonParseException e )
            {
                assertNull( e );
            }
        };
    }

    public static Consumer<HTTP.Response> restContainsNoDeletedEntities()
    {
        return response ->
        {
            try
            {
                for ( JsonNode node : getJsonNodeWithName( response, "rest" ) )
                {
                    assertNull( node.get( "metadata" ).get( "deleted" ) );
                }
            }
            catch ( JsonParseException e )
            {
                assertNull( e );
            }
        };
    }

    public static Consumer<HTTP.Response> graphContainsDeletedRelationships( final int amount )
    {
        return response ->
        {
            try
            {
                Iterator<JsonNode> relationships = getJsonNodeWithName( response, "graph" ).get( "relationships" ).iterator();

                for ( int i = 0; i < amount; ++i )
                {
                    assertTrue( relationships.hasNext() );
                    JsonNode node = relationships.next();
                    assertThat( node.get( "deleted" ).asBoolean() ).isEqualTo( Boolean.TRUE );
                }
                if ( relationships.hasNext() )
                {
                    JsonNode node = relationships.next();
                    fail( "Expected no more nodes, but got a node with id " + node.get( "id" ) );
                }
            }
            catch ( JsonParseException e )
            {
                assertNull( e );
            }
        };
    }

    @SuppressWarnings( "WhileLoopReplaceableByForEach" )
    public static long countNodes( GraphDatabaseService graphdb )
    {
        try ( Transaction transaction = graphdb.beginTx() )
        {
            long count = 0;
            Iterator<Node> allNodes = transaction.getAllNodes().iterator();
            while ( allNodes.hasNext() )
            {
                allNodes.next();
                count++;
            }
            return count;
        }
    }

    public static Condition<? super HTTP.Response> containsNoStackTraces()
    {
        return new Condition<>( response ->
        {
            Map<String,Object> content = response.content();
            var errors = (List<Map<String,Object>>) content.get( "errors" );

            for ( Map<String,Object> error : errors )
            {
                if ( error.containsKey( "stackTrace" ) )
                {
                    return false;
                }
            }
            return true;
        }, "Contains stack traces." );
    }
}
