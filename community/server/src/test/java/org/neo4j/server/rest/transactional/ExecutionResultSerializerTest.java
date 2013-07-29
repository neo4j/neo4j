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
package org.neo4j.server.rest.transactional;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.mockito.internal.stubbing.answers.ThrowsException;

import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.impl.util.TestLogger;
import org.neo4j.server.rest.transactional.error.Neo4jError;
import org.neo4j.server.rest.transactional.error.StatusCode;
import org.neo4j.test.mocking.GraphMock;

import static java.util.Arrays.asList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.impl.util.TestLogger.LogCall.error;
import static org.neo4j.test.Property.property;
import static org.neo4j.test.mocking.GraphMock.node;
import static org.neo4j.test.mocking.GraphMock.path;
import static org.neo4j.test.mocking.Link.link;
import static org.neo4j.test.mocking.Properties.properties;

public class ExecutionResultSerializerTest
{
    @Test
    public void shouldSerializeResponseWithCommitUriOnly() throws Exception
    {
        // given
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, org.neo4j.kernel.impl.util
                .StringLogger.DEV_NULL );

        // when
        serializer.transactionCommitUri( URI.create( "commit/uri/1" ) );
        serializer.finish();

        // then
        String result = output.toString( "UTF-8" );
        assertEquals( "{\"commit\":\"commit/uri/1\",\"results\":[],\"errors\":[]}", result );
    }

    @Test
    public void shouldSerializeResponseWithCommitUriAndResults() throws Exception
    {
        // given
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, StringLogger.DEV_NULL );

        ExecutionResult executionResult = mockExecutionResult( map(
                "column1", "value1",
                "column2", "value2" ) );

        // when
        serializer.transactionCommitUri( URI.create( "commit/uri/1" ) );
        serializer.statementResult( executionResult );
        serializer.finish();

        // then
        String result = output.toString( "UTF-8" );
        assertEquals( "{\"commit\":\"commit/uri/1\",\"results\":[{\"columns\":[\"column1\",\"column2\"]," +
                      "\"data\":[{\"row\":[\"value1\",\"value2\"]}]}],\"errors\":[]}", result );
    }

    @Test
    public void shouldSerializeResponseWithResultsOnly() throws Exception
    {
        // given
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, StringLogger.DEV_NULL );

        ExecutionResult executionResult = mockExecutionResult( map(
                "column1", "value1",
                "column2", "value2" ) );

        // when
        serializer.statementResult( executionResult );
        serializer.finish();

        // then
        String result = output.toString( "UTF-8" );
        assertEquals( "{\"results\":[{\"columns\":[\"column1\",\"column2\"]," +
                      "\"data\":[{\"row\":[\"value1\",\"value2\"]}]}],\"errors\":[]}", result );
    }

    @Test
    public void shouldSerializeResponseWithCommitUriAndResultsAndErrors() throws Exception
    {
        // given
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, StringLogger.DEV_NULL );

        ExecutionResult executionResult = mockExecutionResult( map(
                "column1", "value1",
                "column2", "value2" ) );

        // when
        serializer.transactionCommitUri( URI.create( "commit/uri/1" ) );
        serializer.statementResult( executionResult );
        serializer.errors( asList( new Neo4jError( StatusCode.INVALID_REQUEST_FORMAT, new Exception( "cause1" ) ) ) );
        serializer.finish();

        // then
        String result = output.toString( "UTF-8" );
        assertEquals( "{\"commit\":\"commit/uri/1\",\"results\":[{\"columns\":[\"column1\",\"column2\"]," +
                      "\"data\":[{\"row\":[\"value1\",\"value2\"]}]}]," +
                      "\"errors\":[{\"code\":40001,\"status\":\"INVALID_REQUEST_FORMAT\",\"message\":\"cause1\"}]}",
                      result );
    }

    @Test
    public void shouldSerializeResponseWithResultsAndErrors() throws Exception
    {
        // given
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, StringLogger.DEV_NULL );

        ExecutionResult executionResult = mockExecutionResult( map(
                "column1", "value1",
                "column2", "value2" ) );

        // when
        serializer.statementResult( executionResult );
        serializer.errors( asList( new Neo4jError( StatusCode.INVALID_REQUEST_FORMAT, new Exception( "cause1" ) ) ) );
        serializer.finish();

        // then
        String result = output.toString( "UTF-8" );
        assertEquals( "{\"results\":[{\"columns\":[\"column1\",\"column2\"]," +
                      "\"data\":[{\"row\":[\"value1\",\"value2\"]}]}]," +
                      "\"errors\":[{\"code\":40001,\"status\":\"INVALID_REQUEST_FORMAT\",\"message\":\"cause1\"}]}",
                      result );
    }

    @Test
    public void shouldSerializeResponseWithCommitUriAndErrors() throws Exception
    {
        // given
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, StringLogger.DEV_NULL );

        // when
        serializer.transactionCommitUri( URI.create( "commit/uri/1" ) );
        serializer.errors( asList( new Neo4jError( StatusCode.INVALID_REQUEST_FORMAT, new Exception( "cause1" ) ) ) );
        serializer.finish();

        // then
        String result = output.toString( "UTF-8" );
        assertEquals( "{\"commit\":\"commit/uri/1\",\"results\":[],\"errors\":[{\"code\":40001," +
                      "\"status\":\"INVALID_REQUEST_FORMAT\"," +
                      "\"message\":\"cause1\"}]}", result );
    }

    @Test
    public void shouldSerializeResponseWithErrorsOnly() throws Exception
    {
        // given
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, StringLogger.DEV_NULL );

        // when
        serializer.errors( asList( new Neo4jError( StatusCode.INVALID_REQUEST_FORMAT, new Exception( "cause1" ) ) ) );
        serializer.finish();

        // then
        String result = output.toString( "UTF-8" );
        assertEquals(
                "{\"results\":[],\"errors\":[{\"code\":40001,\"status\":\"INVALID_REQUEST_FORMAT\",\"message\":\"cause1\"}]}",
                result );
    }

    @Test
    public void shouldSerializeResponseWithNoCommitUriResultsOrErrors() throws Exception
    {
        // given
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, StringLogger.DEV_NULL );

        // when
        serializer.finish();

        // then
        String result = output.toString( "UTF-8" );
        assertEquals( "{\"results\":[],\"errors\":[]}", result );
    }

    @Test
    public void shouldSerializeResponseWithMultipleResultRows() throws Exception
    {
        // given
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, StringLogger.DEV_NULL );

        ExecutionResult executionResult = mockExecutionResult( map(
                "column1", "value1",
                "column2", "value2" ), map(
                "column1", "value3",
                "column2", "value4" ) );

        // when
        serializer.statementResult( executionResult );
        serializer.finish();

        // then
        String result = output.toString( "UTF-8" );
        assertEquals( "{\"results\":[{\"columns\":[\"column1\",\"column2\"]," +
                      "\"data\":[{\"row\":[\"value1\",\"value2\"]},{\"row\":[\"value3\",\"value4\"]}]}]," +
                      "\"errors\":[]}", result );
    }

    @Test
    public void shouldSerializeResponseWithMultipleResults() throws Exception
    {
        // given
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, StringLogger.DEV_NULL );

        ExecutionResult executionResult1 = mockExecutionResult( map(
                "column1", "value1",
                "column2", "value2" ) );
        ExecutionResult executionResult2 = mockExecutionResult( map(
                "column3", "value3",
                "column4", "value4" ) );

        // when
        serializer.statementResult( executionResult1 );
        serializer.statementResult( executionResult2 );
        serializer.finish();

        // then
        String result = output.toString( "UTF-8" );
        assertEquals( "{\"results\":[" +
                      "{\"columns\":[\"column1\",\"column2\"],\"data\":[{\"row\":[\"value1\",\"value2\"]}]}," +
                      "{\"columns\":[\"column3\",\"column4\"],\"data\":[{\"row\":[\"value3\",\"value4\"]}]}]," +
                      "\"errors\":[]}", result );
    }

    @Test
    public void shouldSerializeNodeAsMapOfProperties() throws Exception
    {
        // given
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, StringLogger.DEV_NULL );

        ExecutionResult executionResult = mockExecutionResult( map(
                "node", node( 1, properties(
                property( "a", 12 ),
                property( "b", true ),
                property( "c", new int[]{1, 0, 1, 2} ),
                property( "d", new byte[]{1, 0, 1, 2} ),
                property( "e", new String[]{"a", "b", "ääö"} ) ) ) ) );

        // when
        serializer.statementResult( executionResult );
        serializer.finish();

        // then
        String result = output.toString( "UTF-8" );
        assertEquals( "{\"results\":[{\"columns\":[\"node\"]," +
                      "\"data\":[{\"row\":[{\"d\":[1,0,1,2],\"e\":[\"a\",\"b\",\"ääö\"],\"b\":true,\"c\":[1,0,1,2],\"a\":12}]}]}]," +
                      "\"errors\":[]}", result );
    }

    @Test
    public void shouldSerializePathAsListOfMapsOfProperties() throws Exception
    {
        // given
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, StringLogger.DEV_NULL );

        ExecutionResult executionResult = mockExecutionResult( map(
                "path", mockPath( map( "key1", "value1" ), map( "key2", "value2" ), map( "key3", "value3" ) ) ) );

        // when
        serializer.statementResult( executionResult );
        serializer.finish();

        // then
        String result = output.toString( "UTF-8" );
        assertEquals( "{\"results\":[{\"columns\":[\"path\"]," +
                      "\"data\":[{\"row\":[[{\"key1\":\"value1\"},{\"key2\":\"value2\"},{\"key3\":\"value3\"}]]}]}]," +
                      "\"errors\":[]}", result );
    }

    @Test
    public void shouldProduceWellFormedJsonEvenIfResultIteratorThrowsExceptionOnNext() throws Exception
    {
        // given
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, StringLogger.DEV_NULL );

        Map<String, Object> data = map(
                "column1", "value1",
                "column2", "value2" );
        ExecutionResult executionResult = mock( ExecutionResult.class );
        when( executionResult.columns() ).thenReturn( new ArrayList<>( data.keySet() ) );
        @SuppressWarnings("unchecked")
        ResourceIterator<Map<String, Object>> iterator = mock( ResourceIterator.class );
        when( iterator.hasNext() ).thenReturn( true, true, false );
        when( iterator.next() ).thenReturn( data ).thenThrow( new RuntimeException( "Stuff went wrong!" ) );
        when( executionResult.iterator() ).thenReturn( iterator );

        // when
        try
        {
            serializer.statementResult( executionResult );
            fail( "should have thrown exception" );
        }
        catch ( RuntimeException e )
        {
            serializer.errors( asList( new Neo4jError( StatusCode.INTERNAL_STATEMENT_EXECUTION_ERROR, e ) ) );
        }
        serializer.finish();

        // then
        String result = output.toString( "UTF-8" );
        assertEquals(
                "{\"results\":[{\"columns\":[\"column1\",\"column2\"],\"data\":[{\"row\":[\"value1\",\"value2\"]}]}]," +
                "\"errors\":[{\"code\":50001,\"status\":\"INTERNAL_STATEMENT_EXECUTION_ERROR\",\"message\":\"Stuff went wrong!\",\"stackTrace\":***}]}",
                replaceStackTrace( result, "***" ) );
    }

    @Test
    public void shouldProduceWellFormedJsonEvenIfResultIteratorThrowsExceptionOnHasNext() throws Exception
    {
        // given
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, StringLogger.DEV_NULL );

        Map<String, Object> data = map(
                "column1", "value1",
                "column2", "value2" );
        ExecutionResult executionResult = mock( ExecutionResult.class );
        when( executionResult.columns() ).thenReturn( new ArrayList<>( data.keySet() ) );
        @SuppressWarnings("unchecked")
        ResourceIterator<Map<String, Object>> iterator = mock( ResourceIterator.class );
        when( iterator.hasNext() ).thenReturn( true ).thenThrow(
                new RuntimeException( "Stuff went wrong!" ) );
        when( iterator.next() ).thenReturn( data );
        when( executionResult.iterator() ).thenReturn( iterator );

        // when
        try
        {
            serializer.statementResult( executionResult );
            fail( "should have thrown exception" );
        }
        catch ( RuntimeException e )
        {
            serializer.errors( asList( new Neo4jError( StatusCode.INTERNAL_STATEMENT_EXECUTION_ERROR, e ) ) );
        }
        serializer.finish();

        // then
        String result = output.toString( "UTF-8" );
        assertEquals(
                "{\"results\":[{\"columns\":[\"column1\",\"column2\"],\"data\":[{\"row\":[\"value1\",\"value2\"]}]}]," +
                "\"errors\":[{\"code\":50001,\"status\":\"INTERNAL_STATEMENT_EXECUTION_ERROR\",\"message\":\"Stuff went wrong!\"," +
                "\"stackTrace\":***}]}",
                replaceStackTrace( result, "***" ) );
    }

    @Test
    public void shouldProduceResultStreamWithGraphEntries() throws Exception
    {
        // given
        Node[] node = {
                node( 0, properties( property( "name", "node0" ) ), "Node" ),
                node( 1, properties( property( "name", "node1" ) ) ),
                node( 2, properties( property( "name", "node2" ) ), "This", "That" ),
                node( 3, properties( property( "name", "node3" ) ), "Other" )};
        Relationship[] rel = {
                GraphMock.relationship( 0, node[0], "KNOWS", node[1], property( "name", "rel0" ) ),
                GraphMock.relationship( 1, node[2], "LOVES", node[3], property( "name", "rel1" ) )};

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, StringLogger.DEV_NULL );

        // when
        serializer.statementResult( mockExecutionResult(
                map( "node", node[0], "rel", rel[0] ),
                map( "node", node[2], "rel", rel[1] ) ), ResultDataContent.row, ResultDataContent.graph );
        serializer.finish();

        // then
        String result = output.toString( "UTF-8" );

        // Nodes and relationships form sets, so we cannot test for a fixed string, since we don't know the order.
        String node0 = "{\"id\":\"0\",\"labels\":[\"Node\"],\"properties\":{\"name\":\"node0\"}}";
        String node1 = "{\"id\":\"1\",\"labels\":[],\"properties\":{\"name\":\"node1\"}}";
        String node2 = "{\"id\":\"2\",\"labels\":[\"This\",\"That\"],\"properties\":{\"name\":\"node2\"}}";
        String node3 = "{\"id\":\"3\",\"labels\":[\"Other\"],\"properties\":{\"name\":\"node3\"}}";
        String rel0 = "\"relationships\":[{\"id\":\"0\",\"type\":\"KNOWS\",\"startNode\":\"0\",\"endNode\":\"1\",\"properties\":{\"name\":\"rel0\"}}]}";
        String rel1 = "\"relationships\":[{\"id\":\"1\",\"type\":\"LOVES\",\"startNode\":\"2\",\"endNode\":\"3\",\"properties\":{\"name\":\"rel1\"}}]}";
        String row0 = "{\"row\":[{\"name\":\"node0\"},{\"name\":\"rel0\"}],\"graph\":{\"nodes\":[";
        String row1 = "{\"row\":[{\"name\":\"node2\"},{\"name\":\"rel1\"}],\"graph\":{\"nodes\":[";
        int n0 = result.indexOf( node0 );
        int n1 = result.indexOf( node1 );
        int n2 = result.indexOf( node2 );
        int n3 = result.indexOf( node3 );
        int r0 = result.indexOf( rel0 );
        int r1 = result.indexOf( rel1 );
        int _0 = result.indexOf( row0 );
        int _1 = result.indexOf( row1 );
        assertTrue( "result should contain row0", _0 > 0 );
        assertTrue( "result should contain row1 after row0", _1 > _0 );
        assertTrue( "result should contain node0 after row0", n0 > _0 );
        assertTrue( "result should contain node1 after row0", n1 > _0 );
        assertTrue( "result should contain node2 after row1", n2 > _1 );
        assertTrue( "result should contain node3 after row1", n3 > _1 );
        assertTrue( "result should contain rel0 after node0 and node1", r0 > n0 && r0 > n1 );
        assertTrue( "result should contain rel1 after node2 and node3", r1 > n2 && r1 > n3 );
    }

    @Test
    public void shouldLogIOErrors() throws Exception
    {
        // given
        IOException failure = new IOException();
        OutputStream output = mock( OutputStream.class, new ThrowsException( failure ) );
        TestLogger log = new TestLogger();
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, log );

        // when
        serializer.finish();

        // then
        log.assertExactly( error( "Failed to generate JSON output.", failure ) );
    }

    @SafeVarargs
    private static ExecutionResult mockExecutionResult( Map<String, Object>... rows )
    {
        Set<String> keys = new HashSet<>();
        for ( Map<String, Object> row : rows )
        {
            keys.addAll( row.keySet() );
        }
        ExecutionResult executionResult = mock( ExecutionResult.class );
        when( executionResult.columns() ).thenReturn( new ArrayList<>( keys ) );
        final Iterator<Map<String, Object>> inner = asList( rows ).iterator();

        ResourceIterator<Map<String, Object>> iterator = new ResourceIterator<Map<String, Object>>()
        {
            @Override
            public void close()
            {
            }

            @Override
            public boolean hasNext()
            {
                return inner.hasNext();
            }

            @Override
            public Map<String, Object> next()
            {
                return inner.next();
            }

            @Override
            public void remove()
            {
                inner.remove();
            }
        };

        when( executionResult.iterator() ).thenReturn( iterator );
        return executionResult;
    }

    private static Path mockPath( Map<String, Object> startNodeProperties, Map<String, Object> relationshipProperties,
                                  Map<String,Object> endNodeProperties )
    {
        Node startNode = node( 1, properties( startNodeProperties ) );
        Node endNode = node( 2, properties( endNodeProperties ) );
        Relationship relationship = GraphMock.relationship( 1, properties( relationshipProperties ),
                                                            startNode, "RELATED", endNode );
        return path( startNode, link( relationship, endNode ) );
    }

    private String replaceStackTrace( String json, String matchableStackTrace )
    {
        return json.replaceAll( "\"stackTrace\":\"[^\"]*\"", "\"stackTrace\":" + matchableStackTrace );
    }
}
