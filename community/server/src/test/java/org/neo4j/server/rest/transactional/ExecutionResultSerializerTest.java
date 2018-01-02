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
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.codehaus.jackson.JsonNode;
import org.junit.Test;
import org.mockito.internal.stubbing.answers.ThrowsException;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.impl.notification.NotificationCode;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.rest.transactional.error.Neo4jError;
import org.neo4j.test.mocking.GraphMock;
import org.neo4j.test.mocking.Link;

import static java.util.Arrays.asList;

import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.server.rest.domain.JsonHelper.jsonNode;
import static org.neo4j.server.rest.domain.JsonHelper.readJson;
import static org.neo4j.test.Property.property;
import static org.neo4j.test.mocking.GraphMock.link;
import static org.neo4j.test.mocking.GraphMock.node;
import static org.neo4j.test.mocking.GraphMock.path;
import static org.neo4j.test.mocking.GraphMock.relationship;
import static org.neo4j.test.mocking.Properties.properties;

public class ExecutionResultSerializerTest
{
    @Test
    public void shouldSerializeResponseWithCommitUriOnly() throws Exception
    {
        // given
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, null, NullLogProvider.getInstance() );

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
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, null, NullLogProvider.getInstance() );

        Result executionResult = mockExecutionResult( map(
                "column1", "value1",
                "column2", "value2" ) );

        // when
        serializer.transactionCommitUri( URI.create( "commit/uri/1" ) );
        serializer.statementResult( executionResult, false );
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
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, null, NullLogProvider.getInstance() );

        Result executionResult = mockExecutionResult( map(
                "column1", "value1",
                "column2", "value2" ) );

        // when
        serializer.statementResult( executionResult, false );
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
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, null, NullLogProvider.getInstance() );

        Result executionResult = mockExecutionResult( map(
                "column1", "value1",
                "column2", "value2" ) );

        // when
        serializer.transactionCommitUri( URI.create( "commit/uri/1" ) );
        serializer.statementResult( executionResult, false );
        serializer.errors( asList( new Neo4jError( Status.Request.InvalidFormat, new Exception( "cause1" ) ) ) );
        serializer.finish();

        // then
        String result = output.toString( "UTF-8" );
        assertEquals( "{\"commit\":\"commit/uri/1\",\"results\":[{\"columns\":[\"column1\",\"column2\"]," +
                      "\"data\":[{\"row\":[\"value1\",\"value2\"]}]}]," +
                      "\"errors\":[{\"code\":\"Neo.ClientError.Request.InvalidFormat\",\"message\":\"cause1\"}]}",
                      result );
    }

    @Test
    public void shouldSerializeResponseWithResultsAndErrors() throws Exception
    {
        // given
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, null, NullLogProvider.getInstance() );

        Result executionResult = mockExecutionResult( map(
                "column1", "value1",
                "column2", "value2" ) );

        // when
        serializer.statementResult( executionResult, false );
        serializer.errors( asList( new Neo4jError( Status.Request.InvalidFormat, new Exception( "cause1" ) ) ) );
        serializer.finish();

        // then
        String result = output.toString( "UTF-8" );
        assertEquals( "{\"results\":[{\"columns\":[\"column1\",\"column2\"]," +
                      "\"data\":[{\"row\":[\"value1\",\"value2\"]}]}]," +
                      "\"errors\":[{\"code\":\"Neo.ClientError.Request.InvalidFormat\",\"message\":\"cause1\"}]}",
                      result );
    }

    @Test
    public void shouldSerializeResponseWithCommitUriAndErrors() throws Exception
    {
        // given
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, null, NullLogProvider.getInstance() );

        // when
        serializer.transactionCommitUri( URI.create( "commit/uri/1" ) );
        serializer.errors( asList( new Neo4jError( Status.Request.InvalidFormat, new Exception( "cause1" ) ) ) );
        serializer.finish();

        // then
        String result = output.toString( "UTF-8" );
        assertEquals( "{\"commit\":\"commit/uri/1\",\"results\":[],\"errors\":[{\"code\":\"Neo.ClientError.Request.InvalidFormat\"," +
                      "\"message\":\"cause1\"}]}", result );
    }

    @Test
    public void shouldSerializeResponseWithErrorsOnly() throws Exception
    {
        // given
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, null, NullLogProvider.getInstance() );

        // when
        serializer.errors( asList( new Neo4jError( Status.Request.InvalidFormat, new Exception( "cause1" ) ) ) );
        serializer.finish();

        // then
        String result = output.toString( "UTF-8" );
        assertEquals(
                "{\"results\":[],\"errors\":[{\"code\":\"Neo.ClientError.Request.InvalidFormat\",\"message\":\"cause1\"}]}",
                result );
    }

    @Test
    public void shouldSerializeResponseWithNoCommitUriResultsOrErrors() throws Exception
    {
        // given
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, null, NullLogProvider.getInstance() );

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
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, null, NullLogProvider.getInstance() );

        Result executionResult = mockExecutionResult( map(
                "column1", "value1",
                "column2", "value2" ), map(
                "column1", "value3",
                "column2", "value4" ) );

        // when
        serializer.statementResult(executionResult, false);
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
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, null, NullLogProvider.getInstance() );

        Result executionResult1 = mockExecutionResult( map(
                "column1", "value1",
                "column2", "value2" ) );
        Result executionResult2 = mockExecutionResult( map(
                "column3", "value3",
                "column4", "value4" ) );

        // when
        serializer.statementResult( executionResult1, false );
        serializer.statementResult( executionResult2, false );
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
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, null, NullLogProvider.getInstance() );

        Result executionResult = mockExecutionResult( map(
                "node", node( 1, properties(
                property( "a", 12 ),
                property( "b", true ),
                property( "c", new int[]{1, 0, 1, 2} ),
                property( "d", new byte[]{1, 0, 1, 2} ),
                property( "e", new String[]{"a", "b", "ääö"} ) ) ) ) );

        // when
        serializer.statementResult( executionResult, false );
        serializer.finish();

        // then
        String result = output.toString( "UTF-8" );
        assertEquals( "{\"results\":[{\"columns\":[\"node\"]," +
                      "\"data\":[{\"row\":[{\"a\":12,\"b\":true,\"c\":[1,0,1,2],\"d\":[1,0,1,2],\"e\":[\"a\",\"b\",\"ääö\"]}]}]}]," +
                      "\"errors\":[]}", result );
    }

    @Test
    public void shouldSerializeNestedEntities() throws Exception
    {
        // given
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, null, NullLogProvider.getInstance() );

        Node a = node( 1, properties( property( "foo", 12 ) ) );
        Node b = node( 2, properties( property( "bar", false ) ) );
        Relationship r = relationship( 1, properties( property( "baz", "quux" ) ), a, "FRAZZLE", b );
        Result executionResult = mockExecutionResult( map(
                "nested", new TreeMap<>( map(
                        "node", a,
                        "edge", r,
                        "path", path( a, link( r, b ) )
                ) ) ) );

        // when
        serializer.statementResult( executionResult, false );
        serializer.finish();

        // then
        String result = output.toString( "UTF-8" );
        assertEquals( "{\"results\":[{\"columns\":[\"nested\"]," +
                      "\"data\":[{\"row\":[{\"edge\":{\"baz\":\"quux\"},\"node\":{\"foo\":12},\"path\":[{\"foo\":12},{\"baz\":\"quux\"},{\"bar\":false}]}]}]}]," +
                      "\"errors\":[]}", result );
    }

    @Test
    public void shouldSerializePathAsListOfMapsOfProperties() throws Exception
    {
        // given
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, null, NullLogProvider.getInstance() );

        Result executionResult = mockExecutionResult( map(
                "path", mockPath( map( "key1", "value1" ), map( "key2", "value2" ), map( "key3", "value3" ) ) ) );

        // when
        serializer.statementResult( executionResult, false );
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
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, null, NullLogProvider.getInstance() );

        Map<String, Object> data = map(
                "column1", "value1",
                "column2", "value2" );
        Result executionResult = mock( Result.class );
        mockAccept( executionResult );
        when( executionResult.columns() ).thenReturn( new ArrayList<>( data.keySet() ) );
        when( executionResult.hasNext() ).thenReturn( true, true, false );
        when( executionResult.next() ).thenReturn( data ).thenThrow( new RuntimeException( "Stuff went wrong!" ) );

        // when
        try
        {
            serializer.statementResult( executionResult, false );
            fail( "should have thrown exception" );
        }
        catch ( RuntimeException e )
        {
            serializer.errors( asList( new Neo4jError( Status.Statement.ExecutionFailure, e ) ) );
        }
        serializer.finish();

        // then
        String result = output.toString( "UTF-8" );
        assertEquals(
                "{\"results\":[{\"columns\":[\"column1\",\"column2\"],\"data\":[{\"row\":[\"value1\",\"value2\"]}]}]," +
                "\"errors\":[{\"code\":\"Neo.DatabaseError.Statement.ExecutionFailure\",\"message\":\"Stuff went wrong!\",\"stackTrace\":***}]}",
                replaceStackTrace( result, "***" ) );
    }

    @Test
    public void shouldProduceWellFormedJsonEvenIfResultIteratorThrowsExceptionOnHasNext() throws Exception
    {
        // given
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, null, NullLogProvider.getInstance() );

        Map<String, Object> data = map(
                "column1", "value1",
                "column2", "value2" );
        Result executionResult = mock( Result.class );
        mockAccept( executionResult );
        when( executionResult.columns() ).thenReturn( new ArrayList<>( data.keySet() ) );
        when( executionResult.hasNext() ).thenReturn( true ).thenThrow(
                new RuntimeException( "Stuff went wrong!" ) );
        when( executionResult.next() ).thenReturn( data );

        // when
        try
        {
            serializer.statementResult( executionResult, false );
            fail( "should have thrown exception" );
        }
        catch ( RuntimeException e )
        {
            serializer.errors( asList( new Neo4jError( Status.Statement.ExecutionFailure, e ) ) );
        }
        serializer.finish();

        // then
        String result = output.toString( "UTF-8" );
        assertEquals(
                "{\"results\":[{\"columns\":[\"column1\",\"column2\"],\"data\":[{\"row\":[\"value1\",\"value2\"]}]}]," +
                "\"errors\":[{\"code\":\"Neo.DatabaseError.Statement.ExecutionFailure\",\"message\":\"Stuff went wrong!\"," +
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
                relationship( 0, node[0], "KNOWS", node[1], property( "name", "rel0" ) ),
                relationship( 1, node[2], "LOVES", node[3], property( "name", "rel1" ) )};

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, null, NullLogProvider.getInstance() );

        // when
        serializer.statementResult( mockExecutionResult(
                map( "node", node[0], "rel", rel[0] ),
                map( "node", node[2], "rel", rel[1] ) ), false, ResultDataContent.row, ResultDataContent.graph );
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
    public void shouldProduceResultStreamWithLegacyRestFormat() throws Exception
    {
        // given
        Node[] node = {
                node( 0, properties( property( "name", "node0" ) ) ),
                node( 1, properties( property( "name", "node1" ) ) ),
                node( 2, properties( property( "name", "node2" ) ) )};
        Relationship[] rel = {
                relationship( 0, node[0], "KNOWS", node[1], property( "name", "rel0" ) ),
                relationship( 1, node[2], "LOVES", node[1], property( "name", "rel1" ) )};
        Path path = GraphMock.path( node[0], link( rel[0], node[1] ), link( rel[1], node[2] ) );

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ExecutionResultSerializer serializer = new ExecutionResultSerializer(
                output, URI.create( "http://base.uri/" ), NullLogProvider.getInstance() );

        // when
        serializer.statementResult( mockExecutionResult(
                map( "node", node[0], "rel", rel[0], "path", path, "map", map( "n1", node[1], "r1", rel[1] ) )
        ), false, ResultDataContent.rest );
        serializer.finish();

        // then
        String result = output.toString( "UTF-8" );
        JsonNode json = jsonNode( result );
        Map<String, Integer> columns = new HashMap<>();
        int col = 0;
        JsonNode results = json.get( "results" ).get( 0 );
        for ( JsonNode column : results.get( "columns" ) )
        {
            columns.put( column.getTextValue(), col++ );
        }
        JsonNode row = results.get( "data" ).get( 0 ).get( "rest" );
        JsonNode jsonNode = row.get( columns.get( "node" ) );
        JsonNode jsonRel = row.get( columns.get( "rel" ) );
        JsonNode jsonPath = row.get( columns.get( "path" ) );
        JsonNode jsonMap = row.get( columns.get( "map" ) );
        assertEquals( "http://base.uri/node/0", jsonNode.get( "self" ).getTextValue() );
        assertEquals( "http://base.uri/relationship/0", jsonRel.get( "self" ).getTextValue() );
        assertEquals( 2, jsonPath.get( "length" ).getNumberValue() );
        assertEquals( "http://base.uri/node/0", jsonPath.get( "start" ).getTextValue() );
        assertEquals( "http://base.uri/node/2", jsonPath.get( "end" ).getTextValue() );
        assertEquals( "http://base.uri/node/1", jsonMap.get( "n1" ).get( "self" ).getTextValue() );
        assertEquals( "http://base.uri/relationship/1", jsonMap.get( "r1" ).get( "self" ).getTextValue() );
    }

    @Test
    public void shouldProduceResultStreamWithLegacyRestFormatAndNestedMaps() throws Exception
    {
        // given
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ExecutionResultSerializer serializer = new ExecutionResultSerializer(
                output, URI.create( "http://base.uri/" ), NullLogProvider.getInstance() );

        // when
        serializer.statementResult( mockExecutionResult(
                // RETURN {one:{two:['wait for it...', {three: 'GO!'}]}}
                map( "map", map("one", map( "two", asList("wait for it...", map("three", "GO!") ) ) ) )
        ), false, ResultDataContent.rest );
        serializer.finish();

        // then
        String result = output.toString( "UTF-8" );
        JsonNode json = jsonNode(result);
        Map<String, Integer> columns = new HashMap<>();
        int col = 0;
        JsonNode results = json.get( "results" ).get( 0 );
        for ( JsonNode column : results.get( "columns" ) )
        {
            columns.put( column.getTextValue(), col++ );
        }
        JsonNode row = results.get( "data" ).get( 0 ).get( "rest" );
        JsonNode jsonMap = row.get( columns.get( "map" ) );
        assertEquals( "wait for it...", jsonMap.get( "one" ).get( "two" ).get( 0 ).asText() );
        assertEquals( "GO!", jsonMap.get( "one" ).get( "two" ).get( 1 ).get( "three" ).asText() );
    }

    @Test
    public void shouldSerializePlanWithoutChildButAllKindsOfSupportedArguments() throws Exception
    {
        // given
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ExecutionResultSerializer serializer = new ExecutionResultSerializer(
                output, URI.create( "http://base.uri/" ), NullLogProvider.getInstance() );

        String operatorType = "Ich habe einen Plan";

        // This is the full set of types that we allow in plan arguments

        Map<String, Object> args = new HashMap<>();
        args.put( "string", "A String" );
        args.put( "bool", true );
        args.put( "number", 1 );
        args.put( "double", 2.3 );
        args.put( "listOfInts", asList(1, 2, 3) );
        args.put( "listOfListOfInts", asList( asList(1, 2, 3) ) );

        // when
        ExecutionPlanDescription planDescription = mockedPlanDescription( operatorType, NO_IDS, args, NO_PLANS );
        serializer.statementResult( mockExecutionResult( planDescription ), false, ResultDataContent.rest );
        serializer.finish();
        String resultString = output.toString( "UTF-8" );

        // then
        assertIsPlanRoot( resultString );
        Map<String, ?> rootMap = planRootMap( resultString );

        assertEquals( asSet( "operatorType", "identifiers", "children", "string", "bool", "number", "double",
                "listOfInts", "listOfListOfInts" ), rootMap.keySet() );

        assertEquals( operatorType, rootMap.get( "operatorType" ) );
        assertEquals( args.get( "string" ), rootMap.get( "string" ) );
        assertEquals( args.get( "bool" ), rootMap.get( "bool" ) );
        assertEquals( args.get( "number" ), rootMap.get( "number" ) );
        assertEquals( args.get( "double" ), rootMap.get( "double" ) );
        assertEquals( args.get( "listOfInts" ), rootMap.get( "listOfInts" ) );
        assertEquals( args.get( "listOfListOfInts" ), rootMap.get( "listOfListOfInts" ) );
    }

    @Test
    public void shouldSerializePlanWithoutChildButWithIdentifiers() throws Exception
    {
        // given
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ExecutionResultSerializer serializer = new ExecutionResultSerializer(
                output, URI.create( "http://base.uri/" ), NullLogProvider.getInstance() );

        String operatorType = "Ich habe einen Plan";
        String id1 = "id1";
        String id2 = "id2";
        String id3 = "id3";

        // This is the full set of types that we allow in plan arguments

        // when
        ExecutionPlanDescription planDescription =
                mockedPlanDescription( operatorType, asSet( id1, id2, id3 ), NO_ARGS, NO_PLANS );
        serializer.statementResult( mockExecutionResult( planDescription ), false, ResultDataContent.rest );
        serializer.finish();
        String resultString = output.toString( "UTF-8" );

        // then
        assertIsPlanRoot( resultString );
        Map<String,?> rootMap = planRootMap( resultString );

        assertEquals( asSet( "operatorType", "identifiers", "children" ), rootMap.keySet() );

        assertEquals( operatorType, rootMap.get( "operatorType" ) );
        assertEquals( asList( id2, id1, id3 ), rootMap.get( "identifiers" ) );
    }

    @Test
    public void shouldSerializePlanWithChildren() throws Exception
    {
        // given
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ExecutionResultSerializer serializer = new ExecutionResultSerializer(
                output, URI.create( "http://base.uri/" ), NullLogProvider.getInstance() );

        String leftId = "leftId";
        String rightId = "rightId";
        String parentId = "parentId";

        // when
        ExecutionPlanDescription left = mockedPlanDescription( "child", asSet( leftId ), MapUtil.map( "id", 1 ), NO_PLANS );
        ExecutionPlanDescription right = mockedPlanDescription( "child", asSet( rightId ), MapUtil.map( "id", 2 ), NO_PLANS );
        ExecutionPlanDescription parent =
                mockedPlanDescription( "parent", asSet( parentId ), MapUtil.map( "id", 0 ), asList( left, right ) );

        serializer.statementResult( mockExecutionResult( parent ), false, ResultDataContent.rest );
        serializer.finish();

        // then
        String result = output.toString( "UTF-8" );
        JsonNode root = assertIsPlanRoot( result );

        assertEquals( "parent", root.get( "operatorType" ).getTextValue() );
        assertEquals( 0, root.get( "id" ).asLong() );
        assertEquals( asSet( parentId ), identifiersOf( root ) );

        Set<Integer> childIds = new HashSet<>();
        Set<Set<String>> identifiers = new HashSet<>();
        for ( JsonNode child : root.get( "children" ) )
        {
            assertTrue( "Expected object", child.isObject() );
            assertEquals( "child", child.get( "operatorType" ).getTextValue() );
            identifiers.add( identifiersOf( child ) );
            childIds.add( child.get( "id" ).asInt() );
        }
        assertEquals( asSet( 1, 2 ), childIds );
        assertEquals( asSet( asSet( leftId ), asSet( rightId ) ), identifiers );
    }

    private Set<String> identifiersOf( JsonNode root )
    {
        Set<String> parentIds = new HashSet<>();
        for ( JsonNode id : root.get( "identifiers" ) )
        {
            parentIds.add( id.asText() );
        }
        return parentIds;
    }

    private static final Map<String,Object> NO_ARGS = Collections.emptyMap();
    private static final Set<String> NO_IDS = Collections.emptySet();
    private static final List<ExecutionPlanDescription> NO_PLANS = Collections.emptyList();

    private ExecutionPlanDescription mockedPlanDescription( String operatorType,
                                                            Set<String> identifiers,
                                                            Map<String,Object> args,
                                                            List<ExecutionPlanDescription> children )
    {
        ExecutionPlanDescription planDescription = mock( ExecutionPlanDescription.class );
        when( planDescription.getChildren() ).thenReturn( children );
        when( planDescription.getName() ).thenReturn( operatorType );
        when( planDescription.getArguments() ).thenReturn( args );
        when( planDescription.getIdentifiers() ).thenReturn( identifiers );
        return planDescription;
    }

    private JsonNode assertIsPlanRoot( String result ) throws UnsupportedEncodingException, JsonParseException
    {
        JsonNode json = jsonNode( result );
        JsonNode results = json.get( "results" ).get( 0 );

        JsonNode plan = results.get( "plan" );
        assertTrue( "Expected plan to be an object", plan != null && plan.isObject() );

        JsonNode root = plan.get("root");
        assertTrue("Expected plan to be an object", root != null && root.isObject());

        return root;
    }

    @SuppressWarnings("unchecked")
    private Map<String, ?> planRootMap( String resultString ) throws JsonParseException
    {
        Map<String, ?> resultMap = (Map<String, ?>) ((List<?>) ((Map<String, ?>) (readJson( resultString ))).get("results")).get( 0 );
        Map<String, ?> planMap = (Map<String, ?>) (resultMap.get("plan"));
        return (Map<String, ?>) (planMap.get("root"));
    }

    @Test
    public void shouldLogIOErrors() throws Exception
    {
        // given
        IOException failure = new IOException();
        OutputStream output = mock( OutputStream.class, new ThrowsException( failure ) );
        AssertableLogProvider logProvider = new AssertableLogProvider();
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, null, logProvider );

        // when
        serializer.finish();

        // then
        logProvider.assertExactly(
                AssertableLogProvider.inLog( ExecutionResultSerializer.class ).error(
                        is( "Failed to generate JSON output." ), sameInstance( failure ) )
        );
    }

    @Test
    public void shouldAbbreviateWellKnownIOErrors() throws Exception
    {
        // given
        OutputStream output = mock( OutputStream.class, new ThrowsException( new IOException("Broken pipe") ) );
        AssertableLogProvider logProvider = new AssertableLogProvider();
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, null, logProvider );

        // when
        serializer.finish();

        // then
        logProvider.assertExactly(
                AssertableLogProvider.inLog( ExecutionResultSerializer.class ).error( "Unable to reply to request, because the client has closed the connection (Broken pipe)." )
        );
    }

    @Test
    public void shouldReturnNotifications() throws IOException
    {
        // given
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, null, NullLogProvider.getInstance() );

        Notification notification = NotificationCode.CARTESIAN_PRODUCT.notification( new InputPosition( 1, 2, 3 ) );
        List<Notification> notifications = Arrays.asList( notification );
        Result executionResult = mockExecutionResult( null, notifications, map(
                "column1", "value1",
                "column2", "value2" ) );

        // when
        serializer.transactionCommitUri( URI.create( "commit/uri/1" ) );
        serializer.statementResult( executionResult, false );
        serializer.notifications( notifications );
        serializer.finish();

        // then
        String result = output.toString( "UTF-8" );

        assertEquals(
                "{\"commit\":\"commit/uri/1\",\"results\":[{\"columns\":[\"column1\",\"column2\"]," +
                        "\"data\":[{\"row\":[\"value1\",\"value2\"]}]}],\"notifications\":[{\"code\":\"Neo" +
                        ".ClientNotification.Statement.CartesianProduct\",\"severity\":\"WARNING\",\"title\":\"This " +
                        "query builds a cartesian product between disconnected patterns.\",\"description\":\"If a " +
                        "part of a query contains multiple disconnected patterns, this will build a cartesian product" +
                        " between all those parts. This may produce a large amount of data and slow down query " +
                        "processing. While occasionally intended, it may often be possible to reformulate the query " +
                        "that avoids the use of this cross product, perhaps by adding a relationship between the " +
                        "different parts or by using OPTIONAL MATCH\",\"position\":{\"offset\":1,\"line\":2," +
                        "\"column\":3}}],\"errors\":[]}", result );
    }

    @Test
    public void shouldNotReturnNotificationsWhenEmptyNotifications() throws IOException
    {
        // given
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, null, NullLogProvider.getInstance() );

        List<Notification> notifications = Collections.emptyList();
        Result executionResult = mockExecutionResult( null, notifications, map(
                "column1", "value1",
                "column2", "value2" ) );

        // when
        serializer.transactionCommitUri( URI.create( "commit/uri/1" ) );
        serializer.statementResult( executionResult, false );
        serializer.notifications( notifications );
        serializer.finish();

        // then
        String result = output.toString( "UTF-8" );

        assertEquals(
                "{\"commit\":\"commit/uri/1\",\"results\":[{\"columns\":[\"column1\",\"column2\"]," +
                        "\"data\":[{\"row\":[\"value1\",\"value2\"]}]}],\"errors\":[]}", result );
    }

    @Test
    public void shouldNotReturnPositionWhenEmptyPosition() throws IOException
    {
        // given
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, null, NullLogProvider.getInstance() );

        Notification notification = NotificationCode.CARTESIAN_PRODUCT.notification( InputPosition.empty );

        List<Notification> notifications = Arrays.asList( notification );
        Result executionResult = mockExecutionResult( null, notifications, map(
                "column1", "value1",
                "column2", "value2" ) );

        // when
        serializer.transactionCommitUri( URI.create( "commit/uri/1" ) );
        serializer.statementResult( executionResult, false );
        serializer.notifications( notifications );
        serializer.finish();

        // then
        String result = output.toString( "UTF-8" );

        assertEquals(
                "{\"commit\":\"commit/uri/1\",\"results\":[{\"columns\":[\"column1\",\"column2\"]," +
                        "\"data\":[{\"row\":[\"value1\",\"value2\"]}]}],\"notifications\":[{\"code\":\"Neo" +
                        ".ClientNotification.Statement.CartesianProduct\",\"severity\":\"WARNING\",\"title\":\"This " +
                        "query builds a cartesian product between disconnected patterns.\",\"description\":\"If a " +
                        "part of a query contains multiple disconnected patterns, this will build a cartesian product" +
                        " between all those parts. This may produce a large amount of data and slow down query " +
                        "processing. While occasionally intended, it may often be possible to reformulate the query " +
                        "that avoids the use of this cross product, perhaps by adding a relationship between the " +
                        "different parts or by using OPTIONAL MATCH\"}],\"errors\":[]}", result );
    }

    @SafeVarargs
    private static Result mockExecutionResult( Map<String, Object>... rows )
    {
        return mockExecutionResult( null, rows );
    }

    @SafeVarargs
    private static Result mockExecutionResult(ExecutionPlanDescription planDescription, Map<String, Object>... rows )
    {
        return mockExecutionResult( planDescription, Collections.<Notification>emptyList(), rows );
    }

    @SafeVarargs
    private static Result mockExecutionResult( ExecutionPlanDescription planDescription, Iterable<Notification> notifications, Map<String, Object>... rows )
    {
        Set<String> keys = new TreeSet<>();
        for ( Map<String, Object> row : rows )
        {
            keys.addAll( row.keySet() );
        }
        Result executionResult = mock( Result.class );

        when( executionResult.columns() ).thenReturn( new ArrayList<>( keys ) );

        final Iterator<Map<String, Object>> inner = asList( rows ).iterator();
        when( executionResult.hasNext() ).thenAnswer( new Answer<Boolean>()
        {
            @Override
            public Boolean answer( InvocationOnMock invocation ) throws Throwable
            {
                return inner.hasNext();
            }
        } );
        when( executionResult.next() ).thenAnswer( new Answer<Map<String,Object>>()
        {
            @Override
            public Map<String, Object> answer( InvocationOnMock invocation ) throws Throwable
            {
                return inner.next();
            }
        } );

        when( executionResult.getQueryExecutionType() )
                .thenReturn( null != planDescription
                             ? QueryExecutionType.profiled( QueryExecutionType.QueryType.READ_WRITE )
                             : QueryExecutionType.query( QueryExecutionType.QueryType.READ_WRITE ) );
        if ( executionResult.getQueryExecutionType().requestedExecutionPlanDescription() )
        {
            when( executionResult.getExecutionPlanDescription() ).thenReturn( planDescription );
        }
        mockAccept( executionResult );

        when( executionResult.getNotifications() ).thenReturn( notifications );

        return executionResult;
    }

    private static void mockAccept( Result mock )
    {
        doAnswer( new Answer<Void>()
        {
            @Override
            public Void answer( InvocationOnMock invocation ) throws Throwable
            {
                Result result = (Result) invocation.getMock();
                Result.ResultVisitor visitor = (Result.ResultVisitor) invocation.getArguments()[0];
                while ( result.hasNext() )
                {
                    visitor.visit( new MapRow( result.next() ) );
                }
                return null;
            }
        } ).when( mock )
           .accept( (Result.ResultVisitor<RuntimeException>) any( Result.ResultVisitor.class ) );
    }

    private static Path mockPath( Map<String, Object> startNodeProperties, Map<String, Object> relationshipProperties,
                                  Map<String,Object> endNodeProperties )
    {
        Node startNode = node( 1, properties( startNodeProperties ) );
        Node endNode = node( 2, properties( endNodeProperties ) );
        Relationship relationship = relationship( 1, properties( relationshipProperties ),
                                                  startNode, "RELATED", endNode );
        return path( startNode, Link.link( relationship, endNode ) );
    }

    private String replaceStackTrace( String json, String matchableStackTrace )
    {
        return json.replaceAll( "\"stackTrace\":\"[^\"]*\"", "\"stackTrace\":" + matchableStackTrace );
    }
}
