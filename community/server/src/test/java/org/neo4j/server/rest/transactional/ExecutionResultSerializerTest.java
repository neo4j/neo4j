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

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.iterator;
import static org.neo4j.helpers.collection.IteratorUtil.singletonIterator;
import static org.neo4j.helpers.collection.MapUtil.map;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.server.rest.transactional.error.Neo4jError;
import org.neo4j.server.rest.transactional.error.UnableToStartTransactionError;
import org.neo4j.server.rest.transactional.error.UnknownStatementError;
import org.neo4j.server.rest.web.TransactionUriScheme;

public class ExecutionResultSerializerTest
{
    @Test
    public void shouldSerializeSuccessfulExecutionResult() throws Exception
    {
        // Given
        ExecutionResult stmtResult = mock( ExecutionResult.class );

        List<String> expectedColumns = asList( "Number", "String", "Boolean", "PropContainer", "Path" );
        when( stmtResult.columns() ).thenReturn( expectedColumns );

        int theNumber = 1337;
        String theString = "Stuff";
        boolean theBoolean = true;
        PropertyContainer thePropContainer = mockPropertyContainer();
        Path thePath = mockPath();

        when( stmtResult.iterator() ).thenReturn( singletonIterator(
                map( "Number", theNumber,
                        "String", theString,
                        "Boolean", theBoolean,
                        "PropContainer", thePropContainer,
                        "Path", thePath ) ) );

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, txUriScheme );

        // When
        serializer.prologue( 1337l );
        serializer.visitStatementResult( stmtResult );
        serializer.epilogue( NO_ERRORS );

        // Then
        Map<String, Object> result = deserialize( output );
        assertThat( result.keySet(), equalTo( asSet( "results", "errors", "commit" ) ) );
        assertThat( ((List) result.get( "errors" )).size(), equalTo( 0 ) );
        assertThat( ((String) result.get( "commit" )), equalTo( "transaction/1337/commit" ) );

        List<Map<String, Object>> results = listOfMaps( result.get( "results" ) );
        assertThat( results.size(), equalTo( 1 ) );

        List<String> columns = listOfStrings( results.get( 0 ).get( "columns" ) );
        assertThat( asSet( columns ), equalTo( asSet( expectedColumns ) ) );

        List<List<Object>> data = listOfLists( results.get( 0 ).get( "data" ) );
        assertThat( data.size(), is( 1 ) );

        // And all known output types should have been correctly serialized

        List<Object> row = data.get( 0 );
        assertThat( (Integer) row.get( 0 ), equalTo( theNumber ) );
        assertThat( (String) row.get( 1 ), equalTo( theString ) );
        assertThat( (Boolean) row.get( 2 ), equalTo( theBoolean ) );
        assertThat( mapOfStringToObject( row.get( 3 ) ), equalTo( map(
                "a", 12,
                "b", true,
                "c", asList( 1, 0, 1, 2 ),
                "d", asList( 1, 0, 1, 2 ),
                "e", asList( "a", "b", "ääö" ) ) ) );

        //noinspection unchecked
        assertThat( listOfMaps( row.get( 4 ) ), equalTo( asList(
                map(),
                map( "a", 12 ),
                map() ) ) );
    }

    @Test
    public void shouldSerializeResponseWithNoResultsOrErrors() throws Exception
    {
        // Given
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, txUriScheme );

        // When
        serializer.prologue( 1337l );
        serializer.epilogue( NO_ERRORS );

        // Then
        Map<String, Object> result = deserialize( output );

        assertThat( ((List) result.get( "errors" )).size(), equalTo( 0 ) );
        assertThat( ((String) result.get( "commit" )), equalTo( "transaction/1337/commit" ) );
        assertThat( result.keySet().size(), equalTo( 3 ) );
    }

    @Test
    public void shouldSerializeResponseWithWithErrorsButNoResults() throws Exception
    {
        // Given
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, txUriScheme );

        // When
        serializer.prologue( 1337l );
        serializer.epilogue( iterator(
                new UnableToStartTransactionError( null ),
                new UnknownStatementError( "SOME STRING", null ) ) );

        // Then
        Map<String, Object> result = deserialize( output );

        assertThat( ((String) result.get( "commit" )), equalTo( "transaction/1337/commit" ) );
        assertThat( result.keySet().size(), equalTo( 3 ) );
        List<Map<String, Object>> errors = listOfMaps( result.get( "errors" ) );

        assertThat( errors.size(), equalTo( 2 ) );
        assertThat( (Long.valueOf( (Integer) errors.get( 0 ).get( "code" ) )),
                equalTo( Neo4jError.Code.UNABLE_TO_START_TRANSACTION.getCode() ) );
        assertThat( (Long.valueOf( (Integer) errors.get( 1 ).get( "code" ) )),
                equalTo( Neo4jError.Code.UNKNOWN_STATEMENT_ERROR.getCode() ) );
        assertThat( ((String) errors.get( 1 ).get( "message" )),
                containsString( "SOME STRING" ) );
    }

    @Test
    public void shouldProduceValidJSONEvenIfExecutionResultThrowsError() throws Exception
    {
        // Given
        ExecutionResult stmtResult = mock( ExecutionResult.class );

        List<String> expectedColumns = asList( "Number", "String", "Boolean", "PropContainer", "Path" );
        when( stmtResult.columns() ).thenReturn( expectedColumns );
        when( stmtResult.iterator() ).thenReturn( new ExplodingIterator() );

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, txUriScheme );

        // When
        serializer.prologue( 1337l );
        try
        {
            serializer.visitStatementResult( stmtResult );
            fail( "Should have thrown exception" );
        }
        catch ( UnknownStatementError e )
        {
            // expected
        }
        serializer.epilogue( NO_ERRORS );

        // Then result can be deserialized.
        deserialize( output );
    }

    private static final Iterator<Neo4jError> NO_ERRORS = IteratorUtil.iterator();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TransactionUriScheme txUriScheme = new TransactionUriScheme()
    {
        @Override
        public URI txUri( long id )
        {
            return URI.create( "transaction/" + id );
        }

        @Override
        public URI txCommitUri( long id )
        {
            return URI.create( "transaction/" + id + "/commit" );
        }
    };

    private PropertyContainer mockPropertyContainer()
    {
        Map<String, Object> props = map(
                "a", 12,
                "b", true,
                "c", new int[]{1, 0, 1, 2},
                "d", new byte[]{1, 0, 1, 2},
                "e", new String[]{"a", "b", "ääö"} );
        Node node = mock( Node.class );
        when( node.getPropertyKeys() ).thenReturn( props.keySet() );
        for ( Map.Entry<String, Object> entry : props.entrySet() )
        {
            when( node.getProperty( entry.getKey() ) ).thenReturn( entry.getValue() );
        }

        return node;
    }

    private Path mockPath()
    {
        Node firstNode = mock( Node.class );
        when( firstNode.getPropertyKeys() ).thenReturn( Collections.<String>emptyList() );

        Relationship rel = mock( Relationship.class );
        when( rel.getPropertyKeys() ).thenReturn( asList( "a" ) );
        when( rel.getProperty( "a" ) ).thenReturn( 12 );

        Node endNode = mock( Node.class );
        when( endNode.getPropertyKeys() ).thenReturn( Collections.<String>emptyList() );

        Path p = mock( Path.class );
        when( p.iterator() ).thenReturn( IteratorUtil.<PropertyContainer>iterator( firstNode, rel, endNode ) );
        return p;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> listOfMaps( Object value )
    {
        return (List<Map<String, Object>>) value;
    }

    @SuppressWarnings("unchecked")
    private static List<List<Object>> listOfLists( Object value )
    {
        return (List<List<Object>>) value;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> mapOfStringToObject( Object value )
    {
        return (Map<String, Object>) value;
    }

    @SuppressWarnings("unchecked")
    private static List<String> listOfStrings( Object value )
    {
        return (List<String>) value;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> deserialize( ByteArrayOutputStream output ) throws IOException
    {
        return OBJECT_MAPPER.readValue( output.toByteArray(), Map.class );
    }

    private static class ExplodingIterator implements Iterator<Map<String, Object>>
    {
        @Override
        public boolean hasNext()
        {
            return true;
        }

        @Override
        public Map<String, Object> next()
        {
            throw new RuntimeException( "NOBODY EXPECTED THIS!" );
        }

        @Override
        public void remove()
        {
        }
    }
}
