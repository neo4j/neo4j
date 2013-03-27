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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.iterator;
import static org.neo4j.helpers.collection.MapUtil.map;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.RuntimeException;
import java.lang.String;
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

public class ExecutionResultSerializerTest
{

    public static final Iterator<Neo4jError> NO_ERRORS = IteratorUtil.<Neo4jError>iterator();
    static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    public void shouldSerializeEmpty() throws Exception
    {
        // Given
        ByteArrayOutputStream baos = new ByteArrayOutputStream(  );
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( baos );

        // When
        serializer.begin( 1337l );
        serializer.finish( NO_ERRORS );

        // Then
        Map<String, Object> result = deserialize( baos );

        assertThat( ((List)result.get( "errors" )).size(), equalTo( 0 ) );
        assertThat( ((Integer)result.get( "txId" )), equalTo( 1337 ) );
        assertThat( result.keySet().size(), equalTo( 3 ) );
    }

    @Test
    public void shouldSerializeEmptyWithErrors() throws Exception
    {
        // Given
        ByteArrayOutputStream baos = new ByteArrayOutputStream(  );
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( baos );

        // When
        serializer.begin( 1337l );
        serializer.finish( iterator(
                new UnableToStartTransactionError( null ),
                new UnknownStatementError( "SOME STRING", null ) ) );

        // Then
        Map<String, Object> result = deserialize( baos );

        assertThat( ((Integer)result.get( "txId" )), equalTo( 1337 ) );
        assertThat( result.keySet().size(), equalTo( 3 ) );
        List<Map> errors = (List<Map>) result.get( "errors" );

        assertThat( errors.size(), equalTo( 2 ) );
        assertThat( (Long.valueOf( (Integer)errors.get( 0 ).get( "code" ))),
                equalTo( Neo4jError.Code.UNABLE_TO_START_TRANSACTION.getCode() ) );
        assertThat( (Long.valueOf( (Integer)errors.get( 1 ).get( "code" ))),
                equalTo( Neo4jError.Code.UNKNOWN_STATEMENT_ERROR.getCode() ) );
        assertThat( ((String)errors.get( 1 ).get( "message" )),
                containsString("SOME STRING") );
    }

    @Test
    public void shouldSerializeFriendlyExecutionResult() throws Exception
    {
        // Given
        ExecutionResult stmtResult = mock(ExecutionResult.class);

        List<String> expectedColumns = asList( "Number", "String", "Boolean", "PropContainer", "Path" );
        when( stmtResult.columns() ).thenReturn( expectedColumns );

        int theNumber = 1337;
        String theString = "Stuff";
        boolean theBoolean = true;
        PropertyContainer thePropContainer = mockPropertyContainer();
        Path thePath = mockPath();

        when( stmtResult.iterator() ).thenReturn( iterator(
                map("Number", theNumber,
                    "String", theString,
                    "Boolean", theBoolean,
                    "PropContainer", thePropContainer,
                    "Path", thePath) ) );

        ByteArrayOutputStream baos = new ByteArrayOutputStream(  );
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( baos );

        // When
        serializer.begin( 1337l );
        serializer.visitStatementResult( stmtResult );
        serializer.finish( NO_ERRORS );

        // Then
        Map<String, Object> result = deserialize( baos );
        assertThat( result.keySet(), equalTo( asSet( "results", "errors", "txId" ) ) );
        assertThat( ((List)result.get( "errors" )).size(), equalTo( 0 ) );
        assertThat( ((Integer)result.get( "txId" )), equalTo( 1337 ) );

        List<Map<String, Object>> results = (List<Map<String, Object>>) result.get( "results" );
        assertThat( results.size(), equalTo( 1 ) );

        List<String> columns = (List<String>) results.get( 0 ).get( "columns" );
        assertThat( asSet( columns ), equalTo( asSet( expectedColumns ) ) );

        List<List<Object>> data = (List<List<Object>>) results.get( 0 ).get( "data" );
        assertThat( data.size(), is( 1 ) );

        // And all known output types should have been correctly serialized

        List<Object> row = data.get( 0 );
        assertThat( (Integer)row.get( 0 ), equalTo( theNumber ) );
        assertThat( (String) row.get( 1 ), equalTo( theString ) );
        assertThat( (Boolean) row.get( 2 ), equalTo( theBoolean ) );
        assertThat( (Map<String, Object>) row.get( 3 ), equalTo( map(
                "a", 12,
                "b", true,
                "c", asList(1, 0, 1, 2),
                "d", asList(1, 0, 1, 2),
                "e", asList( "a", "b", "ääö") ) ) );

        assertThat( (List<Map<String, Object>>) row.get( 4 ), equalTo( asList(
                map(),
                map("a", 12),
                map() ) ));

    }

    @Test
    public void shouldProduceValidJSONEvenIfExecutionResultThrowsError() throws Exception
    {
        // Given
        ExecutionResult stmtResult = mock(ExecutionResult.class);

        List<String> expectedColumns = asList( "Number", "String", "Boolean", "PropContainer", "Path" );
        when( stmtResult.columns() ).thenReturn( expectedColumns );


        Iterator<Map<String, Object>> rows = mock( Iterator.class );
        when( rows.hasNext() ).thenReturn( true );
        when( rows.next() ).thenThrow( new RuntimeException( "NOBODY EXPECTED THIS!" ) );
        when( stmtResult.iterator() ).thenReturn( rows );

        ByteArrayOutputStream baos = new ByteArrayOutputStream(  );
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( baos );

        // When
        serializer.begin( 1337l );
        try {
            serializer.visitStatementResult( stmtResult );
        } catch( UnknownStatementError e )
        {
            // OK
        }
        serializer.finish( NO_ERRORS );

        // Then
        Map<String, Object> result = deserialize( baos );
    }

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

    private Map deserialize( ByteArrayOutputStream baos ) throws IOException
    {
        return OBJECT_MAPPER.readValue( baos.toByteArray(), Map.class );
    }

}
