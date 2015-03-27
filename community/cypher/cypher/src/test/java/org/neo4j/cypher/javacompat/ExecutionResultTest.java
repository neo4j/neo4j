/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.javacompat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.cypher.ArithmeticException;
import org.neo4j.cypher.ExtendedExecutionResult;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.test.TestGraphDatabaseFactory;
import scala.NotImplementedError;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExecutionResultTest
{
    private GraphDatabaseAPI db;
    private ExecutionEngine engine;

    @Before
    public void setUp() throws Exception
    {
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .newGraphDatabase();
        engine = new ExecutionEngine( db );
    }

    @Test
    public void shouldCloseTransactionsWhenIteratingResults() throws Exception
    {
        // Given an execution result that has been started but not exhausted
        createNode();
        createNode();
        ExecutionResult executionResult = engine.execute( "MATCH (n) RETURN n" );
        ResourceIterator<Map<String, Object>> resultIterator = executionResult.iterator();
        resultIterator.next();
        assertThat( activeTransaction(), is( notNullValue() ) );

        // When
        resultIterator.close();

        // Then
        assertThat( activeTransaction(), is( nullValue() ) );
    }

    @Test
    public void shouldCloseTransactionsWhenIteratingOverSingleColumn() throws Exception
    {
        // Given an execution result that has been started but not exhausted
        createNode();
        createNode();
        ExecutionResult executionResult = engine.execute( "MATCH (n) RETURN n" );
        ResourceIterator<Node> resultIterator = executionResult.columnAs( "n" );
        resultIterator.next();
        assertThat( activeTransaction(), is( notNullValue() ) );

        // When
        resultIterator.close();

        // Then
        assertThat( activeTransaction(), is( nullValue() ) );
    }

    @Test(expected = ArithmeticException.class)
    public void shouldThrowAppropriateException() throws Exception
    {
        engine.execute( "RETURN rand()/0" ).iterator().next();
    }

    @Test
    public void visitor_get_works() throws Exception
    {
        Node n = mock( Node.class );
        Relationship r = mock( Relationship.class );
        ExecutionResult objectUnderTest = createInnerExecutionResult( "a", "a", n, r );

        final List<Object> results = new ArrayList<>();
        objectUnderTest.accept( new Result.ResultVisitor()
        {
            @Override
            public boolean visit( Result.ResultRow row )
            {
                results.add( row.get( "a" ) );
                return true;
            }
        } );

        assertThat( results, containsInAnyOrder( "a", n, r ) );
    }

    @Test
    public void visitor_get_string_works() throws Exception
    {
        ExecutionResult objectUnderTest = createInnerExecutionResult( "a", "a", "b", "c" );

        final List<String> results = new ArrayList<>();
        objectUnderTest.accept( new Result.ResultVisitor()
        {
            @Override
            public boolean visit( Result.ResultRow row )
            {
                results.add( row.getString( "a" ) );
                return true;
            }
        } );

        assertThat( results, containsInAnyOrder( "a", "b", "c" ) );
    }

    @Test
    public void visitor_get_node_works() throws Exception
    {
        Node n1 = mock( Node.class );
        Node n2 = mock( Node.class );
        Node n3 = mock( Node.class );

        ExecutionResult objectUnderTest = createInnerExecutionResult( "a", n1, n2, n3 );

        final List<Node> results = new ArrayList<>();
        objectUnderTest.accept( new Result.ResultVisitor()
        {
            @Override
            public boolean visit( Result.ResultRow row )
            {
                results.add( row.getNode( "a" ) );
                return true;
            }
        } );

        assertThat( results, containsInAnyOrder( n1, n2, n3 ) );
    }

    @Test
    public void when_asking_for_a_node_when_it_is_not_a_node() throws Exception
    {
        ExecutionResult objectUnderTest = createInnerExecutionResult( "a", 42 );

        Result.ResultVisitor a = new Result.ResultVisitor()
        {
            @Override
            public boolean visit( Result.ResultRow row )
            {
                row.getNode( "a" );
                return true;
            }
        };

        try
        {
            objectUnderTest.accept( a );
            fail( "Expected an exception" );
        }
        catch ( NoSuchElementException e )
        {
            assertEquals( e.getMessage(), "The current item in column \"a\" is not a Node" );
        }
    }

    @Test
    public void when_asking_for_a_non_existing_column_throws() throws Exception
    {
        ExecutionResult objectUnderTest = createInnerExecutionResult( "a", 42 );

        Result.ResultVisitor a = new Result.ResultVisitor()
        {
            @Override
            public boolean visit( Result.ResultRow row )
            {
                row.getNode( "does not exist" );
                return true;
            }
        };

        try
        {
            objectUnderTest.accept( a );
            fail( "Expected an exception" );
        }
        catch ( IllegalArgumentException e )
        {
            assertEquals( e.getMessage(), "No column \"does not exist\" exists" );
        }
    }

    @Test
    public void when_asking_for_a_rel_when_it_is_not_a_rel() throws Exception
    {
        ExecutionResult objectUnderTest = createInnerExecutionResult( "a", 42 );

        Result.ResultVisitor a = new Result.ResultVisitor()
        {
            @Override
            public boolean visit( Result.ResultRow row )
            {
                row.getRelationship( "a" );
                return true;
            }
        };

        try
        {
            objectUnderTest.accept( a );
            fail( "Expected an exception" );
        }
        catch ( NoSuchElementException e )
        {
            assertEquals( e.getMessage(), "The current item in column \"a\" is not a Relationship" );
        }
    }

    @Test
    public void null_key_gives_a_friendly_error() throws Exception
    {
        ExecutionResult objectUnderTest = createInnerExecutionResult( "a", 42 );

        Result.ResultVisitor a = new Result.ResultVisitor()
        {
            @Override
            public boolean visit( Result.ResultRow row )
            {
                row.getLong( null );
                return true;
            }
        };

        try
        {
            objectUnderTest.accept( a );
            fail( "Expected an exception" );
        }
        catch ( IllegalArgumentException e )
        {
            assertEquals( e.getMessage(), "No column \"null\" exists" );
        }
    }

    @Test
    public void when_asking_for_a_null_value_nothing_bad_happens() throws Exception
    {
        ExecutionResult objectUnderTest = createInnerExecutionResult( "a", (Object) null );

        final List<Relationship> result = new ArrayList<>();
        Result.ResultVisitor a = new Result.ResultVisitor()
        {
            @Override
            public boolean visit( Result.ResultRow row )
            {
                result.add( row.getRelationship( "a" ) );
                return true;
            }
        };

        objectUnderTest.accept( a );
        assertThat( result, contains( (Relationship) null ) );
    }

    @Test
    public void stop_on_return_false() throws Exception
    {
        ExecutionResult objectUnderTest = createInnerExecutionResult( "a", 1l, 2l, 3l, 4l );

        final List<Long> result = new ArrayList<>();
        Result.ResultVisitor a = new Result.ResultVisitor()
        {
            @Override
            public boolean visit( Result.ResultRow row )
            {
                result.add( row.getLong( "a" ) );
                return false;
            }
        };

        objectUnderTest.accept( a );
        assertThat( result, containsInAnyOrder( 1l ) );
    }

    @Test
    public void no_unnecessary_object_creation() throws Exception
    {
        ExecutionResult objectUnderTest = createInnerExecutionResult( "a", 1l, 2l );

        final Set<Integer> result = new HashSet<>();
        Result.ResultVisitor a = new Result.ResultVisitor()
        {
            @Override
            public boolean visit( Result.ResultRow row )
            {
                result.add( row.hashCode() );
                return true;
            }
        };

        objectUnderTest.accept( a );
        assertThat( result, hasSize( 1 ) );
    }

    @Test
    public void no_outofbounds_on_empty_result() throws Exception
    {
        ExecutionResult objectUnderTest = createInnerExecutionResult( "a" );

        Result.ResultVisitor a = new Result.ResultVisitor()
        {
            @Override
            public boolean visit( Result.ResultRow row )
            {
                fail( "the visit should never be called on empty result" );
                return true;
            }
        };

        objectUnderTest.accept( a );
    }

    @Test(expected = ArithmeticException.class)
    public void shouldThrowAppropriateExceptionAlsoWhenVisiting() throws Exception
    {
        engine.execute( "RETURN rand()/0" ).accept( new Result.ResultVisitor()
        {
            @Override
            public boolean visit( Result.ResultRow row )
            {
                return true;
            }
        } );
    }


    private void createNode()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }
    }

    private org.neo4j.kernel.TopLevelTransaction activeTransaction()
    {
        ThreadToStatementContextBridge bridge = db.getDependencyResolver().resolveDependency(
                ThreadToStatementContextBridge.class );
        return bridge.getTopLevelTransactionBoundToThisThread( false );
    }

    private ExecutionResult createInnerExecutionResult( final String column, final Object... values )
    {
        ExtendedExecutionResult inner = mock( ExtendedExecutionResult.class );

        when( inner.javaIterator() ).thenReturn( new ResourceIterator<Map<String, Object>>()
        {
            int offset = 0;

            @Override
            public boolean hasNext()
            {
                return offset < values.length;
            }

            @Override
            public Map<String, Object> next()
            {
                HashMap<String, Object> result = new HashMap<>();
                result.put( column, values[offset] );
                offset += 1;
                return result;
            }

            @Override
            public void remove()
            {
                throw new NotImplementedError();
            }

            @Override
            public void close()
            {
            }
        } );

        return new ExecutionResult( inner );
    }
}
