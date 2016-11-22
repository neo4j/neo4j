/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.javacompat;

import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.cypher.ArithmeticException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.TopLevelTransaction;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;
import static org.neo4j.helpers.collection.MapUtil.map;

public class ExecutionResultTest
{
    @Rule
    public final ImpermanentDatabaseRule db = new ImpermanentDatabaseRule();

    //TODO this test is not valid for compiled runtime as the transaction will be closed when the iterator was created
    @Test
    public void shouldCloseTransactionsWhenIteratingResults() throws Exception
    {
        // Given an execution result that has been started but not exhausted
        createNode();
        createNode();
        Result executionResult = db.execute( "CYPHER runtime=interpreted MATCH (n) RETURN n" );
        executionResult.next();
        assertThat( activeTransaction(), is( notNullValue() ) );

        // When
        executionResult.close();

        // Then
        assertThat( activeTransaction(), is( nullValue() ) );
    }

    //TODO this test is not valid for compiled runtime as the transaction will be closed when the iterator was created
    @Test
    public void shouldCloseTransactionsWhenIteratingOverSingleColumn() throws Exception
    {
        // Given an execution result that has been started but not exhausted
        createNode();
        createNode();
        Result executionResult = db.execute( "CYPHER runtime=interpreted MATCH (n) RETURN n" );
        ResourceIterator<Node> resultIterator = executionResult.columnAs( "n" );
        resultIterator.next();
        assertThat( activeTransaction(), is( notNullValue() ) );

        // When
        resultIterator.close();

        // Then
        assertThat( activeTransaction(), is( nullValue() ) );
    }

    @Test
    public void shouldThrowAppropriateException() throws Exception
    {
        try
        {
            db.execute( "RETURN rand()/0" ).next();
        }
        catch ( QueryExecutionException ex )
        {
            assertThat( ex.getCause(), instanceOf( QueryExecutionKernelException.class ) );
            assertThat( ex.getCause().getCause(), instanceOf( ArithmeticException.class ) );
        }
    }

    @Test( expected = ArithmeticException.class )
    public void shouldThrowAppropriateExceptionAlsoWhenVisiting() throws Exception
    {
        db.execute( "RETURN rand()/0" ).accept( row -> true );
    }

    @Test
    public void shouldBePossibleToConsumeCompiledExecutionResultsWithIterator()
    {
        // Given
        createNode();
        createNode();

        // When
        List<Map<String,Object>> listResult;
        try ( Result result = db.execute( "CYPHER runtime=compiledExperimentalFeatureNotSupportedForProductionUse MATCH (n) RETURN n" ) )
        {
            listResult = Iterators.asList( result );
        }

        // Then
        assertThat( listResult, hasSize( 2 ) );
    }

    @Test
    public void shouldBePossibleToCloseNotFullyConsumedCompiledExecutionResults()
    {
        // Given
        createNode();
        createNode();

        // When
        Map<String,Object> firstRow = null;
        try ( Result result = db.execute( "CYPHER runtime=compiledExperimentalFeatureNotSupportedForProductionUse MATCH (n) RETURN n" ) )
        {
            if ( result.hasNext() )
            {
                firstRow = result.next();
            }
        }

        // Then
        assertThat( firstRow, notNullValue() );
    }

    @Test
    public void shouldBePossibleToConsumeCompiledExecutionResultsWithVisitor()
    {
        // Given
        createNode();
        createNode();

        // When
        final List<Result.ResultRow> listResult = new ArrayList<>();
        try ( Result result = db.execute( "CYPHER runtime=compiledExperimentalFeatureNotSupportedForProductionUse MATCH (n) RETURN n" ) )
        {
            result.accept( row -> {
                listResult.add( row );
                return true;
            } );
        }

        // Then
        assertThat( listResult, hasSize( 2 ) );
    }

    @Test
    public void shouldBePossibleToCloseNotFullyVisitedCompiledExecutionResult()
    {
        // Given
        createNode();
        createNode();

        // When
        final List<Result.ResultRow> listResult = new ArrayList<>();
        try ( Result result = db.execute( "CYPHER runtime=compiledExperimentalFeatureNotSupportedForProductionUse MATCH (n) RETURN n" ) )
        {
            result.accept( row -> {
                listResult.add( row );
                // return false so that no more result rows would be visited
                return false;
            } );
        }

        // Then
        assertThat( listResult, hasSize( 1 ) );
    }

    @Test
    public void shouldBePossibleToCloseNotConsumedCompiledExecutionResult()
    {
        // Given
        createNode();

        // When
        try ( Result ignore = db.execute( "CYPHER runtime=compiledExperimentalFeatureNotSupportedForProductionUse MATCH (n) RETURN n" ) )
        {
            // Then
            // just close result without consuming it
        }
    }

    private void createNode()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }
    }

    @Test
    public void shouldHandleListsOfPointsAsInput()
    {
        // Given
        Point point1 =
                (Point) db.execute( "RETURN point({latitude: 12.78, longitude: 56.7}) as point" ).next().get( "point" );
        Point point2 =
                (Point) db.execute( "RETURN point({latitude: 12.18, longitude: 56.2}) as point" ).next().get( "point" );

        // When
        double distance = (double) db.execute( "RETURN distance({points}[0], {points}[1]) as dist",
                map( "points", asList( point1, point2 ) ) ).next().get( "dist" );
        // Then
        assertThat( Math.round( distance ), equalTo( 86107L ) );
    }

    @Test
    public void shouldHandleMapWithPointsAsInput()
    {
        // Given
        Point point1 = (Point) db.execute( "RETURN point({latitude: 12.78, longitude: 56.7}) as point"  ).next().get( "point" );
        Point point2 = (Point) db.execute( "RETURN point({latitude: 12.18, longitude: 56.2}) as point"  ).next().get( "point" );

        // When
        double distance = (double) db.execute( "RETURN distance({points}['p1'], {points}['p2']) as dist",
                map( "points", map("p1", point1, "p2", point2) ) ).next().get( "dist" );
        // Then
        assertThat(Math.round( distance ), equalTo(86107L));
    }

    private TopLevelTransaction activeTransaction()
    {
        ThreadToStatementContextBridge bridge = db.getDependencyResolver().resolveDependency(
                ThreadToStatementContextBridge.class );
        KernelTransaction kernelTransaction = bridge.getTopLevelTransactionBoundToThisThread( false );
        return kernelTransaction == null ? null : new TopLevelTransaction( kernelTransaction, null );
    }
}
