/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.javacompat;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.test.rule.EnterpriseDatabaseRule;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.IsNull.notNullValue;

public class ExecutionResultTest
{
    @Rule
    public final EnterpriseDatabaseRule db = new EnterpriseDatabaseRule();

    @Test
    public void shouldBePossibleToConsumeCompiledExecutionResultsWithIterator()
    {
        // Given
        createNode();
        createNode();

        // When
        List<Map<String,Object>> listResult;
        try ( Result result = db.execute( "CYPHER runtime=compiled MATCH (n) RETURN n" ) )
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
        try ( Result result = db.execute( "CYPHER runtime=compiled MATCH (n) RETURN n" ) )
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
        try ( Result result = db.execute( "CYPHER runtime=compiled MATCH (n) RETURN n" ) )
        {
            result.accept( row ->
            {
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
        try ( Result result = db.execute( "CYPHER runtime=compiled MATCH (n) RETURN n" ) )
        {
            result.accept( row ->
            {
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
        try ( Result ignore = db.execute( "CYPHER runtime=compiled MATCH (n) RETURN n" ) )
        {
            // Then
            // just close result without consuming it
        }
    }

    @Test
    public void shouldCreateAndDropUniqueConstraints()
    {
        Result create = db.execute( "CREATE CONSTRAINT ON (n:L) ASSERT n.prop IS UNIQUE" );
        Result drop = db.execute( "DROP CONSTRAINT ON (n:L) ASSERT n.prop IS UNIQUE" );

        Assert.assertThat( create.getQueryStatistics().getConstraintsAdded(), equalTo( 1 ) );
        Assert.assertThat( create.getQueryStatistics().getConstraintsRemoved(), equalTo( 0 ) );
        Assert.assertThat( drop.getQueryStatistics().getConstraintsAdded(), equalTo( 0 ) );
        Assert.assertThat( drop.getQueryStatistics().getConstraintsRemoved(), equalTo( 1 ) );
    }

    @Test
    public void shouldCreateAndDropExistenceConstraints()
    {
        Result create = db.execute( "CREATE CONSTRAINT ON (n:L) ASSERT exists(n.prop)" );
        Result drop = db.execute( "DROP CONSTRAINT ON (n:L) ASSERT exists(n.prop)" );

        Assert.assertThat( create.getQueryStatistics().getConstraintsAdded(), equalTo( 1 ) );
        Assert.assertThat( create.getQueryStatistics().getConstraintsRemoved(), equalTo( 0 ) );
        Assert.assertThat( drop.getQueryStatistics().getConstraintsAdded(), equalTo( 0 ) );
        Assert.assertThat( drop.getQueryStatistics().getConstraintsRemoved(), equalTo( 1 ) );
    }

    @Test
    public void shouldShowRuntimeInExecutionPlanDescription()
    {
        // Given
        Result result = db.execute( "EXPLAIN MATCH (n) RETURN n.prop" );

        // When
        Map<String,Object> arguments = result.getExecutionPlanDescription().getArguments();

        // Then
        assertThat( arguments.get( "version" ), equalTo( "CYPHER 3.3" ) );
        assertThat( arguments.get( "planner" ), equalTo( "COST" ) );
        assertThat( arguments.get( "planner-impl" ), equalTo( "IDP" ) );
        assertThat( arguments.get( "runtime" ), notNullValue() );
        assertThat( arguments.get( "runtime-impl" ), notNullValue() );
    }

    @Test
    public void shouldShowCompiledRuntimeInExecutionPlan()
    {
        // Given
        Result result = db.execute( "EXPLAIN CYPHER runtime=compiled MATCH (n) RETURN n.prop" );

        // When
        Map<String,Object> arguments = result.getExecutionPlanDescription().getArguments();

        // Then
        assertThat( arguments.get( "version" ), equalTo( "CYPHER 3.3" ) );
        assertThat( arguments.get( "planner" ), equalTo( "COST" ) );
        assertThat( arguments.get( "planner-impl" ), equalTo( "IDP" ) );
        assertThat( arguments.get( "runtime" ), equalTo( "COMPILED" ) );
        assertThat( arguments.get( "runtime-impl" ), equalTo( "COMPILED" ) );
    }

    @Test
    public void shouldShowInterpretedRuntimeInExecutionPlan()
    {
        // Given
        Result result = db.execute( "EXPLAIN CYPHER runtime=interpreted MATCH (n) RETURN n.prop" );

        // When
        Map<String,Object> arguments = result.getExecutionPlanDescription().getArguments();

        // Then
        assertThat( arguments.get( "version" ), equalTo( "CYPHER 3.3" ) );
        assertThat( arguments.get( "planner" ), equalTo( "COST" ) );
        assertThat( arguments.get( "planner-impl" ), equalTo( "IDP" ) );
        assertThat( arguments.get( "runtime" ), equalTo( "INTERPRETED" ) );
        assertThat( arguments.get( "runtime-impl" ), equalTo( "INTERPRETED" ) );
    }

    @Test
    public void shouldShowArgumentsExecutionPlan()
    {
        // Given
        Result result = db.execute( "EXPLAIN CALL db.labels" );

        // When
        Map<String,Object> arguments = result.getExecutionPlanDescription().getArguments();

        // Then
        assertThat( arguments.get( "version" ), equalTo( "CYPHER 3.3" ) );
        assertThat( arguments.get( "planner" ), equalTo( "PROCEDURE" ) );
        assertThat( arguments.get( "planner-impl" ), equalTo( "PROCEDURE" ) );
        assertThat( arguments.get( "runtime" ), equalTo( "PROCEDURE" ) );
        assertThat( arguments.get( "runtime-impl" ), equalTo( "PROCEDURE" ) );
    }

    @Test
    public void shouldShowArgumentsInProfileExecutionPlan()
    {
        // Given
        Result result = db.execute( "PROFILE CALL db.labels" );

        // When
        Map<String,Object> arguments = result.getExecutionPlanDescription().getArguments();

        // Then
        assertThat( arguments.get( "version" ), equalTo( "CYPHER 3.3" ) );
        assertThat( arguments.get( "planner" ), equalTo( "PROCEDURE" ) );
        assertThat( arguments.get( "planner-impl" ), equalTo( "PROCEDURE" ) );
        assertThat( arguments.get( "runtime" ), equalTo( "PROCEDURE" ) );
        assertThat( arguments.get( "runtime-impl" ), equalTo( "PROCEDURE" ) );
    }

    @Test
    public void shouldShowArgumentsInSchemaExecutionPlan()
    {
        // Given
        Result result = db.execute( "EXPLAIN CREATE INDEX on :L(prop)" );

        // When
        Map<String,Object> arguments = result.getExecutionPlanDescription().getArguments();

        // Then
        assertThat( arguments.get( "version" ), equalTo( "CYPHER 3.3" ) );
        assertThat( arguments.get( "planner" ), equalTo( "PROCEDURE" ) );
        assertThat( arguments.get( "planner-impl" ), equalTo( "PROCEDURE" ) );
        assertThat( arguments.get( "runtime" ), equalTo( "PROCEDURE" ) );
        assertThat( arguments.get( "runtime-impl" ), equalTo( "PROCEDURE" ) );
    }

    private void createNode()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }
    }
}
