/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cypher.internal.javacompat;

import org.junit.Assert;
import org.hamcrest.CoreMatchers;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.test.rule.EnterpriseDatabaseRule;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
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

        // Then
        // just close result without consuming it
        db.execute( "CYPHER runtime=compiled MATCH (n) RETURN n" ).close();
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
        assertThat( arguments.get( "version" ), equalTo( "CYPHER 3.4" ) );
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
        assertThat( arguments.get( "version" ), equalTo( "CYPHER 3.4" ) );
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
        assertThat( arguments.get( "version" ), equalTo( "CYPHER 3.4" ) );
        assertThat( arguments.get( "planner" ), equalTo( "COST" ) );
        assertThat( arguments.get( "planner-impl" ), equalTo( "IDP" ) );
        assertThat( arguments.get( "runtime" ), equalTo( "INTERPRETED" ) );
        assertThat( arguments.get( "runtime-impl" ), equalTo( "INTERPRETED" ) );
    }

    @Test
    public void shouldShowArgumentsExecutionPlan()
    {
        // Given
        Result result = db.execute( "EXPLAIN CALL db.labels()" );

        // When
        Map<String,Object> arguments = result.getExecutionPlanDescription().getArguments();

        // Then
        assertThat( arguments.get( "version" ), equalTo( "CYPHER 3.4" ) );
        assertThat( arguments.get( "planner" ), equalTo( "PROCEDURE" ) );
        assertThat( arguments.get( "planner-impl" ), equalTo( "PROCEDURE" ) );
        assertThat( arguments.get( "runtime" ), equalTo( "PROCEDURE" ) );
        assertThat( arguments.get( "runtime-impl" ), equalTo( "PROCEDURE" ) );
    }

    @Test
    public void shouldShowArgumentsInProfileExecutionPlan()
    {
        // Given
        Result result = db.execute( "PROFILE CALL db.labels()" );

        // When
        Map<String,Object> arguments = result.getExecutionPlanDescription().getArguments();

        // Then
        assertThat( arguments.get( "version" ), equalTo( "CYPHER 3.4" ) );
        assertThat( arguments.get( "planner" ), equalTo( "PROCEDURE" ) );
        assertThat( arguments.get( "planner-impl" ), equalTo( "PROCEDURE" ) );
        assertThat( arguments.get( "runtime" ), equalTo( "PROCEDURE" ) );
        assertThat( arguments.get( "runtime-impl" ), equalTo( "PROCEDURE" ) );
    }

    @Test
    public void shouldShowArgumentsInSchemaExecutionPlan()
    {
        // Given
        Result result = db.execute( "EXPLAIN CREATE INDEX ON :L(prop)" );

        // When
        Map<String,Object> arguments = result.getExecutionPlanDescription().getArguments();

        // Then
        assertThat( arguments.get( "version" ), equalTo( "CYPHER 3.4" ) );
        assertThat( arguments.get( "planner" ), equalTo( "PROCEDURE" ) );
        assertThat( arguments.get( "planner-impl" ), equalTo( "PROCEDURE" ) );
        assertThat( arguments.get( "runtime" ), equalTo( "PROCEDURE" ) );
        assertThat( arguments.get( "runtime-impl" ), equalTo( "PROCEDURE" ) );
    }

    @Test
    public void shouldReturnListFromSplit()
    {
        assertThat( db.execute( "RETURN split('hello, world', ',') AS s" ).next().get( "s" ), instanceOf( List.class ) );
    }

    @Test
    public void shouldReturnCorrectArrayType()
    {
        // Given
        db.execute( "CREATE (p:Person {names:['adsf', 'adf' ]})" );

        // When
        Object result = db.execute( "MATCH (n) RETURN n.names" ).next().get( "n.names" );

        // Then
        assertThat( result, CoreMatchers.instanceOf( String[].class ) );
    }

    @Test
    public void shouldContainCompletePlanFromFromLegacyVersions()
    {
        for ( String version : new String[]{"2.3", "3.1", "3.3", "3.4"} )
        {
            // Given
            Result result = db.execute( String.format( "EXPLAIN CYPHER %s MATCH (n) RETURN n", version ) );

            // When
            ExecutionPlanDescription description = result.getExecutionPlanDescription();

            // Then
            assertThat( description.getName(), equalTo( "ProduceResults" ) );
            assertThat( description.getChildren().get( 0 ).getName(), equalTo( "AllNodesScan" ) );
        }
    }

    @Test
    public void shouldContainCompleteProfileFromFromLegacyVersions()
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();

            tx.success();
        }

        for ( String version : new String[]{"2.3", "3.1", "3.3", "3.4"} )
        {
            // When
            Result result = db.execute( String.format( "PROFILE CYPHER %s MATCH (n) RETURN n", version ) );
            result.resultAsString();
            ExecutionPlanDescription.ProfilerStatistics stats =
                    result.getExecutionPlanDescription()//ProduceResult
                            .getChildren().get( 0 ) //AllNodesScan
                            .getProfilerStatistics();

            // Then
            assertThat( "Mismatching db-hits for version " + version, stats.getDbHits(), equalTo( 2L ) );
            assertThat( "Mismatching rows for version " + version, stats.getRows(), equalTo( 1L ) );

            //These stats are not available in older versions, but should at least return 0, and >0 for newer
            assertThat( "Mismatching page cache hits for version " + version, stats.getPageCacheHits(), greaterThanOrEqualTo( 0L ) );
            assertThat( "Mismatching page cache misses for version " + version, stats.getPageCacheMisses(), greaterThanOrEqualTo( 0L ) );
            assertThat( "Mismatching page cache hit ratio for version " + version, stats.getPageCacheHitRatio(), greaterThanOrEqualTo( 0.0 ) );
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
}
