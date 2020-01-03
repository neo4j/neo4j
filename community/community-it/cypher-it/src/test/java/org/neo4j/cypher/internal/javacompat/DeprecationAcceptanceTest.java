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
package org.neo4j.cypher.internal.javacompat;

import org.hamcrest.Matcher;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.SeverityLevel;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.procedure.Procedure;

import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class DeprecationAcceptanceTest extends NotificationTestSupport
{
    // DEPRECATED PRE-PARSER OPTIONS

    @Test
    public void deprecatedRulePlanner()
    {
        // when
        Result result = db().execute( "EXPLAIN CYPHER planner=rule RETURN 1" );

        // then
        assertThat( result.getNotifications(), containsItem( deprecatedRulePlanner ) );
        result.close();
    }

    @Test
    public void deprecatedCompiledRuntime()
    {
        // when
        Result result = db().execute( "EXPLAIN CYPHER runtime=compiled RETURN 1" );

        // then
        assertThat( result.getNotifications(), containsItem( deprecatedCompiledRuntime ) );
        result.close();
    }

    // DEPRECATED FUNCTIONS

    @Test
    public void deprecatedToInt()
    {
        Stream.of( "CYPHER 3.1", "CYPHER 3.5" )
                .forEach( version -> assertNotifications( version + " EXPLAIN RETURN toInt('1') AS one",
                                                          containsItem( deprecatedFeatureWarning ) ) );
    }

    @Test
    public void deprecatedUpper()
    {
        Stream.of( "CYPHER 3.1", "CYPHER 3.5" )
                .forEach( version -> assertNotifications( version + " EXPLAIN RETURN upper('foo') AS one",
                                                          containsItem( deprecatedFeatureWarning ) ) );
    }

    @Test
    public void deprecatedLower()
    {
        Stream.of( "CYPHER 3.1", "CYPHER 3.5" )
                .forEach( version -> assertNotifications( version + " EXPLAIN RETURN lower('BAR') AS one",
                                                          containsItem( deprecatedFeatureWarning ) ) );
    }

    @Test
    public void deprecatedRels()
    {
        Stream.of( "CYPHER 3.1", "CYPHER 3.5" )
                .forEach( version -> assertNotifications( version + " EXPLAIN MATCH p = ()-->() RETURN rels(p) AS r",
                                                          containsItem( deprecatedFeatureWarning ) ) );
    }

    @Test
    public void deprecatedFilter()
    {
        assertNotifications( "EXPLAIN WITH [1,2,3] AS list RETURN filter(x IN list WHERE x % 2 = 1) AS odds",
                                                          containsItem( deprecatedFeatureWarning ) );
    }

    @Test
    public void deprecatedExtract()
    {
        assertNotifications( "EXPLAIN WITH [1,2,3] AS list RETURN extract(x IN list | x * 10) AS tens",
                             containsItem( deprecatedFeatureWarning ) );
    }

    @Test
    public void deprecatedParameterSyntax()
    {
        assertNotifications( "EXPLAIN RETURN {param} AS parameter",
                containsItem( deprecatedParameterSyntax ) );
    }

    @Test
    public void deprecatedParameterSyntaxForPropertyMap()
    {
        assertNotifications( "EXPLAIN CREATE (:Label {props})", containsItem( deprecatedParameterSyntax ) );
    }

    @Test
    public void deprecatedFilterShouldNotHitCacheForNewVersion()
    {
        assertNotifications( "EXPLAIN WITH [1,2,3] AS list RETURN filter(x IN list WHERE x % 2 = 1) AS odds",
                containsItem( deprecatedFeatureWarning ) );

        try ( Result result = db().execute( "EXPLAIN WITH [1,2,3] AS list RETURN [x IN list WHERE x % 2 = 1] AS odds" ) )
        {
            assertFalse( result.getNotifications().iterator().hasNext() );
        }

    }

    @Test
    public void deprecatedExtractShouldNotHitCacheForNewVersion()
    {
        assertNotifications( "EXPLAIN WITH [1,2,3] AS list RETURN extract(x IN list | x * 10) AS tens",
                containsItem( deprecatedFeatureWarning ) );

        try ( Result result = db().execute( "EXPLAIN WITH [1,2,3] AS list RETURN [x IN list | x * 10] AS tens" ) )
        {
            assertFalse( result.getNotifications().iterator().hasNext() );
        }
    }

    @Test
    public void deprecatedProcedureCalls() throws Exception
    {
        db().getDependencyResolver().provideDependency( Procedures.class ).get().registerProcedure( TestProcedures.class );
        Stream.of( "CYPHER 3.1", "CYPHER 3.5" ).forEach( version ->
                                                         {
                                                             assertNotifications( version + "explain CALL oldProc()",
                                                                                  containsItem( deprecatedProcedureWarning ) );
                                                             assertNotifications( version + "explain CALL oldProc() RETURN 1",
                                                                                  containsItem( deprecatedProcedureWarning ) );
                                                         } );
    }

    // DEPRECATED PROCEDURE THINGS

    @Test
    public void deprecatedProcedureResultField() throws Exception
    {
        db().getDependencyResolver().provideDependency( Procedures.class ).get().registerProcedure( TestProcedures.class );
        Stream.of( "CYPHER 3.5" ).forEach(
                version -> assertNotifications(
                        version + "explain CALL changedProc() YIELD oldField RETURN oldField",
                        containsItem( deprecatedProcedureReturnFieldWarning )
                ) );
    }

    // DEPRECATED START

    @Test
    public void deprecatedStartAllNodeScan()
    {
        assertNotifications( "EXPLAIN START n=node(*) RETURN n", containsItem( deprecatedStartWarning ) );
    }

    @Test
    public void deprecatedStartNodeById()
    {
        assertNotifications( "EXPLAIN START n=node(1337) RETURN n", containsItem( deprecatedStartWarning ) );
    }

    @Test
    public void deprecatedStartNodeByIds()
    {
        assertNotifications( "EXPLAIN START n=node(42,1337) RETURN n", containsItem( deprecatedStartWarning ) );
    }

    @Test
    public void deprecatedStartNodeIndexSeek()
    {
        try ( Transaction ignore = db().beginTx() )
        {
            db().index().forNodes( "index" );
        }
        assertNotifications( "EXPLAIN START n=node:index(key = 'value') RETURN n", containsItem( deprecatedStartWarning ) );
    }

    @Test
    public void deprecatedStartNodeIndexSearch()
    {
        try ( Transaction ignore = db().beginTx() )
        {
            db().index().forNodes( "index" );
        }
        assertNotifications( "EXPLAIN START n=node:index('key:value*') RETURN n", containsItem( deprecatedStartWarning ) );
    }

    @Test
    public void deprecatedStartAllRelScan()
    {
        assertNotifications( "EXPLAIN START r=relationship(*) RETURN r", containsItem( deprecatedStartWarning ) );
    }

    @Test
    public void deprecatedStartRelById()
    {
        assertNotifications( "EXPLAIN START r=relationship(1337) RETURN r", containsItem( deprecatedStartWarning ) );
    }

    @Test
    public void deprecatedStartRelByIds()
    {
        assertNotifications( "EXPLAIN START r=relationship(42,1337) RETURN r", containsItem( deprecatedStartWarning ) );
    }

    @Test
    public void deprecatedStartRelIndexSeek()
    {
        try ( Transaction ignore = db().beginTx() )
        {
            db().index().forRelationships( "index" );
        }
        assertNotifications( "EXPLAIN START r=relationship:index(key = 'value') RETURN r", containsItem( deprecatedStartWarning ) );
    }

    @Test
    public void deprecatedStartRelIndexSearch()
    {
        try ( Transaction ignore = db().beginTx() )
        {
            db().index().forRelationships( "index" );
        }
        assertNotifications( "EXPLAIN START r=relationship:index('key:value*') RETURN r", containsItem( deprecatedStartWarning ) );
    }

    // DEPRECATED CREATE UNIQUE

    @Test
    public void shouldNotifyWhenUsingCreateUniqueWhenCypherVersionIsDefault()
    {
        // when
        Result result = db().execute( "MATCH (b) WITH b LIMIT 1 CREATE UNIQUE (b)-[:REL]->()" );

        // then
        assertThat( result.getNotifications(), containsItem( deprecatedCreateUnique ) );
        result.close();
    }

    @Test
    public void shouldNotifyWhenUsingCreateUniqueWhenCypherVersionIs3_5()
    {
        // when
        Result result = db().execute( "CYPHER 3.5 MATCH (b) WITH b LIMIT 1 CREATE UNIQUE (b)-[:REL]->()" );
        InputPosition position = new InputPosition( 36, 1, 37 );

        // then
        assertThat( result.getNotifications(), containsItem( deprecatedCreateUnique ) );
        result.close();
    }

    // DEPRECATED SYNTAX

    @Test
    public void deprecatedFutureAmbiguousRelTypeSeparator()
    {
        List<String> deprecatedQueries = Arrays.asList( "explain MATCH (a)-[:A|:B|:C {foo:'bar'}]-(b) RETURN a,b", "explain MATCH (a)-[x:A|:B|:C]-() RETURN a",
                                                        "explain MATCH (a)-[:A|:B|:C*]-() RETURN a" );

        List<String> nonDeprecatedQueries =
                Arrays.asList( "explain MATCH (a)-[:A|B|C {foo:'bar'}]-(b) RETURN a,b", "explain MATCH (a)-[:A|:B|:C]-(b) RETURN a,b",
                               "explain MATCH (a)-[:A|B|C]-(b) RETURN a,b" );

        for ( String query : deprecatedQueries )
        {
            assertNotifications( "CYPHER 3.5 " + query, containsItem( deprecatedSeparatorWarning ) );
        }

        for ( String query : nonDeprecatedQueries )
        {
            assertNotifications( "CYPHER 3.5 " + query, containsNoItem( deprecatedSeparatorWarning ) );
        }
    }

    @Test
    public void deprecatedBindingVariableLengthRelationship()
    {
        assertNotifications( "CYPHER 3.5 explain MATCH ()-[rs*]-() RETURN rs", containsItem( deprecatedBindingWarning
        ) );

        assertNotifications( "CYPHER 3.5 explain MATCH p = ()-[*]-() RETURN relationships(p) AS rs", containsNoItem(
                deprecatedBindingWarning ) );
    }

    // MATCHERS & HELPERS

    public static class ChangedResults
    {
        @Deprecated
        public final String oldField = "deprecated";
        public final String newField = "use this";
    }

    public static class TestProcedures
    {

        @Procedure( "newProc" )
        public void newProc()
        {
        }

        @Deprecated
        @Procedure( name = "oldProc", deprecatedBy = "newProc" )
        public void oldProc()
        {
        }

        @Procedure( "changedProc" )
        public Stream<ChangedResults> changedProc()
        {
            return Stream.of( new ChangedResults() );
        }
    }

    private Matcher<Notification> deprecatedFeatureWarning =
            deprecation( "The query used a deprecated function." );

    private Matcher<Notification> deprecatedRulePlanner =
            deprecation( "The rule planner, which was used to plan this query, is deprecated and will " +
                         "be discontinued soon. If you did not explicitly choose the rule planner, " +
                         "you should try to change your query so that the rule planner is not used" );

    private Matcher<Notification> deprecatedCompiledRuntime =
            deprecation( "The compiled runtime, which was requested to execute this query, is deprecated " +
                         "and will be removed in a future release." );

    private Matcher<Notification> deprecatedStartWarning =
            deprecation( "START has been deprecated and will be removed in a future version. " );

    private Matcher<Notification> deprecatedCreateUnique =
            deprecation( "CREATE UNIQUE is deprecated and will be removed in a future version." );

    private Matcher<Notification> deprecatedProcedureWarning =
            deprecation( "The query used a deprecated procedure." );

    private Matcher<Notification> deprecatedProcedureReturnFieldWarning =
            deprecation( "The query used a deprecated field from a procedure." );

    private Matcher<Notification> deprecatedBindingWarning =
            deprecation( "Binding relationships to a list in a variable length pattern is deprecated." );

    private Matcher<Notification> deprecatedSeparatorWarning =
            deprecation( "The semantics of using colon in the separation of alternative relationship " +
                         "types in conjunction with the use of variable binding, inlined property " +
                         "predicates, or variable length will change in a future version." );

    private Matcher<Notification> deprecatedParameterSyntax =
            deprecation( "The parameter syntax `{param}` is deprecated, please use `$param` instead" );

    private Matcher<Notification> deprecation( String message )
    {
        return notification( "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                             containsString( message ), any( InputPosition.class ), SeverityLevel.WARNING );
    }
}
