/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.SeverityLevel;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.procedure.Procedure;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.containsString;

public class DeprecationAcceptanceTest extends NotificationTestSupport
{
    // DEPRECATED PRE-PARSER OPTIONS

    @Test
    void deprecatedCompiledRuntime()
    {
        // when
        try ( Transaction transaction = db.beginTx() )
        {
            try ( Result result = transaction.execute( "EXPLAIN CYPHER runtime=legacy_compiled RETURN 1" ) )
            {
                // then
                assertThat( result.getNotifications(), containsItem( deprecatedCompiledRuntime ) );
            }
        }
    }

    // DEPRECATED PROCEDURE THINGS

    @Test
    void deprecatedProcedureCalls() throws Exception
    {
        db.getDependencyResolver().provideDependency( GlobalProcedures.class ).get().registerProcedure( TestProcedures.class );
        Stream.of( "CYPHER 3.5", "CYPHER 4.0" ).forEach( version ->
        {
            assertNotifications( version + "explain CALL oldProc()",
                    containsItem( deprecatedProcedureWarning ) );
            assertNotifications( version + "explain CALL oldProc() RETURN 1",
                    containsItem( deprecatedProcedureWarning ) );
        } );
    }

    @Test
    void deprecatedProcedureResultField() throws Exception
    {
        db.getDependencyResolver().provideDependency( GlobalProcedures.class ).get().registerProcedure( TestProcedures.class );
        Stream.of( "CYPHER 4.0" ).forEach(
                version -> assertNotifications(
                        version + "explain CALL changedProc() YIELD oldField RETURN oldField",
                        containsItem( deprecatedProcedureReturnFieldWarning )
                ) );
    }

    // DEPRECATED SYNTAX

    @Test
    void deprecatedFutureAmbiguousRelTypeSeparator()
    {
        List<String> deprecatedQueries = Arrays.asList( "explain MATCH (a)-[:A|:B|:C {foo:'bar'}]-(b) RETURN a,b", "explain MATCH (a)-[x:A|:B|:C]-() RETURN a",
                                                        "explain MATCH (a)-[:A|:B|:C*]-() RETURN a" );

        List<String> nonDeprecatedQueries =
                Arrays.asList( "explain MATCH (a)-[:A|B|C {foo:'bar'}]-(b) RETURN a,b", "explain MATCH (a)-[:A|:B|:C]-(b) RETURN a,b",
                               "explain MATCH (a)-[:A|B|C]-(b) RETURN a,b" );

        for ( String query : deprecatedQueries )
        {
            assertNotifications( "CYPHER 4.0 " + query, containsItem( deprecatedSeparatorWarning ) );
        }

        for ( String query : nonDeprecatedQueries )
        {
            assertNotifications( "CYPHER 4.0 " + query, containsNoItem( deprecatedSeparatorWarning ) );
        }
    }

    @Test
    void deprecatedBindingVariableLengthRelationship()
    {
        assertNotifications( "CYPHER 4.0 explain MATCH ()-[rs*]-() RETURN rs", containsItem( deprecatedBindingWarning
        ) );

        assertNotifications( "CYPHER 4.0 explain MATCH p = ()-[*]-() RETURN relationships(p) AS rs", containsNoItem(
                deprecatedBindingWarning ) );
    }

    @Test
    void deprecatedCreateIndexSyntax()
    {
        assertNotifications( "EXPLAIN CREATE INDEX ON :Label(prop)", containsItem( deprecatedCreateIndexSyntax ) );
    }

    @Test
    void deprecatedDropIndexSyntax()
    {
        assertNotifications( "EXPLAIN DROP INDEX ON :Label(prop)", containsItem( deprecatedDropIndexSyntax ) );
    }

    @Test
    void deprecatedDropNodeKeyConstraintSyntax()
    {
        assertNotifications( "EXPLAIN DROP CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NODE KEY", containsItem( deprecatedDropConstraintSyntax ) );
    }

    @Test
    void deprecatedDropUniquenessConstraintSyntax()
    {
        assertNotifications( "EXPLAIN DROP CONSTRAINT ON (n:Label) ASSERT n.prop IS UNIQUE", containsItem( deprecatedDropConstraintSyntax ) );
    }

    @Test
    void deprecatedDropNodePropertyExistenceConstraintSyntax()
    {
        assertNotifications( "EXPLAIN DROP CONSTRAINT ON (n:Label) ASSERT EXISTS (n.prop)", containsItem( deprecatedDropConstraintSyntax ) );
    }

    @Test
    void deprecatedDropRelationshipPropertyExistenceConstraintSyntax()
    {
        assertNotifications( "EXPLAIN DROP CONSTRAINT ON ()-[r:Type]-() ASSERT EXISTS (r.prop)", containsItem( deprecatedDropConstraintSyntax ) );
    }

    // FUNCTIONALITY DEPRECATED IN 3.5, REMOVED IN 4.0

    @Test
    void deprecatedToInt()
    {
        assertNotifications( "CYPHER 3.5 EXPLAIN RETURN toInt('1') AS one", containsItem( deprecatedFeatureWarning ) );
    }

    @Test
    void deprecatedUpper()
    {
        assertNotifications( "CYPHER 3.5 EXPLAIN RETURN upper('foo') AS upper", containsItem( deprecatedFeatureWarning ) );
    }

    @Test
    void deprecatedLower()
    {
       assertNotifications( "CYPHER 3.5 EXPLAIN RETURN lower('BAR') AS lower", containsItem( deprecatedFeatureWarning ) );
    }

    @Test
    void deprecatedRels()
    {
        assertNotifications( "CYPHER 3.5 EXPLAIN MATCH p = ()-->() RETURN rels(p) AS r", containsItem( deprecatedFeatureWarning ) );
    }

    @Test
    void deprecatedFilter()
    {
        assertNotifications( "CYPHER 3.5 EXPLAIN WITH [1,2,3] AS list RETURN filter(x IN list WHERE x % 2 = 1) AS odds",
                containsItem( deprecatedFeatureWarning ) );
    }

    @Test
    void deprecatedExtract()
    {
        assertNotifications( "CYPHER 3.5 EXPLAIN WITH [1,2,3] AS list RETURN extract(x IN list | x * 10) AS tens",
                containsItem( deprecatedFeatureWarning ) );
    }

    @Test
    void deprecatedParameterSyntax()
    {
        assertNotifications( "CYPHER 3.5 EXPLAIN RETURN {param} AS parameter", containsItem( deprecatedParameterSyntax ) );
    }

    @Test
    void deprecatedParameterSyntaxForPropertyMap()
    {
        assertNotifications( "CYPHER 3.5 EXPLAIN CREATE (:Label {props})", containsItem( deprecatedParameterSyntax ) );
    }

    @Test
    void deprecatedLengthOfString()
    {
        assertNotifications( "CYPHER 3.5 EXPLAIN RETURN length('a string')", containsItem( deprecatedLengthOnNonPath ) );
    }

    @Test
    void deprecatedLengthOfList()
    {
        assertNotifications( "CYPHER 3.5 EXPLAIN RETURN length([1, 2, 3])", containsItem( deprecatedLengthOnNonPath ) );
    }

    @Test
    void deprecatedLengthOfPatternExpression()
    {
        assertNotifications( "CYPHER 3.5 EXPLAIN MATCH (a) WHERE a.name='Alice' RETURN length((a)-->()-->())", containsItem( deprecatedLengthOnNonPath ) );
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

    private Matcher<Notification> deprecatedCompiledRuntime =
            deprecation( "The compiled runtime, which was requested to execute this query, is deprecated " +
                         "and will be removed in a future release." );

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

    private Matcher<Notification> deprecatedCreateIndexSyntax =
            deprecation( "The create index syntax `CREATE INDEX ON :Label(property)` is deprecated, " +
                    "please use `CREATE INDEX FOR (n:Label) ON (n.property)` instead" );

    private Matcher<Notification> deprecatedDropIndexSyntax =
            deprecation( "The drop index syntax `DROP INDEX ON :Label(property)` is deprecated, please use `DROP INDEX index_name` instead" );

    private Matcher<Notification> deprecatedDropConstraintSyntax =
            deprecation( "The drop constraint by schema syntax `DROP CONSTRAINT ON ...` is deprecated, " +
                    "please use `DROP CONSTRAINT constraint_name` instead" );

    private Matcher<Notification> deprecatedLengthOnNonPath =
            deprecation( "Using 'length' on anything that is not a path is deprecated, please use 'size' instead" );

    private static Matcher<Notification> deprecation( String message )
    {
        return notification( "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                             containsString( message ), any( InputPosition.class ), SeverityLevel.WARNING );
    }
}
