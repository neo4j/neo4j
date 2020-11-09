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
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.SeverityLevel;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.procedure.Procedure;

import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.containsString;

public class DeprecationAcceptanceTest extends NotificationTestSupport
{
    // DEPRECATED PROCEDURE THINGS

    @Test
    void deprecatedProcedureCalls() throws Exception
    {
        db.getDependencyResolver().provideDependency( GlobalProcedures.class ).get().registerProcedure( TestProcedures.class );
        assertNotificationsInSupportedVersions( "explain CALL oldProc()", containsItem( deprecatedProcedureWarning ) );
        assertNotificationsInSupportedVersions( "explain CALL oldProc() RETURN 1", containsItem( deprecatedProcedureWarning ) );
    }

    @Test
    void deprecatedProcedureResultField() throws Exception
    {
        db.getDependencyResolver().provideDependency( GlobalProcedures.class ).get().registerProcedure( TestProcedures.class );
        assertNotificationsInSupportedVersions( "explain CALL changedProc() YIELD oldField RETURN oldField",
                containsItem( deprecatedProcedureReturnFieldWarning ) );
    }

    // DEPRECATED SYNTAX in 4.X

    @Test
    void deprecatedOctalLiteralSyntax()
    {
        assertNotificationsInSupportedVersions( "explain RETURN 0123 AS name", containsItem( deprecatedOctalLiteralSyntax ) );
    }

    @Test
    void deprecatedHexLiteralSyntax()
    {
        assertNotificationsInSupportedVersions( "explain RETURN 0X123 AS name", containsItem( deprecatedHexLiteralSyntax ) );
    }

    @Test
    void deprecatedBindingVariableLengthRelationship()
    {
        assertNotificationsInSupportedVersions( "explain MATCH ()-[rs*]-() RETURN rs", containsItem( deprecatedBindingWarning ) );
        assertNotificationsInSupportedVersions( "explain MATCH p = ()-[*]-() RETURN relationships(p) AS rs", containsNoItem( deprecatedBindingWarning ) );
    }

    @Test
    void deprecatedCreateIndexSyntax()
    {
        assertNotificationsInSupportedVersions( "EXPLAIN CREATE INDEX ON :Label(prop)", containsItem( deprecatedCreateIndexSyntax ) );
    }

    @Test
    void deprecatedDropIndexSyntax()
    {
        assertNotificationsInSupportedVersions( "EXPLAIN DROP INDEX ON :Label(prop)", containsItem( deprecatedDropIndexSyntax ) );
    }

    @Test
    void deprecatedDropNodeKeyConstraintSyntax()
    {
        assertNotificationsInSupportedVersions(  "EXPLAIN DROP CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NODE KEY",
                containsItem( deprecatedDropConstraintSyntax ) );
    }

    @Test
    void deprecatedDropUniquenessConstraintSyntax()
    {
        assertNotificationsInSupportedVersions( "EXPLAIN DROP CONSTRAINT ON (n:Label) ASSERT n.prop IS UNIQUE",
                containsItem( deprecatedDropConstraintSyntax ) );
    }

    @Test
    void deprecatedDropNodePropertyExistenceConstraintSyntax()
    {
        assertNotificationsInSupportedVersions( "EXPLAIN DROP CONSTRAINT ON (n:Label) ASSERT EXISTS (n.prop)",
                containsItem( deprecatedDropConstraintSyntax ) );
    }

    @Test
    void deprecatedDropRelationshipPropertyExistenceConstraintSyntax()
    {
        assertNotificationsInSupportedVersions( "EXPLAIN DROP CONSTRAINT ON ()-[r:Type]-() ASSERT EXISTS (r.prop)",
                containsItem( deprecatedDropConstraintSyntax ) );
    }

    @Test
    void deprecatedCreateNodePropertyExistenceConstraintSyntax()
    {
        assertNotificationsInSupportedVersions( "EXPLAIN CREATE CONSTRAINT ON (n:Label) ASSERT EXISTS (n.prop)",
                containsItem( deprecatedCreatePropertyExistenceConstraintSyntax ) );
    }

    @Test
    void deprecatedCreateRelationshipPropertyExistenceConstraintSyntax()
    {
        assertNotificationsInSupportedVersions( "EXPLAIN CREATE CONSTRAINT ON ()-[r:Type]-() ASSERT EXISTS (r.prop)",
                containsItem( deprecatedCreatePropertyExistenceConstraintSyntax ) );
    }

    @Test
    void deprecatedPatternExpressionSyntax()
    {
        // deprecated
        assertNotificationsInSupportedVersions( "EXPLAIN MATCH (a) RETURN (a)--()",
                                                containsItem( deprecatedUseOfPatternExpression ) );
        assertNotificationsInSupportedVersions( "EXPLAIN MATCH (a) WHERE ANY (x IN (a)--() WHERE 1=1) RETURN a",
                                                containsItem( deprecatedUseOfPatternExpression ) );
    }

    @Test
    void notDeprecatedPatternExpressionSyntax()
    {
        // Existential subqueries are not supported in 3.5
        assertNotificationsInSupportedVersions_4_X( "EXPLAIN MATCH (a) WHERE EXISTS {(x) WHERE (x)--()} RETURN a",
                                                    containsNoItem( deprecatedUseOfPatternExpression ) );
        assertNotificationsInSupportedVersions( "EXPLAIN MATCH (a)--() RETURN a",
                                                containsNoItem( deprecatedUseOfPatternExpression ) );
        assertNotificationsInSupportedVersions( "EXPLAIN MATCH (a) WHERE exists((a)--()) RETURN a",
                                                containsNoItem( deprecatedUseOfPatternExpression ) );
        assertNotificationsInSupportedVersions( "EXPLAIN MATCH (a) WHERE (a)--() RETURN a",
                                                containsNoItem( deprecatedUseOfPatternExpression ) );
        assertNotificationsInSupportedVersions( "EXPLAIN MATCH (a) RETURN [p=(a)--(b) | p]",
                                                containsNoItem( deprecatedUseOfPatternExpression ) );
    }

    @Test
    void deprecatedPropertyExistenceSyntaxOnNode()
    {
        assertNotificationsInSupportedVersions( "EXPLAIN MATCH (n) WHERE EXISTS(n.prop) RETURN n", containsItem( deprecatedPropertyExistenceSyntax ) );
    }

    @Test
    void deprecatedPropertyExistenceSyntaxOnNodeWithNot()
    {
        assertNotificationsInSupportedVersions( "EXPLAIN MATCH (n) WHERE NOT EXISTS(n.prop) RETURN n", containsItem( deprecatedPropertyExistenceSyntax ) );
    }

    @Test
    void deprecatedPropertyExistenceSyntaxOnRelationship()
    {
        assertNotificationsInSupportedVersions( "EXPLAIN MATCH ()-[r]-() WITH r WHERE EXISTS(r.prop) RETURN r.prop",
                containsItem( deprecatedPropertyExistenceSyntax ) );
    }

    @Test
    void deprecatedMapExistenceSyntax()
    {
        assertNotificationsInSupportedVersions( "EXPLAIN WITH {key:'blah'} as map RETURN EXISTS(map.key)",
                containsItem( deprecatedPropertyExistenceSyntax ));
    }

    @Test
    void existsOnPathsShouldNotBeDeprecated()
    {
        assertNotificationsInSupportedVersions( "EXPLAIN MATCH (n) WHERE EXISTS( (n)-[:REL]->() ) RETURN count(n)",
                containsNoItem( deprecatedPropertyExistenceSyntax ) );
    }

    @Test
    void existsSubclauseShouldNotBeDeprecated()
    {
        // Note: Exists subclause was introduced in Neo4j 4.0
        assertNotificationsInVersions4_2and4_3("EXPLAIN MATCH (n) WHERE EXISTS { MATCH (n)-[]->() } RETURN n.prop",
                containsNoItem( deprecatedPropertyExistenceSyntax ) );
    }

    // FUNCTIONALITY DEPRECATED IN 3.5, REMOVED IN 4.0

    @Test
    void deprecatedToInt()
    {
        assertNotificationsInLastMajorVersion( "EXPLAIN RETURN toInt('1') AS one", containsItem( deprecatedFeatureWarning ) );
    }

    @Test
    void deprecatedUpper()
    {
        assertNotificationsInLastMajorVersion( "EXPLAIN RETURN upper('foo') AS upper", containsItem( deprecatedFeatureWarning ) );
    }

    @Test
    void deprecatedLower()
    {
       assertNotificationsInLastMajorVersion( "EXPLAIN RETURN lower('BAR') AS lower", containsItem( deprecatedFeatureWarning ) );
    }

    @Test
    void deprecatedRels()
    {
        assertNotificationsInLastMajorVersion( "EXPLAIN MATCH p = ()-->() RETURN rels(p) AS r", containsItem( deprecatedFeatureWarning ) );
    }

    @Test
    void deprecatedFilter()
    {
        assertNotificationsInLastMajorVersion( "EXPLAIN WITH [1,2,3] AS list RETURN filter(x IN list WHERE x % 2 = 1) AS odds",
                containsItem( deprecatedFeatureWarning ) );
    }

    @Test
    void deprecatedExtract()
    {
        assertNotificationsInLastMajorVersion( "EXPLAIN WITH [1,2,3] AS list RETURN extract(x IN list | x * 10) AS tens",
                containsItem( deprecatedFeatureWarning ) );
    }

    @Test
    void deprecatedParameterSyntax()
    {
        assertNotificationsInLastMajorVersion( "EXPLAIN RETURN {param} AS parameter", containsItem( deprecatedParameterSyntax ) );
    }

    @Test
    void deprecatedParameterSyntaxForPropertyMap()
    {
        assertNotificationsInLastMajorVersion( "EXPLAIN CREATE (:Label {props})", containsItem( deprecatedParameterSyntax ) );
    }

    @Test
    void deprecatedLengthOfString()
    {
        assertNotificationsInLastMajorVersion( "EXPLAIN RETURN length('a string')", containsItem( deprecatedLengthOnNonPath ) );
    }

    @Test
    void deprecatedLengthOfList()
    {
        assertNotificationsInLastMajorVersion( "EXPLAIN RETURN length([1, 2, 3])", containsItem( deprecatedLengthOnNonPath ) );
    }

    @Test
    void deprecatedLengthOfPatternExpression()
    {
        assertNotificationsInLastMajorVersion( "EXPLAIN MATCH (a) WHERE a.name='Alice' RETURN length((a)-->()-->())",
                containsItem( deprecatedLengthOnNonPath ) );
    }

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
            assertNotificationsInLastMajorVersion( query, containsItem( deprecatedSeparatorWarning ) );
        }

        // clear caches of the rewritten queries to not keep notifications around
        db.getDependencyResolver().resolveDependency( QueryExecutionEngine.class ).clearQueryCaches();

        for ( String query : nonDeprecatedQueries )
        {
            assertNotificationsInLastMajorVersion( query, containsNoItem( deprecatedSeparatorWarning ) );
        }
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

    private final Matcher<Notification> deprecatedFeatureWarning =
            deprecation( "The query used a deprecated function." );

    private final Matcher<Notification> deprecatedProcedureWarning =
            deprecation( "The query used a deprecated procedure." );

    private final Matcher<Notification> deprecatedProcedureReturnFieldWarning =
            deprecation( "The query used a deprecated field from a procedure." );

    private final Matcher<Notification> deprecatedBindingWarning =
            deprecation( "Binding relationships to a list in a variable length pattern is deprecated." );

    private final Matcher<Notification> deprecatedSeparatorWarning =
            deprecation( "The semantics of using colon in the separation of alternative relationship " +
                         "types in conjunction with the use of variable binding, inlined property " +
                         "predicates, or variable length will change in a future version." );

    private final Matcher<Notification> deprecatedParameterSyntax =
            deprecation( "The parameter syntax `{param}` is deprecated, please use `$param` instead" );

    private final Matcher<Notification> deprecatedCreateIndexSyntax =
            deprecation( "The create index syntax `CREATE INDEX ON :Label(property)` is deprecated, " +
                    "please use `CREATE INDEX FOR (n:Label) ON (n.property)` instead" );

    private final Matcher<Notification> deprecatedDropIndexSyntax =
            deprecation( "The drop index syntax `DROP INDEX ON :Label(property)` is deprecated, please use `DROP INDEX index_name` instead" );

    private final Matcher<Notification> deprecatedDropConstraintSyntax =
            deprecation( "The drop constraint by schema syntax `DROP CONSTRAINT ON ...` is deprecated, " +
                    "please use `DROP CONSTRAINT constraint_name` instead" );

    private final Matcher<Notification> deprecatedCreatePropertyExistenceConstraintSyntax =
            deprecation( "The create property existence constraint syntax `CREATE CONSTRAINT ON ... ASSERT exists(variable.property)` is deprecated, " +
                    "please use `CREATE CONSTRAINT ON ... ASSERT (variable.property) IS NOT NULL` instead" );

    private final Matcher<Notification> deprecatedPropertyExistenceSyntax =
            deprecation( "The property existence syntax `... exists(variable.property)` is deprecated, please use `variable.property IS NOT NULL` instead" );

    private final Matcher<Notification> deprecatedLengthOnNonPath =
            deprecation( "Using 'length' on anything that is not a path is deprecated, please use 'size' instead" );

    private final Matcher<Notification> deprecatedOctalLiteralSyntax =
            deprecation( "The octal integer literal syntax `0123` is deprecated, please use `0o123` instead" );

    private final Matcher<Notification> deprecatedHexLiteralSyntax =
            deprecation( "The hex integer literal syntax `0X123` is deprecated, please use `0x123` instead" );

    private final Matcher<Notification> deprecatedUseOfPatternExpression =
            deprecation( "A pattern expression should only be used in order to test the existence of a pattern. " +
                         "It should therefore only be used in contexts that evaluate to a Boolean, e.g. inside the function exists() or in a WHERE-clause. " +
                         "All other uses are deprecated." );

    private static Matcher<Notification> deprecation( String message )
    {
        return notification( "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                             containsString( message ), any( InputPosition.class ), SeverityLevel.WARNING );
    }
}
