/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.SeverityLevel;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.impl.notification.NotificationCode;
import org.neo4j.graphdb.impl.notification.NotificationDetail;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Procedure;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.impl.notification.NotificationCode.CREATE_UNIQUE_UNAVAILABLE_FALLBACK;
import static org.neo4j.graphdb.impl.notification.NotificationCode.EAGER_LOAD_CSV;
import static org.neo4j.graphdb.impl.notification.NotificationCode.INDEX_HINT_UNFULFILLABLE;
import static org.neo4j.graphdb.impl.notification.NotificationCode.LENGTH_ON_NON_PATH;
import static org.neo4j.graphdb.impl.notification.NotificationCode.RULE_PLANNER_UNAVAILABLE_FALLBACK;
import static org.neo4j.graphdb.impl.notification.NotificationCode.RUNTIME_UNSUPPORTED;
import static org.neo4j.graphdb.impl.notification.NotificationCode.UNBOUNDED_SHORTEST_PATH;
import static org.neo4j.graphdb.impl.notification.NotificationDetail.Factory.index;

public class NotificationAcceptanceTest
{

    @Rule
    public final ImpermanentDatabaseRule rule = new ImpermanentDatabaseRule();

    @Test
    public void shouldNotifyWhenUsingCypher3_1ForTheRulePlannerWhenCypherVersionIsTheDefault() throws Exception
    {
        // when
        Result result = db().execute( "CYPHER planner=rule RETURN 1" );
        InputPosition position = new InputPosition( 20, 1, 21 );

        // then
        assertThat( result.getNotifications(), Matchers.contains( RULE_PLANNER_UNAVAILABLE_FALLBACK.notification( position ) ) );
        Map<String,Object> arguments = result.getExecutionPlanDescription().getArguments();
        assertThat( arguments.get( "version" ), equalTo( "CYPHER 3.1" ) );
        assertThat( arguments.get( "planner" ), equalTo( "RULE" ) );
        result.close();
    }

    @Test
    public void shouldNotifyWhenUsingCypher3_1ForTheRulePlannerWhenCypherVersionIs3_3() throws Exception
    {
        // when
        Result result = db().execute( "CYPHER 3.3 planner=rule RETURN 1" );
        InputPosition position = new InputPosition( 24, 1, 25 );

        // then
        assertThat( result.getNotifications(), Matchers.contains( RULE_PLANNER_UNAVAILABLE_FALLBACK.notification( position ) ) );
        Map<String,Object> arguments = result.getExecutionPlanDescription().getArguments();
        assertThat( arguments.get( "version" ), equalTo( "CYPHER 3.1" ) );
        assertThat( arguments.get( "planner" ), equalTo( "RULE" ) );
        result.close();
    }

    @Test
    public void shouldNotifyWhenUsingCypher3_1ForTheRulePlannerWhenCypherVersionIs3_2() throws Exception
    {
        // when
        Result result = db().execute( "CYPHER 3.2 planner=rule RETURN 1" );
        InputPosition position = new InputPosition( 24, 1, 25 );

        // then
        assertThat( result.getNotifications(), Matchers.contains( RULE_PLANNER_UNAVAILABLE_FALLBACK.notification( position ) ) );
        Map<String,Object> arguments = result.getExecutionPlanDescription().getArguments();
        assertThat( arguments.get( "version" ), equalTo( "CYPHER 3.1" ) );
        assertThat( arguments.get( "planner" ), equalTo( "RULE" ) );
        result.close();
    }

    @Test
    public void shouldNotNotifyWhenUsingTheRulePlannerWhenCypherVersionIsNot3_2() throws Exception
    {
        Stream.of( "CYPHER 3.1", "CYPHER 2.3" ).forEach( version ->
        {
            // when
            Result result = db().execute( version + " planner=rule RETURN 1" );

            // then
            assertThat( Iterables.asList( result.getNotifications() ), empty() );
            Map<String,Object> arguments = result.getExecutionPlanDescription().getArguments();
            assertThat( arguments.get( "version" ), equalTo( version ) );
            assertThat( arguments.get( "planner" ), equalTo( "RULE" ) );
            result.close();
        } );
    }

    @Test
    public void shouldWarnWhenRequestingCompiledRuntimeOnUnsupportedQuery() throws Exception
    {
        Stream.of( "CYPHER 3.1", "CYPHER 3.2", "CYPHER 3.3" ).forEach(
                version -> shouldNotifyInStream( version, "EXPLAIN CYPHER runtime=compiled MATCH (a)-->(b), (c)-->(d) RETURN count(*)", InputPosition.empty,
                        RUNTIME_UNSUPPORTED ) );
    }

    @Test
    public void shouldWarnWhenRequestingSlottedRuntimeOnUnsupportedQuery() throws Exception
    {
        Stream.of( "CYPHER 3.3" ).forEach(
                version -> shouldNotifyInStream( version, "explain cypher runtime=slotted merge (a)-[:X]->(b)", InputPosition.empty, RUNTIME_UNSUPPORTED ) );
    }

    @Test
    public void shouldNotifyWhenUsingCreateUniqueWhenCypherVersionIsDefault() throws Exception
    {
        // when
        Result result = db().execute( "MATCH (b) WITH b LIMIT 1 CREATE UNIQUE (b)-[:REL]->()" );
        InputPosition position = new InputPosition( 25, 1, 26 );

        // then
        assertThat( result.getNotifications(),
                Matchers.contains( CREATE_UNIQUE_UNAVAILABLE_FALLBACK.notification( position ) ) );
        Map<String,Object> arguments = result.getExecutionPlanDescription().getArguments();
        assertThat( arguments.get( "version" ), equalTo( "CYPHER 3.1" ) );
        result.close();
    }

    @Test
    public void shouldNotifyWhenUsingCreateUniqueWhenCypherVersionIs3_3() throws Exception
    {
        // when
        Result result = db().execute( "CYPHER 3.3 MATCH (b) WITH b LIMIT 1 CREATE UNIQUE (b)-[:REL]->()" );
        InputPosition position = new InputPosition( 36, 1, 37 );

        // then
        assertThat( result.getNotifications(), Matchers.contains( CREATE_UNIQUE_UNAVAILABLE_FALLBACK.notification( position ) ) );
        Map<String,Object> arguments = result.getExecutionPlanDescription().getArguments();
        assertThat( arguments.get( "version" ), equalTo( "CYPHER 3.1" ) );
        result.close();
    }

    @Test
    public void shouldNotifyWhenUsingCreateUniqueWhenCypherVersionIs3_2() throws Exception
    {
        // when
        Result result = db().execute( "CYPHER 3.2 MATCH (b) WITH b LIMIT 1 CREATE UNIQUE (b)-[:REL]->()" );
        InputPosition position = new InputPosition( 36, 1, 37 );

        // then
        assertThat( result.getNotifications(), Matchers.contains( CREATE_UNIQUE_UNAVAILABLE_FALLBACK.notification( position ) ) );
        Map<String,Object> arguments = result.getExecutionPlanDescription().getArguments();
        assertThat( arguments.get( "version" ), equalTo( "CYPHER 3.1" ) );
        result.close();
    }

    @Test
    public void shouldNotNotifyWhenUsingCreateUniqueWhenCypherVersionIsNot3_2() throws Exception
    {
        Stream.of( "CYPHER 3.1", "CYPHER 2.3" ).forEach(
                version -> shouldNotNotifyInStream( version, " MATCH (b) WITH b LIMIT 1 CREATE UNIQUE (b)-[:REL]->()" ) );
    }

    @Test
    public void shouldWarnWhenUsingLengthOnNonPath() throws Exception
    {
        Stream.of( "CYPHER 3.1", "CYPHER 3.2", "CYPHER 3.3" ).forEach( version ->
        {
            // pattern
            shouldNotifyInStream( version, "explain match (a) where a.name='Alice' return length((a)-->()-->())", new InputPosition( 63, 1, 64 ),
                    LENGTH_ON_NON_PATH );

            // collection
            shouldNotifyInStream( version, " explain return length([1, 2, 3])", new InputPosition( 33, 1, 34 ), LENGTH_ON_NON_PATH );

            // string
            shouldNotifyInStream( version, " explain return length('a string')", new InputPosition( 33, 1, 34 ), LENGTH_ON_NON_PATH );
        } );
    }

    @Test
    public void shouldNotNotifyWhenUsingLengthOnPath() throws Exception
    {
        Stream.of( "CYPHER 2.3", "CYPHER 3.1", "CYPHER 3.2", "CYPHER 3.3" ).forEach(
                version -> shouldNotNotifyInStream( version, " explain match p=(a)-[*]->(b) return length(p)" ) );
    }

    @Test
    public void shouldNotNotifyWhenUsingSizeOnCollection() throws Exception
    {
        Stream.of( "CYPHER 2.3", "CYPHER 3.1", "CYPHER 3.2", "CYPHER 3.3" ).forEach(
                version -> shouldNotNotifyInStream( version, "explain return size([1, 2, 3])" ) );
    }

    @Test
    public void shouldNotNotifyWhenUsingSizeOnString() throws Exception
    {
        Stream.of( "CYPHER 2.3", "CYPHER 3.1", "CYPHER 3.2", "CYPHER 3.3" ).forEach(
                version -> shouldNotNotifyInStream( version, " explain return size('a string')" ) );
    }

    @Test
    public void shouldNotNotifyForCostUnsupportedUpdateQueryIfPlannerNotExplicitlyRequested() throws Exception
    {
        Stream.of( "CYPHER 2.3", "CYPHER 3.1", "CYPHER 3.2", "CYPHER 3.3" ).forEach(
                version -> shouldNotNotifyInStream( version, " EXPLAIN MATCH (n:Movie) SET n.title = 'The Movie'" ) );
    }

    @Test
    public void shouldNotNotifyForCostSupportedUpdateQuery() throws Exception
    {
        Stream.of( "CYPHER 3.1", "CYPHER 3.2", "CYPHER 3.3" ).forEach( version ->
        {
            shouldNotNotifyInStream( version, "EXPLAIN CYPHER planner=cost MATCH (n:Movie) SET n:Seen" );
            shouldNotNotifyInStream( version, "EXPLAIN CYPHER planner=idp MATCH (n:Movie) SET n:Seen" );
            shouldNotNotifyInStream( version, "EXPLAIN CYPHER planner=dp MATCH (n:Movie) SET n:Seen" );
        } );
    }

    @Test
    public void shouldNotNotifyUsingJoinHintWithCost() throws Exception
    {
        List<String> queries = Arrays.asList( "CYPHER planner=cost EXPLAIN MATCH (a)-->(b) USING JOIN ON b RETURN a, b",
                "CYPHER planner=cost EXPLAIN MATCH (a)-->(x)<--(b) USING JOIN ON x RETURN a, b" );

        Stream.of( "CYPHER 2.3", "CYPHER 3.1", "CYPHER 3.2", "CYPHER 3.3" ).forEach( version ->
        {
            for ( String query : queries )
            {
                assertNotifications( version + query, containsNoItem( joinHintUnsuportedWarning ) );
            }
        } );
    }

    @Test
    public void shouldWarnOnPotentiallyCachedQueries() throws Exception
    {
        Stream.of( "CYPHER 2.3", "CYPHER 3.1", "CYPHER 3.2", "CYPHER 3.3" ).forEach( version ->
        {
            assertNotifications( version + "explain match (a)-->(b), (c)-->(d) return *", containsItem( cartesianProductWarning ) );

            // no warning without explain
            shouldNotNotifyInStream( version, "match (a)-->(b), (c)-->(d) return *" );
        } );
    }

    @Test
    public void shouldWarnOnceWhenSingleIndexHintCannotBeFulfilled() throws Exception
    {
        Stream.of( "CYPHER 2.3", "CYPHER 3.1", "CYPHER 3.2", "CYPHER 3.3" ).forEach(
                version -> shouldNotifyInStreamWithDetail( version, " EXPLAIN MATCH (n:Person) USING INDEX n:Person(name) WHERE n.name = 'John' RETURN n",
                        InputPosition.empty, INDEX_HINT_UNFULFILLABLE, index( "Person", "name" ) ) );
    }

    @Test
    public void shouldWarnOnEachUnfulfillableIndexHint() throws Exception
    {
        String query = " EXPLAIN MATCH (n:Person), (m:Party), (k:Animal) " + "USING INDEX n:Person(name) " + "USING INDEX m:Party(city) " +
                "USING INDEX k:Animal(species) " + "WHERE n.name = 'John' AND m.city = 'Reykjavik' AND k.species = 'Sloth' " + "RETURN n";

        Stream.of( "CYPHER 2.3", "CYPHER 3.1", "CYPHER 3.2", "CYPHER 3.3" ).forEach( version ->
        {
            shouldNotifyInStreamWithDetail( version, query, InputPosition.empty, INDEX_HINT_UNFULFILLABLE, index( "Person", "name" ) );
            shouldNotifyInStreamWithDetail( version, query, InputPosition.empty, INDEX_HINT_UNFULFILLABLE, index( "Party", "city" ) );
            shouldNotifyInStreamWithDetail( version, query, InputPosition.empty, INDEX_HINT_UNFULFILLABLE, index( "Animal", "species" ) );
        } );
    }

    @Test
    public void shouldNotNotifyOnLiteralMaps() throws Exception
    {
        Stream.of( "CYPHER 3.1", "CYPHER 3.2", "CYPHER 3.3" ).forEach( version -> shouldNotNotifyInStream( version, " explain return { id: 42 } " ) );
    }

    @Test
    public void shouldNotNotifyOnNonExistingLabelUsingLoadCSV() throws Exception
    {
        Stream.of( "CYPHER 3.1", "CYPHER 3.2", "CYPHER 3.3" ).forEach( version ->
        {
            // create node
            shouldNotNotifyInStream( version, " EXPLAIN LOAD CSV WITH HEADERS FROM 'file:///fake.csv' AS row CREATE (n:Category)" );

            // merge node
            shouldNotNotifyInStream( version, " EXPLAIN LOAD CSV WITH HEADERS FROM 'file:///fake.csv' AS row MERGE (n:Category)" );

            // set label to node
            shouldNotNotifyInStream( version, " EXPLAIN LOAD CSV WITH HEADERS FROM 'file:///fake.csv' AS row CREATE (n) SET n:Category" );
        } );
    }

    @Test
    public void shouldNotNotifyOnNonExistingRelTypeUsingLoadCSV() throws Exception
    {
        Stream.of( "CYPHER 3.1", "CYPHER 3.2", "CYPHER 3.3" ).forEach( version ->
        {
            // create rel
            shouldNotNotifyInStream( version, " EXPLAIN LOAD CSV WITH HEADERS FROM 'file:///fake.csv' AS row CREATE ()-[:T]->()" );

            // merge rel
            shouldNotNotifyInStream( version, " EXPLAIN LOAD CSV WITH HEADERS FROM 'file:///fake.csv' AS row MERGE ()-[:T]->()" );
        } );
    }

    @Test
    public void shouldNotNotifyOnNonExistingPropKeyIdUsingLoadCSV() throws Exception
    {
        Stream.of( "CYPHER 3.1", "CYPHER 3.2", "CYPHER 3.3" ).forEach( version ->
        {
            // create node
            shouldNotNotifyInStream( version, " EXPLAIN LOAD CSV WITH HEADERS FROM 'file:///fake.csv' AS row CREATE (n) SET n.p = 'a'" );

            // merge node
            shouldNotNotifyInStream( version, " EXPLAIN LOAD CSV WITH HEADERS FROM 'file:///fake.csv' AS row MERGE (n) ON CREATE SET n.p = 'a'" );
        } );
    }

    @Test
    public void shouldNotNotifyOnEagerBeforeLoadCSV() throws Exception
    {
        Stream.of( "CYPHER 3.1", "CYPHER 3.2", "CYPHER 3.3" ).forEach( version -> shouldNotNotifyInStream( version,
                "EXPLAIN MATCH (n) DELETE n WITH * LOAD CSV FROM 'file:///ignore/ignore.csv' AS line MERGE () RETURN line" ) );
    }

    @Test
    public void shouldWarnOnEagerAfterLoadCSV() throws Exception
    {
        Stream.of( "CYPHER 3.1", "CYPHER 3.2", "CYPHER 3.3" ).forEach( version -> shouldNotifyInStream( version,
                "EXPLAIN MATCH (n) LOAD CSV FROM 'file:///ignore/ignore.csv' AS line WITH * DELETE n MERGE () RETURN line", InputPosition.empty,
                EAGER_LOAD_CSV ) );
    }

    @Test
    public void shouldNotNotifyOnLoadCSVWithoutEager() throws Exception
    {
        Stream.of( "CYPHER 3.1", "CYPHER 3.2", "CYPHER 3.3" ).forEach(
                version -> shouldNotNotifyInStream( version, "EXPLAIN LOAD CSV FROM 'file:///ignore/ignore.csv' AS line MATCH (:A) CREATE (:B) RETURN line" ) );
    }

    @Test
    public void shouldNotNotifyOnEagerWithoutLoadCSV() throws Exception
    {
        Stream.of( "CYPHER 3.1", "CYPHER 3.2", "CYPHER 3.3" ).forEach(
                version -> assertNotifications( version + "EXPLAIN MATCH (a), (b) CREATE (c) RETURN *", containsNoItem( eagerOperatorWarning ) ) );
    }

    @Test
    public void shouldWarnOnLargeLabelScansWithLoadCVSMatch() throws Exception
    {
        for ( int i = 0; i < 11; i++ )
        {
            try ( Transaction tx = db().beginTx() )
            {
                db().createNode().addLabel( label( "A" ) );
                tx.success();
            }
        }
        Stream.of( "CYPHER 3.1", "CYPHER 3.2", "CYPHER 3.3" ).forEach(
                version -> assertNotifications( version + "EXPLAIN LOAD CSV FROM 'file:///ignore/ignore.csv' AS line MATCH (a:A) RETURN *",
                        containsNoItem( largeLabelCSVWarning ) ) );
    }

    @Test
    public void shouldWarnOnLargeLabelScansWithLoadCVSMerge() throws Exception
    {
        for ( int i = 0; i < 11; i++ )
        {
            try ( Transaction tx = db().beginTx() )
            {
                db().createNode().addLabel( label( "A" ) );
                tx.success();
            }
        }
        Stream.of( "CYPHER 3.1", "CYPHER 3.2", "CYPHER 3.3" ).forEach(
                version -> assertNotifications( version + "EXPLAIN LOAD CSV FROM 'file:///ignore/ignore.csv' AS line MERGE (a:A) RETURN *",
                        containsNoItem( largeLabelCSVWarning ) ) );
    }

    @Test
    public void shouldNotWarnOnSmallLabelScansWithLoadCVS() throws Exception
    {
        try ( Transaction tx = db().beginTx() )
        {
            db().createNode().addLabel( label( "A" ) );
            tx.success();
        }
        Stream.of( "CYPHER 3.1", "CYPHER 3.2", "CYPHER 3.3" ).forEach( version ->
        {
            shouldNotNotifyInStream( version, "EXPLAIN LOAD CSV FROM 'file:///ignore/ignore.csv' AS line MATCH (a:A) RETURN *" );
            shouldNotNotifyInStream( version, "EXPLAIN LOAD CSV FROM 'file:///ignore/ignore.csv' AS line MERGE (a:A) RETURN *" );
        } );
    }

    @Test
    public void shouldWarnOnDeprecatedToInt() throws Exception
    {
        Stream.of( "CYPHER 3.1", "CYPHER 3.2", "CYPHER 3.3" ).forEach( version ->
                assertNotifications( version + " EXPLAIN RETURN toInt('1') AS one", containsItem( deprecatedFeatureWarning ) ) );
    }

    @Test
    public void shouldWarnOnDeprecatedUpper() throws Exception
    {
        Stream.of( "CYPHER 3.1", "CYPHER 3.2", "CYPHER 3.3" ).forEach( version ->
                assertNotifications( version + " EXPLAIN RETURN upper('foo') AS one", containsItem( deprecatedFeatureWarning ) ) );
    }

    @Test
    public void shouldWarnOnDeprecatedLower() throws Exception
    {
        Stream.of( "CYPHER 3.1", "CYPHER 3.2", "CYPHER 3.3" ).forEach( version ->
                assertNotifications( version + " EXPLAIN RETURN lower('BAR') AS one", containsItem( deprecatedFeatureWarning ) ) );
    }

    @Test
    public void shouldWarnOnDeprecatedRels() throws Exception
    {
        Stream.of( "CYPHER 3.1", "CYPHER 3.2", "CYPHER 3.3" ).forEach( version ->
                assertNotifications( version + " EXPLAIN MATCH p = ()-->() RETURN rels(p) AS r", containsItem( deprecatedFeatureWarning ) ) );
    }

    @Test
    public void shouldWarnOnDeprecatedProcedureCalls() throws Exception
    {
        db().getDependencyResolver().provideDependency( Procedures.class ).get().registerProcedure( TestProcedures.class );
        Stream.of( "CYPHER 3.1", "CYPHER 3.2", "CYPHER 3.3" ).forEach( version ->
        {
            assertNotifications( version + "explain CALL oldProc()", containsItem( deprecatedProcedureWarning ) );
            assertNotifications( version + "explain CALL oldProc() RETURN 1", containsItem( deprecatedProcedureWarning ) );
        } );
    }

    @Test
    public void shouldWarnOnDeprecatedProcedureResultField() throws Exception
    {
        db().getDependencyResolver().provideDependency( Procedures.class ).get().registerProcedure( TestProcedures.class );
        Stream.of( "CYPHER 3.2", "CYPHER 3.3" ).forEach( version -> assertNotifications( version + "explain CALL changedProc() YIELD oldField RETURN oldField",
                containsItem( deprecatedProcedureReturnFieldWarning ) ) );
    }

    @Test
    public void shouldWarnOnUnboundedShortestPath() throws Exception
    {
        Stream.of( "CYPHER 2.3", "CYPHER 3.1", "CYPHER 3.2", "CYPHER 3.3" ).forEach(
                version -> shouldNotifyInStream( version, "EXPLAIN MATCH p = shortestPath((n)-[*]->(m)) RETURN m", new InputPosition( 44, 1, 45 ),
                        UNBOUNDED_SHORTEST_PATH ) );
    }

    @Test
    public void shouldNotNotifyOnDynamicPropertyLookupWithNoLabels() throws Exception
    {
        Stream.of( "CYPHER 2.3", "CYPHER 3.1", "CYPHER 3.2", "CYPHER 3.3" ).forEach( version ->
        {
            db().execute( "CREATE INDEX ON :Person(name)" );
            db().execute( "Call db.awaitIndexes()" );
            shouldNotNotifyInStream( version, "EXPLAIN MATCH (n) WHERE n['key-' + n.name] = 'value' RETURN n" );
        } );
    }

    @Test
    public void shouldWarnOnDynamicPropertyLookupWithBothStaticAndDynamicProperties() throws Exception
    {
        Stream.of( "CYPHER 2.3", "CYPHER 3.1", "CYPHER 3.2", "CYPHER 3.3" ).forEach( version ->
        {
            db().execute( "CREATE INDEX ON :Person(name)" );
            db().execute( "Call db.awaitIndexes()" );
            shouldNotNotifyInStream( version, "EXPLAIN MATCH (n) WHERE n['key-' + n.name] = 'value' RETURN n" );
        } );
    }

    @Test
    public void shouldNotNotifyOnDynamicPropertyLookupWithLabelHavingNoIndex() throws Exception
    {
        Stream.of( "CYPHER 2.3", "CYPHER 3.1", "CYPHER 3.2", "CYPHER 3.3" ).forEach( version ->
        {
            db().execute( "CREATE INDEX ON :Person(name)" );
            db().execute( "Call db.awaitIndexes()" );
            try ( Transaction tx = db().beginTx() )
            {
                db().createNode().addLabel( label( "Foo" ) );
                tx.success();
            }
            shouldNotNotifyInStream( version, "EXPLAIN MATCH (n) WHERE n['key-' + n.name] = 'value' RETURN n" );
        } );
    }

    @Test
    public void shouldWarnOnUnfulfillableIndexSeekUsingDynamicProperty() throws Exception
    {
        List<String> queries = new ArrayList<>();

        // dynamic property lookup with single label
        queries.add( "EXPLAIN MATCH (n:Person) WHERE n['key-' + n.name] = 'value' RETURN n" );

        // dynamic property lookup with explicit label check
        queries.add( "EXPLAIN MATCH (n) WHERE n['key-' + n.name] = 'value' AND (n:Person) RETURN n" );

        // dynamic property lookup with range seek
        queries.add( "EXPLAIN MATCH (n:Person) WHERE n['key-' + n.name] > 10 RETURN n" );

        // dynamic property lookup with range seek (reverse)
        queries.add( "EXPLAIN MATCH (n:Person) WHERE 10 > n['key-' + n.name] RETURN n" );

        // dynamic property lookup with a single label and property existence check with exists
        queries.add( "EXPLAIN MATCH (n:Person) WHERE exists(n['na' + 'me']) RETURN n" );

        // dynamic property lookup with a single label and starts with
        queries.add( "EXPLAIN MATCH (n:Person) WHERE n['key-' + n.name] STARTS WITH 'Foo' RETURN n" );

        // dynamic property lookup with a single label and regex
        queries.add( "EXPLAIN MATCH (n:Person) WHERE n['key-' + n.name] =~ 'Foo*' RETURN n" );

        // dynamic property lookup with a single label and IN
        queries.add( "EXPLAIN MATCH (n:Person) WHERE n['key-' + n.name] IN ['Foo', 'Bar'] RETURN n" );

        Stream.of( "CYPHER 2.3", "CYPHER 3.1", "CYPHER 3.2", "CYPHER 3.3" ).forEach( version ->
        {
            for ( String query : queries )
            {
                db().execute( "CREATE INDEX ON :Person(name)" );
                db().execute( "Call db.awaitIndexes()" );
                assertNotifications( version + query, containsItem( dynamicPropertyWarning ) );
            }
        } );
    }

    @Test
    public void shouldNotNotifyOnDynamicPropertyLookupWithSingleLabelAndNegativePredicate() throws Exception
    {
        Stream.of( "CYPHER 2.3", "CYPHER 3.1", "CYPHER 3.2", "CYPHER 3.3" ).forEach( version ->
        {
            db().execute( "CREATE INDEX ON :Person(name)" );
            db().execute( "Call db.awaitIndexes()" );
            shouldNotNotifyInStream( version, "EXPLAIN MATCH (n:Person) WHERE n['key-' + n.name] <> 'value' RETURN n" );
        } );
    }

    @Test
    public void shouldWarnOnUnfulfillableIndexSeekUsingDynamicPropertyAndMultipleLabels() throws Exception
    {
        Stream.of( "CYPHER 2.3", "CYPHER 3.1", "CYPHER 3.2", "CYPHER 3.3" ).forEach( version ->
        {

            db().execute( "CREATE INDEX ON :Person(name)" );
            db().execute( "CREATE INDEX ON :Jedi(weapon)" );
            db().execute( "Call db.awaitIndexes()" );

            assertNotifications( version + "EXPLAIN MATCH (n:Person:Jedi) WHERE n['key-' + n.name] = 'value' RETURN n",
                    containsItem( dynamicPropertyWarning ) );
        } );
    }

    @Test
    public void shouldWarnOnFutureAmbiguousRelTypeSeparator() throws Exception
    {
        List<String> deprecatedQueries = Arrays.asList( "explain MATCH (a)-[:A|:B|:C {foo:'bar'}]-(b) RETURN a,b", "explain MATCH (a)-[x:A|:B|:C]-() RETURN a",
                "explain MATCH (a)-[:A|:B|:C*]-() RETURN a" );

        List<String> nonDeprecatedQueries =
                Arrays.asList( "explain MATCH (a)-[:A|B|C {foo:'bar'}]-(b) RETURN a,b", "explain MATCH (a)-[:A|:B|:C]-(b) RETURN a,b",
                        "explain MATCH (a)-[:A|B|C]-(b) RETURN a,b" );

        for ( String query : deprecatedQueries )
        {
            assertNotifications( "CYPHER 3.3 " + query, containsItem( deprecatedSeparatorWarning ) );
        }

        for ( String query : nonDeprecatedQueries )
        {
            assertNotifications( "CYPHER 3.3 " + query, containsNoItem( deprecatedSeparatorWarning ) );
        }
    }

    @Test
    public void shouldWarnOnBindingVariableLengthRelationship() throws Exception
    {
        assertNotifications( "CYPHER 3.3 explain MATCH ()-[rs*]-() RETURN rs", containsItem( depracatedBindingWarning ) );

        assertNotifications( "CYPHER 3.3 explain MATCH p = ()-[*]-() RETURN relationships(p) AS rs", containsNoItem( depracatedBindingWarning ) );
    }

    @Test
    public void shouldWarnOnCartesianProduct() throws Exception
    {

        Stream.of( "CYPHER 3.1", "CYPHER 3.2", "CYPHER 3.3" ).forEach( version ->
        {
            assertNotifications( version + "explain match (a)-->(b), (c)-->(d) return *", containsItem( cartesianProductWarning ) );

            assertNotifications( version + "explain cypher runtime=compiled match (a)-->(b), (c)-->(d) return *", containsItem( cartesianProductWarning ) );

            assertNotifications( version + "explain cypher runtime=interpreted match (a)-->(b), (c)-->(d) return *", containsItem( cartesianProductWarning ) );
        } );
    }

    @Test
    public void shouldNotNotifyOnCartesianProductWithoutExplain() throws Exception
    {
        Stream.of( "CYPHER 2.3", "CYPHER 3.1", "CYPHER 3.2", "CYPHER 3.3" ).forEach(
                version -> shouldNotNotifyInStream( version, " match (a)-->(b), (c)-->(d) return *" ) );
    }

    @Test
    public void shouldWarnOnMissingLabel() throws Exception
    {
        assertNotifications( "EXPLAIN MATCH (a:NO_SUCH_THING) RETURN a", containsItem( unknownLabelWarning ) );
    }

    @Test
    public void shouldWarnOnMissingLabelWithCommentInBeginningWithOlderCypherVersions() throws Exception
    {
        assertNotifications( "CYPHER 2.3 EXPLAIN//TESTING \nMATCH (n:X) return n Limit 1", containsItem( unknownLabelWarning ) );

        assertNotifications( "CYPHER 3.1 EXPLAIN//TESTING \nMATCH (n:X) return n Limit 1", containsItem( unknownLabelWarning ) );
    }

    @Test
    public void shouldWarnOnMissingLabelWithCommentInBeginning() throws Exception
    {
        assertNotifications( "EXPLAIN//TESTING \nMATCH (n:X) return n Limit 1", containsItem( unknownLabelWarning ) );
    }

    @Test
    public void shouldWarnOnMissingLabelWithCommentInBeginningTwoLines() throws Exception
    {
        assertNotifications( "//TESTING \n //TESTING \n EXPLAIN MATCH (n)\n MATCH (b:X) return n,b Limit 1", containsItem( unknownLabelWarning ) );
    }

    @Test
    public void shouldWarnOnMissingLabelWithCommentInBeginningOnOneLine() throws Exception
    {
        assertNotifications( "explain /* Testing */ MATCH (n:X) RETURN n", containsItem( unknownLabelWarning ) );
    }

    @Test
    public void shouldWarnOnMissingLabelWithCommentInMiddel() throws Exception
    {
        assertNotifications( "EXPLAIN\nMATCH (n)\n//TESTING \nMATCH (n:X)\nreturn n Limit 1", containsItem( unknownLabelWarning ) );
    }

    @Test
    public void shouldNotNotifyForMissingLabelOnUpdate() throws Exception
    {
        Stream.of( "CYPHER 2.3", "CYPHER 3.1", "CYPHER 3.2", "CYPHER 3.3" ).forEach(
                version -> shouldNotNotifyInStream( version, " EXPLAIN CREATE (n:Person)" ) );
    }

    @Test
    public void shouldWarnOnMissingRelationshipType() throws Exception
    {
        assertNotifications( "EXPLAIN MATCH ()-[a:NO_SUCH_THING]->() RETURN a", containsItem( unknownRelatonshipWarning ) );
    }

    @Test
    public void shouldWarnOnMissingRelationshipTypeWithComment() throws Exception
    {
        assertNotifications( "EXPLAIN /*Comment*/ MATCH ()-[a:NO_SUCH_THING]->() RETURN a", containsItem( unknownRelatonshipWarning ) );
    }

    @Test
    public void shouldWarnOnMissingProperty() throws Exception
    {
        assertNotifications( "EXPLAIN MATCH (a {NO_SUCH_THING: 1337}) RETURN a", containsItem( unknownPropertyKeyWarning ) );
    }

    @Test
    public void shouldNotNotifyForMissingPropertiesOnUpdate() throws Exception
    {
        Stream.of( "CYPHER 2.3", "CYPHER 3.1", "CYPHER 3.2", "CYPHER 3.3" ).forEach(
                version -> shouldNotNotifyInStream( version, " EXPLAIN CREATE (n {prop: 42})" ) );
    }

    @Test
    public void shouldWarnThatStartIsDeprecatedForAllNodeScan()
    {
        assertNotifications( "EXPLAIN START n=node(*) RETURN n", containsItem( deprecatedStartWarning ) );
    }

    @Test
    public void shouldWarnThatStartIsDeprecatedForNodeById()
    {
        assertNotifications( "EXPLAIN START n=node(1337) RETURN n", containsItem( deprecatedStartWarning ) );
    }

    @Test
    public void shouldWarnThatStartIsDeprecatedForNodeByIds()
    {
        assertNotifications( "EXPLAIN START n=node(42,1337) RETURN n", containsItem( deprecatedStartWarning ) );
    }

    @Test
    public void shouldWarnThatStartIsDeprecatedForNodeIndexSeek()
    {
        try ( Transaction ignore = db().beginTx() )
        {
            db().index().forNodes( "index" );
        }
        assertNotifications( "EXPLAIN START n=node:index(key = 'value') RETURN n", containsItem( deprecatedStartWarning ) );
    }

    @Test
    public void shouldWarnThatStartIsDeprecatedForNodeIndexSearch()
    {
        try ( Transaction ignore = db().beginTx() )
        {
            db().index().forNodes( "index" );
        }
        assertNotifications( "EXPLAIN START n=node:index('key:value*') RETURN n", containsItem( deprecatedStartWarning ) );
    }

    @Test
    public void shouldWarnThatStartIsDeprecatedForAllRelScan()
    {
        assertNotifications( "EXPLAIN START r=relationship(*) RETURN r", containsItem( deprecatedStartWarning ) );
    }

    @Test
    public void shouldWarnThatStartIsDeprecatedForRelById()
    {
        assertNotifications( "EXPLAIN START r=relationship(1337) RETURN r", containsItem( deprecatedStartWarning ) );
    }

    @Test
    public void shouldWarnThatStartIsDeprecatedForRelByIds()
    {
        assertNotifications( "EXPLAIN START r=relationship(42,1337) RETURN r", containsItem( deprecatedStartWarning ) );
    }

    @Test
    public void shouldWarnThatStartIsDeprecatedForRelIndexSeek()
    {
        try ( Transaction ignore = db().beginTx() )
        {
            db().index().forRelationships( "index" );
        }
        assertNotifications( "EXPLAIN START r=relationship:index(key = 'value') RETURN r", containsItem( deprecatedStartWarning ) );
    }

    @Test
    public void shouldWarnThatStartIsDeprecatedForRelIndexSearch()
    {
        try ( Transaction ignore = db().beginTx() )
        {
            db().index().forRelationships( "index" );
        }
        assertNotifications( "EXPLAIN START r=relationship:index('key:value*') RETURN r", containsItem( deprecatedStartWarning ) );
    }

    @Test
    public void shouldWarnOnMissingPropertyWithComment() throws Exception
    {
        assertNotifications( "EXPLAIN /*Comment*/ MATCH (a {NO_SUCH_THING: 1337}) RETURN a", containsItem( unknownPropertyKeyWarning ) );
    }

    private void assertNotifications( String query, Matcher<Iterable<Notification>> matchesExpectation )
    {
        try ( Result result = db().execute( query ) )
        {
            assertThat( result.getNotifications(), matchesExpectation );
        }
    }

    private Matcher<Notification> notification(
            String code,
            Matcher<String> description,
            Matcher<InputPosition> position,
            SeverityLevel severity )
    {
        return new TypeSafeMatcher<Notification>()
        {
            @Override
            protected boolean matchesSafely( Notification item )
            {
                return code.equals( item.getCode() ) &&
                       description.matches( item.getDescription() ) &&
                       position.matches( item.getPosition() ) &&
                       severity.equals( item.getSeverity() );
            }

            @Override
            public void describeTo( Description target )
            {
                target.appendText( "Notification{code=" ).appendValue( code )
                        .appendText( ", description=[" ).appendDescriptionOf( description )
                        .appendText( "], position=[" ).appendDescriptionOf( position )
                        .appendText( "], severity=" ).appendValue( severity )
                        .appendText( "}" );
            }
        };
    }

    private GraphDatabaseAPI db()
    {
        return rule.getGraphDatabaseAPI();
    }

    private <T> Matcher<Iterable<T>> containsItem( Matcher<T> itemMatcher )
    {
        return new TypeSafeMatcher<Iterable<T>>()
        {
            @Override
            protected boolean matchesSafely( Iterable<T> items )
            {
                for ( T item : items )
                {
                    if ( itemMatcher.matches( item ) )
                    {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "an iterable containing " ).appendDescriptionOf( itemMatcher );
            }
        };
    }

    private <T> Matcher<Iterable<T>> containsNoItem( Matcher<T> itemMatcher )
    {
        return new TypeSafeMatcher<Iterable<T>>()
        {
            @Override
            protected boolean matchesSafely( Iterable<T> items )
            {
                for ( T item : items )
                {
                    if ( itemMatcher.matches( item ) )
                    {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "an iterable not containing " ).appendDescriptionOf( itemMatcher );
            }
        };
    }

    private void shouldNotifyInStream( String version, String query, InputPosition pos, NotificationCode code )
    {
        //when
        Result result = db().execute( version + query );

        //then
        NotificationCode.Notification notification = code.notification( pos );
        assertThat( Iterables.asList( result.getNotifications() ), Matchers.hasItems( notification ) );
        Map<String,Object> arguments = result.getExecutionPlanDescription().getArguments();
        assertThat( arguments.get( "version" ), equalTo( version ) );
        result.close();
    }

    private void shouldNotifyInStreamWithDetail( String version, String query, InputPosition pos, NotificationCode code, NotificationDetail detail )
    {
        //when
        Result result = db().execute( version + query );

        //then
        NotificationCode.Notification notification = code.notification( pos, detail );
        assertThat( Iterables.asList( result.getNotifications() ), Matchers.hasItems( notification ) );
        Map<String,Object> arguments = result.getExecutionPlanDescription().getArguments();
        assertThat( arguments.get( "version" ), equalTo( version ) );
        result.close();
    }

    private void shouldNotNotifyInStream( String version, String query )
    {
        // when
        Result result = db().execute( version + query );

        // then
        assertThat( Iterables.asList( result.getNotifications() ), empty() );
        Map<String,Object> arguments = result.getExecutionPlanDescription().getArguments();
        assertThat( arguments.get( "version" ), equalTo( version ) );
        result.close();
    }

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

    private Matcher<Notification> cartesianProductWarning = notification( "Neo.ClientNotification.Statement.CartesianProductWarning", containsString(
            "If a part of a query contains multiple disconnected patterns, this will build a " +
                    "cartesian product between all those parts. This may produce a large amount of data and slow down" + " query processing. " +
                    "While occasionally intended, it may often be possible to reformulate the query that avoids the " + "use of this cross " +
                    "product, perhaps by adding a relationship between the different parts or by using OPTIONAL MATCH" ), any( InputPosition.class ),
            SeverityLevel.WARNING );

    private Matcher<Notification> largeLabelCSVWarning = notification( "Neo.ClientNotification.Statement.NoApplicableIndexWarning", containsString(
            "Using LOAD CSV with a large data set in a query where the execution plan contains the " +
                    "Using LOAD CSV followed by a MATCH or MERGE that matches a non-indexed label will most likely " +
                    "not perform well on large data sets. Please consider using a schema index." ), any( InputPosition.class ), SeverityLevel.WARNING );

    private Matcher<Notification> deprecatedFeatureWarning =
            notification( "Neo.ClientNotification.Statement.FeatureDeprecationWarning", containsString( "The query used a deprecated function." ),
                    any( InputPosition.class ), SeverityLevel.WARNING );

    private Matcher<Notification> deprecatedStartWarning = notification( "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
            containsString( "START has been deprecated and will be removed in a future version. " ), any( InputPosition.class ), SeverityLevel.WARNING );

    private Matcher<Notification> deprecatedProcedureWarning =
            notification( "Neo.ClientNotification.Statement.FeatureDeprecationWarning", containsString( "The query used a deprecated procedure." ),
                    any( InputPosition.class ), SeverityLevel.WARNING );

    private Matcher<Notification> deprecatedProcedureReturnFieldWarning =
            notification( "Neo.ClientNotification.Statement.FeatureDeprecationWarning", containsString( "The query used a deprecated field from a procedure." ),
                    any( InputPosition.class ), SeverityLevel.WARNING );

    private Matcher<Notification> depracatedBindingWarning = notification( "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
            containsString( "Binding relationships to a list in a variable length pattern is deprecated." ), any( InputPosition.class ),
            SeverityLevel.WARNING );

    private Matcher<Notification> deprecatedSeparatorWarning = notification( "Neo.ClientNotification.Statement.FeatureDeprecationWarning", containsString(
            "The semantics of using colon in the separation of alternative relationship " +
                    "types in conjunction with the use of variable binding, inlined property " +
                    "predicates, or variable length will change in a future version." ), any( InputPosition.class ), SeverityLevel.WARNING );

    private Matcher<Notification> eagerOperatorWarning = notification( "Neo.ClientNotification.Statement.EagerOperatorWarning", containsString(
            "Using LOAD CSV with a large data set in a query where the execution plan contains the " +
                    "Eager operator could potentially consume a lot of memory and is likely to not perform well. " +
                    "See the Neo4j Manual entry on the Eager operator for more information and hints on " + "how problems could be avoided." ),
            any( InputPosition.class ), SeverityLevel.WARNING );

    private Matcher<Notification> unknownPropertyKeyWarning =
            notification( "Neo.ClientNotification.Statement.UnknownPropertyKeyWarning", containsString( "the missing property name is" ),
                    any( InputPosition.class ), SeverityLevel.WARNING );

    private Matcher<Notification> unknownRelatonshipWarning =
            notification( "Neo.ClientNotification.Statement.UnknownRelationshipTypeWarning", containsString( "the missing relationship type is" ),
                    any( InputPosition.class ), SeverityLevel.WARNING );

    private Matcher<Notification> unknownLabelWarning =
            notification( "Neo.ClientNotification.Statement.UnknownLabelWarning", containsString( "the missing label name is" ), any( InputPosition.class ),
                    SeverityLevel.WARNING );

    private Matcher<Notification> dynamicPropertyWarning = notification( "Neo.ClientNotification.Statement.DynamicPropertyWarning",
            containsString( "Using a dynamic property makes it impossible to use an index lookup for this query" ), any( InputPosition.class ),
            SeverityLevel.WARNING );

    private Matcher<Notification> joinHintUnsuportedWarning = notification( "Neo.Status.Statement.JoinHintUnsupportedWarning",
            containsString( "Using RULE planner is unsupported for queries with join hints, please use COST planner instead" ), any( InputPosition.class ),
            SeverityLevel.WARNING );
}
