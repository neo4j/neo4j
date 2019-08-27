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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.impl.notification.NotificationDetail;
import org.neo4j.internal.helpers.collection.Iterables;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.impl.notification.NotificationCode.EAGER_LOAD_CSV;
import static org.neo4j.graphdb.impl.notification.NotificationCode.INDEX_HINT_UNFULFILLABLE;
import static org.neo4j.graphdb.impl.notification.NotificationCode.LENGTH_ON_NON_PATH;
import static org.neo4j.graphdb.impl.notification.NotificationCode.RUNTIME_UNSUPPORTED;
import static org.neo4j.graphdb.impl.notification.NotificationCode.UNBOUNDED_SHORTEST_PATH;
import static org.neo4j.graphdb.impl.notification.NotificationDetail.Factory.index;

class NotificationAcceptanceTest extends NotificationTestSupport
{

    @Test
    void shouldWarnWhenRequestingCompiledRuntimeOnUnsupportedQuery()
    {
        Stream.of( "CYPHER 3.5", "CYPHER 4.0" ).forEach(
                version -> shouldNotifyInStreamWithDetail( version,
                                                           "EXPLAIN CYPHER runtime=compiled RETURN 1",
                                                           InputPosition.empty,
                                                           RUNTIME_UNSUPPORTED,
                                                           NotificationDetail.Factory.message( "Runtime unsupported",
                                                                                               "This version of Neo4j does not " +
                                                                                                       "support requested runtime: compiled" ) ) );
    }

    @Test
    void shouldWarnWhenRequestingSlottedRuntimeOnUnsupportedQuery()
    {
        Stream.of( "CYPHER 3.5", "CYPHER 4.0" ).forEach(
                version -> shouldNotifyInStreamWithDetail( version,
                                                           "EXPLAIN CYPHER runtime=slotted RETURN 1",
                                                           InputPosition.empty,
                                                           RUNTIME_UNSUPPORTED,
                                                           NotificationDetail.Factory.message( "Runtime unsupported",
                                                                                               "This version of Neo4j does not " +
                                                                                                       "support requested runtime: slotted" ) ) );
    }

    @Test
    void shouldGetErrorWhenUsingCreateUniqueWhenCypherVersionIs3_5()
    {
        // when
        QueryExecutionException exception = assertThrows( QueryExecutionException.class,
                () -> db.executeTransactionally( "CYPHER 3.5 MATCH (b) WITH b LIMIT 1 CREATE UNIQUE (b)-[:REL]->()" ) );
        assertThat( exception.getMessage(), containsString( "CREATE UNIQUE is no longer supported. You can achieve the same result using MERGE" ) );
    }

    @Test
    void shouldWarnWhenUsingLengthOnNonPath()
    {
        Stream.of( "CYPHER 3.5", "CYPHER 4.0" ).forEach( version ->
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
    void shouldNotNotifyWhenUsingLengthOnPath()
    {
        Stream.of( "CYPHER 3.5", "CYPHER 4.0" ).forEach(
                version -> shouldNotNotifyInStream( version, " explain match p=(a)-[*]->(b) return length(p)" ) );
    }

    @Test
    void shouldNotNotifyWhenUsingSizeOnCollection()
    {
        Stream.of( "CYPHER 3.5", "CYPHER 4.0" ).forEach(
                version -> shouldNotNotifyInStream( version, "explain return size([1, 2, 3])" ) );
    }

    @Test
    void shouldNotNotifyWhenUsingSizeOnString()
    {
        Stream.of( "CYPHER 3.5", "CYPHER 4.0" ).forEach(
                version -> shouldNotNotifyInStream( version, " explain return size('a string')" ) );
    }

    @Test
    void shouldNotNotifyForCostUnsupportedUpdateQueryIfPlannerNotExplicitlyRequested()
    {
        Stream.of( "CYPHER 3.5", "CYPHER 4.0" ).forEach(
                version -> shouldNotNotifyInStream( version, " EXPLAIN MATCH (n:Movie) SET n.title = 'The Movie'" ) );
    }

    @Test
    void shouldNotNotifyForCostSupportedUpdateQuery()
    {
        Stream.of( "CYPHER 3.5", "CYPHER 4.0" ).forEach( version ->
        {
            shouldNotNotifyInStream( version, "EXPLAIN CYPHER planner=cost MATCH (n:Movie) SET n:Seen" );
            shouldNotNotifyInStream( version, "EXPLAIN CYPHER planner=idp MATCH (n:Movie) SET n:Seen" );
            shouldNotNotifyInStream( version, "EXPLAIN CYPHER planner=dp MATCH (n:Movie) SET n:Seen" );
        } );
    }

    @Test
    void shouldWarnOnPotentiallyCachedQueries()
    {
        Stream.of( "CYPHER 3.5", "CYPHER 4.0" ).forEach( version ->
        {
            assertNotifications( version + "explain match (a)-->(b), (c)-->(d) return *", containsItem( cartesianProductWarning ) );

            // no warning without explain
            shouldNotNotifyInStream( version, "match (a)-->(b), (c)-->(d) return *" );
        } );
    }

    @Test
    void shouldWarnOnceWhenSingleIndexHintCannotBeFulfilled()
    {
        Stream.of( "CYPHER 3.5", "CYPHER 4.0" ).forEach(
                version -> shouldNotifyInStreamWithDetail( version, " EXPLAIN MATCH (n:Person) USING INDEX n:Person(name) WHERE n.name = 'John' RETURN n",
                        InputPosition.empty, INDEX_HINT_UNFULFILLABLE, index( "Person", "name" ) ) );
    }

    @Test
    void shouldWarnOnEachUnfulfillableIndexHint()
    {
        String query = " EXPLAIN MATCH (n:Person), (m:Party), (k:Animal) " + "USING INDEX n:Person(name) " + "USING INDEX m:Party(city) " +
                "USING INDEX k:Animal(species) " + "WHERE n.name = 'John' AND m.city = 'Reykjavik' AND k.species = 'Sloth' " + "RETURN n";

        Stream.of( "CYPHER 3.5", "CYPHER 4.0" ).forEach( version ->
        {
            shouldNotifyInStreamWithDetail( version, query, InputPosition.empty, INDEX_HINT_UNFULFILLABLE, index( "Person", "name" ) );
            shouldNotifyInStreamWithDetail( version, query, InputPosition.empty, INDEX_HINT_UNFULFILLABLE, index( "Party", "city" ) );
            shouldNotifyInStreamWithDetail( version, query, InputPosition.empty, INDEX_HINT_UNFULFILLABLE, index( "Animal", "species" ) );
        } );
    }

    @Test
    void shouldNotNotifyOnLiteralMaps()
    {
        Stream.of( "CYPHER 3.5", "CYPHER 4.0" ).forEach(
                version -> shouldNotNotifyInStream( version, " explain return { id: 42 } " ) );
    }

    @Test
    void shouldNotNotifyOnNonExistingLabelUsingLoadCSV()
    {
        Stream.of( "CYPHER 3.5", "CYPHER 4.0" ).forEach( version ->
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
    void shouldNotNotifyOnNonExistingRelTypeUsingLoadCSV()
    {
        Stream.of( "CYPHER 3.5", "CYPHER 4.0" ).forEach( version ->
        {
            // create rel
            shouldNotNotifyInStream( version, " EXPLAIN LOAD CSV WITH HEADERS FROM 'file:///fake.csv' AS row CREATE ()-[:T]->()" );

            // merge rel
            shouldNotNotifyInStream( version, " EXPLAIN LOAD CSV WITH HEADERS FROM 'file:///fake.csv' AS row MERGE ()-[:T]->()" );
        } );
    }

    @Test
    void shouldNotNotifyOnNonExistingPropKeyIdUsingLoadCSV()
    {
        Stream.of( "CYPHER 3.5", "CYPHER 4.0" ).forEach( version ->
        {
            // create node
            shouldNotNotifyInStream( version, " EXPLAIN LOAD CSV WITH HEADERS FROM 'file:///fake.csv' AS row CREATE (n) SET n.p = 'a'" );

            // merge node
            shouldNotNotifyInStream( version, " EXPLAIN LOAD CSV WITH HEADERS FROM 'file:///fake.csv' AS row MERGE (n) ON CREATE SET n.p = 'a'" );
        } );
    }

    @Test
    void shouldNotNotifyOnEagerBeforeLoadCSVDelete()
    {
        Stream.of( "CYPHER 3.5", "CYPHER 4.0" ).forEach( version -> shouldNotNotifyInStream( version,
                "EXPLAIN MATCH (n) DELETE n WITH * LOAD CSV FROM 'file:///ignore/ignore.csv' AS line MERGE () RETURN line" ) );
    }

    @Test
    void shouldNotNotifyOnEagerBeforeLoadCSVCreate()
    {
        Stream.of( "CYPHER 3.5", "CYPHER 4.0" ).forEach( version ->
                assertNotifications( version + "EXPLAIN MATCH (a), (b) CREATE (c) WITH c LOAD CSV FROM 'file:///ignore/ignore.csv' AS line RETURN *",
                        containsNoItem( eagerOperatorWarning ) ) );
    }

    @Test
    void shouldWarnOnEagerAfterLoadCSV()
    {
        Stream.of( "CYPHER 3.5", "CYPHER 4.0" ).forEach( version -> shouldNotifyInStream( version,
                "EXPLAIN MATCH (n) LOAD CSV FROM 'file:///ignore/ignore.csv' AS line WITH * DELETE n MERGE () RETURN line", InputPosition.empty,
                EAGER_LOAD_CSV ) );
    }

    @Test
    void shouldNotNotifyOnLoadCSVWithoutEager()
    {
        Stream.of( "CYPHER 3.5", "CYPHER 4.0" ).forEach(
                version -> shouldNotNotifyInStream( version, "EXPLAIN LOAD CSV FROM 'file:///ignore/ignore.csv' AS line MATCH (:A) CREATE (:B) RETURN line" ) );
    }

    @Test
    void shouldNotNotifyOnEagerWithoutLoadCSV()
    {
        Stream.of( "CYPHER 3.5", "CYPHER 4.0" ).forEach(
                version -> assertNotifications( version + "EXPLAIN MATCH (a), (b) CREATE (c) RETURN *", containsNoItem( eagerOperatorWarning ) ) );
    }

    @Test
    void shouldWarnOnLargeLabelScansWithLoadCVSMatch()
    {
        for ( int i = 0; i < 11; i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                db.createNode().addLabel( label( "A" ) );
                tx.commit();
            }
        }
        Stream.of( "CYPHER 3.5", "CYPHER 4.0" ).forEach(
                version -> assertNotifications( version + "EXPLAIN LOAD CSV FROM 'file:///ignore/ignore.csv' AS line MATCH (a:A) RETURN *",
                        containsNoItem( largeLabelCSVWarning ) ) );
    }

    @Test
    void shouldWarnOnLargeLabelScansWithLoadCVSMerge()
    {
        for ( int i = 0; i < 11; i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                db.createNode().addLabel( label( "A" ) );
                tx.commit();
            }
        }
        Stream.of( "CYPHER 3.5", "CYPHER 4.0" ).forEach(
                version -> assertNotifications( version + "EXPLAIN LOAD CSV FROM 'file:///ignore/ignore.csv' AS line MERGE (a:A) RETURN *",
                        containsNoItem( largeLabelCSVWarning ) ) );
    }

    @Test
    void shouldNotWarnOnSmallLabelScansWithLoadCVS()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode().addLabel( label( "A" ) );
            tx.commit();
        }
        Stream.of( "CYPHER 3.5", "CYPHER 4.0" ).forEach( version ->
        {
            shouldNotNotifyInStream( version, "EXPLAIN LOAD CSV FROM 'file:///ignore/ignore.csv' AS line MATCH (a:A) RETURN *" );
            shouldNotNotifyInStream( version, "EXPLAIN LOAD CSV FROM 'file:///ignore/ignore.csv' AS line MERGE (a:A) RETURN *" );
        } );
    }

    @Test
    void shouldWarnOnUnboundedShortestPath()
    {
        Stream.of( "CYPHER 3.5", "CYPHER 4.0" ).forEach(
                version -> shouldNotifyInStream( version, "EXPLAIN MATCH p = shortestPath((n)-[*]->(m)) RETURN m", new InputPosition( 44, 1, 45 ),
                        UNBOUNDED_SHORTEST_PATH ) );
    }

    @Test
    void shouldNotNotifyOnDynamicPropertyLookupWithNoLabels()
    {
        Stream.of( "CYPHER 3.5", "CYPHER 4.0" ).forEach( version ->
        {
            db.executeTransactionally( "CREATE INDEX ON :Person(name)" );
            db.executeTransactionally( "Call db.awaitIndexes()" );
            shouldNotNotifyInStream( version, "EXPLAIN MATCH (n) WHERE n['key-' + n.name] = 'value' RETURN n" );
        } );
    }

    @Test
    void shouldWarnOnDynamicPropertyLookupWithBothStaticAndDynamicProperties()
    {
        Stream.of( "CYPHER 3.5", "CYPHER 4.0" ).forEach( version ->
        {
            db.executeTransactionally( "CREATE INDEX ON :Person(name)" );
            db.executeTransactionally( "Call db.awaitIndexes()" );
            assertNotifications( version + "EXPLAIN MATCH (n:Person) WHERE n.name = 'Tobias' AND n['key-' + n.name] = 'value' RETURN n",
                    containsItem( dynamicPropertyWarning ));
        } );
    }

    @Test
    void shouldNotNotifyOnDynamicPropertyLookupWithLabelHavingNoIndex()
    {
        Stream.of( "CYPHER 3.5", "CYPHER 4.0" ).forEach( version ->
        {
            db.executeTransactionally( "CREATE INDEX ON :Person(name)" );
            db.executeTransactionally( "Call db.awaitIndexes()" );
            try ( Transaction tx = db.beginTx() )
            {
                db.createNode().addLabel( label( "Foo" ) );
                tx.commit();
            }
            shouldNotNotifyInStream( version, "EXPLAIN MATCH (n:Foo) WHERE n['key-' + n.name] = 'value' RETURN n" );

        } );
    }

    @Test
    void shouldWarnOnUnfulfillableIndexSeekUsingDynamicProperty()
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

        Stream.of( "CYPHER 3.5", "CYPHER 4.0" ).forEach( version ->
        {
            for ( String query : queries )
            {
                db.executeTransactionally( "CREATE INDEX ON :Person(name)" );
                db.executeTransactionally( "Call db.awaitIndexes()" );
                assertNotifications( version + query, containsItem( dynamicPropertyWarning ) );
            }
        } );
    }

    @Test
    void shouldNotNotifyOnDynamicPropertyLookupWithSingleLabelAndNegativePredicate()
    {
        Stream.of( "CYPHER 3.5", "CYPHER 4.0" ).forEach( version ->
        {
            db.executeTransactionally( "CREATE INDEX ON :Person(name)" );
            db.executeTransactionally( "Call db.awaitIndexes()" );
            shouldNotNotifyInStream( version, "EXPLAIN MATCH (n:Person) WHERE n['key-' + n.name] <> 'value' RETURN n" );
        } );
    }

    @Test
    void shouldWarnOnUnfulfillableIndexSeekUsingDynamicPropertyAndMultipleLabels()
    {
        Stream.of( "CYPHER 3.5", "CYPHER 4.0" ).forEach( version ->
        {
            db.executeTransactionally( "CREATE INDEX ON :Person(name)" );
            db.executeTransactionally( "Call db.awaitIndexes()" );

            assertNotifications( version + "EXPLAIN MATCH (n:Person:Foo) WHERE n['key-' + n.name] = 'value' RETURN n",
                    containsItem( dynamicPropertyWarning ) );
        } );
    }

    @Test
    void shouldWarnOnUnfulfillableIndexSeekUsingDynamicPropertyAndMultipleIndexedLabels()
    {
        Stream.of( "CYPHER 3.5", "CYPHER 4.0" ).forEach( version ->
        {
            db.executeTransactionally( "CREATE INDEX ON :Person(name)" );
            db.executeTransactionally( "CREATE INDEX ON :Jedi(weapon)" );
            db.executeTransactionally( "Call db.awaitIndexes()" );

            assertNotifications( version + "EXPLAIN MATCH (n:Person:Jedi) WHERE n['key-' + n.name] = 'value' RETURN n",
                    containsItem( dynamicPropertyWarning ) );
        } );
    }

    @Test
    void shouldWarnOnCartesianProduct()
    {

        assertNotifications( "explain match (a)-->(b), (c)-->(d) return *", containsItem( cartesianProductWarning ) );

        assertNotifications( "explain cypher runtime=compiled match (a)-->(b), (c)-->(d) return *",
                containsItem( cartesianProductWarning ) );

        assertNotifications( "explain cypher runtime=interpreted match (a)-->(b), (c)-->(d) return *",
                containsItem( cartesianProductWarning ) );
    }

    @Test
    void shouldNotNotifyOnCartesianProductWithoutExplain()
    {
        Stream.of( "CYPHER 3.5", "CYPHER 4.0" ).forEach(
                version -> shouldNotNotifyInStream( version, " match (a)-->(b), (c)-->(d) return *" ) );
    }

    @Test
    void shouldWarnOnMissingLabel()
    {
        assertNotifications( "EXPLAIN MATCH (a:NO_SUCH_THING) RETURN a", containsItem( unknownLabelWarning ) );
    }

    @Test
    void shouldWarnOnMisspelledLabel()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode().addLabel( label( "Person" ) );
            tx.commit();
        }

        Stream.of( "CYPHER 3.5", "CYPHER 4.0" ).forEach( version ->
        {
            assertNotifications(version + "EXPLAIN MATCH (n:Preson) RETURN *", containsItem( unknownLabelWarning ) );
            shouldNotNotifyInStream( version, "EXPLAIN MATCH (n:Person) RETURN *" );
        });
    }

    @Test
    void shouldWarnOnMissingLabelWithCommentInBeginning()
    {
        assertNotifications( "EXPLAIN//TESTING \nMATCH (n:X) return n Limit 1", containsItem( unknownLabelWarning ) );
    }

    @Test
    void shouldWarnOnMissingLabelWithCommentInBeginningTwoLines()
    {
        assertNotifications( "//TESTING \n //TESTING \n EXPLAIN MATCH (n)\n MATCH (b:X) return n,b Limit 1", containsItem( unknownLabelWarning ) );
    }

    @Test
    void shouldWarnOnMissingLabelWithCommentInBeginningOnOneLine()
    {
        assertNotifications( "explain /* Testing */ MATCH (n:X) RETURN n", containsItem( unknownLabelWarning ) );
    }

    @Test
    void shouldWarnOnMissingLabelWithCommentInMiddle()
    {
        assertNotifications( "EXPLAIN\nMATCH (n)\n//TESTING \nMATCH (n:X)\nreturn n Limit 1", containsItem( unknownLabelWarning ) );
    }

    @Test
    void shouldNotNotifyForMissingLabelOnUpdate()
    {
        Stream.of( "CYPHER 3.5", "CYPHER 4.0" ).forEach(
                version -> shouldNotNotifyInStream( version, " EXPLAIN CREATE (n:Person)" ) );
    }

    @Test
    void shouldWarnOnMissingRelationshipType()
    {
        assertNotifications( "EXPLAIN MATCH ()-[a:NO_SUCH_THING]->() RETURN a", containsItem( unknownRelationshipWarning ) );
    }

    @Test
    void shouldWarnOnMisspelledRelationship()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode().addLabel( label( "Person" ) );
            tx.commit();
        }

        Stream.of( "CYPHER 3.5", "CYPHER 4.0" ).forEach( version ->
        {
            db.executeTransactionally( "CREATE (n)-[r:R]->(m)" );
            assertNotifications(version + "EXPLAIN MATCH ()-[r:r]->() RETURN *", containsItem( unknownRelationshipWarning ) );
            shouldNotNotifyInStream( version, "EXPLAIN MATCH ()-[r:R]->() RETURN *" );
        });
    }

    @Test
    void shouldWarnOnMissingRelationshipTypeWithComment()
    {
        assertNotifications( "EXPLAIN /*Comment*/ MATCH ()-[a:NO_SUCH_THING]->() RETURN a", containsItem( unknownRelationshipWarning ) );
    }

    @Test
    void shouldWarnOnMissingProperty()
    {
        assertNotifications( "EXPLAIN MATCH (a {NO_SUCH_THING: 1337}) RETURN a", containsItem( unknownPropertyKeyWarning ) );
    }

    @Test
    void shouldWarnOnMisspelledProperty()
    {
        db.executeTransactionally( "CREATE (n {prop : 42})" );

        Stream.of( "CYPHER 3.5", "CYPHER 4.0" ).forEach( version ->
        {
            db.executeTransactionally( "CREATE (n)-[r:R]->(m)" );
            assertNotifications(version + "EXPLAIN MATCH (n) WHERE n.propp = 43 RETURN n", containsItem( unknownPropertyKeyWarning ) );
            shouldNotNotifyInStream( version, "EXPLAIN MATCH (n) WHERE n.prop = 43 RETURN n" );
        });
    }

    @Test
    void shouldWarnOnMissingPropertyWithComment()
    {
        assertNotifications( "EXPLAIN /*Comment*/ MATCH (a {NO_SUCH_THING: 1337}) RETURN a", containsItem( unknownPropertyKeyWarning ) );
    }

    @Test
    void shouldNotNotifyForMissingPropertiesOnUpdate()
    {
        Stream.of( "CYPHER 3.5", "CYPHER 4.0" ).forEach(
                version -> shouldNotNotifyInStream( version, " EXPLAIN CREATE (n {prop: 42})" ) );
    }

    @Test
    void shouldGiveCorrectPositionWhetherFromCacheOrNot()
    {
        // Given
        String cachedQuery = "MATCH (a:L1) RETURN a";
        String nonCachedQuery = "MATCH (a:L2) RETURN a";
        //make sure we cache the query
        int limit = db.getDependencyResolver().resolveDependency( Config.class )
                .get( GraphDatabaseSettings.cypher_expression_recompilation_limit );
        try ( Transaction transaction = db.beginTx() )
        {
            for ( int i = 0; i < limit + 1; i++ )
            {
                db.execute( cachedQuery ).resultAsString();
            }
            transaction.commit();
        }

        // When
        try ( Transaction transaction = db.beginTx() )
        {
            Notification cachedNotification = Iterables.asList( db.execute( "EXPLAIN " + cachedQuery ).getNotifications() ).get( 0 );
            Notification nonCachedNotication = Iterables.asList( db.execute( "EXPLAIN " + nonCachedQuery ).getNotifications() ).get( 0 );

            // Then
            assertThat( cachedNotification.getPosition(), equalTo( new InputPosition( 17, 1, 18 ) ) );
            assertThat( nonCachedNotication.getPosition(), equalTo( new InputPosition( 17, 1, 18 ) ) );
        }
    }
}
