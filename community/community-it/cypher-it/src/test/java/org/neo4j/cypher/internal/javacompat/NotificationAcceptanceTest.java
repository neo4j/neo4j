/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.javacompat;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.graphdb.Label.label;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.exceptions.IndexHintException.IndexHintIndexType;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.notifications.NotificationCodeWithDescription;
import org.neo4j.notifications.NotificationDetail;

class NotificationAcceptanceTest extends NotificationTestSupport {

    @Test
    void shouldWarnWhenRequestingSlottedRuntimeOnUnsupportedQuery() {
        shouldNotifyInStream(
                "EXPLAIN CYPHER runtime=pipelined RETURN 1",
                NotificationCodeWithDescription.runtimeUnsupported(
                        InputPosition.empty,
                        "runtime=pipelined",
                        "runtime=slotted",
                        "This version of Neo4j does not support the requested runtime: `pipelined`"));
    }

    @Test
    void shouldNotNotifyForCostUnsupportedUpdateQueryIfPlannerNotExplicitlyRequested() {
        shouldNotNotifyInStream("EXPLAIN MATCH (n:Movie) SET n.title = 'The Movie'");
    }

    @Test
    void shouldNotNotifyForCostSupportedUpdateQuery() {
        shouldNotNotifyInStream("EXPLAIN CYPHER planner=cost MATCH (n:Movie) SET n:Seen");
        shouldNotNotifyInStream("EXPLAIN CYPHER planner=idp MATCH (n:Movie) SET n:Seen");
        shouldNotNotifyInStream("EXPLAIN CYPHER planner=dp MATCH (n:Movie) SET n:Seen");
    }

    @Test
    void shouldWarnOnPotentiallyCachedQueries() {
        assertNotifications("explain match (a)-->(b), (c)-->(d) return *", contains(cartesianProductNotification));
    }

    @Test
    void shouldWarnOnceWhenSingleIndexHintCannotBeFulfilled() {
        shouldNotifyInStream(
                " EXPLAIN MATCH (n:Person) USING INDEX n:Person(name) WHERE n.name = 'John' RETURN n",
                NotificationCodeWithDescription.indexHintUnfulfillable(
                        InputPosition.empty,
                        NotificationDetail.indexHint(EntityType.NODE, IndexHintIndexType.ANY, "n", "Person", "name"),
                        NotificationDetail.index(IndexHintIndexType.ANY, "Person", List.of("name"))));
    }

    @Test
    void shouldWarnOnEachUnfulfillableIndexHint() {
        String query = " EXPLAIN MATCH (n:Person), (m:Party), (k:Animal), (o:Other)"
                + "USING INDEX n:Person(name) "
                + "USING INDEX m:Party(city) "
                + "USING INDEX k:Animal(species) "
                + "USING TEXT INDEX o:Other(text)"
                + "WHERE n.name = 'John' AND m.city = 'Reykjavik' AND k.species = 'Sloth' AND o.text STARTS WITH 'a' "
                + "RETURN n";

        shouldNotifyInStream(
                query,
                NotificationCodeWithDescription.indexHintUnfulfillable(
                        InputPosition.empty,
                        NotificationDetail.indexHint(EntityType.NODE, IndexHintIndexType.ANY, "n", "Person", "name"),
                        NotificationDetail.index(IndexHintIndexType.ANY, "Person", List.of("name"))));
        shouldNotifyInStream(
                query,
                NotificationCodeWithDescription.indexHintUnfulfillable(
                        InputPosition.empty,
                        NotificationDetail.indexHint(EntityType.NODE, IndexHintIndexType.ANY, "m", "Party", "city"),
                        NotificationDetail.index(IndexHintIndexType.ANY, "Party", List.of("city"))));
        shouldNotifyInStream(
                query,
                NotificationCodeWithDescription.indexHintUnfulfillable(
                        InputPosition.empty,
                        NotificationDetail.indexHint(EntityType.NODE, IndexHintIndexType.ANY, "k", "Animal", "species"),
                        NotificationDetail.index(IndexHintIndexType.ANY, "Animal", List.of("species"))));
        shouldNotifyInStream(
                query,
                NotificationCodeWithDescription.indexHintUnfulfillable(
                        InputPosition.empty,
                        NotificationDetail.indexHint(EntityType.NODE, IndexHintIndexType.TEXT, "o", "Other", "text"),
                        NotificationDetail.index(IndexHintIndexType.TEXT, "Other", List.of("text"))));
    }

    @Test
    void shouldNotNotifyOnLiteralMaps() {
        shouldNotNotifyInStream("explain return { id: 42 } ");
    }

    @Test
    void shouldNotNotifyOnNonExistingLabelUsingLoadCSV() {
        // create node
        shouldNotNotifyInStream("EXPLAIN LOAD CSV WITH HEADERS FROM 'file:///fake.csv' AS row CREATE (n:Category)");

        // merge node
        shouldNotNotifyInStream("EXPLAIN LOAD CSV WITH HEADERS FROM 'file:///fake.csv' AS row MERGE (n:Category)");

        // set label to node
        shouldNotNotifyInStream(
                "EXPLAIN LOAD CSV WITH HEADERS FROM 'file:///fake.csv' AS row CREATE (n) SET n:Category");
    }

    @Test
    void shouldNotNotifyOnNonExistingRelTypeUsingLoadCSV() {
        // create rel
        shouldNotNotifyInStream("EXPLAIN LOAD CSV WITH HEADERS FROM 'file:///fake.csv' AS row CREATE ()-[:T]->()");

        // merge rel
        shouldNotNotifyInStream("EXPLAIN LOAD CSV WITH HEADERS FROM 'file:///fake.csv' AS row MERGE ()-[:T]->()");
    }

    @Test
    void shouldNotNotifyOnNonExistingPropKeyIdUsingLoadCSV() {
        // create node
        shouldNotNotifyInStream(
                "EXPLAIN LOAD CSV WITH HEADERS FROM 'file:///fake.csv' AS row CREATE (n) SET n.p = 'a'");

        // merge node
        shouldNotNotifyInStream(
                "EXPLAIN LOAD CSV WITH HEADERS FROM 'file:///fake.csv' AS row MERGE (n) ON CREATE SET n.p = 'a'");
    }

    @Test
    void shouldNotNotifyOnEagerBeforeLoadCSVDelete() {
        shouldNotNotifyInStream(
                "EXPLAIN MATCH (n) DELETE n WITH * LOAD CSV FROM 'file:///ignore/ignore.csv' AS line MERGE () RETURN line");
    }

    @Test
    void shouldNotNotifyOnEagerBeforeLoadCSVCreate() {
        assertNotifications(
                "EXPLAIN MATCH (a), (b) CREATE (c) WITH c LOAD CSV FROM 'file:///ignore/ignore.csv' AS line RETURN *",
                doesNotContain(eagerOperatorNotification));
    }

    @Test
    void shouldWarnOnEagerAfterLoadCSV() {
        shouldNotifyInStream(
                "EXPLAIN MATCH (n) LOAD CSV FROM 'file:///ignore/ignore.csv' AS line WITH * DELETE n MERGE () RETURN line",
                NotificationCodeWithDescription.eagerLoadCsv(InputPosition.empty));
    }

    @Test
    void shouldNotNotifyOnLoadCSVWithoutEager() {
        shouldNotNotifyInStream(
                "EXPLAIN LOAD CSV FROM 'file:///ignore/ignore.csv' AS line MATCH (:A) CREATE (:B) RETURN line");
    }

    @Test
    void shouldNotNotifyOnEagerWithoutLoadCSV() {
        assertNotifications("EXPLAIN MATCH (a), (b) CREATE (c) RETURN *", doesNotContain(eagerOperatorNotification));
    }

    @Test
    void shouldWarnOnLargeLabelScansWithLoadCVSMatch() {
        for (int i = 0; i < 11; i++) {
            try (Transaction tx = db.beginTx()) {
                tx.createNode().addLabel(label("A"));
                tx.commit();
            }
        }
        assertNotifications(
                "EXPLAIN LOAD CSV FROM 'file:///ignore/ignore.csv' AS line MATCH (a:A) RETURN *",
                doesNotContain(largeLabelCSVNotification));
    }

    @Test
    void shouldWarnOnLargeLabelScansWithLoadCVSMerge() {
        for (int i = 0; i < 11; i++) {
            try (Transaction tx = db.beginTx()) {
                tx.createNode().addLabel(label("A"));
                tx.commit();
            }
        }
        assertNotifications(
                "EXPLAIN LOAD CSV FROM 'file:///ignore/ignore.csv' AS line MERGE (a:A) RETURN *",
                doesNotContain(largeLabelCSVNotification));
    }

    @Test
    void shouldNotWarnOnSmallLabelScansWithLoadCVS() {
        try (Transaction tx = db.beginTx()) {
            tx.createNode().addLabel(label("A"));
            tx.commit();
        }
        shouldNotNotifyInStream("EXPLAIN LOAD CSV FROM 'file:///ignore/ignore.csv' AS line MATCH (a:A) RETURN *");
        shouldNotNotifyInStream("EXPLAIN LOAD CSV FROM 'file:///ignore/ignore.csv' AS line MERGE (a:A) RETURN *");
    }

    @Test
    void shouldWarnOnUnboundedShortestPath() {
        shouldNotifyInStream(
                "EXPLAIN MATCH p = shortestPath((n)-[*]->(m)) RETURN m",
                NotificationCodeWithDescription.unboundedShortestPath(new InputPosition(31, 1, 32), "(n)-[*]->(m)"));
    }

    @Test
    void shouldNotNotifyOnDynamicPropertyLookupWithNoLabels() {
        db.executeTransactionally("CREATE INDEX FOR (n:Person) ON (n.name)");
        db.executeTransactionally("Call db.awaitIndexes()");
        shouldNotNotifyInStream("EXPLAIN MATCH (n) WHERE n['key-' + n.name] = 'value' RETURN n");
    }

    @Test
    void shouldNotNotifyOnDynamicPropertyWhenIndexIsUsedForVariableAnyway() {
        db.executeTransactionally("CREATE INDEX FOR (n:Person) ON (n.name)");
        db.executeTransactionally("Call db.awaitIndexes()");
        shouldNotNotifyInStream(
                "EXPLAIN MATCH (n:Person) WHERE n.name = 'Tobias' AND n['key-' + n.name] = 'value' RETURN n");
    }

    @Test
    void shouldNotNotifyOnDynamicPropertyLookupWithLabelHavingNoIndex() {
        db.executeTransactionally("CREATE INDEX FOR (n:Person) ON (n.name)");
        db.executeTransactionally("Call db.awaitIndexes()");
        try (Transaction tx = db.beginTx()) {
            tx.createNode().addLabel(label("Foo"));
            tx.commit();
        }
        shouldNotNotifyInStream("EXPLAIN MATCH (n:Foo) WHERE n['key-' + n.name] = 'value' RETURN n");
    }

    @Test
    void shouldNotifyOnDynamicPropertyLookupWithPredicateHavingNoIndex() {
        db.executeTransactionally("CREATE INDEX FOR (n:Person) ON (n.name)");
        db.executeTransactionally("Call db.awaitIndexes()");
        assertNotifications(
                "EXPLAIN MATCH (n:Person) WHERE n.foo = 'Tobias' AND n['key-' + n.name] = 'value' RETURN n",
                contains(dynamicPropertyNotification));
    }

    @Test
    void shouldWarnOnUnfulfillableIndexSeekUsingDynamicProperty() {
        List<String> queries = new ArrayList<>();

        // dynamic property lookup with single label
        queries.add("EXPLAIN MATCH (n:Person) WHERE n['key-' + n.name] = 'value' RETURN n");

        // dynamic property lookup with explicit label check
        queries.add("EXPLAIN MATCH (n) WHERE n['key-' + n.name] = 'value' AND (n:Person) RETURN n");

        // dynamic property lookup with range seek
        queries.add("EXPLAIN MATCH (n:Person) WHERE n['key-' + n.name] > 10 RETURN n");

        // dynamic property lookup with range seek (reverse)
        queries.add("EXPLAIN MATCH (n:Person) WHERE 10 > n['key-' + n.name] RETURN n");

        // dynamic property lookup with a single label and property existence check with IS NOT NULL
        queries.add("EXPLAIN MATCH (n:Person) WHERE n['na' + 'me'] IS NOT NULL RETURN n");

        // dynamic property lookup with a single label and starts with
        queries.add("EXPLAIN MATCH (n:Person) WHERE n['key-' + n.name] STARTS WITH 'Foo' RETURN n");

        // dynamic property lookup with a single label and regex
        queries.add("EXPLAIN MATCH (n:Person) WHERE n['key-' + n.name] =~ 'Foo*' RETURN n");

        // dynamic property lookup with a single label and IN
        queries.add("EXPLAIN MATCH (n:Person) WHERE n['key-' + n.name] IN ['Foo', 'Bar'] RETURN n");

        db.executeTransactionally("CREATE INDEX FOR (n:Person) ON (n.name)");
        db.executeTransactionally("Call db.awaitIndexes()");
        for (String query : queries) {
            assertNotifications(query, contains(dynamicPropertyNotification));
        }
    }

    @Test
    void shouldNotNotifyOnDynamicPropertyLookupWithSingleLabelAndNegativePredicate() {
        db.executeTransactionally("CREATE INDEX FOR (n:Person) ON (n.name)");
        db.executeTransactionally("Call db.awaitIndexes()");
        shouldNotNotifyInStream("EXPLAIN MATCH (n:Person) WHERE n['key-' + n.name] <> 'value' RETURN n");
    }

    @Test
    void shouldWarnOnUnfulfillableIndexSeekUsingDynamicPropertyAndMultipleLabels() {
        db.executeTransactionally("CREATE INDEX FOR (n:Person) ON (n.name)");
        db.executeTransactionally("Call db.awaitIndexes()");
        assertNotifications(
                "EXPLAIN MATCH (n:Person:Foo) WHERE n['key-' + n.name] = 'value' RETURN n",
                contains(dynamicPropertyNotification));
    }

    @Test
    void shouldWarnOnUnfulfillableIndexSeekUsingDynamicPropertyAndMultipleIndexedLabels() {
        db.executeTransactionally("CREATE INDEX FOR (n:Person) ON (n.name)");
        db.executeTransactionally("CREATE INDEX FOR (n:Jedi) ON (n.weapon)");
        db.executeTransactionally("Call db.awaitIndexes()");
        assertNotifications(
                "EXPLAIN MATCH (n:Person:Jedi) WHERE n['key-' + n.name] = 'value' RETURN n",
                contains(dynamicPropertyNotification));
    }

    @Test
    void shouldWarnOnCartesianProduct() {

        assertNotifications("explain match (a)-->(b), (c)-->(d) return *", contains(cartesianProductNotification));

        assertNotifications(
                "explain cypher runtime=interpreted match (a)-->(b), (c)-->(d) return *",
                contains(cartesianProductNotification));
    }

    @Test
    void shouldNotifyOnCartesianProductWithoutExplain() {
        assertNotifications("match (a)-->(b), (c)-->(d) return *", contains(cartesianProductNotification));
    }

    @Test
    void shouldWarnOnMissingLabel() {
        assertNotifications("EXPLAIN MATCH (a:NO_SUCH_THING) RETURN a", contains(unknownLabelNotification));
    }

    @Test
    void shouldWarnOnMisspelledLabel() {
        try (Transaction tx = db.beginTx()) {
            tx.createNode().addLabel(label("Person"));
            tx.commit();
        }

        assertNotifications("EXPLAIN MATCH (n:Preson) RETURN *", contains(unknownLabelNotification));
        shouldNotNotifyInStream("EXPLAIN MATCH (n:Person) RETURN *");
    }

    @Test
    void shouldWarnOnMissingLabelWithCommentInBeginning() {
        assertNotifications("EXPLAIN//TESTING \nMATCH (n:X) return n Limit 1", contains(unknownLabelNotification));
    }

    @Test
    void shouldWarnOnMissingLabelWithCommentInBeginningTwoLines() {
        assertNotifications(
                "//TESTING \n //TESTING \n EXPLAIN MATCH (n)\n MATCH (b:X) return n,b Limit 1",
                contains(unknownLabelNotification));
    }

    @Test
    void shouldWarnOnMissingLabelWithCommentInBeginningOnOneLine() {
        assertNotifications("explain /* Testing */ MATCH (n:X) RETURN n", contains(unknownLabelNotification));
    }

    @Test
    void shouldWarnOnMissingLabelWithCommentInMiddle() {
        assertNotifications(
                "EXPLAIN\nMATCH (n)\n//TESTING \nMATCH (n:X)\nreturn n Limit 1", contains(unknownLabelNotification));
    }

    @Test
    void shouldNotNotifyForMissingLabelOnUpdate() {
        shouldNotNotifyInStream("EXPLAIN CREATE (n:Person)");
    }

    @Test
    void shouldWarnOnMissingRelationshipType() {
        assertNotifications(
                "EXPLAIN MATCH ()-[a:NO_SUCH_THING]->() RETURN a", contains(unknownRelationshipNotification));
    }

    @Test
    void shouldWarnOnMisspelledRelationship() {
        try (Transaction tx = db.beginTx()) {
            tx.createNode().addLabel(label("Person"));
            tx.commit();
        }

        db.executeTransactionally("CREATE (n)-[r:R]->(m)");
        assertNotifications("EXPLAIN MATCH ()-[r:r]->() RETURN *", contains(unknownRelationshipNotification));
        shouldNotNotifyInStream("EXPLAIN MATCH ()-[r:R]->() RETURN *");
    }

    @Test
    void shouldWarnOnMissingRelationshipTypeWithComment() {
        assertNotifications(
                "EXPLAIN /*Comment*/ MATCH ()-[a:NO_SUCH_THING]->() RETURN a",
                contains(unknownRelationshipNotification));
    }

    @Test
    void shouldWarnOnMissingProperty() {
        assertNotifications(
                "EXPLAIN MATCH (a {NO_SUCH_THING: 1337}) RETURN a", contains(unknownPropertyKeyNotification));
    }

    @Test
    void shouldWarnOnMisspelledProperty() {
        db.executeTransactionally("CREATE (n {prop : 42})");
        db.executeTransactionally("CREATE (n)-[r:R]->(m)");
        assertNotifications("EXPLAIN MATCH (n) WHERE n.propp = 43 RETURN n", contains(unknownPropertyKeyNotification));
        shouldNotNotifyInStream("EXPLAIN MATCH (n) WHERE n.prop = 43 RETURN n");
    }

    @Test
    void shouldWarnOnMissingPropertyWithComment() {
        assertNotifications(
                "EXPLAIN /*Comment*/ MATCH (a {NO_SUCH_THING: 1337}) RETURN a",
                contains(unknownPropertyKeyNotification));
    }

    @Test
    void shouldNotNotifyForMissingPropertiesOnUpdate() {
        shouldNotNotifyInStream("EXPLAIN CREATE (n {prop: 42})");
    }

    @Test
    void shouldGiveCorrectPositionWhetherFromCacheOrNot() {
        // Given
        String cachedQuery = "MATCH (a:L1) RETURN a";
        String nonCachedQuery = "MATCH (a:L2) RETURN a";
        // make sure we cache the query
        int limit = db.getDependencyResolver()
                .resolveDependency(Config.class)
                .get(GraphDatabaseInternalSettings.cypher_expression_recompilation_limit);
        try (Transaction transaction = db.beginTx()) {
            for (int i = 0; i < limit + 1; i++) {
                transaction.execute(cachedQuery).resultAsString();
            }
            transaction.commit();
        }

        // When
        try (Transaction transaction = db.beginTx()) {
            Notification cachedNotification = Iterables.asList(
                            transaction.execute("EXPLAIN " + cachedQuery).getNotifications())
                    .get(0);
            Notification nonCachedNotication = Iterables.asList(
                            transaction.execute("EXPLAIN " + nonCachedQuery).getNotifications())
                    .get(0);

            // Then
            assertThat(cachedNotification.getPosition()).isEqualTo(new InputPosition(17, 1, 18));
            assertThat(nonCachedNotication.getPosition()).isEqualTo(new InputPosition(17, 1, 18));
        }
    }

    @Test
    void shouldWarnOnExecute() {
        assertNotifications("MATCH (a {NO_SUCH_THING: 1337}) RETURN a", contains(unknownPropertyKeyNotification));
    }

    @Test
    void shouldWarnOnRuntimeInterpreted() {
        shouldNotifyInStream(
                "EXPLAIN CYPHER runtime=interpreted RETURN 1",
                NotificationCodeWithDescription.deprecatedRuntimeOption(
                        InputPosition.empty,
                        "'runtime=interpreted' is deprecated, please use 'runtime=slotted' instead",
                        "runtime=interpreted",
                        "runtime=slotted"));
    }

    @Test
    void shouldPreserveDeprecationNotificationsWhenHittingAstCache() {
        db.executeTransactionally("MATCH (a)-[:A|:B]-() RETURN a");
        assertNotifications(
                "CYPHER replan=force EXPLAIN MATCH (a)-[:A|:B]-() RETURN a",
                contains(deprecatedRelationshipTypeSeparator));
    }
}
