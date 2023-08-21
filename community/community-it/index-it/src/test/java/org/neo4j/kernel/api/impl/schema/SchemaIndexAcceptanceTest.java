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
package org.neo4j.kernel.api.impl.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.helpers.collection.Iterables.count;
import static org.neo4j.internal.helpers.collection.Iterators.loop;
import static org.neo4j.internal.helpers.collection.MapUtil.map;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema.IndexState;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@EphemeralTestDirectoryExtension
public class SchemaIndexAcceptanceTest {
    @Inject
    private EphemeralFileSystemAbstraction fs;

    @Inject
    private TestDirectory testDirectory;

    private GraphDatabaseService db;
    private final Label label = label("PERSON");
    private final String propertyKey = "key";
    private DatabaseManagementService managementService;

    @BeforeEach
    void before() {
        db = newDb(fs);
    }

    @AfterEach
    void after() {
        managementService.shutdown();
    }

    @Test
    void creatingIndexOnExistingDataBuildsIndexWhichWillBeOnlineNextStartup() {
        Node node1;
        Node node2;
        Node node3;
        try (Transaction tx = db.beginTx()) {
            node1 = createNode(tx, label, "name", "One");
            node2 = createNode(tx, label, "name", "Two");
            node3 = createNode(tx, label, "name", "Three");
            tx.commit();
        }

        createIndex(db, label, propertyKey);

        restart();

        try (Transaction transaction = db.beginTx()) {
            assertThat(findNodesByLabelAndProperty(label, "name", "One", transaction))
                    .containsOnly(node1);
            assertThat(findNodesByLabelAndProperty(label, "name", "Two", transaction))
                    .containsOnly(node2);
            assertThat(findNodesByLabelAndProperty(label, "name", "Three", transaction))
                    .containsOnly(node3);
        }
    }

    @Test
    void shouldIndexArrays() {
        long[] arrayPropertyValue = {42, 23, 87};
        createIndex(db, label, propertyKey);
        Node node1;
        try (Transaction tx = db.beginTx()) {
            node1 = createNode(tx, label, propertyKey, arrayPropertyValue);
            tx.commit();
        }

        restart();

        try (Transaction tx = db.beginTx()) {
            assertThat(getIndexes(tx, label))
                    .extracting(i -> tx.schema().getIndexState(i))
                    .containsOnly(IndexState.ONLINE);
        }
        try (Transaction transaction = db.beginTx()) {
            assertThat(findNodesByLabelAndProperty(label, propertyKey, arrayPropertyValue, transaction))
                    .containsOnly(node1);
            assertThat(findNodesByLabelAndProperty(label, propertyKey, new long[] {42, 23}, transaction))
                    .isEmpty();
            assertThat(findNodesByLabelAndProperty(
                            label, propertyKey, Arrays.toString(arrayPropertyValue), transaction))
                    .isEmpty();
            transaction.commit();
        }
    }

    @Test
    void shouldIndexStringArrays() {
        String[] arrayPropertyValue = {"A, B", "C"};
        createIndex(db, label, propertyKey);
        Node node1;
        try (Transaction tx = db.beginTx()) {
            node1 = createNode(tx, label, propertyKey, arrayPropertyValue);
            tx.commit();
        }

        restart();

        try (Transaction tx = db.beginTx()) {
            assertThat(getIndexes(tx, label))
                    .extracting(i -> tx.schema().getIndexState(i))
                    .containsOnly(IndexState.ONLINE);
        }
        try (Transaction transaction = db.beginTx()) {
            assertThat(findNodesByLabelAndProperty(label, propertyKey, arrayPropertyValue, transaction))
                    .containsOnly(node1);
            assertThat(findNodesByLabelAndProperty(label, propertyKey, new String[] {"A", "B, C"}, transaction))
                    .isEmpty();
            assertThat(findNodesByLabelAndProperty(
                            label, propertyKey, Arrays.toString(arrayPropertyValue), transaction))
                    .isEmpty();
        }
    }

    @Test
    void shouldIndexArraysPostPopulation() {
        long[] arrayPropertyValue = {42, 23, 87};
        Node node1;
        try (Transaction tx = db.beginTx()) {
            node1 = createNode(tx, label, propertyKey, arrayPropertyValue);
            tx.commit();
        }

        createIndex(db, label, propertyKey);

        restart();

        try (Transaction tx = db.beginTx()) {
            assertThat(getIndexes(tx, label))
                    .extracting(i -> tx.schema().getIndexState(i))
                    .containsOnly(IndexState.ONLINE);
        }
        try (Transaction transaction = db.beginTx()) {
            assertThat(findNodesByLabelAndProperty(label, propertyKey, arrayPropertyValue, transaction))
                    .containsOnly(node1);
            assertThat(findNodesByLabelAndProperty(label, propertyKey, new long[] {42, 23}, transaction))
                    .isEmpty();
            assertThat(findNodesByLabelAndProperty(
                            label, propertyKey, Arrays.toString(arrayPropertyValue), transaction))
                    .isEmpty();
        }
    }

    @Test
    void recoveryAfterCreateAndDropIndex() {
        // GIVEN
        IndexDefinition indexDefinition = createIndex(db, label, propertyKey);
        createSomeData(label, propertyKey);
        doStuff(db, label, propertyKey);
        dropIndex(indexDefinition);
        doStuff(db, label, propertyKey);

        // WHEN
        crashAndRestart();

        // THEN
        try (Transaction transaction = db.beginTx()) {
            assertThat(getIndexes(transaction, label)).isEmpty();
        }
    }

    private GraphDatabaseService newDb(EphemeralFileSystemAbstraction fileSystemAbstraction) {
        managementService = new TestDatabaseManagementServiceBuilder(testDirectory.homePath())
                .setFileSystem(new UncloseableDelegatingFileSystemAbstraction(fileSystemAbstraction))
                .build();
        return managementService.database(DEFAULT_DATABASE_NAME);
    }

    private void crashAndRestart() {
        EphemeralFileSystemAbstraction crashSnapshot = fs.snapshot();
        managementService.shutdown();
        db = newDb(crashSnapshot);
    }

    private void restart() {
        managementService.shutdown();
        db = newDb(fs);
    }

    private static Node createNode(Transaction tx, Label label, Object... properties) {
        Node node = tx.createNode(label);
        for (Map.Entry<String, Object> property : map(properties).entrySet()) {
            node.setProperty(property.getKey(), property.getValue());
        }
        return node;
    }

    private void dropIndex(IndexDefinition indexDefinition) {
        try (Transaction tx = db.beginTx()) {
            tx.schema().getIndexByName(indexDefinition.getName()).drop();
            tx.commit();
        }
    }

    private static void doStuff(GraphDatabaseService db, Label label, String propertyKey) {
        try (Transaction tx = db.beginTx();
                ResourceIterator<Node> nodes = tx.findNodes(label, propertyKey, 3323)) {
            for (Node node : loop(nodes)) {
                count(node.getLabels());
            }
        }
    }

    private void createSomeData(Label label, String propertyKey) {
        try (Transaction tx = db.beginTx()) {
            Node node = tx.createNode(label);
            node.setProperty(propertyKey, "yeah");
            tx.commit();
        }
    }

    private static List<Node> findNodesByLabelAndProperty(
            Label label, String propertyName, Object value, Transaction transaction) {
        return Iterators.asList(transaction.findNodes(label, propertyName, value));
    }

    private static Iterable<IndexDefinition> getIndexes(Transaction transaction, Label label) {
        return transaction.schema().getIndexes(label);
    }

    private static IndexDefinition createIndex(GraphDatabaseService db, Label label, String property) {
        IndexDefinition indexDefinition;
        try (Transaction tx = db.beginTx()) {
            indexDefinition = tx.schema().indexFor(label).on(property).create();
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
        }
        return indexDefinition;
    }
}
