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
package org.neo4j.graphdb;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.StringSearchMode.CONTAINS;
import static org.neo4j.graphdb.StringSearchMode.PREFIX;
import static org.neo4j.graphdb.StringSearchMode.SUFFIX;
import static org.neo4j.graphdb.schema.IndexType.RANGE;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.condition;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.IndexMonitor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;

@Neo4jLayoutExtension
public class TextIndexIT {
    @Inject
    protected DatabaseLayout databaseLayout;

    @Test
    void shouldNotAllowTextIndexCreationForMultipleTokens() {
        // Given
        var dbms = new TestDatabaseManagementServiceBuilder(databaseLayout).build();
        var db = dbms.database(DEFAULT_DATABASE_NAME);
        var relations = new RelationshipType[] {RelationshipType.withName("FRIEND"), RelationshipType.withName("FROM")};
        var labels = new Label[] {label("PERSON"), label("EMPLOYEE")};

        // Then
        try (var tx = db.beginTx()) {
            assertThrows(IllegalArgumentException.class, () -> tx.schema()
                    .indexFor(labels)
                    .on("name")
                    .withIndexType(IndexType.TEXT)
                    .create());
            assertThrows(IllegalArgumentException.class, () -> tx.schema()
                    .indexFor(relations)
                    .on("name")
                    .withIndexType(IndexType.TEXT)
                    .create());
        }
        dbms.shutdown();
    }

    @Test
    void shouldRejectIndexCreationWithCompositeKeys() {
        var dbms = new TestDatabaseManagementServiceBuilder(databaseLayout).build();
        var db = dbms.database(DEFAULT_DATABASE_NAME);
        var rel = RelationshipType.withName("FRIEND");
        var label = label("PERSON");

        try (var tx = db.beginTx()) {
            assertUnsupported(() -> tx.schema()
                    .indexFor(label)
                    .on("key1")
                    .on("key2")
                    .withIndexType(IndexType.TEXT)
                    .create());
            assertUnsupported(() -> tx.schema()
                    .indexFor(rel)
                    .on("key1")
                    .on("key2")
                    .withIndexType(IndexType.TEXT)
                    .create());
        }
        dbms.shutdown();
    }

    private void assertUnsupported(Executable executable) {
        var message =
                assertThrows(UnsupportedOperationException.class, executable).getMessage();
        assertThat(message).isEqualTo("Composite indexes are not supported for TEXT index type.");
    }

    @Test
    void shouldCreateAndDropIndexWithCypher() {
        // Given
        var nodeIndex = "some_node_text_index";
        var relationshipIndex = "some_rel_text_index";
        var person = label("PERSON");
        var relation = RelationshipType.withName("FRIEND");
        var dbms = new TestDatabaseManagementServiceBuilder(databaseLayout).build();
        var db = dbms.database(DEFAULT_DATABASE_NAME);

        // When
        try (var tx = db.beginTx()) {
            tx.execute(format("CREATE TEXT INDEX %s FOR (p:PERSON) ON (p.name)", nodeIndex));
            tx.execute(format("CREATE TEXT INDEX %s FOR ()-[r:FRIEND]-() ON (r.since)", relationshipIndex));
            tx.commit();
        }

        // Then
        try (var tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(5, MINUTES);
            assertThat(tx.schema().getIndexByName(nodeIndex)).isNotNull();
            assertThat(tx.schema().getIndexByName(relationshipIndex)).isNotNull();
        }

        // When
        try (var tx = db.beginTx()) {
            tx.execute(format("DROP INDEX %s", nodeIndex));
            tx.execute(format("DROP INDEX %s", relationshipIndex));
            tx.commit();
        }

        // Then
        try (var tx = db.beginTx()) {
            assertThrows(IllegalArgumentException.class, () -> tx.schema().getIndexByName(nodeIndex));
            assertThrows(IllegalArgumentException.class, () -> tx.schema().getIndexByName(relationshipIndex));
        }

        dbms.shutdown();
    }

    @Test
    void shouldCreateAndDropTextIndexes() {
        // Given
        var nodeIndex = "some_node_text_index";
        var relationshipIndex = "some_rel_text_index";
        var person = label("PERSON");
        var relation = RelationshipType.withName("FRIEND");
        var dbms = new TestDatabaseManagementServiceBuilder(databaseLayout).build();
        var db = dbms.database(DEFAULT_DATABASE_NAME);

        // When
        try (var tx = db.beginTx()) {
            tx.schema()
                    .indexFor(person)
                    .on("name")
                    .withName(nodeIndex)
                    .withIndexType(IndexType.TEXT)
                    .create();
            tx.schema()
                    .indexFor(relation)
                    .on("name")
                    .withName(relationshipIndex)
                    .withIndexType(IndexType.TEXT)
                    .create();
            tx.commit();
        }

        // Then
        try (var tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(5, MINUTES);
            assertThat(tx.schema().getIndexByName(nodeIndex)).isNotNull();
            assertThat(tx.schema().getIndexByName(relationshipIndex)).isNotNull();
        }

        // When
        try (var tx = db.beginTx()) {
            tx.schema().getIndexByName(nodeIndex).drop();
            tx.schema().getIndexByName(relationshipIndex).drop();
            tx.commit();
        }

        // Then
        try (var tx = db.beginTx()) {
            assertThrows(IllegalArgumentException.class, () -> tx.schema().getIndexByName(nodeIndex));
            assertThrows(IllegalArgumentException.class, () -> tx.schema().getIndexByName(relationshipIndex));
        }

        dbms.shutdown();
    }

    @Test
    void shouldFindNodesUsingTextIndex() {
        // Given a database with different index types
        var person = label("PERSON");
        var monitor = new IndexAccessMonitor();
        var dbms = new TestDatabaseManagementServiceBuilder(databaseLayout)
                .setMonitors(monitor.monitors())
                .build();
        var db = dbms.database(DEFAULT_DATABASE_NAME);
        try (var tx = db.beginTx()) {
            tx.schema()
                    .indexFor(person)
                    .on("name")
                    .withIndexType(IndexType.TEXT)
                    .create();
            tx.schema().indexFor(person).on("name").withIndexType(RANGE).create();
            tx.commit();
        }
        try (var tx = db.beginTx()) {
            tx.createNode(person).setProperty("name", "David Smith Adams");
            tx.createNode(person).setProperty("name", "Smith Evans");
            tx.createNode(person).setProperty("name", "Smith James");
            tx.createNode(person).setProperty("name", "Luke Smith");
            tx.commit();
        }

        // And monitor watching index access
        monitor.reset();

        // When the nodes are queried
        try (var tx = db.beginTx()) {
            assertThat(Iterators.count(tx.findNodes(person, "name", "Smith", CONTAINS)))
                    .isEqualTo(4);
            assertThat(Iterators.count(tx.findNodes(person, "name", "Unknown", CONTAINS)))
                    .isEqualTo(0);
            assertThat(Iterators.count(tx.findNodes(person, "name", "Smith", PREFIX)))
                    .isEqualTo(2);
            assertThat(Iterators.count(tx.findNodes(person, "name", "Smith", SUFFIX)))
                    .isEqualTo(1);
        }

        // Then all queries touch only text index
        assertThat(monitor.accessed(org.neo4j.internal.schema.IndexType.TEXT)).isEqualTo(4);
        assertThat(monitor.accessed(org.neo4j.internal.schema.IndexType.RANGE)).isEqualTo(0);
        dbms.shutdown();
    }

    @Test
    void shouldFindRelationshipsUsingTextIndex() {
        // Given a database with different index types
        var person = label("PERSON");
        var relation = RelationshipType.withName("FRIEND");
        var monitor = new IndexAccessMonitor();
        var dbms = new TestDatabaseManagementServiceBuilder(databaseLayout)
                .setMonitors(monitor.monitors())
                .build();
        var db = dbms.database(DEFAULT_DATABASE_NAME);
        try (var tx = db.beginTx()) {
            tx.schema()
                    .indexFor(relation)
                    .on("since")
                    .withIndexType(IndexType.TEXT)
                    .create();
            tx.schema().indexFor(relation).on("since").withIndexType(RANGE).create();
            tx.commit();
        }
        try (var tx = db.beginTx()) {
            tx.createNode(person)
                    .createRelationshipTo(tx.createNode(person), relation)
                    .setProperty("since", "two years");
            tx.createNode(person)
                    .createRelationshipTo(tx.createNode(person), relation)
                    .setProperty("since", "five years, two months");
            tx.createNode(person)
                    .createRelationshipTo(tx.createNode(person), relation)
                    .setProperty("since", "three months");
            tx.commit();
        }

        // And an index monitor
        monitor.reset();

        // When the relationships are queried
        try (var tx = db.beginTx()) {
            assertThat(Iterators.count(tx.findRelationships(relation, "since", "years", CONTAINS)))
                    .isEqualTo(2);
            assertThat(Iterators.count(tx.findRelationships(relation, "since", "unknown", CONTAINS)))
                    .isEqualTo(0);
            assertThat(Iterators.count(tx.findRelationships(relation, "since", "five", PREFIX)))
                    .isEqualTo(1);
            assertThat(Iterators.count(tx.findRelationships(relation, "since", "months", SUFFIX)))
                    .isEqualTo(2);
        }

        // Then all queries touch only text index
        assertThat(monitor.accessed(org.neo4j.internal.schema.IndexType.TEXT)).isEqualTo(4);
        assertThat(monitor.accessed(org.neo4j.internal.schema.IndexType.RANGE)).isEqualTo(0);
        dbms.shutdown();
    }

    @Test
    void shouldRecoverIndexUpdatesAfterCrash() {
        // Given a database with some index updates
        var person = label("PERSON");
        var fs = new EphemeralFileSystemAbstraction();
        var dbms = startDbms(fs, new Monitors());
        var db = dbms.database(DEFAULT_DATABASE_NAME);
        try (var tx = db.beginTx()) {
            tx.schema()
                    .indexFor(person)
                    .on("name")
                    .withIndexType(IndexType.TEXT)
                    .create();
            tx.commit();
        }
        try (var tx = db.beginTx()) {
            tx.createNode(person).setProperty("name", "David Smith Adams");
            tx.commit();
        }

        // When the db crashes
        var crashedFs = fs.snapshot();
        dbms.shutdown();

        // And restarted with crashed file system
        var monitor = new IndexAccessMonitor();
        dbms = startDbms(crashedFs, monitor.monitors());
        db = dbms.database(DEFAULT_DATABASE_NAME);

        // Then the index updates are recovered
        try (var tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, MINUTES);
            assertThat(Iterators.count(tx.findNodes(person, "name", "Smith", CONTAINS)))
                    .isEqualTo(1);
            assertThat(monitor.accessed(org.neo4j.internal.schema.IndexType.TEXT))
                    .isEqualTo(1);
        }

        dbms.shutdown();
    }

    @Test
    void shouldSampleIndex() {
        // Given a database with different index types
        var person = label("PERSON");
        var monitor = new IndexAccessMonitor();
        var dbms = new TestDatabaseManagementServiceBuilder(databaseLayout)
                .setMonitors(monitor.monitors())
                .build();
        var db = (GraphDatabaseAPI) dbms.database(DEFAULT_DATABASE_NAME);

        // And few nodes
        try (var tx = db.beginTx()) {
            for (int i = 0; i < 5; i++) {
                tx.createNode(person).setProperty("name", "name" + i);
            }
            tx.commit();
        }

        // When an index is created
        createTextIndex(db, person, "idx_name");

        // And more nodes are updated
        try (var tx = db.beginTx()) {
            tx.createNode(person).setProperty("name", "new node");
            tx.findNode(person, "name", "name0").setProperty("name", "updated name");
            tx.findNode(person, "name", "name1").removeProperty("name");
            tx.commit();
        }

        // Then the index sample is updated
        assertEventually(
                () -> indexSample(db, "idx_name"),
                condition(sample -> sample.indexSize() == 5 && sample.sampleSize() >= 5 && sample.uniqueValues() >= 5),
                1,
                MINUTES);
        dbms.shutdown();
    }

    @Test
    void shouldNotIndexNonTextProperties() {
        // Given
        var person = label("PERSON");
        var monitor = new IndexAccessMonitor();
        var dbms = new TestDatabaseManagementServiceBuilder(databaseLayout)
                .setMonitors(monitor.monitors())
                .build();
        var db = (GraphDatabaseAPI) dbms.database(DEFAULT_DATABASE_NAME);
        createTextIndex(db, person, "idx_name");

        // When a non text property is indexed
        Node node;
        try (var tx = db.beginTx()) {
            node = tx.createNode(person);
            node.setProperty("name", 42);
            tx.commit();
        }

        // Then we should still be able to find it and not search the index for it
        try (var tx = db.beginTx()) {
            assertThat(tx.findNode(person, "name", 42)).isEqualTo(node);
            assertThat(monitor.accessed(org.neo4j.internal.schema.IndexType.TEXT))
                    .isEqualTo(0);
        }
        dbms.shutdown();
    }

    private void createTextIndex(GraphDatabaseAPI db, Label person, String indexName) {
        try (var tx = db.beginTx()) {
            tx.schema()
                    .indexFor(person)
                    .on("name")
                    .withIndexType(IndexType.TEXT)
                    .withName(indexName)
                    .create();
            tx.commit();
        }
    }

    private DatabaseManagementService startDbms(FileSystemAbstraction fs, Monitors monitors) {
        return new TestDatabaseManagementServiceBuilder(databaseLayout)
                .setFileSystem(fs)
                .setMonitors(monitors)
                .build();
    }

    static class IndexAccessMonitor extends IndexMonitor.MonitorAdapter {
        private final Map<org.neo4j.internal.schema.IndexType, Integer> counts = new HashMap<>();

        @Override
        public void queried(IndexDescriptor descriptor) {
            counts.putIfAbsent(descriptor.getIndexType(), 0);
            counts.computeIfPresent(descriptor.getIndexType(), (type, value) -> value + 1);
        }

        public int accessed(org.neo4j.internal.schema.IndexType type) {
            return counts.getOrDefault(type, 0);
        }

        Monitors monitors() {
            var monitors = new Monitors();
            monitors.addMonitorListener(this);
            return monitors;
        }

        void reset() {
            counts.clear();
        }
    }

    private IndexSample indexSample(GraphDatabaseAPI db, String indexName) {
        try (var tx = db.beginTx()) {
            var index = (IndexDefinitionImpl) tx.schema().getIndexByName(indexName);
            return db.getDependencyResolver()
                    .resolveDependency(IndexStatisticsStore.class)
                    .indexSample(index.getIndexReference().getId());
        }
    }
}
