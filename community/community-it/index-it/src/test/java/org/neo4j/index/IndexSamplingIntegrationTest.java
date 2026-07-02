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
package org.neo4j.index;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.kernel.api.KernelTransaction.Type.EXPLICIT;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.CommonDatabaseStores;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.SkipOnSpd;

@Neo4jLayoutExtension
class IndexSamplingIntegrationTest {
    @Inject
    private Neo4jLayout layout;

    @Inject
    private FileSystemAbstraction fs;

    private static final String TOKEN = "Person";
    private static final String PROPERTY = "name";
    private static final String SCHEMA_NAME = "schema_name";
    private static final long ENTITIES = 1000;
    private static final String[] NAMES = {"Neo4j", "Neo", "Graph", "Apa"};

    @ParameterizedTest
    @EnumSource(Entity.class)
    @SkipOnSpd(reason = "We fetch index data only from the graph shard")
    void shouldSampleNotUniqueIndex(Entity entity) throws Throwable {
        // Given / When
        MutableInt deletions = new MutableInt();
        populateDatabaseThenTriggerIndexResamplingOnNextStartup(db -> {
            try (Transaction tx = db.beginTx()) {
                entity.createIndex(tx, SCHEMA_NAME, TOKEN, PROPERTY);
                tx.commit();
            }

            try (Transaction tx = db.beginTx()) {
                tx.schema().awaitIndexOnline(SCHEMA_NAME, 1, TimeUnit.MINUTES);
                tx.commit();
            }

            try (Transaction tx = db.beginTx()) {
                for (int i = 0; i < ENTITIES; i++) {
                    entity.createEntity(tx, TOKEN, PROPERTY, NAMES[i % NAMES.length]);
                }
                tx.commit();
            }

            try (Transaction tx = db.beginTx()) {
                for (int i = 0; i < (ENTITIES / 10); i++) {
                    entity.deleteFirstFound(tx, TOKEN, PROPERTY, NAMES[i % NAMES.length]);
                    deletions.increment();
                }
                tx.commit();
            }
        });

        // Then

        // lucene will consider also the delete nodes, native won't
        IndexSample indexSample = fetchIndexSamplingValues();
        int deletedNodes = deletions.intValue();
        assertThat(indexSample.uniqueValues()).as("Unique values").isEqualTo(NAMES.length);
        assertThat(indexSample.sampleSize())
                .as("Sample size")
                .isGreaterThanOrEqualTo(ENTITIES - deletedNodes)
                .isLessThanOrEqualTo(ENTITIES);
        // but regardless, the deleted nodes should not be considered in the index size value
        assertThat(indexSample.updates()).as("Updates").isEqualTo(0);
        assertThat(indexSample.indexSize()).as("Index size").isEqualTo(ENTITIES - deletedNodes);
    }

    @ParameterizedTest
    @EnumSource(value = Entity.class)
    @SkipOnSpd(reason = "We fetch index data only from the graph shard")
    void shouldSampleUniqueIndex(Entity entity) throws Throwable {
        // Given / When
        MutableInt deletions = new MutableInt();
        populateDatabaseThenTriggerIndexResamplingOnNextStartup(db -> {
            try (Transaction tx = db.beginTx()) {
                entity.createConstraint(tx, SCHEMA_NAME, TOKEN, PROPERTY);
                tx.commit();
            }

            try (Transaction tx = db.beginTx()) {
                for (int i = 0; i < ENTITIES; i++) {
                    entity.createEntity(tx, TOKEN, PROPERTY, "" + i);
                }
                tx.commit();
            }

            try (Transaction tx = db.beginTx()) {
                for (int i = 0; i < ENTITIES; i++) {
                    if (i % 10 == 0) {
                        entity.deleteFirstFound(tx, TOKEN, PROPERTY, "" + i);
                        deletions.increment();
                    }
                }
                tx.commit();
            }
        });

        // Then
        IndexSample indexSample = fetchIndexSamplingValues();
        int deletedNodes = deletions.intValue();
        assertThat(indexSample.uniqueValues()).as("Unique values").isEqualTo(ENTITIES - deletedNodes);
        assertThat(indexSample.sampleSize()).as("Sample size").isEqualTo(ENTITIES - deletedNodes);
        assertThat(indexSample.updates()).as("Updates").isEqualTo(0);
        assertThat(indexSample.indexSize()).as("Index size").isEqualTo(ENTITIES - deletedNodes);
    }

    private void populateDatabaseThenTriggerIndexResamplingOnNextStartup(Consumer<GraphDatabaseService> consumer)
            throws IOException {
        DatabaseLayout databaseLayout;
        try (DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder(layout).build()) {
            GraphDatabaseService db = managementService.database(DEFAULT_DATABASE_NAME);
            consumer.accept(db);
            databaseLayout = ((GraphDatabaseAPI) db).databaseLayout();
        }

        triggerIndexResamplingOnNextStartup(databaseLayout, fs);
    }

    private static IndexDescriptor indexId(KernelTransaction tx) {
        return tx.schemaRead().indexGetForName(SCHEMA_NAME);
    }

    private IndexSample fetchIndexSamplingValues() throws IndexNotFoundKernelException, TransactionFailureException {
        try (DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder(layout).build()) {
            // Then
            GraphDatabaseService db = managementService.database(DEFAULT_DATABASE_NAME);
            GraphDatabaseAPI api = (GraphDatabaseAPI) db;
            Kernel kernel = api.getDependencyResolver().resolveDependency(Kernel.class);
            try (KernelTransaction tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
                return tx.schemaRead().indexSample(indexId(tx));
            }
        }
    }

    private static void triggerIndexResamplingOnNextStartup(DatabaseLayout layout, FileSystemAbstraction fs)
            throws IOException {
        // Trigger index resampling on next at startup
        layout.pathForStore(CommonDatabaseStores.INDEX_STATISTICS).delete(fs);
    }

    private enum Entity {
        NODE {
            @Override
            void createIndex(Transaction tx, String schemaName, String token, String property) {
                tx.schema()
                        .indexFor(Label.label(token))
                        .on(property)
                        .withName(schemaName)
                        .create();
            }

            @Override
            void createConstraint(Transaction tx, String schemaName, String token, String property) {
                tx.schema()
                        .constraintFor(Label.label(token))
                        .assertPropertyIsUnique(property)
                        .withName(schemaName)
                        .create();
            }

            @Override
            void createEntity(Transaction tx, String token, String property, String value) {
                tx.createNode(Label.label(token)).setProperty(property, value);
            }

            @Override
            void deleteFirstFound(Transaction tx, String token, String property, String value) {
                try (ResourceIterator<Node> nodes = tx.findNodes(Label.label(token), property, value)) {
                    nodes.next().delete();
                }
            }
        },
        RELATIONSHIP {
            @Override
            void createIndex(Transaction tx, String schemaName, String token, String property) {
                tx.schema()
                        .indexFor(RelationshipType.withName(token))
                        .on(property)
                        .withName(schemaName)
                        .create();
            }

            @Override
            void createConstraint(Transaction tx, String schemaName, String token, String property) {
                tx.schema()
                        .constraintFor(RelationshipType.withName(token))
                        .assertPropertyIsUnique(property)
                        .withName(schemaName)
                        .create();
            }

            @Override
            void createEntity(Transaction tx, String token, String property, String value) {
                Node from = tx.createNode();
                Node to = tx.createNode();
                from.createRelationshipTo(to, RelationshipType.withName(token)).setProperty(property, value);
            }

            @Override
            void deleteFirstFound(Transaction tx, String token, String property, String value) {
                try (ResourceIterator<Relationship> rels =
                        tx.findRelationships(RelationshipType.withName(token), property, value)) {
                    rels.next().delete();
                }
            }
        };

        abstract void createIndex(Transaction tx, String schemaName, String token, String property);

        abstract void createConstraint(Transaction tx, String schemaName, String token, String property);

        abstract void createEntity(Transaction tx, String token, String property, String value);

        abstract void deleteFirstFound(Transaction tx, String token, String property, String value);
    }
}
