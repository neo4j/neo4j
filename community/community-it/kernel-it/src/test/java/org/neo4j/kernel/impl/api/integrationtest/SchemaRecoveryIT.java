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
package org.neo4j.kernel.impl.api.integrationtest;

import static org.assertj.core.api.Assumptions.assumeThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.assertj.core.util.Streams;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.internal.recordstorage.RecordStorageEngineFactory;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@EphemeralTestDirectoryExtension
class SchemaRecoveryIT {
    @Inject
    private volatile EphemeralFileSystemAbstraction fs;

    @Inject
    private TestDirectory testDirectory;

    private GraphDatabaseAPI db;
    private DatabaseManagementService managementService;

    @AfterEach
    void shutdownDatabase() {
        if (db != null) {
            managementService.shutdown();
            db = null;
        }
    }

    @Test
    void schemaTransactionsShouldSurviveRecovery() {
        // given
        Label label = label("User");
        String property = "email";
        startDb();

        long initialConstraintCount;
        long initialIndexCount;
        try (Transaction tx = db.beginTx()) {
            initialConstraintCount =
                    Streams.stream(tx.schema().getConstraints()).count();
            initialIndexCount = Streams.stream(tx.schema().getIndexes()).count();
        }

        try (Transaction tx = db.beginTx()) {
            tx.schema().constraintFor(label).assertPropertyIsUnique(property).create();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.createNode(label).setProperty(property, "neo4j@neo4j.com");
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(1, TimeUnit.HOURS);
            tx.commit();
        }
        killDb();

        // when
        startDb();

        // then
        assertEquals(initialConstraintCount + 1, constraints(db).size());
        assertEquals(initialIndexCount + 1, indexes(db).size());
    }

    @Test
    void inconsistentlyFlushedTokensShouldBeRecovered() {
        // given
        Label label = label("User");
        String property = "email";
        startDb();
        assumeThat(db.getDependencyResolver().resolveDependency(StorageEngineFactory.class))
                .as("Separate tokens & token names is a record storage thing")
                .isInstanceOf(RecordStorageEngineFactory.class);

        long initialConstraintCount;
        long initialIndexCount;
        try (Transaction tx = db.beginTx()) {
            initialConstraintCount =
                    Streams.stream(tx.schema().getConstraints()).count();
            initialIndexCount = Streams.stream(tx.schema().getIndexes()).count();
        }

        try (Transaction tx = db.beginTx()) {
            tx.schema().constraintFor(label).assertPropertyIsUnique(property).create();
            tx.commit();
        }

        // Flush the property token store, but NOT the property token ~name~ store. This means tokens will refer to
        // unused dynamic records for their names.
        RecordStorageEngine storageEngine = db.getDependencyResolver().resolveDependency(RecordStorageEngine.class);
        storageEngine.testAccessNeoStores().getPropertyKeyTokenStore().flush(FileFlushEvent.NULL, NULL_CONTEXT);

        killDb();

        // when
        startDb();

        // then assert that we can still read the schema correctly.
        assertEquals(initialConstraintCount + 1, constraints(db).size());
        assertEquals(initialIndexCount + 1, indexes(db).size());
    }

    private void startDb() {
        if (db != null) {
            managementService.shutdown();
        }

        managementService = configure(
                        new TestDatabaseManagementServiceBuilder(testDirectory.homePath()).setFileSystem(fs))
                .build();
        db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
    }

    protected TestDatabaseManagementServiceBuilder configure(TestDatabaseManagementServiceBuilder builder) {
        return builder;
    }

    private void killDb() {
        if (db != null) {
            fs = fs.snapshot();
            managementService.shutdown();
        }
    }

    private static List<ConstraintDefinition> constraints(GraphDatabaseService database) {
        try (Transaction tx = database.beginTx()) {
            return Iterables.asList(tx.schema().getConstraints());
        }
    }

    private static List<IndexDefinition> indexes(GraphDatabaseService database) {
        try (Transaction tx = database.beginTx()) {
            return Iterables.asList(tx.schema().getIndexes());
        }
    }
}
