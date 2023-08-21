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
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.kernel.api.KernelTransaction.Type.EXPLICIT;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.kernel.ZippedStoreCommunity;
import org.neo4j.kernel.api.index.IndexUsageStats;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
public class IndexUsageStatsBehindKernelVersionIT {

    @Inject
    private TestDirectory testDirectory;

    private DatabaseManagementService dbms;

    @AfterEach
    void tearDown() {
        if (dbms != null) {
            dbms.shutdown();
        }
    }

    @Test
    void shouldReportDefaultValuesWhenKernelVersionIsOld() throws KernelException, IOException {
        // Given
        var storeWithOldKernelVersion = ZippedStoreCommunity.REC_AF11_V50_EMPTY;
        storeWithOldKernelVersion.unzip(testDirectory.homePath());
        GraphDatabaseAPI db = database();
        var indexName = createIndex(db);

        // When
        singleIndexRead(db, indexName);
        triggerReportUsageStatistics(db);

        // Then
        try (var tx = db.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            var stats = getIndexUsageStats(tx, indexName);
            assertThat(stats.trackedSince()).isEqualTo(0);
            assertThat(stats.lastRead()).isEqualTo(0);
            assertThat(stats.readCount()).isEqualTo(0);
        }
    }

    @Test
    void assertDeliverCorrectValueAfterUpgradeToLatestKernelVersion() throws KernelException, IOException {
        // Given
        var storeWithOldKernelVersion = ZippedStoreCommunity.REC_AF11_V50_EMPTY;
        storeWithOldKernelVersion.unzip(testDirectory.homePath());
        GraphDatabaseAPI db = database();
        var indexName = createIndex(db);

        // When
        triggerUpgrade(db);
        singleIndexRead(db, indexName);
        triggerReportUsageStatistics(db);

        // Then
        try (var tx = db.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            var stats = getIndexUsageStats(tx, indexName);
            assertThat(stats.trackedSince()).isGreaterThan(0);
            assertThat(stats.lastRead()).isGreaterThan(0);
            assertThat(stats.readCount()).isEqualTo(1);
        }
    }

    private IndexUsageStats getIndexUsageStats(InternalTransaction tx, String indexName)
            throws IndexNotFoundKernelException {
        var ktx = tx.kernelTransaction();
        var index = ktx.schemaRead().indexGetForName(indexName);
        return ktx.schemaRead().indexUsageStats(index);
    }

    private void triggerUpgrade(GraphDatabaseAPI db) {
        var system = dbms.database(SYSTEM_DATABASE_NAME);
        system.executeTransactionally("CALL dbms.upgrade()");
        createWriteTransaction(db);
    }

    private void createWriteTransaction(GraphDatabaseAPI db) {
        try (Transaction tx = db.beginTx()) {
            tx.createNode().delete();
            tx.commit();
        }
    }

    private String createIndex(GraphDatabaseAPI db) {
        var indexName = "index";
        try (Transaction tx = db.beginTx()) {
            tx.schema()
                    .indexFor(Label.label("Label"))
                    .on("prop")
                    .withName(indexName)
                    .create();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(1, TimeUnit.MINUTES);
        }
        return indexName;
    }

    private void singleIndexRead(GraphDatabaseAPI db, String indexName) throws KernelException {
        try (Transaction tx = db.beginTx()) {
            var ktx = ((TransactionImpl) tx).kernelTransaction();
            var schemaRead = ktx.schemaRead();
            var dataRead = ktx.dataRead();

            var descriptor = schemaRead.indexGetForName(indexName);
            var indexReadSession = dataRead.indexReadSession(descriptor);
            try (var cursor = ktx.cursors().allocateNodeValueIndexCursor(ktx.cursorContext(), ktx.memoryTracker())) {
                dataRead.nodeIndexSeek(
                        QueryContext.NULL_CONTEXT,
                        indexReadSession,
                        cursor,
                        unconstrained(),
                        PropertyIndexQuery.allEntries());
                while (cursor.next()) {
                    // Ignore
                }
            }
            tx.commit();
        }
    }

    private GraphDatabaseAPI database() {
        dbms = new TestDatabaseManagementServiceBuilder(testDirectory.homePath())
                .setConfig(GraphDatabaseInternalSettings.automatic_upgrade_enabled, false)
                .build();
        return (GraphDatabaseAPI) dbms.database(DEFAULT_DATABASE_NAME);
    }

    private void triggerReportUsageStatistics(GraphDatabaseAPI db) {
        db.getDependencyResolver().resolveDependency(IndexingService.class).reportUsageStatistics();
    }
}
