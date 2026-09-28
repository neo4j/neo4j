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
package org.neo4j.community.edition;

import static java.lang.String.valueOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.helpers.collection.Iterables.count;
import static org.neo4j.internal.helpers.collection.Iterators.count;
import static org.neo4j.kernel.database.NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.ConsistencyCheckIncompleteException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SkipOnSpd;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.wal.CommandBatchCursor;
import org.neo4j.wal.LogicalTransactionStore;
import org.neo4j.wal.WALService;

@TestDirectoryExtension
class CommunitySystemDatabaseIT {
    @Inject
    private TestDirectory testDirectory;

    private GraphDatabaseService database;
    private DatabaseContextProvider<?> databaseContextProvider;
    private GraphDatabaseAPI defaultDb;
    private GraphDatabaseAPI systemDb;
    private DatabaseManagementService managementService;

    @BeforeEach
    void setUp() {
        managementService = new TestDatabaseManagementServiceBuilder(testDirectory.homePath())
                .noOpSystemGraphInitializer()
                .build();
        database = managementService.database(DEFAULT_DATABASE_NAME);
        databaseContextProvider = getDatabaseManager(database);
        defaultDb = getDatabaseByName(
                databaseContextProvider,
                databaseContextProvider
                        .databaseIdRepository()
                        .getByName(DEFAULT_DATABASE_NAME)
                        .get());
        systemDb = getDatabaseByName(databaseContextProvider, NAMED_SYSTEM_DATABASE_ID);
    }

    @AfterEach
    void tearDown() {
        managementService.shutdown();
    }

    @Test
    void systemAndDefaultDatabasesAvailable() {
        assertNotNull(defaultDb);
        assertNotNull(systemDb);
        assertNotSame(defaultDb, systemDb);
    }

    @Test
    @SkipOnSpd(reason = "We have a very different system graph in enterprise (getAllLabels mismatch)")
    void systemDatabaseDataNotAvailableInDefaultDatabase() {
        Label systemLabel = label("systemLabel");
        try (Transaction transaction = systemDb.beginTx()) {
            Node node = transaction.createNode(systemLabel);
            node.setProperty("a", "b");
            transaction.commit();
        }
        try (Transaction transaction = defaultDb.beginTx()) {
            assertEquals(0, count(transaction.findNodes(systemLabel)));
            assertEquals(0, count(transaction.getAllLabels()));
        }
        try (Transaction transaction = systemDb.beginTx()) {
            assertEquals(1, count(transaction.findNodes(systemLabel)));
            assertEquals(1, count(transaction.getAllLabels()));
        }
    }

    @SkipOnSpd(
            reason =
                    "For cluster/SPD there are topology maintenance jobs that runs in the background "
                            + "and potentially creates transactions in the system db. Specifically for this test there's a race where "
                            + "TopologyGraphInstanceVersionUpdater can be run after getting the systemTxCountBefore and therefor "
                            + "show up in the systemTxCountAfter, causing an unexpected tx count. We basically don't need to run "
                            + "this test for SPD specifically to show that the dbms distinguishes between system db and other db tx logs",
            notes = SkipOnSpd.Note.irrelevant)
    @Test
    void separateTransactionLogsForSystemDatabase() throws IOException {
        int systemDatabaseTransactions = 100;
        int defaultDatabaseTransactions = 15;

        var systemTxCountBefore = countTransactionInLogicalStore(systemDb);
        var defaultTxCountBefore = countTransactionInLogicalStore(defaultDb);

        for (int i = 0; i < systemDatabaseTransactions; i++) {
            try (Transaction transaction = systemDb.beginTx()) {
                Node nodeA = transaction.createNode();
                Node nodeB = transaction.createNode();
                nodeA.createRelationshipTo(nodeB, RelationshipType.withName(valueOf(i)));
                transaction.commit();
            }
        }

        for (int i = 0; i < defaultDatabaseTransactions; i++) {
            try (Transaction transaction = defaultDb.beginTx()) {
                transaction.createNode(label(valueOf(i)));
                transaction.commit();
            }
        }

        var systemTxCountAfter = countTransactionInLogicalStore(systemDb);
        var defaultTxCountAfter = countTransactionInLogicalStore(defaultDb);

        assertThat(systemTxCountAfter - systemTxCountBefore).isEqualTo(systemDatabaseTransactions * 2);
        assertThat(defaultTxCountAfter - defaultTxCountBefore).isEqualTo(defaultDatabaseTransactions * 2);
    }

    @Test
    void differentDatabaseHaveDifferentTxLogsDirectories() {
        WALService systemWALService = systemDb.getDependencyResolver().resolveDependency(WALService.class);
        WALService defaultWALService = defaultDb.getDependencyResolver().resolveDependency(WALService.class);
        assertNotEquals(defaultWALService.logFilesDirectory(), systemWALService.logFilesDirectory());
    }

    @Test
    void systemAndDefaultDatabasesAreConsistentAfterShutdown() throws ConsistencyCheckIncompleteException {
        int systemDatabaseTransactions = 100;
        int defaultDatabaseTransactions = 15;
        DatabaseLayout systemDatabaseLayout = systemDb.databaseLayout();
        DatabaseLayout defaultDbLayout = defaultDb.databaseLayout();

        for (int i = 0; i < systemDatabaseTransactions; i++) {
            try (Transaction transaction = systemDb.beginTx()) {
                Node nodeA = transaction.createNode();
                Node nodeB = transaction.createNode();
                nodeA.createRelationshipTo(nodeB, RelationshipType.withName(valueOf(i)));
                transaction.commit();
            }
        }

        for (int i = 0; i < defaultDatabaseTransactions; i++) {
            try (Transaction transaction = defaultDb.beginTx()) {
                transaction.createNode(label(valueOf(i)));
                transaction.commit();
            }
        }

        managementService.shutdown();

        assertTrue(runConsistencyCheck(systemDatabaseLayout).isSuccessful());
        assertTrue(runConsistencyCheck(defaultDbLayout).isSuccessful());
    }

    @Test
    void systemDatabaseEnabledByDefault() {
        GraphDatabaseService databaseWithSystemDb = null;
        DatabaseManagementService managementService = null;
        try {
            Path disabledSystemDbDirectory = testDirectory.homePath("withSystemDd");
            managementService = new TestDatabaseManagementServiceBuilder(disabledSystemDbDirectory).build();
            databaseWithSystemDb = managementService.database(DEFAULT_DATABASE_NAME);
            DatabaseContextProvider<?> databaseContextProvider = getDatabaseManager(databaseWithSystemDb);
            assertTrue(databaseContextProvider
                    .getDatabaseContext(NAMED_SYSTEM_DATABASE_ID)
                    .isPresent());
        } finally {
            if (databaseWithSystemDb != null) {
                managementService.shutdown();
            }
        }
    }

    private static ConsistencyCheckService.Result runConsistencyCheck(DatabaseLayout databaseLayout)
            throws ConsistencyCheckIncompleteException {
        return new ConsistencyCheckService(databaseLayout).runFullConsistencyCheck();
    }

    private static int countTransactionInLogicalStore(GraphDatabaseAPI facade) throws IOException {
        LogicalTransactionStore transactionStore =
                facade.getDependencyResolver().resolveDependency(LogicalTransactionStore.class);
        var logFile = facade.getDependencyResolver()
                .resolveDependency(WALService.class)
                .getLogFile();
        var lowestVersion = logFile.getLogRangeInfo().lowestVersion();
        var baseTxId = logFile.extractHeader(lowestVersion).getLastAppendIndex(); // First one in file is this +1
        try (CommandBatchCursor cursor = transactionStore.getCommandBatches(baseTxId + 1)) {
            var count = 0;
            while (cursor.next()) {
                count++;
            }
            return count;
        }
    }

    private static GraphDatabaseAPI getDatabaseByName(
            DatabaseContextProvider<?> databaseContextProvider, NamedDatabaseId namedDatabaseId) {
        return databaseContextProvider
                .getDatabaseContext(namedDatabaseId)
                .orElseThrow(IllegalStateException::new)
                .databaseFacade();
    }

    private static DatabaseContextProvider<?> getDatabaseManager(GraphDatabaseService database) {
        return ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency(DatabaseContextProvider.class);
    }
}
