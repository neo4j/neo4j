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
package org.neo4j.kernel.recovery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.kernel.impl.api.CompleteTransaction;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.txid.TransactionIdGenerator;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatch;
import org.neo4j.kernel.impl.transaction.log.CommandBatchCursor;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionCommitmentFactory;
import org.neo4j.kernel.impl.transaction.tracing.TransactionWriteEvent;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@EphemeralTestDirectoryExtension
class KernelRecoveryTest {
    @Inject
    private EphemeralFileSystemAbstraction fileSystem;

    @Inject
    private TestDirectory testDirectory;

    private DatabaseManagementService managementService;

    @AfterEach
    void cleanUp() {
        if (managementService != null) {
            managementService.shutdown();
        }
    }

    @Test
    void shouldHandleWritesProperlyAfterRecovery() throws Exception {
        // Given
        GraphDatabaseService db = newDB(fileSystem, "main");
        // We don't want to include any transactions that may have run on start-up since they
        // will have run on start-up of rebuilt db already.
        long txIdToExtractFrom = getLastClosedTransactionId((GraphDatabaseAPI) db) + 1;
        long node1 = createNode(db, "k", "v1");

        // And given the power goes out
        List<CommittedCommandBatch> transactions = new ArrayList<>();
        long node2;
        try (EphemeralFileSystemAbstraction crashedFs = fileSystem.snapshot()) {
            managementService.shutdown();
            db = newDB(crashedFs, "main");
            node2 = createNode(db, "k", "v2");
            extractTransactions((GraphDatabaseAPI) db, transactions, txIdToExtractFrom);
            managementService.shutdown();
        }

        // Then both those nodes should be there, i.e. they are properly there in the log
        GraphDatabaseService rebuilt = newDB(fileSystem, "rebuilt");
        applyTransactions(transactions, (GraphDatabaseAPI) rebuilt);
        try (Transaction tx = rebuilt.beginTx()) {
            assertEquals("v1", tx.getNodeById(node1).getProperty("k"));
            assertEquals("v2", tx.getNodeById(node2).getProperty("k"));
            tx.commit();
        }
    }

    private static void applyTransactions(List<CommittedCommandBatch> transactions, GraphDatabaseAPI rebuilt)
            throws TransactionFailureException {
        DependencyResolver dependencyResolver = rebuilt.getDependencyResolver();
        StorageEngine storageEngine = dependencyResolver.resolveDependency(StorageEngine.class);
        TransactionCommitProcess commitProcess = dependencyResolver.resolveDependency(TransactionCommitProcess.class);
        var commitmentFactory = dependencyResolver.resolveDependency(TransactionCommitmentFactory.class);
        var transactionIdGenerator = dependencyResolver.resolveDependency(TransactionIdGenerator.class);
        try (var storeCursors = storageEngine.createStorageCursors(NULL_CONTEXT)) {
            for (CommittedCommandBatch transaction : transactions) {
                commitProcess.commit(
                        new CompleteTransaction(
                                transaction,
                                NULL_CONTEXT,
                                storeCursors,
                                commitmentFactory.newCommitment(),
                                transactionIdGenerator),
                        TransactionWriteEvent.NULL,
                        TransactionApplicationMode.EXTERNAL);
            }
        }
    }

    private static void extractTransactions(
            GraphDatabaseAPI db, List<CommittedCommandBatch> transactions, long txIdToStartFrom)
            throws java.io.IOException {
        LogicalTransactionStore txStore = db.getDependencyResolver().resolveDependency(LogicalTransactionStore.class);
        try (CommandBatchCursor cursor = txStore.getCommandBatches(txIdToStartFrom)) {
            cursor.forAll(transactions::add);
        }
    }

    private static long getLastClosedTransactionId(GraphDatabaseAPI database) {
        MetadataProvider metaDataStore = database.getDependencyResolver().resolveDependency(MetadataProvider.class);
        return metaDataStore.getLastClosedTransaction().transactionId().id();
    }

    private GraphDatabaseService newDB(FileSystemAbstraction fs, String name) {
        managementService = new TestDatabaseManagementServiceBuilder(testDirectory.directory(name))
                .setFileSystem(new UncloseableDelegatingFileSystemAbstraction(fs))
                .build();
        return managementService.database(DEFAULT_DATABASE_NAME);
    }

    private static long createNode(GraphDatabaseService db, String key, Object value) {
        long nodeId;
        try (Transaction tx = db.beginTx()) {
            Node node = tx.createNode();
            node.setProperty(key, value);
            nodeId = node.getId();
            tx.commit();
        }
        return nodeId;
    }
}
