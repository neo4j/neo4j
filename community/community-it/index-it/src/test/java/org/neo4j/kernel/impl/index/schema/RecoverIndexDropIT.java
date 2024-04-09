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
package org.neo4j.kernel.impl.index.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.Iterables.count;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.test.TestLabels.LABEL_ONE;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.PhysicalFlushableLogChannel;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.api.index.IndexMap;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatch;
import org.neo4j.kernel.impl.transaction.log.CommandBatchCursor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.recovery.RecoveryMonitor;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.utils.TestDirectory;

/**
 * Issue came up when observing that recovering an INDEX DROP command didn't actually call {@link IndexProxy#drop()},
 * and actually did nothing to that {@link IndexProxy} except removing it from its {@link IndexMap}.
 * This would have {@link IndexingService} forget about that index and at shutdown not call {@link IndexProxy#close(CursorContext)},
 * resulting in open page cache files, for any page cache mapped native index files.
 *
 * This would be a problem if the INDEX DROP command was present in the transaction log, but the db had been killed
 * before the command had been applied and so the files would still remain, and not be dropped either when that command
 * was recovered.
 */
@Neo4jLayoutExtension
class RecoverIndexDropIT {
    private static final String KEY = "key";

    @Inject
    private DefaultFileSystemAbstraction fs;

    @Inject
    private TestDirectory directory;

    @Inject
    private DatabaseLayout databaseLayout;

    TestDatabaseManagementServiceBuilder configure(TestDatabaseManagementServiceBuilder builder) {
        return builder;
    }

    @Test
    void shouldDropIndexOnRecovery() throws IOException {
        // given a transaction stream ending in an INDEX DROP command.
        CommittedCommandBatch dropTransaction = prepareDropTransaction();
        DatabaseManagementService managementService = configure(
                        new TestDatabaseManagementServiceBuilder(databaseLayout))
                .build();
        GraphDatabaseService db = managementService.database(DEFAULT_DATABASE_NAME);
        long initialIndexCount = currentIndexCount(db);
        createIndex(db);
        StorageEngineFactory storageEngineFactory =
                ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(StorageEngineFactory.class);
        managementService.shutdown();
        appendDropTransactionToTransactionLog(
                databaseLayout.getTransactionLogsDirectory(), dropTransaction, storageEngineFactory);

        // when recovering this (the drop transaction with the index file intact)
        Monitors monitors = new Monitors();
        AssertRecoveryIsPerformed recoveryMonitor = new AssertRecoveryIsPerformed();
        monitors.addMonitorListener(recoveryMonitor);
        managementService = configure(new TestDatabaseManagementServiceBuilder(databaseLayout).setMonitors(monitors))
                .build();
        db = managementService.database(DEFAULT_DATABASE_NAME);
        try {
            assertTrue(recoveryMonitor.recoveryWasRequired);

            // then
            assertEquals(initialIndexCount, currentIndexCount(db));
        } finally {
            // and the ability to shut down w/o failing on still open files
            managementService.shutdown();
        }
    }

    private static long currentIndexCount(GraphDatabaseService db) {
        try (Transaction tx = db.beginTx()) {
            return count(tx.schema().getIndexes());
        }
    }

    private static IndexDefinition createIndex(GraphDatabaseService db) {
        try (Transaction tx = db.beginTx()) {
            IndexDefinition index = tx.schema().indexFor(LABEL_ONE).on(KEY).create();
            tx.commit();
            return index;
        }
    }

    private void appendDropTransactionToTransactionLog(
            Path transactionLogsDirectory, CommittedCommandBatch dropBatch, StorageEngineFactory storageEngineFactory)
            throws IOException {
        LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder(transactionLogsDirectory, fs)
                .withStorageEngineFactory(storageEngineFactory)
                .build();
        LogFile logFile = logFiles.getLogFile();

        try (ReadableLogChannel reader =
                logFile.getReader(logFile.extractHeader(0).getStartPosition())) {
            LogEntryReader logEntryReader = new VersionAwareLogEntryReader(
                    storageEngineFactory.commandReaderFactory(), LatestVersions.BINARY_VERSIONS);
            while (logEntryReader.readLogEntry(reader) != null) {}
            LogPosition position = logEntryReader.lastPosition();
            StoreChannel storeChannel = fs.write(logFile.getLogFileForVersion(logFile.getHighestLogVersion()));
            storeChannel.position(position.getByteOffset());
            try (var writeChannel = new PhysicalFlushableLogChannel(
                    storeChannel, new HeapScopedBuffer(100, ByteOrder.LITTLE_ENDIAN, INSTANCE))) {
                new LogEntryWriter<>(writeChannel, LatestVersions.BINARY_VERSIONS).serialize(dropBatch);
            }
        }
    }

    private CommittedCommandBatch prepareDropTransaction() throws IOException {
        DatabaseManagementService managementService = configure(
                        new TestDatabaseManagementServiceBuilder(directory.directory("preparation")))
                .build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        try {
            // Create index
            IndexDefinition index;
            index = createIndex(db);
            try (Transaction tx = db.beginTx()) {
                tx.schema().getIndexByName(index.getName()).drop();
                tx.commit();
            }
            return extractLastTransaction(db);
        } finally {
            managementService.shutdown();
        }
    }

    private static CommittedCommandBatch extractLastTransaction(GraphDatabaseAPI db) throws IOException {
        LogicalTransactionStore txStore = db.getDependencyResolver().resolveDependency(LogicalTransactionStore.class);
        CommittedCommandBatch transaction = null;
        try (CommandBatchCursor cursor = txStore.getCommandBatches(TransactionIdStore.BASE_TX_ID + 1)) {
            while (cursor.next()) {
                transaction = cursor.get();
            }
        }
        return transaction;
    }

    private static class AssertRecoveryIsPerformed implements RecoveryMonitor {
        boolean recoveryWasRequired;

        @Override
        public void recoveryRequired(LogPosition recoveryPosition) {
            recoveryWasRequired = true;
        }
    }
}
