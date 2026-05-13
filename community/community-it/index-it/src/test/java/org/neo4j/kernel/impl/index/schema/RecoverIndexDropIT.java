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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.Config.defaults;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.Iterables.count;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.test.TestLabels.LABEL_ONE;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.io.fs.ChannelNativeAccessor;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.api.index.IndexMap;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatchRepresentation;
import org.neo4j.kernel.impl.transaction.log.CommandBatchCursor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.PhysicalFlushableLogPositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.recovery.Recovery;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
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
@TestDirectoryExtension
class RecoverIndexDropIT {
    private static final String KEY = "key";

    @Inject
    private DefaultFileSystemAbstraction fs;

    @Inject
    private TestDirectory directory;

    @Test
    void shouldDropIndexOnRecovery() throws Exception {
        // given a transaction stream ending in an INDEX DROP command.
        CommittedCommandBatchRepresentation dropTransaction = prepareDropTransaction();
        DatabaseLayout databaseLayout;
        long initialIndexCount;
        StorageEngineFactory storageEngineFactory;
        try (DatabaseManagementService managementService =
                new TestDatabaseManagementServiceBuilder(directory.homePath()).build()) {
            GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
            databaseLayout = db.databaseLayout();
            initialIndexCount = currentIndexCount(db);
            createIndex(db);
            storageEngineFactory = db.getDependencyResolver().resolveDependency(StorageEngineFactory.class);
        }
        appendDropTransactionToTransactionLog(
                databaseLayout.getTransactionLogsDirectory(), dropTransaction, storageEngineFactory);

        assertThat(Recovery.isRecoveryRequired(fs, databaseLayout, defaults(), INSTANCE))
                .isTrue();
        // when recovering this (the drop transaction with the index file intact)
        try (DatabaseManagementService managementService =
                new TestDatabaseManagementServiceBuilder(directory.homePath()).build()) {
            // then
            assertEquals(initialIndexCount, currentIndexCount(managementService.database(DEFAULT_DATABASE_NAME)));
        } // and the ability to shut down w/o failing on still open files
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
            Path transactionLogsDirectory,
            CommittedCommandBatchRepresentation dropBatch,
            StorageEngineFactory storageEngineFactory)
            throws IOException {
        LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder(transactionLogsDirectory, fs)
                .withStorageEngineFactory(storageEngineFactory)
                .build();
        LogFile logFile = logFiles.getLogFile();

        LogHeader logHeader = logFile.extractHeader(0);
        try (ReadableLogChannel reader = logFile.getReader(logHeader.getStartPosition())) {
            LogEntryReader logEntryReader = new VersionAwareLogEntryReader(
                    storageEngineFactory.commandReaderFactory(), LatestVersions.BINARY_VERSIONS, INSTANCE);
            while (logEntryReader.readLogEntry(reader) != null) {}
            LogPosition position = logEntryReader.lastPosition();
            Path logFileForVersion =
                    logFile.getLogFileForVersion(logFile.getLogRangeInfo().highestVersion());
            StoreChannel storeChannel = fs.write(logFileForVersion);
            storeChannel.position(position.getByteOffset());

            try (PhysicalFlushableLogPositionAwareChannel physicalFlushableLogPositionAwareChannel =
                    new PhysicalFlushableLogPositionAwareChannel(
                            new PhysicalLogVersionedStoreChannel(
                                    storeChannel,
                                    logHeader.getLogVersion(),
                                    logHeader.getLogFormatVersion(),
                                    logFileForVersion,
                                    ChannelNativeAccessor.EMPTY_ACCESSOR,
                                    DatabaseTracer.NULL),
                            logHeader,
                            INSTANCE)) {
                new LogEntryWriter<>(physicalFlushableLogPositionAwareChannel, LatestVersions.BINARY_VERSIONS)
                        .serialize(dropBatch);
            }
        }
    }

    private CommittedCommandBatchRepresentation prepareDropTransaction() throws IOException {
        try (DatabaseManagementService dbms =
                new TestDatabaseManagementServiceBuilder(directory.directory("preparation")).build()) {
            GraphDatabaseAPI db = (GraphDatabaseAPI) dbms.database(DEFAULT_DATABASE_NAME);
            // Create index
            IndexDefinition index;
            index = createIndex(db);
            try (Transaction tx = db.beginTx()) {
                tx.schema().getIndexByName(index.getName()).drop();
                tx.commit();
            }
            return extractLastTransaction(db);
        }
    }

    private static CommittedCommandBatchRepresentation extractLastTransaction(GraphDatabaseAPI db) throws IOException {
        LogicalTransactionStore txStore = db.getDependencyResolver().resolveDependency(LogicalTransactionStore.class);
        CommittedCommandBatchRepresentation transaction = null;
        try (CommandBatchCursor cursor = txStore.getCommandBatches(TransactionIdStore.BASE_TX_ID + 1)) {
            while (cursor.next()) {
                transaction = cursor.get();
            }
        }
        return transaction;
    }
}
