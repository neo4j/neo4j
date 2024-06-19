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
package org.neo4j.kernel.diagnostics.providers;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.log.entry.LogSegments.UNKNOWN_LOG_SEGMENT_SIZE;
import static org.neo4j.logging.LogAssertions.assertThat;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION;
import static org.neo4j.test.LatestVersions.LATEST_LOG_FORMAT;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.io.device.DeviceMapper;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.transaction.log.CheckpointInfo;
import org.neo4j.kernel.impl.transaction.log.EmptyLogTailMetadata;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.kernel.impl.transaction.log.entry.v50.LogEntryDetachedCheckpointV5_0;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.kernel.impl.transaction.log.files.checkpoint.CheckpointFile;
import org.neo4j.kernel.impl.transaction.log.files.checkpoint.CheckpointInfoFactory;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.InternalLog;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class TransactionRangeDiagnosticsTest {
    @Inject
    TestDirectory directory;

    @Inject
    FileSystemAbstraction fs;

    @Test
    void shouldLogCorrectTransactionLogDiagnosticsForNoTransactionLogs() throws IOException {
        // GIVEN
        Database database = databaseWithLogFilesContainingLowestTxId(noLogs());
        AssertableLogProvider logProvider = new AssertableLogProvider();
        InternalLog logger = logProvider.getLog(getClass());

        // WHEN
        new TransactionRangeDiagnostics(database).dump(logger::info);

        // THEN
        assertThat(logProvider)
                .containsMessages("Transaction log files stored on file store:")
                .containsMessages(" - no transactions found")
                .containsMessages(" - no checkpoints found");
    }

    @Test
    void shouldLogCorrectTransactionLogDiagnosticsForTransactionsInOldestLog() throws Exception {
        // GIVEN
        long logVersion = 2;
        long prevLogLastTxId = 45;
        Database database =
                databaseWithLogFilesContainingLowestTxId(logWithTransactions(logVersion, logVersion, prevLogLastTxId));
        AssertableLogProvider logProvider = new AssertableLogProvider();
        InternalLog logger = logProvider.getLog(getClass());

        // WHEN
        new TransactionRangeDiagnostics(database).dump(logger::info);

        // THEN
        assertThat(logProvider)
                .containsMessages("oldest append index " + (prevLogLastTxId + 1), "version " + logVersion)
                .containsMessages("existing transaction log versions")
                .containsMessages("no checkpoints found");
    }

    @Test
    void shouldLogCorrectTransactionLogDiagnosticsForTransactionsInSecondOldestLog() throws Exception {
        // GIVEN
        long logVersion = 2;
        long prevLogLastTxId = 45;
        Database database = databaseWithLogFilesContainingLowestTxId(
                logWithTransactionsInNextToOldestLog(logVersion, prevLogLastTxId));
        AssertableLogProvider logProvider = new AssertableLogProvider();
        InternalLog logger = logProvider.getLog(getClass());

        // WHEN
        new TransactionRangeDiagnostics(database).dump(logger::info);

        // THEN
        assertThat(logProvider)
                .containsMessages("oldest append index " + (prevLogLastTxId + 1), "version " + (logVersion + 1))
                .containsMessages("no checkpoints found");
    }

    @Test
    void shouldLogCorrectTransactionLogDiagnosticsForTransactionsAndCheckpointLogs() throws Exception {
        // GIVEN
        long txLogLowVersion = 2;
        long txLogHighVersion = 10;
        long checkpointLogLowVersion = 0;
        long checkpointLogHighVersion = 3;
        StoreId storeId = new StoreId(12345, 56789, "engine-1", "format-1", 1, 1);
        LogPosition checkpointLogPosition = new LogPosition(checkpointLogHighVersion, 34);
        LogPosition afterCheckpointLogPosition = new LogPosition(checkpointLogHighVersion, 36);
        LogPosition readerPostPosition = new LogPosition(checkpointLogHighVersion, 36);
        TransactionId transactionId = new TransactionId(37, 42, LATEST_KERNEL_VERSION, 38, 39, 40);
        Database database = databaseWithLogFilesContainingLowestTxId(logs(
                transactionLogsWithTransaction(txLogLowVersion, txLogHighVersion, 42),
                checkpointLogsWithLastCheckpoint(
                        checkpointLogLowVersion,
                        checkpointLogHighVersion,
                        CheckpointInfoFactory.ofLogEntry(
                                new LogEntryDetachedCheckpointV5_0(
                                        LatestVersions.LATEST_KERNEL_VERSION,
                                        transactionId,
                                        checkpointLogPosition,
                                        1234,
                                        storeId,
                                        "testing"),
                                checkpointLogPosition,
                                afterCheckpointLogPosition,
                                readerPostPosition,
                                null,
                                null))));
        AssertableLogProvider logProvider = new AssertableLogProvider();
        InternalLog logger = logProvider.getLog(getClass());

        // WHEN
        new TransactionRangeDiagnostics(database).dump(logger::info);

        // THEN
        assertThat(logProvider)
                .containsMessages("existing transaction log versions: " + txLogLowVersion + "-" + txLogHighVersion)
                .containsMessages("existing checkpoint log versions: " + checkpointLogLowVersion + "-"
                        + checkpointLogHighVersion);
    }

    @Test
    void shouldLogNoCheckpointFoundForEmptyPresentCheckpointLog() throws IOException {
        // GIVEN
        Database database = databaseWithLogFilesContainingLowestTxId(
                logs(transactionLogs -> {}, checkpointLogsWithLastCheckpoint(0, 0, null)));
        AssertableLogProvider logProvider = new AssertableLogProvider();
        InternalLog logger = logProvider.getLog(getClass());

        // WHEN
        new TransactionRangeDiagnostics(database).dump(logger::info);

        // THEN
        assertThat(logProvider)
                .containsMessages("no transactions found")
                .containsMessages("existing checkpoint log versions: 0-0")
                .containsMessages("no checkpoints found");
    }

    @Test
    void shouldLogTransactionFilesInOrder() throws IOException {
        // GIVEN
        Database database = databaseWithLogFilesContainingLowestTxId(logWithTransactions(0, 11, 1));
        AssertableLogProvider logProvider = new AssertableLogProvider();
        InternalLog logger = logProvider.getLog(getClass());

        // WHEN
        new TransactionRangeDiagnostics(database).dump(logger::info);

        // THEN
        assertThat(logProvider)
                .containsMessagesInOrder(
                        "files: (filename : creation date - size)",
                        "neostore.transaction.db.0:", // Hard to assert on date here
                        "- 31B",
                        "neostore.transaction.db.1:",
                        "- 31B",
                        "neostore.transaction.db.2:",
                        "- 31B",
                        "neostore.transaction.db.10:",
                        "- 31B",
                        "neostore.transaction.db.11:",
                        "- 31B",
                        "total size of files: 372B");
    }

    private Database databaseWithLogFilesContainingLowestTxId(LogFiles files) {
        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependency(DeviceMapper.UNKNOWN_MAPPER);
        LogTailMetadata logTailMetadata = dependencies.satisfyDependency(files.getTailMetadata());
        TransactionIdStore txIdStore = dependencies.satisfyDependency(mock(TransactionIdStore.class));
        when(txIdStore.getLastClosedTransactionId())
                .thenReturn(logTailMetadata.getLastCommittedTransaction().id());
        dependencies.satisfyDependency(files);
        dependencies.satisfyDependency(fs);

        Database database = mock(Database.class);
        when(database.getDependencyResolver()).thenReturn(dependencies);
        return database;
    }

    private LogFiles logWithTransactionsInNextToOldestLog(long logVersion, long prevLogLastAppendIndex)
            throws IOException {
        LogFiles files = logWithTransactions(logVersion, logVersion + 1, prevLogLastAppendIndex);
        var logFile = files.getLogFile();
        when(logFile.hasAnyEntries(logVersion)).thenReturn(false);
        return files;
    }

    private ThrowingConsumer<CheckpointFile, IOException> checkpointLogsWithLastCheckpoint(
            long lowVersion, long highVersion, CheckpointInfo lastCheckpoint) {
        return checkpointLogs -> {
            when(checkpointLogs.getLowestLogVersion()).thenReturn(lowVersion);
            when(checkpointLogs.getHighestLogVersion()).thenReturn(highVersion);
            when(checkpointLogs.findLatestCheckpoint()).thenReturn(Optional.ofNullable(lastCheckpoint));
        };
    }

    private LogFiles logWithTransactions(long lowVersion, long highVersion, long headerAppendIndex) throws IOException {
        return logs(transactionLogsWithTransaction(lowVersion, highVersion, headerAppendIndex), checkpointLogs -> {});
    }

    private ThrowingConsumer<LogFile, IOException> transactionLogsWithTransaction(
            long lowVersion, long highVersion, long headerAppendIndex) {
        return transactionLogs -> {
            when(transactionLogs.getLowestLogVersion()).thenReturn(lowVersion);
            when(transactionLogs.getHighestLogVersion()).thenReturn(highVersion);
            TransactionLogFilesHelper helper = TransactionLogFilesHelper.forTransactions(fs, directory.homePath());
            for (long version = lowVersion; version <= highVersion; version++) {
                when(transactionLogs.hasAnyEntries(version)).thenReturn(true);
                when(transactionLogs.versionExists(version)).thenReturn(true);
                Path logFile = helper.getLogFileForVersion(version);
                try (StoreChannel write = fs.write(logFile)) {
                    write.writeAll(ByteBuffer.wrap("Some text to mock a tx log file".getBytes()));
                }
                when(transactionLogs.extractHeader(version))
                        .thenReturn(LATEST_LOG_FORMAT.newHeader(
                                version,
                                headerAppendIndex,
                                new StoreId(12345, 56789, "engine-1", "format-1", 1, 1),
                                UNKNOWN_LOG_SEGMENT_SIZE,
                                BASE_TX_CHECKSUM,
                                LATEST_KERNEL_VERSION));
            }

            when(transactionLogs.getMatchedFiles()).thenReturn(helper.getMatchedFiles());
        };
    }

    private LogFiles noLogs() throws IOException {
        return logs(
                transactionLogs -> {
                    when(transactionLogs.getMatchedFiles()).thenReturn(new Path[0]);
                    when(transactionLogs.getLowestLogVersion()).thenReturn(-1L);
                },
                checkpointFiles -> when(checkpointFiles.getLowestLogVersion()).thenReturn(-1L));
    }

    private LogFiles logs(
            ThrowingConsumer<LogFile, IOException> transactionLogs,
            ThrowingConsumer<CheckpointFile, IOException> checkpointLogs)
            throws IOException {
        LogFiles files = mock(TransactionLogFiles.class);
        when(files.logFilesDirectory()).thenReturn(directory.homePath());

        LogFile transactionFiles = mock(LogFile.class);
        when(files.getLogFile()).thenReturn(transactionFiles);
        transactionLogs.accept(transactionFiles);

        CheckpointFile checkpointFiles = mock(CheckpointFile.class);
        when(files.getCheckpointFile()).thenReturn(checkpointFiles);
        when(files.getTailMetadata()).thenReturn(new EmptyLogTailMetadata(Config.defaults()));
        checkpointLogs.accept(checkpointFiles);
        return files;
    }
}
