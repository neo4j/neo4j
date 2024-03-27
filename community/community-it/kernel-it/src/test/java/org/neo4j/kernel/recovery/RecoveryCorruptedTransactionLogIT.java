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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.common.Subject.ANONYMOUS;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.checkpoint_logical_log_keep_threshold;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.fail_on_corrupted_log_files;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.kernel.impl.api.TransactionToApply.NOT_SPECIFIED_CHUNK_ID;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.TX_START;
import static org.neo4j.logging.LogAssertions.assertThat;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_TRANSACTION_ID;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION_PROVIDER;
import static org.neo4j.test.LatestVersions.LATEST_LOG_FORMAT;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.dbms.database.StandaloneDatabaseContext;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.nativeimpl.NativeAccessProvider;
import org.neo4j.internal.recordstorage.Command;
import org.neo4j.internal.recordstorage.LogCommandSerialization;
import org.neo4j.internal.recordstorage.RecordStorageCommandReaderFactory;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.fs.StoreFileChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatch;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.CompleteTransaction;
import org.neo4j.kernel.impl.transaction.log.FlushableLogPositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.InMemoryVersionableReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.DetachedCheckpointAppender;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.entry.IncompleteLogHeaderException;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.UnsupportedLogVersionException;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.v57.DetachedCheckpointLogEntrySerializerV5_7;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.kernel.impl.transaction.log.files.checkpoint.CheckpointFile;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreIdProvider;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.RandomExtension;

@Neo4jLayoutExtension
@ExtendWith(RandomExtension.class)
class RecoveryCorruptedTransactionLogIT {
    private static final int CHECKPOINT_RECORD_SIZE = DetachedCheckpointLogEntrySerializerV5_7.RECORD_LENGTH_BYTES;
    private static final LogCommandSerialization LATEST_LOG_SERIALIZATION =
            RecordStorageCommandReaderFactory.INSTANCE.get(LatestVersions.LATEST_KERNEL_VERSION);

    @Inject
    private DefaultFileSystemAbstraction fileSystem;

    @Inject
    private DatabaseLayout databaseLayout;

    @Inject
    private RandomSupport random;

    private static final int HEADER_OFFSET = LATEST_LOG_FORMAT.getHeaderSize();
    private final AssertableLogProvider logProvider = new AssertableLogProvider(true);
    private final RecoveryMonitor recoveryMonitor = new RecoveryMonitor();
    private final CorruptedCheckpointMonitor corruptedFilesMonitor = new CorruptedCheckpointMonitor();
    private final Monitors monitors = new Monitors();
    private LogFiles logFiles;
    private TestDatabaseManagementServiceBuilder databaseFactory;
    private StorageEngineFactory storageEngineFactory;
    // Some transactions can have been run on start-up, so this is the offset the first transaction of a test will have.
    private long txOffsetAfterStart;

    @BeforeEach
    void setUp() {
        monitors.addMonitorListener(recoveryMonitor);
        monitors.addMonitorListener(corruptedFilesMonitor);
        databaseFactory = new TestDatabaseManagementServiceBuilder(databaseLayout)
                .setConfig(checkpoint_logical_log_keep_threshold, 25)
                .setInternalLogProvider(logProvider)
                .setMonitors(monitors)
                .setFileSystem(fileSystem);

        txOffsetAfterStart = startStopDatabaseAndGetTxOffset();
    }

    @Test
    void recoverFromLastCorruptedNotFullyWrittenCheckpointRecord() throws IOException {
        for (int iteration = 0; iteration < 10; iteration++) {
            int bytesToTrim = random.nextInt(1, CHECKPOINT_RECORD_SIZE);

            DatabaseManagementService managementService = databaseFactory.build();
            GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
            logFiles = buildDefaultLogFiles(getStoreId(database));

            TransactionIdStore transactionIdStore = getTransactionIdStore(database);
            LogPosition logOffsetBeforeTestTransactions =
                    transactionIdStore.getLastClosedTransaction().logPosition();
            long lastClosedTransactionBeforeStart = transactionIdStore.getLastClosedTransactionId();
            for (int i = 0; i < 10; i++) {
                generateTransaction(database);
            }
            long numberOfClosedTransactions =
                    getTransactionIdStore(database).getLastClosedTransactionId() - lastClosedTransactionBeforeStart;

            DependencyResolver dependencyResolver = database.getDependencyResolver();
            var databaseCheckpointer = dependencyResolver
                    .resolveDependency(TransactionLogFiles.class)
                    .getCheckpointFile();
            databaseCheckpointer
                    .getCheckpointAppender()
                    .checkPoint(
                            LogCheckPointEvent.NULL,
                            transactionIdStore.getLastCommittedTransaction(),
                            LatestVersions.LATEST_KERNEL_VERSION,
                            logOffsetBeforeTestTransactions,
                            Instant.now(),
                            "Fallback checkpoint.");
            managementService.shutdown();

            truncateBytesFromLastCheckpointLogFile(bytesToTrim);
            startStopDbRecoveryOfCorruptedLogs();

            assertEquals(numberOfClosedTransactions, recoveryMonitor.getNumberOfRecoveredTransactions());
            assertThat(logFiles.getCheckpointFile().getDetachedCheckpointFiles())
                    .hasSize(2);
            assertEquals(0, corruptedFilesMonitor.getNumberOfCorruptedCheckpointFiles());

            removeDatabaseDirectories();
        }
    }

    @Test
    void recoverFromLastCorruptedBrokenCheckpointRecord() throws IOException {
        for (int iteration = 0; iteration < 10; iteration++) {
            int bytesToAdd = random.nextInt(1, CHECKPOINT_RECORD_SIZE);

            DatabaseManagementService managementService = databaseFactory.build();
            GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
            logFiles = buildDefaultLogFiles(getStoreId(database));

            TransactionIdStore transactionIdStore = getTransactionIdStore(database);
            LogPosition logOffsetBeforeTestTransactions =
                    transactionIdStore.getLastClosedTransaction().logPosition();
            long lastClosedTransactionBeforeStart = transactionIdStore.getLastClosedTransactionId();
            for (int i = 0; i < 10; i++) {
                generateTransaction(database);
            }
            long numberOfClosedTransactions =
                    transactionIdStore.getLastClosedTransactionId() - lastClosedTransactionBeforeStart;

            DependencyResolver dependencyResolver = database.getDependencyResolver();
            var databaseCheckpointer = dependencyResolver
                    .resolveDependency(TransactionLogFiles.class)
                    .getCheckpointFile();
            databaseCheckpointer
                    .getCheckpointAppender()
                    .checkPoint(
                            LogCheckPointEvent.NULL,
                            transactionIdStore.getLastCommittedTransaction(),
                            LatestVersions.LATEST_KERNEL_VERSION,
                            logOffsetBeforeTestTransactions,
                            Instant.now(),
                            "Fallback checkpoint.");
            managementService.shutdown();

            removeLastCheckpointRecordFromLastLogFile();
            appendRandomBytesAfterLastCheckpointRecordFromLastLogFile(bytesToAdd);
            startStopDbRecoveryOfCorruptedLogs();

            assertEquals(numberOfClosedTransactions, recoveryMonitor.getNumberOfRecoveredTransactions());
            assertEquals(iteration + 1, corruptedFilesMonitor.getNumberOfCorruptedCheckpointFiles());

            assertThat(logFiles.getCheckpointFile().getDetachedCheckpointFiles())
                    .hasSize(2);

            removeDatabaseDirectories();
        }
    }

    @Test
    void doNotRotateIfRecoveryIsRequiredButThereAreNoUnreadableData() throws IOException {
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        logFiles = buildDefaultLogFiles(getStoreId(database));

        TransactionIdStore transactionIdStore = getTransactionIdStore(database);
        LogPosition logOffsetBeforeTestTransactions =
                transactionIdStore.getLastClosedTransaction().logPosition();
        long lastClosedTransactionBeforeStart = transactionIdStore.getLastClosedTransactionId();
        for (int i = 0; i < 10; i++) {
            generateTransaction(database);
        }
        long numberOfClosedTransactions =
                transactionIdStore.getLastClosedTransactionId() - lastClosedTransactionBeforeStart;

        DependencyResolver dependencyResolver = database.getDependencyResolver();
        var databaseCheckpointer =
                dependencyResolver.resolveDependency(TransactionLogFiles.class).getCheckpointFile();
        databaseCheckpointer
                .getCheckpointAppender()
                .checkPoint(
                        LogCheckPointEvent.NULL,
                        transactionIdStore.getLastCommittedTransaction(),
                        LatestVersions.LATEST_KERNEL_VERSION,
                        logOffsetBeforeTestTransactions,
                        Instant.now(),
                        "Fallback checkpoint.");
        managementService.shutdown();

        removeLastCheckpointRecordFromLastLogFile();
        startStopDatabase();

        assertEquals(numberOfClosedTransactions, recoveryMonitor.getNumberOfRecoveredTransactions());
        assertEquals(0, corruptedFilesMonitor.getNumberOfCorruptedCheckpointFiles());

        assertThat(logFiles.getCheckpointFile().getDetachedCheckpointFiles()).hasSize(1);
    }

    @Test
    void evenTruncateNewerTransactionLogFile() throws IOException {
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        logFiles = buildDefaultLogFiles(getStoreId(database));
        TransactionIdStore transactionIdStore = getTransactionIdStore(database);
        long lastClosedTransactionBeforeStart = transactionIdStore.getLastClosedTransactionId();
        for (int i = 0; i < 10; i++) {
            generateTransaction(database);
        }
        long numberOfClosedTransactions =
                getTransactionIdStore(database).getLastClosedTransactionId() - lastClosedTransactionBeforeStart;
        managementService.shutdown();
        removeLastCheckpointRecordFromLastLogFile();
        addRandomBytesToLastLogFile(this::randomNonZeroByte);

        startStopDbRecoveryOfCorruptedLogs();

        assertEquals(numberOfClosedTransactions, recoveryMonitor.getNumberOfRecoveredTransactions());
    }

    @Test
    void doNotTruncateNewerTransactionLogFileWhenFailOnError() throws IOException {
        DatabaseManagementService managementService1 = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService1.database(DEFAULT_DATABASE_NAME);
        logFiles = buildDefaultLogFiles(getStoreId(database));
        for (int i = 0; i < 10; i++) {
            generateTransaction(database);
        }
        managementService1.shutdown();
        removeLastCheckpointRecordFromLastLogFile();
        addRandomBytesToLastLogFile(this::randomInvalidVersionsBytes);

        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        try {
            DatabaseStateService dbStateService =
                    db.getDependencyResolver().resolveDependency(DatabaseStateService.class);
            assertTrue(dbStateService.causeOfFailure(db.databaseId()).isPresent());
            assertThat(dbStateService.causeOfFailure(db.databaseId()).get())
                    .hasRootCauseInstanceOf(UnsupportedLogVersionException.class);
        } finally {
            managementService.shutdown();
        }
    }

    @Test
    void truncateNewerTransactionLogFileWhenForced() throws IOException {
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        TransactionIdStore transactionIdStore = getTransactionIdStore(database);
        long numberOfClosedTransactionsAfterStartup = transactionIdStore.getLastClosedTransactionId();
        logFiles = buildDefaultLogFiles(getStoreId(database));
        long lastTxSize = 0;
        long totalTxSize = 0;
        for (int i = 0; i < 10; i++) {
            long size = generateTransaction(database);
            lastTxSize = size;
            totalTxSize += size;
        }
        long numberOfTransactionsToRecover =
                transactionIdStore.getLastClosedTransactionId() - numberOfClosedTransactionsAfterStartup;
        managementService.shutdown();

        removeLastCheckpointRecordFromLastLogFile();
        Supplier<Byte> randomBytesSupplier = this::randomInvalidVersionsBytes;
        BytesCaptureSupplier capturingSupplier = new BytesCaptureSupplier(randomBytesSupplier);
        addRandomBytesToLastLogFile(capturingSupplier);
        assertFalse(recoveryMonitor.wasRecoveryRequired());

        startStopDbRecoveryOfCorruptedLogs();

        try {
            assertEquals(numberOfTransactionsToRecover, recoveryMonitor.getNumberOfRecoveredTransactions());
            assertTrue(recoveryMonitor.wasRecoveryRequired());
            assertThat(logProvider)
                    .containsMessages(
                            "Fail to read transaction log version 0.",
                            "Fail to read transaction log version 0. Last valid transaction start offset is: "
                                    + (totalTxSize - lastTxSize + txOffsetAfterStart) + ".");
        } catch (Throwable t) {
            throw new RuntimeException("Generated random bytes: " + capturingSupplier.getCapturedBytes(), t);
        }
    }

    @ParameterizedTest(name = "[{index}] ({0})")
    @MethodSource("corruptedLogEntryWriters")
    void recoverFirstCorruptedTransactionSingleFileNoCheckpoint(
            String testName, LogEntryWriterWrapper logEntryWriterWrapper) throws IOException {
        addCorruptedCommandsToLastLogFile(logEntryWriterWrapper);

        startStopDbRecoveryOfCorruptedLogs();

        assertThat(logProvider)
                .containsMessages(
                        "Fail to read transaction log version 0.",
                        "Fail to read first transaction of log version 0.",
                        "Recovery required from position LogPosition{logVersion=0, byteOffset=" + txOffsetAfterStart
                                + "}",
                        "Fail to recover database. Any transactional logs after position LogPosition{logVersion=0, "
                                + "byteOffset=" + txOffsetAfterStart + "} can not be recovered and will be truncated.");

        logFiles = buildDefaultLogFiles(new StoreId(4, 5, "engine-1", "format-1", 1, 2));
        assertEquals(0, logFiles.getLogFile().getHighestLogVersion());
        if (NativeAccessProvider.getNativeAccess().isAvailable()) {
            assertEquals(
                    ByteUnit.mebiBytes(1),
                    Files.size(logFiles.getCheckpointFile().getCurrentFile()));
        } else {
            assertEquals(
                    LATEST_LOG_FORMAT.getHeaderSize()
                            + CHECKPOINT_RECORD_SIZE * 4 /* checkpoint for setup, start and stop */,
                    Files.size(logFiles.getCheckpointFile().getCurrentFile()));
        }
    }

    @Test
    void failToStartWithTransactionLogsWithDataAfterLastEntry() throws IOException {
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        logFiles = buildDefaultLogFiles(getStoreId(database));
        long txSize = generateTransaction(database);
        managementService.shutdown();

        writeRandomBytesAfterLastCommandInLastLogFile(() -> ByteBuffer.wrap(new byte[] {1, 2, 3, 4, 5}));

        startStopDatabase();
        assertThat(logProvider)
                .assertExceptionForLogMessage("Fail to read transaction log version 0.")
                .hasMessageContaining("Transaction log file with version 0 has some data available after last readable "
                        + "log entry. Last readable position " + (txSize + txOffsetAfterStart));
    }

    @Test
    void startWithTransactionLogsWithDataAfterLastEntryAndCorruptedLogsRecoveryEnabled() throws IOException {
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        logFiles = buildDefaultLogFiles(getStoreId(database));
        long txSize = generateTransaction(database);
        long initialTransactionOffset = txOffsetAfterStart + txSize;
        assertEquals(initialTransactionOffset, getLastClosedTransactionOffset(database));
        managementService.shutdown();

        writeRandomBytesAfterLastCommandInLastLogFile(() -> ByteBuffer.wrap(new byte[] {1, 2, 3, 4, 5}));

        managementService =
                databaseFactory.setConfig(fail_on_corrupted_log_files, false).build();
        try {
            assertThat(logProvider)
                    .containsMessages("Recovery required from position " + "LogPosition{logVersion=0, byteOffset="
                            + initialTransactionOffset + "}")
                    .assertExceptionForLogMessage("Fail to read transaction log version 0.")
                    .hasMessageContaining(
                            "Transaction log file with version 0 has some data available after last readable log entry. "
                                    + "Last readable position " + initialTransactionOffset);
            GraphDatabaseAPI restartedDb = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
            assertEquals(initialTransactionOffset, getLastClosedTransactionOffset(restartedDb));
        } finally {
            managementService.shutdown();
        }
    }

    @Test
    void failToStartWithNotLastTransactionLogHavingZerosInTheEnd() throws IOException {
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        logFiles = buildDefaultLogFiles(getStoreId(database));
        generateTransaction(database);
        managementService.shutdown();

        try (Lifespan lifespan = new Lifespan(logFiles)) {
            Path originalFile = logFiles.getLogFile().getHighestLogFile();
            logFiles.getLogFile().rotate();

            // append zeros in the end of previous file causing illegal suffix
            try (StoreFileChannel writeChannel = fileSystem.write(originalFile)) {
                writeChannel.position(writeChannel.size());
                for (int i = 0; i < 10; i++) {
                    writeChannel.writeAll(ByteBuffer.wrap(new byte[] {0, 0, 0, 0, 0}));
                }
            }
        }

        startStopDatabase();
        assertThat(logProvider)
                .assertExceptionForLogMessage("Fail to read transaction log version 0.")
                .hasMessageContaining("Transaction log files with version 0 has 50 unreadable bytes");
    }

    @Test
    void startWithNotLastTransactionLogHavingZerosInTheEndAndCorruptedLogRecoveryEnabled() throws IOException {
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        logFiles = buildDefaultLogFiles(getStoreId(database));
        long txSize = generateTransaction(database);
        managementService.shutdown();

        long originalLogDataLength;
        Path firstLogFile;
        try (Lifespan lifespan = new Lifespan(logFiles)) {
            LogFile logFile = logFiles.getLogFile();
            LogPosition readablePosition = getLastReadablePosition(logFile);
            firstLogFile = logFiles.getLogFile().getHighestLogFile();
            originalLogDataLength = readablePosition.getByteOffset();
            logFile.rotate();

            // append zeros in the end of previous file causing illegal suffix
            try (StoreFileChannel writeChannel = fileSystem.write(firstLogFile)) {
                writeChannel.position(writeChannel.size());
                for (int i = 0; i < 10; i++) {
                    writeChannel.writeAll(ByteBuffer.wrap(new byte[] {0, 0, 0, 0, 0}));
                }
            }
        }

        startStopDbRecoveryOfCorruptedLogs();

        assertEquals(originalLogDataLength, fileSystem.getFileSize(firstLogFile));

        assertThat(logProvider)
                .containsMessages("Recovery required from position LogPosition{logVersion=0, byteOffset="
                        + (txSize + txOffsetAfterStart) + "}")
                .assertExceptionForLogMessage("Fail to read transaction log version 0.")
                .hasMessage("Transaction log files with version 0 has 50 unreadable bytes. Was able to read upto "
                        + (txSize + txOffsetAfterStart)
                        + " but "
                        + (txSize + 50 + txOffsetAfterStart) + " is available.");
    }

    @Test
    void restoreCheckpointLogVersionFromFileVersion() throws IOException {
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        logFiles = buildDefaultLogFiles(getStoreId(database));
        generateTransaction(database);
        managementService.shutdown();

        int rotations = 10;
        try (Lifespan lifespan = new Lifespan(logFiles)) {
            CheckpointFile checkpointFile = logFiles.getCheckpointFile();
            DetachedCheckpointAppender checkpointAppender =
                    (DetachedCheckpointAppender) checkpointFile.getCheckpointAppender();

            for (int i = 0; i < rotations; i++) {
                checkpointAppender.checkPoint(
                        LogCheckPointEvent.NULL,
                        UNKNOWN_TRANSACTION_ID,
                        LatestVersions.LATEST_KERNEL_VERSION,
                        new LogPosition(0, HEADER_OFFSET),
                        Instant.now(),
                        "test" + i);
                checkpointAppender.rotate();
            }
        }

        for (int i = rotations - 1; i > 0; i--) {
            var restartedDbms = databaseFactory.build();
            try {
                var metadataProvider = ((GraphDatabaseAPI) restartedDbms.database(DEFAULT_DATABASE_NAME))
                        .getDependencyResolver()
                        .resolveDependency(MetadataProvider.class);
                assertEquals(i, metadataProvider.getCheckpointLogVersion());
            } finally {
                restartedDbms.shutdown();
            }
            // we remove 3 checkpoints: 1 from shutdown and 1 from recovery and one that we created in a loop before
            removeLastCheckpointRecordFromLastLogFile();
            removeLastCheckpointRecordFromLastLogFile();
            removeLastCheckpointRecordFromLastLogFile();
        }
    }

    @Test
    void startWithoutProblemsIfRotationForcedBeforeFileEnd() throws IOException {
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        logFiles = buildDefaultLogFiles(getStoreId(database));
        generateTransaction(database);
        managementService.shutdown();

        try (Lifespan lifespan = new Lifespan(logFiles)) {
            Path originalFile = logFiles.getLogFile().getHighestLogFile();
            // append zeros in the end of file before rotation should not be problematic since rotation will prepare tx
            // log file and truncate
            // in it its current position.
            try (StoreFileChannel writeChannel = fileSystem.write(originalFile)) {
                writeChannel.position(writeChannel.size());
                for (int i = 0; i < 10; i++) {
                    writeChannel.writeAll(ByteBuffer.wrap(new byte[] {0, 0, 0, 0, 0}));
                }
            }
            logFiles.getLogFile().rotate();
        }

        startStopDatabase();
        assertThat(logProvider).doesNotContainMessage("Fail to read transaction log version 0.");
    }

    @Test
    void detectCorruptedCheckpointFileWithDataAfterLastRecord() throws IOException {
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        logFiles = buildDefaultLogFiles(getStoreId(database));
        generateTransaction(database);
        managementService.shutdown();

        appendDataAfterLastCheckpointRecordFromLastLogFile();

        var dbms = databaseFactory.build();
        try {
            var context = getDefaultDbContext(dbms);
            assertFalse(context.database().isStarted());
            assertTrue(context.isFailed());
            assertThat(context.failureCause())
                    .rootCause()
                    .hasMessageContaining(
                            "Checkpoint log file with version 0 has some data available after last readable log entry.");
        } finally {
            dbms.shutdown();
        }
    }

    @Test
    void detectAndStartWithCorruptedCheckpointFileWithDataAfterLastRecord() throws IOException {
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        logFiles = buildDefaultLogFiles(getStoreId(database));
        generateTransaction(database);
        managementService.shutdown();

        appendDataAfterLastCheckpointRecordFromLastLogFile();

        var dbms = databaseFactory.setConfig(fail_on_corrupted_log_files, false).build();
        try {
            var context = getDefaultDbContext(dbms);
            assertTrue(context.database().isStarted());
        } finally {
            dbms.shutdown();
        }
    }

    @Test
    void startWithoutProblemsIfRotationForcedBeforeFileEndAndCorruptedLogFilesRecoveryEnabled() throws IOException {
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        logFiles = buildDefaultLogFiles(getStoreId(database));
        generateTransaction(database);
        managementService.shutdown();

        try (Lifespan lifespan = new Lifespan(logFiles)) {
            Path originalFile = logFiles.getLogFile().getHighestLogFile();
            // append zeros in the end of file before rotation should not be problematic since rotation will prepare tx
            // log file and truncate
            // in it its current position.
            try (StoreFileChannel writeChannel = fileSystem.write(originalFile)) {
                writeChannel.position(writeChannel.size());
                for (int i = 0; i < 10; i++) {
                    writeChannel.writeAll(ByteBuffer.wrap(new byte[] {0, 0, 0, 0, 0}));
                }
            }
            logFiles.getLogFile().rotate();
        }

        startStopDbRecoveryOfCorruptedLogs();
        assertThat(logProvider).doesNotContainMessage("Fail to read transaction log version 0.");
    }

    @Test
    void failToRecoverFirstCorruptedTransactionSingleFileNoCheckpointIfFailOnCorruption() throws IOException {
        addCorruptedCommandsToLastLogFile(CorruptedLogEntryWriter::new);

        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        try {

            DatabaseStateService dbStateService =
                    db.getDependencyResolver().resolveDependency(DatabaseStateService.class);
            assertTrue(dbStateService.causeOfFailure(db.databaseId()).isPresent());
            assertThat(dbStateService.causeOfFailure(db.databaseId()).get())
                    .hasRootCauseInstanceOf(NegativeArraySizeException.class);
        } finally {
            managementService.shutdown();
        }
    }

    @Test
    void failToRecoverFirstCorruptedTransactionSingleFileNoCheckpointIfFailOnCorruptionVersion() throws IOException {
        addCorruptedCommandsToLastLogFile(CorruptedLogEntryVersionWriter::new);

        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        try {

            DatabaseStateService dbStateService =
                    db.getDependencyResolver().resolveDependency(DatabaseStateService.class);
            assertTrue(dbStateService.causeOfFailure(db.databaseId()).isPresent());
            assertThat(dbStateService.causeOfFailure(db.databaseId()).get())
                    .hasRootCauseInstanceOf(UnsupportedLogVersionException.class);
        } finally {
            managementService.shutdown();
        }
    }

    @ParameterizedTest(name = "[{index}] ({0})")
    @MethodSource("corruptedLogEntryWriters")
    void recoverNotAFirstCorruptedTransactionSingleFileNoCheckpoint(
            String testName, LogEntryWriterWrapper logEntryWriterWrapper) throws IOException {
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        logFiles = buildDefaultLogFiles(getStoreId(database));
        TransactionIdStore transactionIdStore = getTransactionIdStore(database);
        long lastClosedTransactionBeforeStart = transactionIdStore.getLastClosedTransactionId();
        long txSizes = 0;
        for (int i = 0; i < 10; i++) {
            txSizes += generateTransaction(database);
        }
        long numberOfTransactions = transactionIdStore.getLastClosedTransactionId() - lastClosedTransactionBeforeStart;
        managementService.shutdown();

        Path highestLogFile = logFiles.getLogFile().getHighestLogFile();
        long originalFileLength = getLastReadablePosition(highestLogFile).getByteOffset();
        removeLastCheckpointRecordFromLastLogFile();

        addCorruptedCommandsToLastLogFile(logEntryWriterWrapper);
        long modifiedFileLength = fileSystem.getFileSize(highestLogFile);

        assertThat(modifiedFileLength).isGreaterThan(originalFileLength);

        startStopDbRecoveryOfCorruptedLogs();

        assertThat(logProvider)
                .containsMessages(
                        "Fail to read transaction log version 0.",
                        "Recovery required from position LogPosition{logVersion=0, byteOffset=" + txOffsetAfterStart
                                + "}",
                        "Fail to recover database.",
                        "Any transactional logs after position LogPosition{logVersion=0, byteOffset="
                                + (txSizes + txOffsetAfterStart) + "} can not be recovered and will be truncated.");

        assertEquals(0, logFiles.getLogFile().getHighestLogVersion());
        assertEquals(numberOfTransactions, recoveryMonitor.getNumberOfRecoveredTransactions());
        assertEquals(originalFileLength, fileSystem.getFileSize(highestLogFile));
        // 2 shutdowns will create a checkpoint and recovery that will be triggered by removing tx logs for default db
        // during the setup and starting db as part of the test, plus checkpoints on creation
        assertEquals(
                LATEST_LOG_FORMAT.getHeaderSize() + 4 * CHECKPOINT_RECORD_SIZE,
                Files.size(logFiles.getCheckpointFile().getCurrentFile()));
    }

    @ParameterizedTest(name = "[{index}] ({0})")
    @MethodSource("corruptedLogEntryWriters")
    void recoverNotAFirstCorruptedTransactionMultipleFilesNoCheckpoints(
            String testName, LogEntryWriterWrapper logEntryWriterWrapper) throws IOException {
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        logFiles = buildDefaultLogFiles(getStoreId(database));
        TransactionIdStore transactionIdStore = getTransactionIdStore(database);
        long lastClosedTransactionBeforeStart = transactionIdStore.getLastClosedTransactionId();
        long txSize = generateTransactionsAndRotate(database, 3);
        long additionalTxSizes = 0;
        for (int i = 0; i < 7; i++) {
            additionalTxSizes += generateTransaction(database);
        }
        long numberOfTransactions = transactionIdStore.getLastClosedTransactionId() - lastClosedTransactionBeforeStart;
        managementService.shutdown();

        Path highestLogFile = logFiles.getLogFile().getHighestLogFile();
        long originalFileLength = getLastReadablePosition(highestLogFile).getByteOffset();
        removeLastCheckpointRecordFromLastLogFile();

        addCorruptedCommandsToLastLogFile(logEntryWriterWrapper);
        long modifiedFileLength = fileSystem.getFileSize(highestLogFile);

        assertThat(modifiedFileLength).isGreaterThan(originalFileLength);

        startStopDbRecoveryOfCorruptedLogs();

        assertThat(logProvider)
                .containsMessages(
                        "Fail to read transaction log version 3.",
                        "Recovery required from position LogPosition{logVersion=0, byteOffset=" + txOffsetAfterStart
                                + "}",
                        "Fail to recover database.",
                        "Any transactional logs after position LogPosition{logVersion=3, byteOffset="
                                + (txSize + additionalTxSizes + HEADER_OFFSET)
                                + "} can not be recovered and will be truncated.");

        assertEquals(3, logFiles.getLogFile().getHighestLogVersion());
        assertEquals(numberOfTransactions, recoveryMonitor.getNumberOfRecoveredTransactions());
        assertEquals(originalFileLength, fileSystem.getFileSize(highestLogFile));
        // 2 shutdowns will create a checkpoint and recovery that will be triggered by removing tx logs for default db
        // during the setup and starting db as part of the test
        assertEquals(
                LATEST_LOG_FORMAT.getHeaderSize() + 4 * CHECKPOINT_RECORD_SIZE,
                Files.size(logFiles.getCheckpointFile().getCurrentFile()));
    }

    @ParameterizedTest(name = "[{index}] ({0})")
    @MethodSource("corruptedLogEntryWriters")
    void recoverNotAFirstCorruptedTransactionMultipleFilesMultipleCheckpoints(
            String testName, LogEntryWriterWrapper logEntryWriterWrapper) throws IOException {
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        logFiles = buildDefaultLogFiles(getStoreId(database));
        long transactionsToRecover = 7;
        long txSize = generateTransactionsAndRotateWithCheckpoint(database, 3);
        long additionalTxSizes = 0;
        for (int i = 0; i < transactionsToRecover; i++) {
            additionalTxSizes += generateTransaction(database);
        }
        managementService.shutdown();

        Path highestLogFile = logFiles.getLogFile().getHighestLogFile();
        long originalFileLength = getLastReadablePosition(highestLogFile).getByteOffset();
        removeLastCheckpointRecordFromLastLogFile();

        addCorruptedCommandsToLastLogFile(logEntryWriterWrapper);
        long modifiedFileLength = fileSystem.getFileSize(highestLogFile);

        assertThat(modifiedFileLength).isGreaterThan(originalFileLength);

        startStopDbRecoveryOfCorruptedLogs();

        assertThat(logProvider)
                .containsMessages(
                        "Fail to read transaction log version 3.",
                        "Recovery required from position LogPosition{logVersion=3, byteOffset="
                                + (txSize + HEADER_OFFSET) + "}",
                        "Fail to recover database.",
                        "Any transactional logs after position LogPosition{logVersion=3, byteOffset="
                                + (txSize + additionalTxSizes + HEADER_OFFSET)
                                + "} can not be recovered and will be truncated.");

        assertEquals(3, logFiles.getLogFile().getHighestLogVersion());
        assertEquals(transactionsToRecover, recoveryMonitor.getNumberOfRecoveredTransactions());
        assertEquals(originalFileLength, fileSystem.getFileSize(highestLogFile));
        assertEquals(
                LATEST_LOG_FORMAT.getHeaderSize() + 7 * CHECKPOINT_RECORD_SIZE,
                Files.size(logFiles.getCheckpointFile().getCurrentFile()));
    }

    @ParameterizedTest(name = "[{index}] ({0})")
    @MethodSource("corruptedLogEntryWriters")
    void recoverFirstCorruptedTransactionAfterCheckpointInLastLogFile(
            String testName, LogEntryWriterWrapper logEntryWriterWrapper) throws IOException {
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        logFiles = buildDefaultLogFiles(getStoreId(database));
        long txSize = generateTransactionsAndRotate(database, 5);
        managementService.shutdown();

        Path highestLogFile = logFiles.getLogFile().getHighestLogFile();
        long originalFileLength = getLastReadablePosition(highestLogFile).getByteOffset();
        addCorruptedCommandsToLastLogFile(logEntryWriterWrapper);
        long modifiedFileLength = fileSystem.getFileSize(highestLogFile);

        assertThat(modifiedFileLength).isGreaterThan(originalFileLength);

        startStopDbRecoveryOfCorruptedLogs();

        assertThat(logProvider)
                .containsMessages(
                        "Fail to read transaction log version 5.",
                        "Fail to read first transaction of log version 5.",
                        "Recovery required from position LogPosition{logVersion=5, byteOffset="
                                + (txSize + HEADER_OFFSET) + "}",
                        "Fail to recover database. Any transactional logs after position LogPosition{logVersion=5, byteOffset="
                                + (txSize + HEADER_OFFSET) + "} can not be recovered and will be truncated.");

        assertEquals(5, logFiles.getLogFile().getHighestLogVersion());
        assertEquals(originalFileLength, fileSystem.getFileSize(highestLogFile));
        // 2 shutdowns will create a checkpoint and recovery that will be triggered by removing tx logs for default db
        // during the setup and starting db as part of the test
        if (NativeAccessProvider.getNativeAccess().isAvailable()) {
            assertEquals(
                    ByteUnit.mebiBytes(1),
                    Files.size(logFiles.getCheckpointFile().getCurrentFile()));
        } else {
            assertEquals(
                    LATEST_LOG_FORMAT.getHeaderSize() + 5 * CHECKPOINT_RECORD_SIZE,
                    Files.size(logFiles.getCheckpointFile().getCurrentFile()));
        }
    }

    @Test
    void repetitiveRecoveryOfCorruptedLogs() throws IOException {
        DatabaseManagementService service = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) service.database(DEFAULT_DATABASE_NAME);
        logFiles = buildDefaultLogFiles(getStoreId(database));
        generateTransactionsAndRotate(database, 4, false);
        service.shutdown();
        removeLastCheckpointRecordFromLastLogFile();

        int expectedRecoveredTransactions = 7;
        while (expectedRecoveredTransactions > 0) {
            truncateBytesFromLastLogFile(1 + random.nextInt(10));
            startStopDbRecoveryOfCorruptedLogs();
            int numberOfRecoveredTransactions = recoveryMonitor.getNumberOfRecoveredTransactions();
            assertEquals(expectedRecoveredTransactions, numberOfRecoveredTransactions);
            expectedRecoveredTransactions--;
            removeLastCheckpointRecordFromLastLogFile();
        }
    }

    private static StoreId getStoreId(GraphDatabaseAPI database) {
        return database.getDependencyResolver()
                .resolveDependency(StoreIdProvider.class)
                .getStoreId();
    }

    private static TransactionIdStore getTransactionIdStore(GraphDatabaseAPI database) {
        return database.getDependencyResolver().resolveDependency(TransactionIdStore.class);
    }

    private void removeLastCheckpointRecordFromLastLogFile() throws IOException {
        CheckpointFile checkpointFile = logFiles.getCheckpointFile();
        var checkpoint = checkpointFile.findLatestCheckpoint();
        if (checkpoint.isPresent()) {
            LogPosition logPosition = checkpoint.get().checkpointEntryPosition();
            try (StoreChannel storeChannel =
                    fileSystem.write(checkpointFile.getDetachedCheckpointFileForVersion(logPosition.getLogVersion()))) {
                storeChannel.truncate(logPosition.getByteOffset());
            }
        }
    }

    private void appendDataAfterLastCheckpointRecordFromLastLogFile() throws IOException {
        CheckpointFile checkpointFile = logFiles.getCheckpointFile();
        var checkpoint = checkpointFile.findLatestCheckpoint();
        if (checkpoint.isPresent()) {
            LogPosition logPosition = checkpoint.get().channelPositionAfterCheckpoint();
            try (StoreChannel storeChannel =
                    fileSystem.write(checkpointFile.getDetachedCheckpointFileForVersion(logPosition.getLogVersion()))) {
                storeChannel.position(logPosition.getByteOffset() + 300);
                storeChannel.writeAll(ByteBuffer.wrap("DeaD BeaF".getBytes()));
            }
        }
    }

    private void appendRandomBytesAfterLastCheckpointRecordFromLastLogFile(int bytesToAdd) throws IOException {
        CheckpointFile checkpointFile = logFiles.getCheckpointFile();
        var checkpoint = checkpointFile.findLatestCheckpoint();
        if (checkpoint.isPresent()) {
            LogPosition logPosition = checkpoint.get().channelPositionAfterCheckpoint();
            try (StoreChannel storeChannel =
                    fileSystem.write(checkpointFile.getDetachedCheckpointFileForVersion(logPosition.getLogVersion()))) {
                storeChannel.position(logPosition.getByteOffset());
                var array = new byte[bytesToAdd];
                do {
                    random.nextBytes(array);
                    // zero at the beginning marks end of records
                    array[0] = randomNonZeroByte();
                } while (!checkpointEntryLooksCorrupted(array));
                storeChannel.writeAll(ByteBuffer.wrap(array));
            }
        }
    }

    private boolean checkpointEntryLooksCorrupted(byte[] array) {
        var testReader = new VersionAwareLogEntryReader(version -> null, LatestVersions.BINARY_VERSIONS);
        var ch = new InMemoryVersionableReadableClosablePositionAwareChannel();
        for (byte b : array) {
            ch.put(b);
        }
        try {
            testReader.readLogEntry(ch);
            return false;
        } catch (Exception e) {
            return true;
        }
    }

    private void truncateBytesFromLastCheckpointLogFile(long bytesToTrim) throws IOException {
        CheckpointFile checkpointFile = logFiles.getCheckpointFile();
        var checkpoint = checkpointFile.findLatestCheckpoint();
        if (checkpoint.isPresent()) {
            LogPosition logPosition = checkpoint.get().channelPositionAfterCheckpoint();
            try (StoreChannel storeChannel =
                    fileSystem.write(checkpointFile.getDetachedCheckpointFileForVersion(logPosition.getLogVersion()))) {
                storeChannel.truncate(logPosition.getByteOffset() - bytesToTrim);
            }
        }
    }

    private void truncateBytesFromLastLogFile(long bytesToTrim) throws IOException {
        if (logFiles.getLogFile().getHighestLogVersion() > 0) {
            Path highestLogFile = logFiles.getLogFile().getHighestLogFile();
            long readableOffset = getLastReadablePosition(highestLogFile).getByteOffset();
            if (bytesToTrim > readableOffset) {
                fileSystem.deleteFile(highestLogFile);
                if (logFiles.logFiles().length > 0) {
                    truncateBytesFromLastLogFile(bytesToTrim); // start truncating from next file
                }
            } else {
                fileSystem.truncate(highestLogFile, readableOffset - bytesToTrim);
            }
        }
    }

    private void writeRandomBytesAfterLastCommandInLastLogFile(Supplier<ByteBuffer> source) throws IOException {
        int someRandomPaddingAfterEndOfDataInLogFile = random.nextInt(1, 10);
        try (Lifespan lifespan = new Lifespan()) {
            LogFile transactionLogFile = logFiles.getLogFile();
            lifespan.add(logFiles);

            LogPosition position = getLastReadablePosition(transactionLogFile);

            try (StoreFileChannel writeChannel =
                    fileSystem.write(logFiles.getLogFile().getHighestLogFile())) {
                writeChannel.position(position.getByteOffset() + someRandomPaddingAfterEndOfDataInLogFile);
                for (int i = 0; i < 10; i++) {
                    writeChannel.writeAll(source.get());
                }
            }
        }
    }

    private LogPosition getLastReadablePosition(Path logFile) throws IOException {
        VersionAwareLogEntryReader entryReader = new VersionAwareLogEntryReader(
                storageEngineFactory.commandReaderFactory(), LatestVersions.BINARY_VERSIONS);
        LogFile txLogFile = logFiles.getLogFile();
        long logVersion = txLogFile.getLogVersion(logFile);
        LogPosition startPosition = txLogFile.extractHeader(logVersion).getStartPosition();
        try (ReadableLogChannel reader = openTransactionFileChannel(logVersion, startPosition)) {
            while (entryReader.readLogEntry(reader) != null) {
                // scroll to the end of readable entries
            }
        } catch (IncompleteLogHeaderException e) {
            return new LogPosition(logVersion, 0);
        }
        return entryReader.lastPosition();
    }

    private ReadAheadLogChannel openTransactionFileChannel(long logVersion, LogPosition startPosition)
            throws IOException {
        PhysicalLogVersionedStoreChannel storeChannel = logFiles.getLogFile().openForVersion(logVersion);
        storeChannel.position(startPosition.getByteOffset());
        return new ReadAheadLogChannel(storeChannel, EmptyMemoryTracker.INSTANCE);
    }

    private LogPosition getLastReadablePosition(LogFile logFile) throws IOException {
        VersionAwareLogEntryReader entryReader = new VersionAwareLogEntryReader(
                storageEngineFactory.commandReaderFactory(), LatestVersions.BINARY_VERSIONS);
        LogPosition startPosition = logFile.extractHeader(logFiles.getLogFile().getHighestLogVersion())
                .getStartPosition();
        try (ReadableLogChannel reader = logFile.getReader(startPosition)) {
            while (entryReader.readLogEntry(reader) != null) {
                // scroll to the end of readable entries
            }
        }
        return entryReader.lastPosition();
    }

    private void addRandomBytesToLastLogFile(Supplier<Byte> byteSource) throws IOException {
        try (Lifespan lifespan = new Lifespan()) {
            LogFile transactionLogFile = logFiles.getLogFile();
            lifespan.add(logFiles);

            var channel = transactionLogFile.getTransactionLogWriter().getChannel();
            for (int i = 0; i < 10; i++) {
                channel.put(byteSource.get());
            }
        }
    }

    private byte randomInvalidVersionsBytes() {
        int highestVersionByte = KernelVersion.VERSIONS.stream()
                .filter(version -> version != KernelVersion.GLORIOUS_FUTURE)
                .mapToInt(KernelVersion::version)
                .max()
                .orElseThrow();
        return (byte) random.nextInt(highestVersionByte + 1, Byte.MAX_VALUE);
    }

    /**
     * Used when appending extra randomness at the end of tx log.
     * Use non-zero bytes, randomly generated zero can be treated as "0" kernel version, marking end-of-records in pre-allocated tx log file.
     */
    private byte randomNonZeroByte() {
        var b = (byte) random.nextInt(Byte.MIN_VALUE, Byte.MAX_VALUE);
        if (b != 0) {
            return b;
        }
        return Byte.MAX_VALUE;
    }

    private void addCorruptedCommandsToLastLogFile(LogEntryWriterWrapper logEntryWriterWrapper) throws IOException {
        var versionRepository = new SimpleLogVersionRepository(getInitialVersion(logFiles), 0);
        LogFiles internalLogFiles = LogFilesBuilder.builder(databaseLayout, fileSystem, LATEST_KERNEL_VERSION_PROVIDER)
                .withLogVersionRepository(versionRepository)
                .withTransactionIdStore(new SimpleTransactionIdStore())
                .withStoreId(new StoreId(4, 5, "engine-1", "format-1", 1, 2))
                .withStorageEngineFactory(StorageEngineFactory.selectStorageEngine(Config.defaults()))
                .build();
        try (Lifespan lifespan = new Lifespan(internalLogFiles)) {
            LogFile transactionLogFile = internalLogFiles.getLogFile();
            LogEntryWriter<FlushableLogPositionAwareChannel> realLogEntryWriter =
                    transactionLogFile.getTransactionLogWriter().getWriter();
            LogEntryWriter<FlushableLogPositionAwareChannel> wrappedLogEntryWriter =
                    logEntryWriterWrapper.wrap(realLogEntryWriter);
            TransactionLogWriter writer = new TransactionLogWriter(
                    realLogEntryWriter.getChannel(), wrappedLogEntryWriter, LATEST_KERNEL_VERSION_PROVIDER);
            List<StorageCommand> commands = new ArrayList<>();
            commands.add(new Command.PropertyCommand(
                    LATEST_LOG_SERIALIZATION, new PropertyRecord(1), new PropertyRecord(2)));
            commands.add(new Command.NodeCommand(LATEST_LOG_SERIALIZATION, new NodeRecord(2), new NodeRecord(3)));
            CompleteTransaction transaction = new CompleteTransaction(
                    commands, UNKNOWN_CONSENSUS_INDEX, 0, 0, 0, 0, LatestVersions.LATEST_KERNEL_VERSION, ANONYMOUS);
            writer.append(transaction, 1000, NOT_SPECIFIED_CHUNK_ID, BASE_TX_CHECKSUM, LogPosition.UNSPECIFIED);
        }
    }

    private static long getInitialVersion(LogFiles logFiles) {
        return logFiles == null ? 0 : logFiles.getLogFile().getHighestLogVersion();
    }

    private static long getLastClosedTransactionOffset(GraphDatabaseAPI database) {
        return getLastClosedTransaction(database).getByteOffset();
    }

    private static LogPosition getLastClosedTransaction(GraphDatabaseAPI database) {
        MetadataProvider metaDataStore = database.getDependencyResolver().resolveDependency(MetadataProvider.class);
        return metaDataStore.getLastClosedTransaction().logPosition();
    }

    private LogFiles buildDefaultLogFiles(StoreId storeId) throws IOException {
        return LogFilesBuilder.builder(databaseLayout, fileSystem, LATEST_KERNEL_VERSION_PROVIDER)
                .withLogVersionRepository(new SimpleLogVersionRepository())
                .withTransactionIdStore(new SimpleTransactionIdStore())
                .withStoreId(storeId)
                .withLogProvider(logProvider)
                .withStorageEngineFactory(StorageEngineFactory.selectStorageEngine(Config.defaults()))
                .build();
    }

    private static long generateTransactionsAndRotateWithCheckpoint(GraphDatabaseAPI database, int logFilesToGenerate)
            throws IOException {
        return generateTransactionsAndRotate(database, logFilesToGenerate, true);
    }

    private static long generateTransactionsAndRotate(GraphDatabaseAPI database, int logFilesToGenerate)
            throws IOException {
        return generateTransactionsAndRotate(database, logFilesToGenerate, false);
    }

    private static long generateTransactionsAndRotate(
            GraphDatabaseAPI database, int logFilesToGenerate, boolean checkpoint) throws IOException {
        DependencyResolver resolver = database.getDependencyResolver();
        LogFiles logFiles = resolver.resolveDependency(LogFiles.class);
        CheckPointer checkpointer = resolver.resolveDependency(CheckPointer.class);
        long lastTxSize = -1;
        while (logFiles.getLogFile().getHighestLogVersion() < logFilesToGenerate) {
            logFiles.getLogFile().rotate();
            lastTxSize = generateTransaction(database);
            if (checkpoint) {
                checkpointer.forceCheckPoint(new SimpleTriggerInfo("testForcedCheckpoint"));
            }
        }
        return lastTxSize;
    }

    private static long generateTransaction(GraphDatabaseAPI database) {
        LogPosition lastTx = getLastClosedTransaction(database);
        LogFiles logFiles = database.getDependencyResolver().resolveDependency(LogFiles.class);
        long initialOffset = logFiles.getLogFile().getCurrentLogVersion() > lastTx.getLogVersion()
                ? HEADER_OFFSET
                : lastTx.getByteOffset();

        try (Transaction transaction = database.beginTx()) {
            Node startNode = transaction.createNode(Label.label("startNode"));
            startNode.setProperty("key", "value");
            Node endNode = transaction.createNode(Label.label("endNode"));
            endNode.setProperty("key", "value");
            startNode.createRelationshipTo(endNode, RelationshipType.withName("connects"));
            transaction.commit();
        }
        return getLastClosedTransactionOffset(database) - initialOffset;
    }

    private void startStopDbRecoveryOfCorruptedLogs() {
        DatabaseManagementService managementService =
                databaseFactory.setConfig(fail_on_corrupted_log_files, false).build();
        managementService.shutdown();
    }

    private void startStopDatabase() {
        DatabaseManagementService managementService = databaseFactory.build();
        storageEngineFactory = ((GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME))
                .getDependencyResolver()
                .resolveDependency(StorageEngineFactory.class);
        managementService.shutdown();
    }

    private long startStopDatabaseAndGetTxOffset() {
        DatabaseManagementService managementService = databaseFactory.build();
        final GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        storageEngineFactory = database.getDependencyResolver().resolveDependency(StorageEngineFactory.class);
        long offset = getLastClosedTransactionOffset(database);
        managementService.shutdown();
        return offset;
    }

    private void removeDatabaseDirectories() throws IOException {
        fileSystem.delete(databaseLayout.databaseDirectory());
        fileSystem.delete(databaseLayout.getTransactionLogsDirectory());
    }

    private StandaloneDatabaseContext getDefaultDbContext(DatabaseManagementService dbms) {
        return (StandaloneDatabaseContext) ((GraphDatabaseAPI) dbms.database(SYSTEM_DATABASE_NAME))
                .getDependencyResolver()
                .resolveDependency(DatabaseContextProvider.class)
                .getDatabaseContext(DEFAULT_DATABASE_NAME)
                .orElseThrow();
    }

    private static Stream<Arguments> corruptedLogEntryWriters() {
        return Stream.of(
                Arguments.of("CorruptedLogEntryWriter", (LogEntryWriterWrapper) CorruptedLogEntryWriter::new),
                Arguments.of(
                        "CorruptedLogEntryVersionWriter", (LogEntryWriterWrapper) CorruptedLogEntryVersionWriter::new));
    }

    @FunctionalInterface
    private interface LogEntryWriterWrapper {
        default <T extends WritableChannel> LogEntryWriter<T> wrap(LogEntryWriter<T> logEntryWriter) {
            return to(logEntryWriter.getChannel());
        }

        <T extends WritableChannel> LogEntryWriter<T> to(T channel);
    }

    private static class CorruptedLogEntryWriter<T extends WritableChannel> extends LogEntryWriter<T> {
        CorruptedLogEntryWriter(T channel) {
            super(channel, LatestVersions.BINARY_VERSIONS);
        }

        @Override
        public void writeStartEntry(
                KernelVersion version,
                long timeWritten,
                long latestCommittedTxWhenStarted,
                int previousChecksum,
                byte[] additionalHeaderData)
                throws IOException {
            channel.put(version.version()).put(TX_START);
        }
    }

    private static class CorruptedLogEntryVersionWriter<T extends WritableChannel> extends LogEntryWriter<T> {
        CorruptedLogEntryVersionWriter(T channel) {
            super(channel, LatestVersions.BINARY_VERSIONS);
        }

        /**
         * Use a non-existing log entry version. Implementation stolen from {@link LogEntryWriter#writeStartEntry(KernelVersion, long, long, int, byte[])}.
         */
        @Override
        public void writeStartEntry(
                KernelVersion version,
                long timeWritten,
                long latestCommittedTxWhenStarted,
                int previousChecksum,
                byte[] additionalHeaderData)
                throws IOException {
            byte nonExistingLogEntryVersion = (byte) (LatestVersions.LATEST_KERNEL_VERSION.version() + 10);
            channel.put(nonExistingLogEntryVersion).put(TX_START);
            channel.putLong(timeWritten)
                    .putLong(latestCommittedTxWhenStarted)
                    .putInt(previousChecksum)
                    .putInt(additionalHeaderData.length)
                    .put(additionalHeaderData, additionalHeaderData.length);
        }
    }

    private static class RecoveryMonitor implements org.neo4j.kernel.recovery.RecoveryMonitor {
        private final List<Long> recoveredBatches = new ArrayList<>();
        private int numberOfRecoveredTransactions;
        private final AtomicBoolean recoveryRequired = new AtomicBoolean();

        @Override
        public void recoveryRequired(LogPosition recoveryPosition) {
            recoveryRequired.set(true);
            numberOfRecoveredTransactions = 0;
        }

        @Override
        public void batchRecovered(CommittedCommandBatch committedBatch) {
            recoveredBatches.add(committedBatch.txId());
            if (committedBatch.commandBatch().isLast()) {
                numberOfRecoveredTransactions++;
            }
        }

        boolean wasRecoveryRequired() {
            return recoveryRequired.get();
        }

        int getNumberOfRecoveredTransactions() {
            return numberOfRecoveredTransactions;
        }
    }

    private static class CorruptedCheckpointMonitor implements LogTailScannerMonitor {
        private final AtomicInteger corruptedFileCounter = new AtomicInteger();

        @Override
        public void corruptedLogFile(long version, Throwable t) {}

        @Override
        public void corruptedCheckpointFile(long version, Throwable t) {
            corruptedFileCounter.incrementAndGet();
        }

        int getNumberOfCorruptedCheckpointFiles() {
            return corruptedFileCounter.get();
        }
    }

    private static class BytesCaptureSupplier implements Supplier<Byte> {
        private final Supplier<Byte> generator;
        private final List<Byte> capturedBytes = new ArrayList<>();

        BytesCaptureSupplier(Supplier<Byte> generator) {
            this.generator = generator;
        }

        @Override
        public Byte get() {
            Byte data = generator.get();
            capturedBytes.add(data);
            return data;
        }

        public List<Byte> getCapturedBytes() {
            return capturedBytes;
        }
    }
}
