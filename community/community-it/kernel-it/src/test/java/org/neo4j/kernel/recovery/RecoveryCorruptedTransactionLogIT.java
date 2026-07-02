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
import static org.neo4j.configuration.GraphDatabaseInternalSettings.ignore_corrupt_schema;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.io.pagecache.PageCache.PAGE_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.TX_COMMIT;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.TX_START;
import static org.neo4j.logging.LogAssertions.assertThat;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.AppendIndexProvider.UNKNOWN_APPEND_INDEX;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CHUNK_ID;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_TRANSACTION_ID;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION_WITHOUT_ENVELOPES;
import static org.neo4j.test.LatestVersions.LATEST_LOG_FORMAT;
import static org.neo4j.test.LatestVersions.LATEST_RUNTIME_VERSION_WITHOUT_ENVELOPES;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.dbms.database.StandaloneDatabaseContext;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.nativeimpl.NativeAccessProvider;
import org.neo4j.internal.recordstorage.Command;
import org.neo4j.internal.recordstorage.LogCommandSerialization;
import org.neo4j.internal.recordstorage.RecordStorageCommandReaderFactory;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemUtils;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.fs.StoreFileChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.layout.CommonDatabaseStores;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.kernel.BinarySupportedKernelVersions;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatchRepresentation;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.CompleteCommandBatch;
import org.neo4j.kernel.impl.transaction.log.FlushableLogPositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.InMemoryVersionableReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.ReadAheadUtils;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerImpl;
import org.neo4j.kernel.impl.transaction.log.checkpoint.DetachedCheckpointAppender;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.entry.BadLogEntryException;
import org.neo4j.kernel.impl.transaction.log.entry.IncompleteLogHeaderException;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.impl.transaction.log.entry.UnsupportedLogVersionException;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.v522.DetachedCheckpointLogEntrySerializerV5_22;
import org.neo4j.kernel.impl.transaction.log.enveloped.InvalidLogEnvelopeReadException;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.kernel.impl.transaction.log.files.checkpoint.CheckpointFile;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.Leases;
import org.neo4j.storageengine.api.LogMetadataProvider;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreIdProvider;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.RandomSupportExtension;
import org.neo4j.test.extension.SkipOnSpd;

@SkipOnSpd(
        reason = "Assertions are quite specific tx log characteristic of a single db and starting db on "
                + "failed recovery throws explicit exception in SPD setup, not in single-db setup")
@Neo4jLayoutExtension
@RandomSupportExtension
class RecoveryCorruptedTransactionLogIT {
    private int CHECKPOINT_RECORD_SIZE;
    private LogCommandSerialization LATEST_LOG_SERIALIZATION;
    private BinarySupportedKernelVersions BINARY_VERSIONS;
    private Config config;

    @Inject
    DefaultFileSystemAbstraction fileSystem;

    @Inject
    private DatabaseLayout databaseLayout;

    @Inject
    private RandomSupport random;

    private static final int HEADER_OFFSET = LATEST_LOG_FORMAT.getHeaderSize();
    final AssertableLogProvider logProvider = new AssertableLogProvider(true);
    final RecoveryMonitor recoveryMonitor = new RecoveryMonitor();
    private final CorruptedCheckpointMonitor corruptedFilesMonitor = new CorruptedCheckpointMonitor();
    private final Monitors monitors = new Monitors();
    LogFiles logFiles;
    TestDatabaseManagementServiceBuilder databaseFactory;
    private StorageEngineFactory storageEngineFactory;
    // Some transactions can have been run on start-up, so this is the offset the first transaction of a test will have.
    private long txOffsetAfterStart;

    @BeforeEach
    void setUp() {
        CHECKPOINT_RECORD_SIZE = checkpointRecordSize();
        LATEST_LOG_SERIALIZATION = RecordStorageCommandReaderFactory.INSTANCE.get(kernelVersion());
        config = Config.defaults(additionalConfig());
        BINARY_VERSIONS = new BinarySupportedKernelVersions(config);

        monitors.addMonitorListener(recoveryMonitor);
        monitors.addMonitorListener(corruptedFilesMonitor);
        databaseFactory = new TestDatabaseManagementServiceBuilder(databaseLayout)
                .setConfig(additionalConfig())
                .setConfig(checkpoint_logical_log_keep_threshold, 25)
                .setInternalLogProvider(logProvider)
                .setMonitors(monitors)
                .setFileSystem(fileSystem);

        txOffsetAfterStart = startStopDatabaseAndGetTxOffset();
    }

    protected int checkpointRecordSize() {
        return DetachedCheckpointLogEntrySerializerV5_22.checkPointRecordSizeDependingOnVersion(false);
    }

    protected KernelVersion kernelVersion() {
        return LATEST_KERNEL_VERSION_WITHOUT_ENVELOPES;
    }

    protected Map<Setting<?>, Object> additionalConfig() {
        return Map.of(
                GraphDatabaseInternalSettings.latest_kernel_version,
                LATEST_KERNEL_VERSION_WITHOUT_ENVELOPES.version(),
                GraphDatabaseInternalSettings.latest_runtime_version,
                LATEST_RUNTIME_VERSION_WITHOUT_ENVELOPES.getVersion(),
                GraphDatabaseInternalSettings.allow_new_log_format_on_upgrade_or_create,
                false);
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
                    transactionIdStore.getHighestGapFreeClosedTransaction().logPosition();
            long lastClosedTransactionBeforeStart = transactionIdStore.getHighestGapFreeClosedTransactionId();
            for (int i = 0; i < 10; i++) {
                generateTransaction(database);
            }
            long numberOfClosedTransactions = getTransactionIdStore(database).getHighestGapFreeClosedTransactionId()
                    - lastClosedTransactionBeforeStart;

            DependencyResolver dependencyResolver = database.getDependencyResolver();
            var databaseCheckpointer = dependencyResolver
                    .resolveDependency(TransactionLogFiles.class)
                    .getCheckpointFile();
            databaseCheckpointer
                    .getCheckpointAppender()
                    .checkPoint(
                            LogCheckPointEvent.NULL,
                            transactionIdStore.getLastCommittedTransaction(),
                            transactionIdStore.getLastCommittedTransaction().id() + 1,
                            kernelVersion(),
                            logOffsetBeforeTestTransactions,
                            logOffsetBeforeTestTransactions,
                            Instant.now(),
                            "Fallback checkpoint.");
            managementService.shutdown();

            truncateBytesFromLastCheckpointLogFile(bytesToTrim);
            startStopDbRecoveryOfCorruptedLogs();

            assertEquals(numberOfClosedTransactions, recoveryMonitor.getNumberOfRecoveredTransactions());
            assertThat(logFiles.getCheckpointFile().getMatchedFiles()).hasSize(1);
            assertEquals(0, corruptedFilesMonitor.getNumberOfCorruptedCheckpointFiles());

            removeDatabaseDirectories();
        }
    }

    protected int minimumBytesToConsiderARecordBroken() {
        return 3;
    }

    @Test
    void recoverFromLastCorruptedBrokenCheckpointRecord() throws IOException {
        for (int iteration = 0; iteration < 10; iteration++) {
            int bytesToAdd = random.nextInt(minimumBytesToConsiderARecordBroken(), CHECKPOINT_RECORD_SIZE + 1);

            DatabaseManagementService managementService = databaseFactory.build();
            GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
            logFiles = buildDefaultLogFiles(getStoreId(database));

            TransactionIdStore transactionIdStore = getTransactionIdStore(database);
            LogPosition logOffsetBeforeTestTransactions =
                    transactionIdStore.getHighestGapFreeClosedTransaction().logPosition();
            long lastClosedTransactionBeforeStart = transactionIdStore.getHighestGapFreeClosedTransactionId();
            for (int i = 0; i < 10; i++) {
                generateTransaction(database);
            }
            long numberOfClosedTransactions =
                    transactionIdStore.getHighestGapFreeClosedTransactionId() - lastClosedTransactionBeforeStart;

            DependencyResolver dependencyResolver = database.getDependencyResolver();
            var databaseCheckpointer = dependencyResolver
                    .resolveDependency(TransactionLogFiles.class)
                    .getCheckpointFile();
            databaseCheckpointer
                    .getCheckpointAppender()
                    .checkPoint(
                            LogCheckPointEvent.NULL,
                            transactionIdStore.getLastCommittedTransaction(),
                            transactionIdStore.getLastCommittedTransaction().id() + 1,
                            kernelVersion(),
                            logOffsetBeforeTestTransactions,
                            logOffsetBeforeTestTransactions,
                            Instant.now(),
                            "Fallback checkpoint.");
            managementService.shutdown();

            removeLastCheckpointRecordFromLastLogFile();
            appendRandomBytesAfterLastCheckpointRecordFromLastLogFile(bytesToAdd);
            startStopDbRecoveryOfCorruptedLogs();

            assertEquals(numberOfClosedTransactions, recoveryMonitor.getNumberOfRecoveredTransactions());
            assertEquals(iteration + 1, corruptedFilesMonitor.getNumberOfCorruptedCheckpointFiles());

            assertThat(logFiles.getCheckpointFile().getMatchedFiles()).hasSize(1);

            removeDatabaseDirectories();
        }
    }

    @Test
    void recoverFromLastCorruptedBrokenCheckpointRecordButNotReachingEndOfFile() throws IOException {
        long numberOfClosedTransactions;
        try (DatabaseManagementService managementService = databaseFactory.build()) {
            GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
            logFiles = buildDefaultLogFiles(getStoreId(database));

            TransactionIdStore transactionIdStore = getTransactionIdStore(database);
            LogPosition logOffsetBeforeTestTransactions =
                    transactionIdStore.getHighestGapFreeClosedTransaction().logPosition();
            long lastClosedTransactionBeforeStart = transactionIdStore.getHighestGapFreeClosedTransactionId();
            for (int i = 0; i < 10; i++) {
                generateTransaction(database);
            }
            numberOfClosedTransactions =
                    transactionIdStore.getHighestGapFreeClosedTransactionId() - lastClosedTransactionBeforeStart;

            DependencyResolver dependencyResolver = database.getDependencyResolver();
            var databaseCheckpointer = dependencyResolver
                    .resolveDependency(TransactionLogFiles.class)
                    .getCheckpointFile();
            databaseCheckpointer
                    .getCheckpointAppender()
                    .checkPoint(
                            LogCheckPointEvent.NULL,
                            transactionIdStore.getLastCommittedTransaction(),
                            transactionIdStore.getLastCommittedTransaction().id() + 1,
                            kernelVersion(),
                            logOffsetBeforeTestTransactions,
                            logOffsetBeforeTestTransactions,
                            Instant.now(),
                            "Fallback checkpoint.");
        }

        replacePartOfCheckpointWithZeroes();
        startStopDbRecoveryOfCorruptedLogs();

        assertEquals(numberOfClosedTransactions, recoveryMonitor.getNumberOfRecoveredTransactions());
        assertEquals(0, corruptedFilesMonitor.getNumberOfCorruptedCheckpointFiles());

        assertThat(logFiles.getCheckpointFile().getMatchedFiles()).hasSize(1);

        removeDatabaseDirectories();
    }

    @Test
    void recoverWithNoValidCheckpoints() throws IOException {
        long numberOfClosedTransactions;
        try (DatabaseManagementService managementService = databaseFactory.build()) {
            GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
            logFiles = buildDefaultLogFiles(getStoreId(database));

            TransactionIdStore transactionIdStore = getTransactionIdStore(database);
            for (int i = 0; i < 10; i++) {
                generateTransaction(database);
            }
            numberOfClosedTransactions =
                    transactionIdStore.getHighestGapFreeClosedTransactionId() - TransactionIdStore.BASE_TX_ID;
        }

        replacePartOfFirstCheckpointAndRestOfFileWithZeroes();
        startStopDbRecoveryOfCorruptedLogs();

        assertEquals(numberOfClosedTransactions, recoveryMonitor.getNumberOfRecoveredTransactions());
        assertEquals(0, corruptedFilesMonitor.getNumberOfCorruptedCheckpointFiles());

        assertThat(logFiles.getCheckpointFile().getMatchedFiles()).hasSize(1);
        assertThat(logFiles.getCheckpointFile().reachableCheckpoints()).hasSize(2); // Recovery completed and shutdown

        removeDatabaseDirectories();
    }

    @Test
    void recoverWithNoCheckpointFile() throws IOException {
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        logFiles = buildDefaultLogFiles(getStoreId(database));

        TransactionIdStore transactionIdStore = getTransactionIdStore(database);
        for (int i = 0; i < 10; i++) {
            generateTransaction(database);
        }
        long numberOfClosedTransactions =
                transactionIdStore.getHighestGapFreeClosedTransactionId() - TransactionIdStore.BASE_TX_ID;
        managementService.shutdown();

        fileSystem.deleteFile(logFiles.getCheckpointFile().getCurrentFile());
        startStopDbRecoveryOfCorruptedLogs();

        assertEquals(numberOfClosedTransactions, recoveryMonitor.getNumberOfRecoveredTransactions());
        assertEquals(0, corruptedFilesMonitor.getNumberOfCorruptedCheckpointFiles());

        assertThat(logFiles.getCheckpointFile().getMatchedFiles()).hasSize(1);
        assertThat(logFiles.getCheckpointFile().reachableCheckpoints()).hasSize(2); // Recovery completed and shutdown

        removeDatabaseDirectories();
    }

    @Test
    void recoverWithSingleCheckpointFileWithBrokenHeader() throws IOException {
        // This only works if the header is incomplete, for a corrupted header it would throw
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        logFiles = buildDefaultLogFiles(getStoreId(database));

        TransactionIdStore transactionIdStore = getTransactionIdStore(database);
        for (int i = 0; i < 10; i++) {
            generateTransaction(database);
        }
        long numberOfClosedTransactions =
                transactionIdStore.getHighestGapFreeClosedTransactionId() - TransactionIdStore.BASE_TX_ID;
        managementService.shutdown();

        Path currentCheckpointFile = logFiles.getCheckpointFile().getLogFileForVersion(0);
        fileSystem.truncate(currentCheckpointFile, 12);

        startStopDbRecoveryOfCorruptedLogs();

        assertEquals(numberOfClosedTransactions, recoveryMonitor.getNumberOfRecoveredTransactions());
        assertEquals(0, corruptedFilesMonitor.getNumberOfCorruptedCheckpointFiles());

        assertThat(logFiles.getCheckpointFile().getMatchedFiles()).hasSize(1);
        assertThat(logFiles.getCheckpointFile().reachableCheckpoints()).hasSize(2); // Recovery completed and shutdown

        removeDatabaseDirectories();
    }

    @Test
    void doNotRotateIfRecoveryIsRequiredButThereAreNoUnreadableData() throws IOException {
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        logFiles = buildDefaultLogFiles(getStoreId(database));

        TransactionIdStore transactionIdStore = getTransactionIdStore(database);
        LogPosition logOffsetBeforeTestTransactions =
                transactionIdStore.getHighestGapFreeClosedTransaction().logPosition();
        long lastClosedTransactionBeforeStart = transactionIdStore.getHighestGapFreeClosedTransactionId();
        for (int i = 0; i < 10; i++) {
            generateTransaction(database);
        }
        long numberOfClosedTransactions =
                transactionIdStore.getHighestGapFreeClosedTransactionId() - lastClosedTransactionBeforeStart;

        DependencyResolver dependencyResolver = database.getDependencyResolver();
        var databaseCheckpointer =
                dependencyResolver.resolveDependency(TransactionLogFiles.class).getCheckpointFile();
        databaseCheckpointer
                .getCheckpointAppender()
                .checkPoint(
                        LogCheckPointEvent.NULL,
                        transactionIdStore.getLastCommittedTransaction(),
                        transactionIdStore.getLastCommittedTransaction().id() + 7,
                        kernelVersion(),
                        logOffsetBeforeTestTransactions,
                        logOffsetBeforeTestTransactions,
                        Instant.now(),
                        "Fallback checkpoint.");
        managementService.shutdown();

        removeLastCheckpointRecordFromLastLogFile();
        startStopDatabase();

        assertEquals(numberOfClosedTransactions, recoveryMonitor.getNumberOfRecoveredTransactions());
        assertEquals(0, corruptedFilesMonitor.getNumberOfCorruptedCheckpointFiles());

        assertThat(logFiles.getCheckpointFile().getMatchedFiles()).hasSize(1);
    }

    @Test
    void evenTruncateNewerTransactionLogFile() throws IOException {
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        logFiles = buildDefaultLogFiles(getStoreId(database));
        TransactionIdStore transactionIdStore = getTransactionIdStore(database);
        long lastClosedTransactionBeforeStart = transactionIdStore.getHighestGapFreeClosedTransactionId();
        for (int i = 0; i < 10; i++) {
            generateTransaction(database);
        }
        long numberOfClosedTransactions = getTransactionIdStore(database).getHighestGapFreeClosedTransactionId()
                - lastClosedTransactionBeforeStart;
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
        addRandomBytesToLastLogFile(
                this::randomInvalidVersionsBytes, Math.max(minimumBytesToConsiderARecordBroken(), 10));

        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        try {
            DatabaseStateService<?> dbStateService =
                    db.getDependencyResolver().resolveDependency(DatabaseStateService.class);
            assertTrue(dbStateService.causeOfFailure(db.databaseId()).isPresent());
            assertThat(dbStateService.causeOfFailure(db.databaseId()).get())
                    .rootCause()
                    .isInstanceOfAny(UnsupportedLogVersionException.class, InvalidLogEnvelopeReadException.class);
        } finally {
            managementService.shutdown();
        }
    }

    @Test
    void truncateNewerTransactionLogFileWhenForced() throws IOException {
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        TransactionIdStore transactionIdStore = getTransactionIdStore(database);
        long numberOfClosedTransactionsAfterStartup = transactionIdStore.getHighestGapFreeClosedTransactionId();
        logFiles = buildDefaultLogFiles(getStoreId(database));
        long lastTxSize = 0;
        long totalTxSize = 0;
        for (int i = 0; i < 10; i++) {
            long size = generateTransaction(database);
            lastTxSize = size;
            totalTxSize += size;
        }
        long numberOfTransactionsToRecover =
                transactionIdStore.getHighestGapFreeClosedTransactionId() - numberOfClosedTransactionsAfterStartup;
        managementService.shutdown();

        removeLastCheckpointRecordFromLastLogFile();
        Supplier<Byte> randomBytesSupplier = this::randomInvalidVersionsBytes;
        BytesCaptureSupplier capturingSupplier = new BytesCaptureSupplier(randomBytesSupplier);
        addRandomBytesToLastLogFile(capturingSupplier, Math.max(minimumBytesToConsiderARecordBroken(), 10));
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
                        "Recovery required from position LogPosition{logVersion=0, byteOffset=" + txOffsetAfterStart
                                + "}",
                        "Fail to recover database. Any transactional logs after position LogPosition{logVersion=0, "
                                + "byteOffset=" + txOffsetAfterStart + "} can not be recovered and will be truncated.");

        logFiles = buildDefaultLogFiles(new StoreId(4, 5, "engine-1", "format-1", 1, 2));
        assertEquals(0, logFiles.getLogFile().getLogRangeInfo().highestVersion());
        if (NativeAccessProvider.getNativeAccess().isAvailable()) {
            assertEquals(
                    ByteUnit.mebiBytes(1),
                    Files.size(logFiles.getCheckpointFile().getCurrentFile()));
        } else {
            assertEquals(
                    getLastFileStartPosition(logFiles.getLogFile()).getByteOffset()
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

        writeRandomBytesAfterLastCommandInLastLogFile(() -> ByteBuffer.wrap(new byte[] {1, 2, 3, 4, 6}));

        startStopDatabase();
        assertThat(logProvider)
                .assertExceptionForLogMessage("Fail to read transaction log version 0.")
                .message()
                .containsAnyOf(
                        "Failure to read transaction log file number 0. Unreadable bytes are encountered after last readable position.",
                        "Invalid envelope type: 6");
    }

    @Test
    void startWithTransactionLogsWithDataAfterLastEntryAndCorruptedLogsRecoveryEnabled() throws IOException {
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        long txSize = generateTransaction(database);
        long initialTransactionOffset = txOffsetAfterStart + txSize;
        assertEquals(initialTransactionOffset, getLastClosedTransactionOffset(database));

        logFiles = buildDefaultLogFiles(database);
        final var transactionLogFile = logFiles.getLogFile().getLogRangeInfo().highestFile();
        final var position = getLastReadablePosition(transactionLogFile).lastReadable;

        managementService.shutdown();

        writeRandomBytesAfterLastCommandInLastLogFile(
                transactionLogFile, position, () -> ByteBuffer.wrap(new byte[] {1, 2, 3, 4, 5}));

        managementService =
                databaseFactory.setConfig(fail_on_corrupted_log_files, false).build();
        try {
            assertThat(logProvider)
                    .containsMessages("Recovery required from position " + "LogPosition{logVersion=0, byteOffset="
                            + initialTransactionOffset + "}")
                    .assertExceptionForLogMessage("Fail to read transaction log version 0.")
                    .message()
                    .containsAnyOf(
                            "Failure to read transaction log file number 0. Unreadable bytes are encountered after last readable position.",
                            "Envelope span segment boundary",
                            "Unexpected data found at end of buffer at position");
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
            Path originalFile = logFiles.getLogFile().getLogRangeInfo().highestFile();
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
            firstLogFile = logFiles.getLogFile().getLogRangeInfo().highestFile();
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
                        UNKNOWN_TRANSACTION_ID.id() + 8,
                        kernelVersion(),
                        new LogPosition(0, HEADER_OFFSET),
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
                        .resolveDependency(LogMetadataProvider.class);
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
            Path originalFile = logFiles.getLogFile().getLogRangeInfo().highestFile();
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

        LogPosition logPosition = appendDataAfterLastCheckpointRecordFromLastLogFile();

        var dbms = databaseFactory.setConfig(fail_on_corrupted_log_files, false).build();
        try {
            var context = getDefaultDbContext(dbms);
            assertTrue(context.database().isStarted());

            // The appended garbage should have been truncated and a new checkpoint made
            assertThat(Files.size(logFiles.getCheckpointFile().getLogFileForVersion(logPosition.getLogVersion())))
                    .isEqualTo(logPosition.getByteOffset() + CHECKPOINT_RECORD_SIZE);
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
            Path originalFile = logFiles.getLogFile().getLogRangeInfo().highestFile();
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

            DatabaseStateService<?> dbStateService =
                    db.getDependencyResolver().resolveDependency(DatabaseStateService.class);
            assertTrue(dbStateService.causeOfFailure(db.databaseId()).isPresent());
            // This assertion has the potential to be brittle, as the expected failure depends in part on how much of
            // the corrupted data we read before hitting a fail state. That can change unpredictably with data shape.
            assertThat(dbStateService.causeOfFailure(db.databaseId()).get())
                    .rootCause()
                    .isInstanceOfAny(NegativeArraySizeException.class, BadLogEntryException.class);
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

            DatabaseStateService<?> dbStateService =
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
        long lastClosedTransactionBeforeStart = transactionIdStore.getHighestGapFreeClosedTransactionId();
        long txSizes = 0;
        for (int i = 0; i < 10; i++) {
            txSizes += generateTransaction(database);
        }
        long numberOfTransactions =
                transactionIdStore.getHighestGapFreeClosedTransactionId() - lastClosedTransactionBeforeStart;
        managementService.shutdown();

        Path highestLogFile = logFiles.getLogFile().getLogRangeInfo().highestFile();
        Positions positions = getLastReadablePosition(highestLogFile);
        long originalFileLength = positions.lastReadable.getByteOffset();
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

        assertEquals(0, logFiles.getLogFile().getLogRangeInfo().highestVersion());
        assertEquals(numberOfTransactions, recoveryMonitor.getNumberOfRecoveredTransactions());
        assertEquals(originalFileLength, fileSystem.getFileSize(highestLogFile));
        // 2 shutdowns will create a checkpoint and recovery that will be triggered by removing tx logs for default db
        // during the setup and starting db as part of the test, plus checkpoints on creation
        assertEquals(
                positions.startPosition.getByteOffset() + 4 * CHECKPOINT_RECORD_SIZE,
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
        long lastClosedTransactionBeforeStart = transactionIdStore.getHighestGapFreeClosedTransactionId();
        long txSize = generateTransactionsAndRotate(database, 3);
        long additionalTxSizes = 0;
        for (int i = 0; i < 7; i++) {
            additionalTxSizes += generateTransaction(database);
        }
        long numberOfTransactions =
                transactionIdStore.getHighestGapFreeClosedTransactionId() - lastClosedTransactionBeforeStart;
        managementService.shutdown();

        Path highestLogFile = logFiles.getLogFile().getLogRangeInfo().highestFile();
        Positions positions = getLastReadablePosition(highestLogFile);
        long originalFileLength = positions.lastReadable.getByteOffset();
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
                                + (txSize + additionalTxSizes + positions.startPosition.getByteOffset())
                                + "} can not be recovered and will be truncated.");

        assertEquals(3, logFiles.getLogFile().getLogRangeInfo().highestVersion());
        assertEquals(numberOfTransactions, recoveryMonitor.getNumberOfRecoveredTransactions());
        assertEquals(originalFileLength, fileSystem.getFileSize(highestLogFile));
        // 2 shutdowns will create a checkpoint and recovery that will be triggered by removing tx logs for default db
        // during the setup and starting db as part of the test
        assertEquals(
                positions.startPosition.getByteOffset() + 4 * CHECKPOINT_RECORD_SIZE,
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

        Path highestLogFile = logFiles.getLogFile().getLogRangeInfo().highestFile();
        Positions positions = getLastReadablePosition(highestLogFile);
        long originalFileLength = positions.lastReadable.getByteOffset();
        removeLastCheckpointRecordFromLastLogFile();

        addCorruptedCommandsToLastLogFile(logEntryWriterWrapper);
        long modifiedFileLength = fileSystem.getFileSize(highestLogFile);

        assertThat(modifiedFileLength).isGreaterThan(originalFileLength);

        startStopDbRecoveryOfCorruptedLogs();

        assertThat(logProvider)
                .containsMessages(
                        "Fail to read transaction log version 3.",
                        "Recovery required from position LogPosition{logVersion=3, byteOffset="
                                + (txSize + positions.startPosition.getByteOffset()) + "}",
                        "Fail to recover database.",
                        "Any transactional logs after position LogPosition{logVersion=3, byteOffset="
                                + (txSize + additionalTxSizes + positions.startPosition.getByteOffset())
                                + "} can not be recovered and will be truncated.");

        assertEquals(3, logFiles.getLogFile().getLogRangeInfo().highestVersion());
        assertEquals(transactionsToRecover, recoveryMonitor.getNumberOfRecoveredTransactions());
        assertEquals(originalFileLength, fileSystem.getFileSize(highestLogFile));
        assertEquals(
                positions.startPosition.getByteOffset() + 7 * CHECKPOINT_RECORD_SIZE,
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

        Path highestLogFile = logFiles.getLogFile().getLogRangeInfo().highestFile();
        Positions positions = getLastReadablePosition(highestLogFile);
        long originalFileLength = positions.lastReadable.getByteOffset();
        addCorruptedCommandsToLastLogFile(logEntryWriterWrapper);
        long modifiedFileLength = fileSystem.getFileSize(highestLogFile);

        assertThat(modifiedFileLength).isGreaterThan(originalFileLength);

        startStopDbRecoveryOfCorruptedLogs();

        assertThat(logProvider)
                .containsMessages(
                        "Fail to read transaction log version 5.",
                        "Recovery required from position LogPosition{logVersion=5, byteOffset="
                                + (txSize + positions.startPosition.getByteOffset()) + "}",
                        "Fail to recover database. Any transactional logs after position LogPosition{logVersion=5, byteOffset="
                                + (txSize + positions.startPosition.getByteOffset())
                                + "} can not be recovered and will be truncated.");

        assertEquals(5, logFiles.getLogFile().getLogRangeInfo().highestVersion());
        assertEquals(originalFileLength, fileSystem.getFileSize(highestLogFile));
        // 2 shutdowns will create a checkpoint and recovery that will be triggered by removing tx logs for default db
        // during the setup and starting db as part of the test
        if (NativeAccessProvider.getNativeAccess().isAvailable()) {
            assertEquals(
                    ByteUnit.mebiBytes(1),
                    Files.size(logFiles.getCheckpointFile().getCurrentFile()));
        } else {
            assertEquals(
                    positions.startPosition.getByteOffset() + 5 * CHECKPOINT_RECORD_SIZE,
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

    @Test
    void dontTruncateOnCorruptedLogsWithoutSettingForZeroByteInWrongPlace() throws IOException {
        // This test doesn't make sense for envelopes. Either the zero byte is first in the header, or first in the
        // content, either way the checksum validation would discover it. And this test where it is first in the
        // header doesn't break the tx because the first byte of the checksum is zero.
        Assumptions.assumeTrue(kernelVersion().isLessThan(KernelVersion.VERSION_ENVELOPED_TRANSACTION_LOGS_GUARANTEED));

        // There was a case where corrupted transaction logs could be truncated even without the
        // fail on truncate setting disabled. If the first byte on a transaction boundary was 0 the reader
        // would just think it reached the end of the file and say there were no more transactions and then
        // truncate would happen.
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        logFiles = buildDefaultLogFiles(getStoreId(database));

        TransactionIdStore transactionIdStore = getTransactionIdStore(database);
        for (int i = 0; i < 10; i++) {
            generateTransaction(database);
        }
        LogPosition logOffsetAfterTestTransactions =
                transactionIdStore.getHighestGapFreeClosedTransaction().logPosition();
        for (int i = 0; i < 2; i++) {
            generateTransaction(database);
        }

        // Destroy the tx log a bit so we can only recover the first 10 transactions
        DependencyResolver dependencyResolver = database.getDependencyResolver();
        LogFiles logFiles1 = dependencyResolver.resolveDependency(LogFiles.class);
        TransactionLogWriter transactionLogWriter = logFiles1.getLogFile().getTransactionLogWriter();
        FlushableLogPositionAwareChannel channel = transactionLogWriter.getChannel();
        LogPositionMarker logPositionMarker = new LogPositionMarker();
        logPositionMarker.mark(
                logOffsetAfterTestTransactions.getLogVersion(), logOffsetAfterTestTransactions.getByteOffset());
        channel.setLogPosition(logPositionMarker);
        channel.putLong(0L);
        managementService.shutdown();
        removeLastCheckpointRecordFromLastLogFile();

        managementService = databaseFactory.build();
        try {
            GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
            DatabaseStateService<?> dbStateService =
                    db.getDependencyResolver().resolveDependency(DatabaseStateService.class);
            assertTrue(dbStateService.causeOfFailure(db.databaseId()).isPresent());
            assertThat(dbStateService
                            .causeOfFailure(db.databaseId())
                            .get()
                            .getCause()
                            .getMessage())
                    .contains("Unreadable bytes are encountered after last readable position.");
        } finally {
            managementService.shutdown();
        }
    }

    @Test
    void truncateOnCorruptedLogsWithSettingForZeroByteInWrongPlace() throws IOException {
        // This test doesn't make sense for envelopes. Either the zero byte is first in the header, or first in the
        // content, either way the checksum validation would discover it. And this test where it is first in the
        // header doesn't break the tx because the first byte of the checksum is zero.
        Assumptions.assumeTrue(kernelVersion().isLessThan(KernelVersion.VERSION_ENVELOPED_TRANSACTION_LOGS_GUARANTEED));

        // There was a case where corrupted transaction logs could be truncated even without reporting it.
        // If the first byte on a transaction boundary was 0 the reader
        // would just think it reached the end of the file and say there were no more transactions and then
        // truncate would happen.
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        logFiles = buildDefaultLogFiles(getStoreId(database));

        TransactionIdStore transactionIdStore = getTransactionIdStore(database);
        LogPosition logOffsetBeforeTestTransactions =
                transactionIdStore.getHighestGapFreeClosedTransaction().logPosition();
        long lastClosedTransactionBeforeStart = transactionIdStore.getHighestGapFreeClosedTransactionId();
        for (int i = 0; i < 10; i++) {
            generateTransaction(database);
        }
        long numberOfClosedTransactions =
                transactionIdStore.getHighestGapFreeClosedTransactionId() - lastClosedTransactionBeforeStart;
        LogPosition logOffsetAfterTestTransactions =
                transactionIdStore.getHighestGapFreeClosedTransaction().logPosition();
        for (int i = 0; i < 2; i++) {
            generateTransaction(database);
        }

        // Destroy the tx log a bit so we can only recover the first 10 transactions
        DependencyResolver dependencyResolver = database.getDependencyResolver();
        LogFiles logFiles1 = dependencyResolver.resolveDependency(LogFiles.class);
        TransactionLogWriter transactionLogWriter = logFiles1.getLogFile().getTransactionLogWriter();
        FlushableLogPositionAwareChannel channel = transactionLogWriter.getChannel();
        LogPositionMarker logPositionMarker = new LogPositionMarker();
        logPositionMarker.mark(
                logOffsetAfterTestTransactions.getLogVersion(), logOffsetAfterTestTransactions.getByteOffset());
        channel.setLogPosition(logPositionMarker);
        channel.putLong(0L);
        managementService.shutdown();
        removeLastCheckpointRecordFromLastLogFile();

        startStopDbRecoveryOfCorruptedLogs();

        assertEquals(numberOfClosedTransactions, recoveryMonitor.getNumberOfRecoveredTransactions());
        assertThat(logProvider)
                .containsMessages(
                        "Recovery required from position LogPosition{logVersion=0, byteOffset="
                                + logOffsetBeforeTestTransactions.getByteOffset() + "}",
                        "Fail to recover database.",
                        "Any transactional logs after position LogPosition{logVersion=0, byteOffset="
                                + logOffsetAfterTestTransactions.getByteOffset()
                                + "} can not be recovered and will be truncated.");
    }

    @Test
    void shouldRecoverWithMalformedSchema() throws Exception {
        DatabaseLayout databaseLayout;
        // Given
        try (DatabaseManagementService dbms = databaseFactory.build()) {
            GraphDatabaseAPI database = (GraphDatabaseAPI) dbms.database(DEFAULT_DATABASE_NAME);
            databaseLayout = database.databaseLayout();
            Label person = Label.label("Person");
            String name = "name";
            try (Transaction tx = database.beginTx()) {
                Node node = tx.createNode(person);
                node.setProperty(name, "John");
                tx.commit();
            }
            try (Transaction tx = database.beginTx()) {
                tx.schema().indexFor(person).on(name).create();
                tx.commit();
            }
            try (Transaction tx = database.beginTx()) {
                tx.schema().awaitIndexesOnline(1, TimeUnit.MINUTES);
            }
            database.getDependencyResolver()
                    .resolveDependency(CheckPointerImpl.class)
                    .forceCheckPoint(new SimpleTriggerInfo("test"));
            try (Transaction tx = database.beginTx()) {
                Node node = tx.createNode(person);
                node.setProperty(name, "Lisa");
                tx.commit();
            }
            logFiles = buildDefaultLogFiles(getStoreId(database));
        }
        // When
        removeLastCheckpointRecordFromLastLogFile();
        Path schemaStore =
                databaseLayout.pathForStore(CommonDatabaseStores.SCHEMAS).baseSegment();

        byte[] data = FileSystemUtils.readAllBytes(fileSystem, schemaStore, INSTANCE);
        int numPages = data.length / PAGE_SIZE;
        if (numPages > 1) {
            // A GBPTree (block store). Let's find the string and break it
            for (int i = 0; i < data.length - 5; i++) {
                if ("INDEX".equals(new String(data, i, 5))) {
                    data[i] = 'i';
                }
            }
        } else {
            // A Record store, lets fill it with -1
            Arrays.fill(data, (numPages - 1) * PAGE_SIZE, numPages * PAGE_SIZE, (byte) -1);
        }
        FileSystemUtils.writeAllBytes(fileSystem, schemaStore, data, INSTANCE);

        // Then
        try (DatabaseManagementService dbms =
                databaseFactory.setConfig(ignore_corrupt_schema, false).build()) {
            GraphDatabaseAPI database = (GraphDatabaseAPI) dbms.database(DEFAULT_DATABASE_NAME);
            assertThat(database.isAvailable()).isFalse();
            assertThat(logProvider).containsMessages("Exception occurred while starting the database");
        }
        assertThat(Recovery.isRecoveryRequired(fileSystem, databaseLayout, config, INSTANCE))
                .isTrue();

        // And then
        try (DatabaseManagementService dbms =
                databaseFactory.setConfig(ignore_corrupt_schema, true).build()) {
            GraphDatabaseAPI database = (GraphDatabaseAPI) dbms.database(DEFAULT_DATABASE_NAME);
            assertThat(database.isAvailable()).as(logProvider::serialize).isTrue();
        }
        assertThat(Recovery.isRecoveryRequired(fileSystem, databaseLayout, config, INSTANCE))
                .isFalse();
    }

    static StoreId getStoreId(GraphDatabaseAPI database) {
        return database.getDependencyResolver()
                .resolveDependency(StoreIdProvider.class)
                .getStoreId();
    }

    static TransactionIdStore getTransactionIdStore(GraphDatabaseAPI database) {
        return database.getDependencyResolver().resolveDependency(TransactionIdStore.class);
    }

    void removeLastCheckpointRecordFromLastLogFile() throws IOException {
        CheckpointFile checkpointFile = logFiles.getCheckpointFile();
        var checkpoint = checkpointFile.findLatestCheckpoint();
        if (checkpoint.isPresent()) {
            LogPosition logPosition = checkpoint.get().checkpointEntryPosition();
            try (StoreChannel storeChannel =
                    fileSystem.write(checkpointFile.getLogFileForVersion(logPosition.getLogVersion()))) {
                storeChannel.truncate(logPosition.getByteOffset());
            }
        }
    }

    private void replacePartOfCheckpointWithZeroes() throws IOException {
        CheckpointFile checkpointFile = logFiles.getCheckpointFile();
        var checkpoint = checkpointFile.findLatestCheckpoint();
        if (checkpoint.isPresent()) {
            LogPosition logPosition = checkpoint.get().checkpointEntryPosition();
            try (StoreChannel storeChannel =
                    fileSystem.write(checkpointFile.getLogFileForVersion(logPosition.getLogVersion()))) {
                storeChannel.position(logPosition.getByteOffset() + 15);
                storeChannel.writeAll(ByteBuffers.allocate(CHECKPOINT_RECORD_SIZE, ByteOrder.LITTLE_ENDIAN, INSTANCE));
            }
        }
    }

    private void replacePartOfFirstCheckpointAndRestOfFileWithZeroes() throws IOException {
        CheckpointFile checkpointFile = logFiles.getCheckpointFile();
        var checkpoints = checkpointFile.reachableCheckpoints();
        if (!checkpoints.isEmpty()) {
            LogPosition logPosition = checkpoints.get(0).checkpointEntryPosition();
            try (StoreChannel storeChannel =
                    fileSystem.write(checkpointFile.getLogFileForVersion(logPosition.getLogVersion()))) {
                // Assuming they are in same file
                long corruptionPoint = logPosition.getByteOffset() + 15;
                long lastPoint = checkpoints
                        .get(checkpoints.size() - 1)
                        .checkpointFilePostReadPosition()
                        .getByteOffset();
                storeChannel.position(corruptionPoint);
                storeChannel.writeAll(
                        ByteBuffers.allocate((int) (lastPoint - corruptionPoint), ByteOrder.LITTLE_ENDIAN, INSTANCE));
            }
        }
    }

    private LogPosition appendDataAfterLastCheckpointRecordFromLastLogFile() throws IOException {
        CheckpointFile checkpointFile = logFiles.getCheckpointFile();
        var checkpoint = checkpointFile.findLatestCheckpoint();
        if (checkpoint.isPresent()) {
            LogPosition logPosition = checkpoint.get().channelPositionAfterCheckpoint();
            try (StoreChannel storeChannel =
                    fileSystem.write(checkpointFile.getLogFileForVersion(logPosition.getLogVersion()))) {
                storeChannel.position(logPosition.getByteOffset() + 300);
                storeChannel.writeAll(ByteBuffer.wrap("DeaD BeaF".getBytes()));
            }
            return logPosition;
        }
        return LogPosition.UNSPECIFIED;
    }

    private void appendRandomBytesAfterLastCheckpointRecordFromLastLogFile(int bytesToAdd) throws IOException {
        CheckpointFile checkpointFile = logFiles.getCheckpointFile();
        var checkpoint = checkpointFile.findLatestCheckpoint();
        if (checkpoint.isPresent()) {
            LogPosition logPosition = checkpoint.get().channelPositionAfterCheckpoint();
            try (StoreChannel storeChannel =
                    fileSystem.write(checkpointFile.getLogFileForVersion(logPosition.getLogVersion()))) {
                storeChannel.position(logPosition.getByteOffset());
                var array = new byte[bytesToAdd];
                do {
                    random.nextBytes(array);
                    array[0] = (byte)
                            random.nextInt(KernelVersion.EARLIEST.version(), LATEST_KERNEL_VERSION.version() + 1);
                } while (!checkpointEntryLooksCorrupted(array));
                storeChannel.writeAll(ByteBuffer.wrap(array));
            }
        }
    }

    private boolean checkpointEntryLooksCorrupted(byte[] array) {
        var testReader = new VersionAwareLogEntryReader(version -> null, BINARY_VERSIONS, INSTANCE);
        var ch = new InMemoryVersionableReadableClosablePositionAwareChannel();
        ch.putVersion(array[0]);
        ch.putAll(ByteBuffer.wrap(array).position(1));
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
                    fileSystem.write(checkpointFile.getLogFileForVersion(logPosition.getLogVersion()))) {
                storeChannel.truncate(logPosition.getByteOffset() - bytesToTrim);
            }
        }
    }

    private void truncateBytesFromLastLogFile(long bytesToTrim) throws IOException {
        if (logFiles.getLogFile().getLogRangeInfo().highestVersion() > 0) {
            Path highestLogFile = logFiles.getLogFile().getLogRangeInfo().highestFile();
            long readableOffset =
                    getLastReadablePosition(highestLogFile).lastReadable.getByteOffset();
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
        writeRandomBytesAfterLastCommandInLastLogFile(source, 10);
    }

    private void writeRandomBytesAfterLastCommandInLastLogFile(Supplier<ByteBuffer> source, int bytesToAdd)
            throws IOException {
        try (Lifespan lifespan = new Lifespan()) {
            LogFile transactionLogFile = logFiles.getLogFile();
            lifespan.add(logFiles);

            LogPosition position = getLastReadablePosition(transactionLogFile);

            try (StoreFileChannel writeChannel =
                    fileSystem.write(transactionLogFile.getLogRangeInfo().highestFile())) {
                writeChannel.position(position.getByteOffset());
                for (int i = 0; i < bytesToAdd; i++) {
                    writeChannel.writeAll(source.get());
                }
            }
        }
    }

    private void writeRandomBytesAfterLastCommandInLastLogFile(
            Path transactionLogFile, LogPosition position, Supplier<ByteBuffer> source) throws IOException {
        int someRandomPaddingAfterEndOfDataInLogFile = random.nextInt(1, 10);
        try (StoreFileChannel writeChannel = fileSystem.write(transactionLogFile)) {
            writeChannel.position(position.getByteOffset() + someRandomPaddingAfterEndOfDataInLogFile);
            for (int i = 0; i < 10; i++) {
                writeChannel.writeAll(source.get());
            }
        }
    }

    record Positions(LogPosition startPosition, LogPosition lastReadable) {}

    private Positions getLastReadablePosition(Path logFile) throws IOException {
        VersionAwareLogEntryReader entryReader =
                new VersionAwareLogEntryReader(storageEngineFactory.commandReaderFactory(), BINARY_VERSIONS, INSTANCE);
        LogFile txLogFile = logFiles.getLogFile();
        long logVersion = txLogFile.getLogVersion(logFile);
        LogPosition startPosition = txLogFile.extractHeader(logVersion).getStartPosition();
        try (ReadableLogChannel reader = openTransactionFileChannel(logVersion, startPosition)) {
            while (entryReader.readLogEntry(reader) != null) {
                // scroll to the end of readable entries
            }
        } catch (IncompleteLogHeaderException e) {
            return new Positions(startPosition, new LogPosition(logVersion, 0));
        }
        return new Positions(startPosition, entryReader.lastPosition());
    }

    private LogPosition getLastFileStartPosition(LogFile logFile) throws IOException {
        return logFile.extractHeader(logFiles.getLogFile().getLogRangeInfo().highestVersion())
                .getStartPosition();
    }

    private ReadableLogChannel openTransactionFileChannel(long logVersion, LogPosition startPosition)
            throws IOException {
        final var logFile = logFiles.getLogFile();
        final var channel = ReadAheadUtils.newChannel(logFile, logVersion, INSTANCE);
        channel.position(startPosition.getByteOffset());
        return channel;
    }

    private LogPosition getLastReadablePosition(LogFile logFile) throws IOException {
        VersionAwareLogEntryReader entryReader =
                new VersionAwareLogEntryReader(storageEngineFactory.commandReaderFactory(), BINARY_VERSIONS, INSTANCE);
        LogPosition startPosition = logFile.extractHeader(
                        logFiles.getLogFile().getLogRangeInfo().highestVersion())
                .getStartPosition();
        try (ReadableLogChannel reader = logFile.getReader(startPosition)) {
            while (entryReader.readLogEntry(reader) != null) {
                // scroll to the end of readable entries
            }
        }
        return entryReader.lastPosition();
    }

    private void addRandomBytesToLastLogFile(Supplier<Byte> byteSource) throws IOException {
        writeRandomBytesAfterLastCommandInLastLogFile(() -> ByteBuffer.wrap(new byte[] {byteSource.get()}));
    }

    private void addRandomBytesToLastLogFile(Supplier<Byte> byteSource, int bytesToAdd) throws IOException {
        writeRandomBytesAfterLastCommandInLastLogFile(() -> ByteBuffer.wrap(new byte[] {byteSource.get()}), bytesToAdd);
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
        LogFiles internalLogFiles = LogFilesBuilder.writeableBuilder(
                        databaseLayout,
                        fileSystem,
                        this::kernelVersion,
                        () -> LogFormat.fromKernelVersion(kernelVersion()))
                .withLogVersionRepository(versionRepository)
                .withStoreId(new StoreId(4, 5, "engine-1", "format-1", 1, 2))
                .withStorageEngineFactory(StorageEngineFactory.selectStorageEngine(config))
                .withConfig(config)
                .build();
        try (Lifespan lifespan = new Lifespan(internalLogFiles)) {
            LogFile transactionLogFile = internalLogFiles.getLogFile();
            LogEntryWriter<FlushableLogPositionAwareChannel> realLogEntryWriter =
                    transactionLogFile.getTransactionLogWriter().getWriter();
            LogEntryWriter<FlushableLogPositionAwareChannel> wrappedLogEntryWriter =
                    logEntryWriterWrapper.wrap(realLogEntryWriter, BINARY_VERSIONS);
            TransactionLogWriter writer = new TransactionLogWriter(
                    realLogEntryWriter.getChannel(),
                    wrappedLogEntryWriter,
                    this::kernelVersion,
                    LogRotation.NO_ROTATION);
            List<StorageCommand> commands = new ArrayList<>();
            commands.add(new Command.PropertyCommand(
                    LATEST_LOG_SERIALIZATION, new PropertyRecord(1), new PropertyRecord(2)));
            commands.add(new Command.NodeCommand(LATEST_LOG_SERIALIZATION, new NodeRecord(2), new NodeRecord(3)));
            CompleteCommandBatch transaction = new CompleteCommandBatch(
                    commands, UNKNOWN_CONSENSUS_INDEX, 0, 0, 0, 0, Leases.NO_LEASES, kernelVersion(), ANONYMOUS);
            writer.append(
                    transaction,
                    1000,
                    1001,
                    UNKNOWN_CHUNK_ID,
                    BASE_TX_CHECKSUM,
                    UNKNOWN_APPEND_INDEX,
                    LogAppendEvent.NULL);
        }
    }

    private static long getInitialVersion(LogFiles logFiles) {
        return logFiles == null ? 0 : logFiles.getLogFile().getLogRangeInfo().highestVersion();
    }

    private static long getLastClosedTransactionOffset(GraphDatabaseAPI database) {
        return getLastClosedTransaction(database).getByteOffset();
    }

    private static LogPosition getLastClosedTransaction(GraphDatabaseAPI database) {
        LogMetadataProvider logMetadataProvider =
                database.getDependencyResolver().resolveDependency(LogMetadataProvider.class);
        return logMetadataProvider.getHighestGapFreeClosedTransaction().logPosition();
    }

    LogFiles buildDefaultLogFiles(StoreId storeId) throws IOException {
        return LogFilesBuilder.writeableBuilder(
                        databaseLayout,
                        fileSystem,
                        this::kernelVersion,
                        () -> LogFormat.fromKernelVersion(kernelVersion()))
                .withStoreId(storeId)
                .withLogProvider(logProvider)
                .withStorageEngineFactory(StorageEngineFactory.selectStorageEngine(config))
                .withConfig(config)
                .build();
    }

    private LogFiles buildDefaultLogFiles(GraphDatabaseAPI database) throws IOException {
        return LogFilesBuilder.writeableBuilder(
                        databaseLayout,
                        fileSystem,
                        this::kernelVersion,
                        () -> LogFormat.fromKernelVersion(kernelVersion()))
                .withDependencies(database.getDependencyResolver())
                .withLogProvider(logProvider)
                .withConfig(config)
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
        while (logFiles.getLogFile().getLogRangeInfo().highestVersion() < logFilesToGenerate) {
            logFiles.getLogFile().rotate();
            lastTxSize = generateTransaction(database);
            if (checkpoint) {
                checkpointer.forceCheckPoint(new SimpleTriggerInfo("testForcedCheckpoint"));
            }
        }
        return lastTxSize;
    }

    static long generateTransaction(GraphDatabaseAPI database) throws IOException {
        LogPosition lastTx = getLastClosedTransaction(database);
        LogFiles logFiles = database.getDependencyResolver().resolveDependency(LogFiles.class);
        LogFile logFile = logFiles.getLogFile();
        long initialOffset = logFile.getCurrentLogVersion() > lastTx.getLogVersion()
                ? logFile.extractHeader(logFile.getCurrentLogVersion())
                        .getStartPosition()
                        .getByteOffset()
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

    void startStopDatabase() {
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
        default <T extends WritableChannel> LogEntryWriter<T> wrap(
                LogEntryWriter<T> logEntryWriter, BinarySupportedKernelVersions versions) {
            return to(logEntryWriter.getChannel(), versions);
        }

        <T extends WritableChannel> LogEntryWriter<T> to(T channel, BinarySupportedKernelVersions versions);
    }

    private static class CorruptedLogEntryWriter<T extends WritableChannel> extends LogEntryWriter<T> {
        CorruptedLogEntryWriter(T channel, BinarySupportedKernelVersions versions) {
            super(channel, versions);
        }

        @Override
        public void writeStartEntry(
                KernelVersion version,
                long timeWritten,
                long latestCommittedTxWhenStarted,
                long appendIndex,
                long transactionSequenceNumber,
                int previousChecksum,
                byte[] additionalHeaderData)
                throws IOException {
            channel.beginChecksumForWriting();
            channel.putVersion(version.version()).put(TX_START);
        }
    }

    private static class CorruptedLogEntryVersionWriter<T extends WritableChannel> extends LogEntryWriter<T> {
        CorruptedLogEntryVersionWriter(T channel, BinarySupportedKernelVersions versions) {
            super(channel, versions);
        }

        /**
         * Use a non-existing log entry version. Implementation stolen from
         * {@link LogEntryWriter#writeStartEntry(KernelVersion, long, long, long, long, int, byte[])}.
         */
        @Override
        public void writeStartEntry(
                KernelVersion version,
                long timeWritten,
                long latestCommittedTxWhenStarted,
                long appendIndex,
                long transactionSequenceNumber,
                int previousChecksum,
                byte[] additionalHeaderData)
                throws IOException {
            byte nonExistingLogEntryVersion = (byte) (LATEST_KERNEL_VERSION.version() + 10);
            channel.beginChecksumForWriting();
            channel.putVersion(nonExistingLogEntryVersion).put(TX_START);
            channel.putLong(timeWritten)
                    .putLong(latestCommittedTxWhenStarted)
                    .putInt(previousChecksum)
                    .putInt(additionalHeaderData.length)
                    .put(additionalHeaderData, additionalHeaderData.length);
        }

        // Envelope channel uses the last kernel version seen when writing the envelope header. Need to override commit
        // entry writer as well to get non-existing version in the header.
        @Override
        public int writeCommitEntry(KernelVersion kernelVersion, long transactionId, long timeWritten)
                throws IOException {
            byte nonExistingLogEntryVersion = (byte) (LATEST_KERNEL_VERSION.version() + 10);
            channel.putVersion(nonExistingLogEntryVersion).put(TX_COMMIT);
            channel.putLong(transactionId).putLong(timeWritten);
            return channel.putChecksum();
        }
    }

    static class RecoveryMonitor implements org.neo4j.kernel.recovery.RecoveryMonitor {
        private final List<Long> recoveredBatches = new ArrayList<>();
        private int numberOfRecoveredTransactions;
        private final AtomicBoolean recoveryRequired = new AtomicBoolean();

        @Override
        public void recoveryRequired(RecoveryStartInformation recoveryStartInfo) {
            recoveryRequired.set(true);
            numberOfRecoveredTransactions = 0;
        }

        @Override
        public void batchRecovered(CommittedCommandBatchRepresentation committedBatch) {
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
