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

import static java.util.UUID.randomUUID;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.kernel.database.DatabaseIdFactory.from;
import static org.neo4j.kernel.impl.transaction.log.LogIndexEncoding.encodeLogIndex;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryFactory.newCommitEntry;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryFactory.newStartEntry;
import static org.neo4j.kernel.impl.transaction.log.entry.LogFormat.writeLogHeader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogSegments.UNKNOWN_LOG_SEGMENT_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.v57.DetachedCheckpointLogEntrySerializerV5_7.RECORD_LENGTH_BYTES;
import static org.neo4j.kernel.impl.transaction.log.files.ChannelNativeAccessor.EMPTY_ACCESSOR;
import static org.neo4j.kernel.recovery.RecoveryStartInformation.NO_RECOVERY_REQUIRED;
import static org.neo4j.kernel.recovery.RecoveryStartInformationProvider.NO_MONITOR;
import static org.neo4j.kernel.recovery.RecoveryStartupChecker.EMPTY_CHECKER;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION;
import static org.neo4j.test.LatestVersions.LATEST_LOG_FORMAT;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.dbms.database.DatabaseStartAbortedException;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.internal.nativeimpl.NativeAccessProvider;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.database.DatabaseStartupController;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatch;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.SimpleAppendIndexProvider;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.FlushableLogPositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.PhysicalFlushableLogPositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionMetadataCache;
import org.neo4j.kernel.impl.transaction.log.checkpoint.DetachedCheckpointAppender;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.InternalLog;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.AppendIndexProvider;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.time.Clocks;

@Neo4jLayoutExtension
class TransactionLogsRecoveryTest {
    @Inject
    private DefaultFileSystemAbstraction fileSystem;

    @Inject
    private DatabaseLayout databaseLayout;

    @Inject
    private TestDirectory testDirectory;

    private final AppendIndexProvider appendIndexProvider = new SimpleAppendIndexProvider();
    private final LogVersionRepository logVersionRepository = new SimpleLogVersionRepository();
    private final StoreId storeId = new StoreId(1, 2, "engine-1", "format-1", 3, 4);
    private final TransactionIdStore transactionIdStore = new SimpleTransactionIdStore(
            5L, 6L, LATEST_KERNEL_VERSION, 0, BASE_TX_COMMIT_TIMESTAMP, UNKNOWN_CONSENSUS_INDEX, 0, 0);
    private final int logVersion = 0;

    private LogEntry lastCommittedTxStartEntry;
    private LogEntry lastCommittedTxCommitEntry;
    private LogEntry expectedStartEntry;
    private LogEntry expectedCommitEntry;
    private final Monitors monitors = new Monitors();
    private final SimpleLogVersionRepository versionRepository = new SimpleLogVersionRepository();
    private LogFiles logFiles;
    private Path storeDir;
    private Lifecycle schemaLife;
    private LifeSupport life;

    @BeforeEach
    void setUp() throws Exception {
        storeDir = testDirectory.homePath();
        logFiles = buildLogFiles();
        life = new LifeSupport();
        life.add(logFiles);
        life.start();
        schemaLife = new LifecycleAdapter();
    }

    @AfterEach
    void tearDown() {
        life.shutdown();
    }

    @Test
    void shouldRecoverExistingData() throws Exception {
        var contextFactory = new CursorContextFactory(NULL, EMPTY_CONTEXT_SUPPLIER);
        LogFile logFile = logFiles.getLogFile();
        Path file = logFile.getLogFileForVersion(logVersion);

        writeSomeData(file, dataWriters -> {
            LogEntryWriter<?> writer = dataWriters.writer();
            LogPositionAwareChannel channel = dataWriters.channel();
            LogPositionMarker marker = new LogPositionMarker();

            // last committed tx
            int previousChecksum = BASE_TX_CHECKSUM;
            channel.getCurrentLogPosition(marker);
            LogPosition lastCommittedTxPosition = marker.newPosition();
            byte[] headerData = encodeLogIndex(1);
            writer.writeStartEntry(LATEST_KERNEL_VERSION, 2L, 3L, 4L, previousChecksum, headerData);
            lastCommittedTxStartEntry = newStartEntry(
                    LATEST_KERNEL_VERSION, 2L, 3L, 4L, previousChecksum, headerData, lastCommittedTxPosition);
            previousChecksum = writer.writeCommitEntry(LATEST_KERNEL_VERSION, 4L, 5L);
            lastCommittedTxCommitEntry = newCommitEntry(LATEST_KERNEL_VERSION, 4L, 5L, previousChecksum);

            // checkpoint pointing to the previously committed transaction
            var checkpointFile = logFiles.getCheckpointFile();
            var checkpointAppender = checkpointFile.getCheckpointAppender();
            checkpointAppender.checkPoint(
                    LogCheckPointEvent.NULL,
                    new TransactionId(4L, 7L, LATEST_KERNEL_VERSION, 2, 5L, 6L),
                    5L,
                    LATEST_KERNEL_VERSION,
                    lastCommittedTxPosition,
                    lastCommittedTxPosition,
                    Instant.now(),
                    "test");

            // tx committed after checkpoint
            channel.getCurrentLogPosition(marker);
            writer.writeStartEntry(LATEST_KERNEL_VERSION, 6L, 4L, 5L, previousChecksum, headerData);
            expectedStartEntry = newStartEntry(
                    LATEST_KERNEL_VERSION, 6L, 4L, 5L, previousChecksum, headerData, marker.newPosition());

            previousChecksum = writer.writeCommitEntry(LATEST_KERNEL_VERSION, 5L, 7L);
            expectedCommitEntry = newCommitEntry(LATEST_KERNEL_VERSION, 5L, 7L, previousChecksum);

            return true;
        });

        LifeSupport life = new LifeSupport();
        LogsRecoveryMonitor monitor = new LogsRecoveryMonitor();
        try {
            var recoveryLogFiles = buildLogFiles();
            life.add(recoveryLogFiles);
            StorageEngine storageEngine = mock(StorageEngine.class);
            when(storageEngine.createStorageCursors(any())).thenReturn(mock(StoreCursors.class));
            Config config = Config.defaults();

            TransactionMetadataCache metadataCache = new TransactionMetadataCache();
            LogicalTransactionStore txStore = new PhysicalLogicalTransactionStore(
                    recoveryLogFiles,
                    metadataCache,
                    TestCommandReaderFactory.INSTANCE,
                    monitors,
                    false,
                    config,
                    fileSystem);
            CorruptedLogsTruncator logPruner =
                    new CorruptedLogsTruncator(storeDir, recoveryLogFiles, fileSystem, INSTANCE);
            monitors.addMonitorListener(monitor);
            life.add(new TransactionLogsRecovery(
                    new DefaultRecoveryService(
                            storageEngine,
                            transactionIdStore,
                            txStore,
                            versionRepository,
                            recoveryLogFiles,
                            LatestVersions.LATEST_KERNEL_VERSION_PROVIDER,
                            NO_MONITOR,
                            mock(InternalLog.class),
                            Clocks.systemClock(),
                            false,
                            LatestVersions.BINARY_VERSIONS,
                            contextFactory) {
                        private int nr;

                        @Override
                        public RecoveryApplier getRecoveryApplier(
                                TransactionApplicationMode mode,
                                CursorContextFactory contextFactory,
                                String tracerTag) {
                            RecoveryApplier actual = super.getRecoveryApplier(mode, contextFactory, tracerTag);
                            if (mode.isReverseStep()) {
                                return actual;
                            }

                            return new RecoveryApplier() {
                                @Override
                                public void close() throws Exception {
                                    actual.close();
                                }

                                @Override
                                public boolean visit(CommittedCommandBatch commandBatch) throws Exception {
                                    actual.visit(commandBatch);
                                    if (commandBatch instanceof CommittedTransactionRepresentation tx) {
                                        switch (nr++) {
                                            case 0 -> {
                                                assertEquals(lastCommittedTxStartEntry, tx.startEntry());
                                                assertEquals(lastCommittedTxCommitEntry, tx.commitEntry());
                                            }
                                            case 1 -> {
                                                assertEquals(expectedStartEntry, tx.startEntry());
                                                assertEquals(expectedCommitEntry, tx.commitEntry());
                                            }
                                            default -> fail("Too many recovered transactions");
                                        }
                                    }
                                    return false;
                                }
                            };
                        }
                    },
                    logPruner,
                    schemaLife,
                    monitor,
                    ProgressMonitorFactory.NONE,
                    false,
                    EMPTY_CHECKER,
                    RecoveryPredicate.ALL,
                    false,
                    contextFactory,
                    RecoveryMode.FULL));

            life.start();

            assertTrue(monitor.isRecoveryRequired());
            assertEquals(2, monitor.recoveredBatches());
        } finally {
            life.shutdown();
        }
    }

    @Test
    void shouldSeeThatACleanDatabaseShouldNotRequireRecovery() throws Exception {
        Path file = logFiles.getLogFile().getLogFileForVersion(logVersion);
        var contextFactory = new CursorContextFactory(NULL, EMPTY_CONTEXT_SUPPLIER);

        LogPositionMarker marker = new LogPositionMarker();
        writeSomeData(file, dataWriters -> {
            LogEntryWriter<?> writer = dataWriters.writer();
            LogPositionAwareChannel channel = dataWriters.channel();
            TransactionId transactionId = new TransactionId(4L, 7L, LATEST_KERNEL_VERSION, BASE_TX_CHECKSUM, 5L, 6L);

            // last committed tx
            channel.getCurrentLogPosition(marker);
            writer.writeStartEntry(LATEST_KERNEL_VERSION, 2L, 3L, 4L, BASE_TX_CHECKSUM, EMPTY_BYTE_ARRAY);
            writer.writeCommitEntry(LATEST_KERNEL_VERSION, 4L, 5L);

            // check point
            channel.getCurrentLogPosition(marker);
            var checkpointFile = logFiles.getCheckpointFile();
            var checkpointAppender = checkpointFile.getCheckpointAppender();
            checkpointAppender.checkPoint(
                    LogCheckPointEvent.NULL,
                    transactionId,
                    transactionId.id() + 7,
                    LATEST_KERNEL_VERSION,
                    marker.newPosition(),
                    marker.newPosition(),
                    Instant.now(),
                    "test");
            return true;
        });

        LifeSupport life = new LifeSupport();
        RecoveryMonitor monitor = mock(RecoveryMonitor.class);
        try {
            StorageEngine storageEngine = mock(StorageEngine.class);
            Config config = Config.defaults();

            TransactionMetadataCache metadataCache = new TransactionMetadataCache();
            LogicalTransactionStore txStore = new PhysicalLogicalTransactionStore(
                    logFiles, metadataCache, TestCommandReaderFactory.INSTANCE, monitors, false, config, fileSystem);
            CorruptedLogsTruncator logPruner = new CorruptedLogsTruncator(storeDir, logFiles, fileSystem, INSTANCE);
            monitors.addMonitorListener(new RecoveryMonitor() {
                @Override
                public void recoveryRequired(RecoveryStartInformation recoveryStartInfo) {
                    fail("Recovery should not be required");
                }
            });
            life.add(new TransactionLogsRecovery(
                    new DefaultRecoveryService(
                            storageEngine,
                            transactionIdStore,
                            txStore,
                            versionRepository,
                            logFiles,
                            LatestVersions.LATEST_KERNEL_VERSION_PROVIDER,
                            NO_MONITOR,
                            mock(InternalLog.class),
                            Clocks.systemClock(),
                            false,
                            LatestVersions.BINARY_VERSIONS,
                            contextFactory),
                    logPruner,
                    schemaLife,
                    monitor,
                    ProgressMonitorFactory.NONE,
                    false,
                    EMPTY_CHECKER,
                    RecoveryPredicate.ALL,
                    false,
                    contextFactory,
                    RecoveryMode.FULL));

            life.start();

            verifyNoInteractions(monitor);
        } finally {
            life.shutdown();
        }
    }

    @Test
    void shouldTruncateLogAfterSinglePartialTransaction() throws Exception {
        // GIVEN
        Path file = logFiles.getLogFile().getLogFileForVersion(logVersion);
        final LogPositionMarker marker = new LogPositionMarker();

        writeSomeData(file, dataWriters -> {
            LogEntryWriter<?> writer = dataWriters.writer();
            FlushableLogPositionAwareChannel channel = dataWriters.channel();

            // incomplete tx
            channel.getCurrentLogPosition(marker); // <-- marker has the last good position
            writer.writeStartEntry(LATEST_KERNEL_VERSION, 5L, 4L, 78, 9, EMPTY_BYTE_ARRAY);
            channel.putChecksum();

            return true;
        });

        // WHEN
        boolean recoveryRequired = recovery(storeDir);

        // THEN
        assertTrue(recoveryRequired);
        assertEquals(marker.getByteOffset(), Files.size(file));
    }

    @Test
    void doNotTruncateCheckpointsAfterLastTransaction() throws IOException {
        Path file = logFiles.getLogFile().getLogFileForVersion(logVersion);
        LogPositionMarker marker = new LogPositionMarker();
        writeSomeData(file, dataWriters -> {
            LogEntryWriter<?> writer = dataWriters.writer();
            LogPositionAwareChannel channel = dataWriters.channel();
            writer.writeStartEntry(LATEST_KERNEL_VERSION, 1L, 1L, 2L, BASE_TX_CHECKSUM, EMPTY_BYTE_ARRAY);
            TransactionId transactionId = new TransactionId(1L, 5L, LATEST_KERNEL_VERSION, BASE_TX_CHECKSUM, 2L, 4L);

            writer.writeCommitEntry(LATEST_KERNEL_VERSION, 1L, 2L);
            channel.getCurrentLogPosition(marker);
            var checkpointFile = logFiles.getCheckpointFile();
            var checkpointAppender = checkpointFile.getCheckpointAppender();
            checkpointAppender.checkPoint(
                    LogCheckPointEvent.NULL,
                    transactionId,
                    transactionId.id() + 7,
                    LATEST_KERNEL_VERSION,
                    marker.newPosition(),
                    marker.newPosition(),
                    Instant.now(),
                    "test");

            // write incomplete tx to trigger recovery
            writer.writeStartEntry(LATEST_KERNEL_VERSION, 5L, 4L, 28, 2, EMPTY_BYTE_ARRAY);
            writer.getChannel().putChecksum();
            return true;
        });
        assertTrue(recovery(storeDir));

        assertThat(file).hasSize(marker.getByteOffset());
        assertEquals(
                LATEST_LOG_FORMAT.getHeaderSize() + RECORD_LENGTH_BYTES /* one checkpoint */,
                ((DetachedCheckpointAppender) logFiles.getCheckpointFile().getCheckpointAppender())
                        .getCurrentPosition());

        if (NativeAccessProvider.getNativeAccess().isAvailable()) {
            assertEquals(
                    ByteUnit.mebiBytes(1),
                    Files.size(logFiles.getCheckpointFile().getCurrentFile()));
        } else {
            assertEquals(
                    LATEST_LOG_FORMAT.getHeaderSize() + RECORD_LENGTH_BYTES /* one checkpoint */,
                    Files.size(logFiles.getCheckpointFile().getCurrentFile()));
        }
    }

    @Test
    void shouldTruncateInvalidCheckpointAndAllCorruptTransactions() throws IOException {
        Path file = logFiles.getLogFile().getLogFileForVersion(logVersion);
        LogPositionMarker marker = new LogPositionMarker();
        writeSomeData(file, dataWriters -> {
            LogEntryWriter<?> writer = dataWriters.writer();
            LogPositionAwareChannel channel = dataWriters.channel();
            writer.writeStartEntry(LATEST_KERNEL_VERSION, 1L, 1L, 2L, BASE_TX_CHECKSUM, EMPTY_BYTE_ARRAY);
            writer.writeCommitEntry(LATEST_KERNEL_VERSION, 1L, 2L);
            TransactionId transactionId = new TransactionId(1L, 4L, LATEST_KERNEL_VERSION, BASE_TX_CHECKSUM, 2L, 3L);

            channel.getCurrentLogPosition(marker);
            var checkpointFile = logFiles.getCheckpointFile();
            var checkpointAppender = checkpointFile.getCheckpointAppender();
            checkpointAppender.checkPoint(
                    LogCheckPointEvent.NULL,
                    transactionId,
                    transactionId.id() + 7,
                    LATEST_KERNEL_VERSION,
                    marker.newPosition(),
                    marker.newPosition(),
                    Instant.now(),
                    "valid checkpoint");
            checkpointAppender.checkPoint(
                    LogCheckPointEvent.NULL,
                    transactionId,
                    transactionId.id() + 7,
                    LATEST_KERNEL_VERSION,
                    new LogPosition(marker.getLogVersion() + 1, marker.getByteOffset()),
                    new LogPosition(marker.getLogVersion() + 1, marker.getByteOffset()),
                    Instant.now(),
                    "invalid checkpoint");

            // incomplete tx
            writer.writeStartEntry(LATEST_KERNEL_VERSION, 5L, 4L, 27, 1, EMPTY_BYTE_ARRAY);
            return true;
        });
        assertTrue(recovery(storeDir));

        assertEquals(marker.getByteOffset(), Files.size(file));
        assertEquals(
                LATEST_LOG_FORMAT.getHeaderSize() + RECORD_LENGTH_BYTES /* one checkpoint */,
                Files.size(logFiles.getCheckpointFile().getCurrentFile()));
    }

    @Test
    void shouldTruncateLogAfterLastCompleteTransactionAfterSuccessfulRecovery() throws Exception {
        // GIVEN
        Path file = logFiles.getLogFile().getLogFileForVersion(logVersion);
        final LogPositionMarker marker = new LogPositionMarker();

        writeSomeData(file, dataWriters -> {
            LogEntryWriter<?> writer = dataWriters.writer();
            LogPositionAwareChannel channel = dataWriters.channel();

            // last committed tx
            int previousChecksum = BASE_TX_CHECKSUM;
            writer.writeStartEntry(LATEST_KERNEL_VERSION, 2L, 3L, 4L, previousChecksum, EMPTY_BYTE_ARRAY);
            previousChecksum = writer.writeCommitEntry(LATEST_KERNEL_VERSION, 4L, 5L);

            // incomplete tx
            channel.getCurrentLogPosition(marker); // <-- marker has the last good position
            writer.writeStartEntry(LATEST_KERNEL_VERSION, 5L, 4L, 4L, previousChecksum, EMPTY_BYTE_ARRAY);

            return true;
        });

        // WHEN
        boolean recoveryRequired = recovery(storeDir);

        // THEN
        assertTrue(recoveryRequired);
        assertEquals(marker.getByteOffset(), Files.size(file));
    }

    @Test
    void shouldTellTransactionIdStoreAfterSuccessfulRecovery() throws Exception {
        // GIVEN
        Path file = logFiles.getLogFile().getLogFileForVersion(logVersion);
        final LogPositionMarker marker = new LogPositionMarker();

        final byte[] additionalHeaderData = EMPTY_BYTE_ARRAY;
        final long transactionId = 4;
        final long commitTimestamp = 5;
        writeSomeData(file, dataWriters -> {
            LogEntryWriter<?> writer = dataWriters.writer();
            LogPositionAwareChannel channel = dataWriters.channel();

            // last committed tx
            writer.writeStartEntry(LATEST_KERNEL_VERSION, 2L, 3L, 4L, BASE_TX_CHECKSUM, additionalHeaderData);
            writer.writeCommitEntry(LATEST_KERNEL_VERSION, transactionId, commitTimestamp);
            channel.getCurrentLogPosition(marker);

            return true;
        });

        // WHEN
        boolean recoveryRequired = recovery(storeDir);

        // THEN
        assertTrue(recoveryRequired);
        var lastClosedTransaction = transactionIdStore.getLastClosedTransaction();
        LogPosition logPosition = lastClosedTransaction.logPosition();
        assertEquals(transactionId, lastClosedTransaction.transactionId().id());
        assertEquals(
                commitTimestamp,
                transactionIdStore.getLastCommittedTransaction().commitTimestamp());
        assertEquals(logVersion, logPosition.getLogVersion());
        assertEquals(marker.getByteOffset(), logPosition.getByteOffset());
    }

    @Test
    void shouldInitSchemaLifeWhenRecoveryNotRequired() throws Exception {
        Lifecycle schemaLife = mock(Lifecycle.class);
        var contextFactory = new CursorContextFactory(NULL, EMPTY_CONTEXT_SUPPLIER);

        RecoveryService recoveryService = mock(RecoveryService.class);
        when(recoveryService.getRecoveryStartInformation()).thenReturn(NO_RECOVERY_REQUIRED);

        CorruptedLogsTruncator logPruner = new CorruptedLogsTruncator(storeDir, logFiles, fileSystem, INSTANCE);
        RecoveryMonitor monitor = mock(RecoveryMonitor.class);

        TransactionLogsRecovery logsRecovery = new TransactionLogsRecovery(
                recoveryService,
                logPruner,
                schemaLife,
                monitor,
                ProgressMonitorFactory.NONE,
                true,
                EMPTY_CHECKER,
                RecoveryPredicate.ALL,
                false,
                contextFactory,
                RecoveryMode.FULL);

        logsRecovery.init();

        verify(monitor, never()).recoveryRequired(any());
        verify(schemaLife).init();
    }

    @Test
    void shouldFailRecoveryWhenCanceled() throws Exception {
        Path file = logFiles.getLogFile().getLogFileForVersion(logVersion);
        final LogPositionMarker marker = new LogPositionMarker();

        final long transactionId = 4;
        final long commitTimestamp = 5;
        writeSomeData(file, writers -> {
            LogEntryWriter<?> writer = writers.writer();
            LogPositionAwareChannel channel = writers.channel();

            // last committed tx
            writer.writeStartEntry(LATEST_KERNEL_VERSION, 2L, 3L, 4L, BASE_TX_CHECKSUM, EMPTY_BYTE_ARRAY);
            writer.writeCommitEntry(LATEST_KERNEL_VERSION, transactionId, commitTimestamp);
            channel.getCurrentLogPosition(marker);

            return true;
        });

        RecoveryMonitor monitor = mock(RecoveryMonitor.class);
        var startupController = mock(DatabaseStartupController.class);
        var databaseId = from("db", randomUUID());
        when(startupController.shouldAbortStartup()).thenReturn(false, true);
        var recoveryStartupChecker = new RecoveryStartupChecker(startupController, databaseId);
        var logsTruncator = mock(CorruptedLogsTruncator.class);

        assertThatThrownBy(() -> recovery(storeDir, recoveryStartupChecker))
                .rootCause()
                .isInstanceOf(DatabaseStartAbortedException.class);

        verify(logsTruncator, never()).truncate(any(), any());
        verify(monitor, never()).recoveryCompleted(anyLong(), any(RecoveryMode.class));
    }

    private boolean recovery(Path storeDir) throws IOException {
        return recovery(storeDir, EMPTY_CHECKER);
    }

    private boolean recovery(Path storeDir, RecoveryStartupChecker startupChecker) throws IOException {
        LifeSupport life = new LifeSupport();
        var contextFactory = new CursorContextFactory(NULL, EMPTY_CONTEXT_SUPPLIER);

        final AtomicBoolean recoveryRequired = new AtomicBoolean();
        RecoveryMonitor monitor = new RecoveryMonitor() {
            @Override
            public void recoveryRequired(RecoveryStartInformation recoveryStartInfo) {
                recoveryRequired.set(true);
            }
        };
        try {
            var logFiles = buildLogFiles();
            life.add(logFiles);
            StorageEngine storageEngine = mock(StorageEngine.class);
            when(storageEngine.createStorageCursors(any())).thenReturn(mock(StoreCursors.class));
            Config config = Config.defaults();

            TransactionMetadataCache metadataCache = new TransactionMetadataCache();
            LogicalTransactionStore txStore = new PhysicalLogicalTransactionStore(
                    logFiles, metadataCache, TestCommandReaderFactory.INSTANCE, monitors, false, config, fileSystem);
            CorruptedLogsTruncator logPruner = new CorruptedLogsTruncator(storeDir, logFiles, fileSystem, INSTANCE);
            monitors.addMonitorListener(monitor);
            life.add(new TransactionLogsRecovery(
                    new DefaultRecoveryService(
                            storageEngine,
                            transactionIdStore,
                            txStore,
                            versionRepository,
                            logFiles,
                            LatestVersions.LATEST_KERNEL_VERSION_PROVIDER,
                            NO_MONITOR,
                            mock(InternalLog.class),
                            Clocks.systemClock(),
                            false,
                            LatestVersions.BINARY_VERSIONS,
                            contextFactory),
                    logPruner,
                    schemaLife,
                    monitor,
                    ProgressMonitorFactory.NONE,
                    false,
                    startupChecker,
                    RecoveryPredicate.ALL,
                    true,
                    contextFactory,
                    RecoveryMode.FULL));

            life.start();
        } finally {
            life.shutdown();
        }
        return recoveryRequired.get();
    }

    private void writeSomeData(Path file, Visitor<DataWriters, IOException> visitor) throws IOException {

        try (var versionedStoreChannel = new PhysicalLogVersionedStoreChannel(
                        fileSystem.write(file),
                        logVersion,
                        LATEST_LOG_FORMAT,
                        file,
                        EMPTY_ACCESSOR,
                        DatabaseTracer.NULL);
                var writableLogChannel =
                        new PhysicalFlushableLogPositionAwareChannel(versionedStoreChannel, null, INSTANCE)) {
            writeLogHeader(
                    versionedStoreChannel,
                    LATEST_LOG_FORMAT.newHeader(
                            logVersion, 3L, storeId, UNKNOWN_LOG_SEGMENT_SIZE, BASE_TX_CHECKSUM, LATEST_KERNEL_VERSION),
                    INSTANCE);
            writableLogChannel.beginChecksumForWriting();
            LogEntryWriter<?> first = new LogEntryWriter<>(writableLogChannel, LatestVersions.BINARY_VERSIONS);
            visitor.visit(new DataWriters(first, writableLogChannel));
        }
    }

    private record DataWriters(LogEntryWriter<?> writer, FlushableLogPositionAwareChannel channel) {}

    private LogFiles buildLogFiles() throws IOException {
        return LogFilesBuilder.builder(databaseLayout, fileSystem, LatestVersions.LATEST_KERNEL_VERSION_PROVIDER)
                .withLogVersionRepository(logVersionRepository)
                .withTransactionIdStore(transactionIdStore)
                .withAppendIndexProvider(appendIndexProvider)
                .withCommandReaderFactory(TestCommandReaderFactory.INSTANCE)
                .withStoreId(storeId)
                .withConfig(Config.newBuilder()
                        .set(GraphDatabaseInternalSettings.fail_on_corrupted_log_files, false)
                        .build())
                .build();
    }

    private static final class LogsRecoveryMonitor implements RecoveryMonitor {
        private int batchCounter;
        private boolean recoveryRequired;

        @Override
        public void batchRecovered(CommittedCommandBatch committedBatch) {
            batchCounter++;
        }

        @Override
        public void recoveryRequired(RecoveryStartInformation recoveryStartInfo) {
            recoveryRequired = true;
        }

        public boolean isRecoveryRequired() {
            return recoveryRequired;
        }

        public int recoveredBatches() {
            return batchCounter;
        }
    }
}
