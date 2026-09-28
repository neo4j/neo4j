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
package org.neo4j.kernel.api;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Duration.ofDays;
import static java.util.OptionalLong.empty;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.collection.Dependencies.dependenciesOf;
import static org.neo4j.configuration.GraphDatabaseSettings.CheckpointPolicy.PERIODIC;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.AppendIndexProvider.UNKNOWN_APPEND_INDEX;
import static org.neo4j.storageengine.api.LogVersionRepository.UNKNOWN_LOG_OFFSET;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION_WITHOUT_ENVELOPES;
import static org.neo4j.test.LatestVersions.LATEST_LOG_FORMAT;
import static org.neo4j.test.LatestVersions.LATEST_RUNTIME_VERSION_WITHOUT_ENVELOPES;
import static org.neo4j.test.extension.SkipOnSpd.Note.incompatible;
import static org.neo4j.wal.entry.LogSegments.DEFAULT_LOG_SEGMENT_SIZE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.database.DbmsRuntimeVersion;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.version.VersionStorageTracer;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.api.database.transaction.LogChannel;
import org.neo4j.kernel.api.database.transaction.TransactionLogChannels;
import org.neo4j.kernel.api.database.transaction.TransactionLogService;
import org.neo4j.kernel.availability.AvailabilityRequirement;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.api.tracer.DefaultDatabaseTracer;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.kernel.impl.transaction.tracing.StoreApplyEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionRollbackEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionWriteEvent;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.lock.LockTracer;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.LogMetadataProvider;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;
import org.neo4j.test.extension.SkipOnSpd;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.util.concurrent.BinaryLatch;
import org.neo4j.wal.LogAppendEvent;
import org.neo4j.wal.LogFile;
import org.neo4j.wal.LogPosition;
import org.neo4j.wal.LogicalTransactionStore;
import org.neo4j.wal.ReadableLogChannel;
import org.neo4j.wal.TransactionMetadataCache;
import org.neo4j.wal.WALService;
import org.neo4j.wal.checkpoint.CheckPointer;
import org.neo4j.wal.checkpoint.SimpleTriggerInfo;
import org.neo4j.wal.entry.LogFormat;
import org.neo4j.wal.entry.LogHeader;
import org.neo4j.wal.files.LogRangeInfo;
import org.neo4j.wal.files.LogTailInformation;
import org.neo4j.wal.files.TransactionLogFile;
import org.neo4j.wal.files.checkpoint.CheckpointLogFile;
import org.neo4j.wal.rotation.monitor.LogRotationMonitorAdapter;

@DbmsExtension(configurationCallback = "configure")
@RandomSupportExtension
class TransactionLogServiceIT {
    private static final long THRESHOLD = kibiBytes(256);

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private GraphDatabaseAPI databaseAPI;

    @Inject
    private TransactionLogService logService;

    @Inject
    private WALService walService;

    @Inject
    private CheckPointer checkPointer;

    @Inject
    private LogicalTransactionStore transactionStore;

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private LogMetadataProvider metadataProvider;

    @Inject
    private DatabaseAvailabilityGuard availabilityGuard;

    @Inject
    private RandomSupport random;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {

        var tracers = new InjectableBeforeApplyTracers();

        builder.setExternalDependencies(dependenciesOf(tracers));

        builder.setConfig(GraphDatabaseSettings.logical_log_rotation_threshold, THRESHOLD)
                .setConfig(GraphDatabaseSettings.check_point_policy, PERIODIC)
                .setConfig(GraphDatabaseSettings.check_point_interval_time, ofDays(10))
                .setConfig(GraphDatabaseSettings.keep_logical_logs, "2 files");
    }

    @Test
    void rotationDuringTransactionLogReadingKeepNonAffectedChannelsOpen() throws IOException {
        var propertyValue = random.nextAsciiStringOfLength((int) THRESHOLD);

        // execute test transaction to create any tokens to avoid tx ids tricks
        createNodeInIsolatedTransaction("any");
        int numberOfTransactions = 30;
        long lastAppendIndexBeforeWorkload = metadataProvider.getLastAppendIndex();
        for (int i = 0; i < numberOfTransactions; i++) {
            createNodeInIsolatedTransaction(propertyValue);
        }

        try (TransactionLogChannels logReaders = logService.logFilesChannels(lastAppendIndexBeforeWorkload + 30)) {
            List<LogChannel> logFileChannels = logReaders.getChannels();
            // Tx split over several files with envelopes
            int expectedLogChannelsSize = LogFormat.fromConfigAndKernelVersion(Config.defaults(), LATEST_KERNEL_VERSION)
                            .usesSegments()
                    ? 3
                    : 1;
            assertThat(logFileChannels).hasSize(expectedLogChannelsSize);
            assertThat(walService.logFiles()).hasSizeGreaterThanOrEqualTo(numberOfTransactions);

            checkPointer.forceCheckPoint(new SimpleTriggerInfo("Test checkpoint"));

            // 2 desired non-empty tx log files + 1 newly rotated empty, 1 checkpoint log
            // or in the case of envelopes: 3 non-empty files to get at least one whole chunk, and 1 checkpoint log
            assertThat(walService.logFiles()).hasSize(4);

            for (LogChannel logChannel : logFileChannels) {
                StoreChannel channel = logChannel.channel();
                assertTrue(channel.isOpen());
                assertDoesNotThrow(channel::size);
            }
        }
    }

    @Test
    void rotationDuringTransactionLogReading() throws IOException {
        var propertyValue = random.nextAsciiStringOfLength((int) THRESHOLD);

        int numberOfTransactions = 30;
        for (int i = 0; i < numberOfTransactions; i++) {
            createNodeInIsolatedTransaction(propertyValue);
        }

        try (TransactionLogChannels logReaders = logService.logFilesChannels(2)) {
            List<LogChannel> logFileChannels = logReaders.getChannels();
            assertThat(logFileChannels).hasSizeGreaterThanOrEqualTo(numberOfTransactions);
            assertThat(walService.logFiles()).hasSizeGreaterThanOrEqualTo(numberOfTransactions);

            checkPointer.forceCheckPoint(new SimpleTriggerInfo("Test checkpoint"));

            // 2 desired non-empty tx log files + 1 newly rotated empty, 1 checkpoint log
            // or in the case of envelopes: 3 non-empty files to get at least one whole chunk, and 1 checkpoint log
            int txLogsAfterCheckpoint = 3;
            // the transaction log service did not return the last (empty) transaction log file
            var visibleTxLogsAfterCheckpoints =
                    LogFormat.fromConfigAndKernelVersion(Config.defaults(), LATEST_KERNEL_VERSION)
                                    .usesSegments()
                            ? txLogsAfterCheckpoint
                            : txLogsAfterCheckpoint - 1;
            int checkpointLogs = 1;

            assertThat(walService.logFiles()).hasSize(txLogsAfterCheckpoint + checkpointLogs);

            var closedChannels = logFileChannels.subList(0, logFileChannels.size() - visibleTxLogsAfterCheckpoints);
            var openChannels = logFileChannels.subList(
                    logFileChannels.size() - visibleTxLogsAfterCheckpoints, logFileChannels.size());
            assertEquals(closedChannels.size() + openChannels.size(), logFileChannels.size());

            for (LogChannel logChannel : closedChannels) {
                StoreChannel channel = logChannel.channel();
                assertFalse(channel.isOpen());
                assertThrows(ClosedChannelException.class, channel::size);
            }
            for (LogChannel logChannel : openChannels) {
                StoreChannel channel = logChannel.channel();
                assertTrue(channel.isOpen());
                assertDoesNotThrow(channel::size);
            }
        }
    }

    @Test
    void closingReadersDoesAutomaticCleanup() throws Exception {
        var propertyValue = random.nextAsciiStringOfLength((int) THRESHOLD);

        int numberOfTransactions = 30;
        for (int i = 0; i < numberOfTransactions; i++) {
            createNodeInIsolatedTransaction(propertyValue);
        }

        try (TransactionLogChannels logReaders = logService.logFilesChannels(2)) {
            List<LogChannel> logFileChannels = logReaders.getChannels();
            assertThat(logFileChannels).hasSizeGreaterThanOrEqualTo(numberOfTransactions);
            assertThat(walService.logFiles()).hasSizeGreaterThanOrEqualTo(numberOfTransactions);

            assertThat(((TransactionLogFile) walService.getLogFile()).getExternalFileReaders())
                    .isNotEmpty();
        }

        assertThat(((TransactionLogFile) walService.getLogFile()).getExternalFileReaders())
                .isEmpty();
    }

    @Test
    void requestLogFileChannelsOfInvalidTransactions() {
        assertThrows(IllegalArgumentException.class, () -> logService.logFilesChannels(-1));
        assertThrows(IllegalArgumentException.class, () -> logService.logFilesChannels(100));
    }

    @Test
    void requireDirectByteBufferForLogFileAppending() {
        availabilityGuard.require(new AvailabilityRequirement("Database unavailable"));

        assertThrows(
                IllegalArgumentException.class,
                () -> logService.append(
                        ByteBuffer.allocate(5),
                        empty(),
                        Optional.of(LATEST_KERNEL_VERSION.version()),
                        BASE_TX_CHECKSUM,
                        UNKNOWN_LOG_OFFSET,
                        Optional.of(LATEST_LOG_FORMAT.getVersionByte())));
    }

    @Test
    void failBulkAppendOnNonAvailableDatabase() {
        assertThrows(
                IllegalStateException.class,
                () -> logService.append(
                        ByteBuffer.wrap(new byte[] {1, 2, 3, 4, 5}),
                        empty(),
                        Optional.of(LATEST_KERNEL_VERSION.version()),
                        BASE_TX_CHECKSUM,
                        UNKNOWN_LOG_OFFSET,
                        Optional.of(LATEST_LOG_FORMAT.getVersionByte())));
    }

    @Test
    void bulkAppendToTransactionLogsDoesNotChangeLastCommittedTransactionOffset() throws IOException {
        availabilityGuard.require(new AvailabilityRequirement("Database unavailable"));

        var metadataBefore = metadataProvider.getHighestGapFreeClosedTransaction();
        var buffer = createBuffer().put(new byte[] {1, 2, 3, 4, 5});
        try {
            for (int i = 0; i < 100; i++) {
                buffer.rewind();
                logService.append(
                        buffer,
                        empty(),
                        Optional.of(LATEST_KERNEL_VERSION.version()),
                        BASE_TX_CHECKSUM,
                        UNKNOWN_LOG_OFFSET,
                        Optional.of(LATEST_LOG_FORMAT.getVersionByte()));
            }
        } finally {
            ByteBuffers.releaseBuffer(buffer, INSTANCE);
        }

        assertEquals(metadataBefore, metadataProvider.getHighestGapFreeClosedTransaction());
    }

    @Test
    void bulkAppendWithRotationDoesNotChangeLastClosedMetadata() throws IOException {
        availabilityGuard.require(new AvailabilityRequirement("Database unavailable"));

        var metadataBefore = metadataProvider.getHighestGapFreeClosedTransaction();
        long logVersionBefore = metadataProvider.getCurrentLogVersion();

        int appendIterations = 100;
        var appendData = createBuffer()
                .put(random.nextAsciiStringOfLength((int) (THRESHOLD + 1)).getBytes(UTF_8));
        try {
            for (int i = 0; i < appendIterations; i++) {
                logService.append(
                        appendData,
                        OptionalLong.of(i + 7),
                        Optional.of(LATEST_KERNEL_VERSION.version()),
                        BASE_TX_CHECKSUM,
                        UNKNOWN_LOG_OFFSET,
                        Optional.of(LATEST_LOG_FORMAT.getVersionByte()));
                appendData.rewind();
            }
        } finally {
            ByteBuffers.releaseBuffer(appendData, INSTANCE);
        }

        assertEquals(metadataBefore, metadataProvider.getHighestGapFreeClosedTransaction());

        // pruning is also not here since metadata store is not upgraded
        Path[] matchedFiles = walService.getLogFile().getMatchedFiles();
        assertThat(matchedFiles).hasSize((int) (logVersionBefore + appendIterations));
    }

    @Test
    void bulkAppendWithRotationUpdatesMetadataProviderLogVersion() throws IOException {
        availabilityGuard.require(new AvailabilityRequirement("Database unavailable"));

        long logVersionBefore = metadataProvider.getCurrentLogVersion();

        int appendIterations = 100;
        var appendData = createBuffer()
                .put(random.nextAsciiStringOfLength((int) (THRESHOLD + 1)).getBytes(UTF_8));
        try {
            for (int i = 0; i < appendIterations; i++) {
                logService.append(
                        appendData,
                        OptionalLong.of(i + 7),
                        Optional.of(LATEST_KERNEL_VERSION.version()),
                        BASE_TX_CHECKSUM,
                        UNKNOWN_LOG_OFFSET,
                        Optional.of(LATEST_LOG_FORMAT.getVersionByte()));
                appendData.rewind();
            }
        } finally {
            ByteBuffers.releaseBuffer(appendData, INSTANCE);
        }

        var logVersionAfter = metadataProvider.getCurrentLogVersion();
        assertThat(logVersionAfter)
                .isEqualTo(logVersionBefore + appendIterations - 1)
                .isNotEqualTo(logVersionBefore);
    }

    @Test
    void bulkAppendRotatedLogFilesHaveCorrectSupplierTransactionsFromHeader() throws IOException {
        availabilityGuard.require(new AvailabilityRequirement("Database unavailable"));

        long logVersionBefore = metadataProvider.getCurrentLogVersion();

        int appendIterations = 100;
        int indexShift = 10;
        var appendData = createBuffer()
                .put(random.nextAsciiStringOfLength((int) (THRESHOLD + 1)).getBytes(UTF_8));
        try {
            for (int i = 0; i < appendIterations; i++) {
                logService.append(
                        appendData,
                        OptionalLong.of(indexShift + i),
                        Optional.of(LATEST_KERNEL_VERSION.version()),
                        BASE_TX_CHECKSUM,
                        UNKNOWN_LOG_OFFSET,
                        Optional.of(LATEST_LOG_FORMAT.getVersionByte()));
                appendData.rewind();
            }
        } finally {
            ByteBuffers.releaseBuffer(appendData, INSTANCE);
        }

        LogFile logFile = walService.getLogFile();
        for (int version = (int) logVersionBefore + 1; version < logVersionBefore + appendIterations; version++) {
            assertEquals(
                    indexShift - 1 + version, logFile.extractHeader(version).getLastAppendIndex());
        }
    }

    @Test
    void bulkAppendForEnvelopesRotatesOnNewFileOnSender() throws IOException {
        // Only interesting for envelopes
        assumeTrue(LATEST_LOG_FORMAT.usesSegments());
        // Currently calls with append index represent first append or new file
        availabilityGuard.require(new AvailabilityRequirement("Database unavailable"));

        LogFile logFile = walService.getLogFile();
        LogPosition startPosition = logFile.getTransactionLogWriter().getCurrentPosition();

        int dataSize = DEFAULT_LOG_SEGMENT_SIZE / 2;
        var appendData = ByteBuffers.allocateDirect(dataSize, ByteOrder.LITTLE_ENDIAN, INSTANCE)
                .put(random.nextAsciiStringOfLength(dataSize).getBytes(UTF_8))
                .rewind();
        try {
            logService.append(
                    appendData,
                    OptionalLong.of(1),
                    Optional.of(LATEST_KERNEL_VERSION.version()),
                    BASE_TX_CHECKSUM,
                    UNKNOWN_LOG_OFFSET,
                    Optional.of(LATEST_LOG_FORMAT.getVersionByte()));
            appendData.rewind();

            logService.append(
                    appendData,
                    OptionalLong.of(2),
                    Optional.of(LATEST_KERNEL_VERSION.version()),
                    BASE_TX_CHECKSUM,
                    DEFAULT_LOG_SEGMENT_SIZE,
                    Optional.of(LATEST_LOG_FORMAT.getVersionByte()));
        } finally {
            ByteBuffers.releaseBuffer(appendData, INSTANCE);
        }

        LogRangeInfo logRangeInfo = logFile.getLogRangeInfo();
        assertThat(logRangeInfo.highestVersion()).isEqualTo(logRangeInfo.lowestVersion() + 1);
        assertThat(Files.size(logFile.getLogFileForVersion(logRangeInfo.lowestVersion())))
                .isEqualTo(startPosition.getByteOffset() + dataSize);
        assertThat(logFile.getTransactionLogWriter().getCurrentPosition())
                .isEqualTo(new LogPosition(logRangeInfo.highestVersion(), DEFAULT_LOG_SEGMENT_SIZE + dataSize));
    }

    @Test
    void bulkAppendForEnvelopesDoesntRotateOnNewFileOnSenderIfAlreadyRotated() throws IOException {
        // Only interesting for envelopes
        assumeTrue(LATEST_LOG_FORMAT.usesSegments());
        // Currently calls with append index represent first append or new file
        availabilityGuard.require(new AvailabilityRequirement("Database unavailable"));

        LogFile logFile = walService.getLogFile();
        logFile.rotate();

        LogRangeInfo logRangeInfo = logFile.getLogRangeInfo();
        assertThat(logRangeInfo.highestVersion()).isEqualTo(logRangeInfo.lowestVersion() + 1);

        int dataSize = DEFAULT_LOG_SEGMENT_SIZE / 2;
        var appendData = ByteBuffers.allocateDirect(dataSize, ByteOrder.LITTLE_ENDIAN, INSTANCE)
                .put(random.nextAsciiStringOfLength(dataSize).getBytes(UTF_8))
                .rewind();
        try {
            logService.append(
                    appendData,
                    OptionalLong.of(1),
                    Optional.of(LATEST_KERNEL_VERSION.version()),
                    BASE_TX_CHECKSUM,
                    DEFAULT_LOG_SEGMENT_SIZE,
                    Optional.of(LATEST_LOG_FORMAT.getVersionByte()));
        } finally {
            ByteBuffers.releaseBuffer(appendData, INSTANCE);
        }

        logRangeInfo = logFile.getLogRangeInfo();
        assertThat(logRangeInfo.highestVersion()).isEqualTo(logRangeInfo.lowestVersion() + 1);
        assertThat(logFile.getTransactionLogWriter().getCurrentPosition())
                .isEqualTo(new LogPosition(logRangeInfo.highestVersion(), DEFAULT_LOG_SEGMENT_SIZE + dataSize));
    }

    @Test
    void bulkAppendRotatedLogFilesMonitorEvents() throws IOException {
        availabilityGuard.require(new AvailabilityRequirement("Database unavailable"));

        BulkAppendLogRotationMonitor monitorListener = new BulkAppendLogRotationMonitor();
        databaseAPI.getDependencyResolver().resolveDependency(Monitors.class).addMonitorListener(monitorListener);

        int appendIterations = 100;
        int transactionalShift = 10;
        var appendData = createBuffer()
                .put(random.nextAsciiStringOfLength((int) (THRESHOLD + 1)).getBytes(UTF_8));
        try {
            for (int i = 0; i < appendIterations; i++) {
                logService.append(
                        appendData,
                        OptionalLong.of(transactionalShift + i),
                        Optional.of(LATEST_KERNEL_VERSION.version()),
                        BASE_TX_CHECKSUM,
                        UNKNOWN_LOG_OFFSET,
                        Optional.of(LATEST_LOG_FORMAT.getVersionByte()));
                appendData.rewind();
            }
        } finally {
            ByteBuffers.releaseBuffer(appendData, INSTANCE);
        }

        List<Long> observedVersions = monitorListener.getObservedVersions();
        assertThat(observedVersions)
                .hasSize(99)
                .containsExactlyElementsOf(LongStream.range(0, 99).boxed().collect(Collectors.toList()));
    }

    @Test
    void bulkAppendRotatedLogFilesTracingEvents() throws IOException {
        availabilityGuard.require(new AvailabilityRequirement("Database unavailable"));

        DatabaseTracers databaseTracers = databaseAPI.getDependencyResolver().resolveDependency(DatabaseTracers.class);
        assertEquals(0, databaseTracers.getDatabaseTracer().numberOfLogRotations());

        int appendIterations = 100;
        int transactionalShift = 10;
        var appendData = createBuffer()
                .put(random.nextAsciiStringOfLength((int) (THRESHOLD + 1)).getBytes(UTF_8));
        try {
            for (int i = 0; i < appendIterations; i++) {
                logService.append(
                        appendData,
                        OptionalLong.of(transactionalShift + i),
                        Optional.of(LATEST_KERNEL_VERSION.version()),
                        BASE_TX_CHECKSUM,
                        UNKNOWN_LOG_OFFSET,
                        Optional.of(LATEST_LOG_FORMAT.getVersionByte()));
                appendData.rewind();
            }
        } finally {
            ByteBuffers.releaseBuffer(appendData, INSTANCE);
        }

        // first append is not rotated
        var expectedRotations = appendIterations - 1;
        assertEquals(expectedRotations, databaseTracers.getDatabaseTracer().numberOfLogRotations());
    }

    @Test
    void restoreOnCurrentLogVersion() throws IOException {
        availabilityGuard.require(new AvailabilityRequirement("Database unavailable"));
        long logVersionBefore = metadataProvider.getCurrentLogVersion();

        int appendIterations = 100;
        LogPosition previousPosition = null;
        var appendData = createBuffer()
                .put(random.nextAsciiStringOfLength((int) (THRESHOLD + 1)).getBytes(UTF_8));
        try {
            for (int i = 0; i < appendIterations; i++) {
                var position = logService.append(
                        appendData,
                        OptionalLong.empty(),
                        Optional.of(LATEST_KERNEL_VERSION.version()),
                        BASE_TX_CHECKSUM,
                        UNKNOWN_LOG_OFFSET,
                        Optional.of(LATEST_LOG_FORMAT.getVersionByte()));
                if (previousPosition != null) {
                    assertEquals(previousPosition, position);
                }
                logService.restore(position);
                previousPosition = position;
                appendData.rewind();
            }
        } finally {
            ByteBuffers.releaseBuffer(appendData, INSTANCE);
        }

        assertEquals(logVersionBefore, walService.getLogFile().getLogRangeInfo().highestVersion());
    }

    @Test
    void restoreInitialLogVersionAndAppend() throws IOException {
        availabilityGuard.require(new AvailabilityRequirement("Database unavailable"));
        long logVersionBefore = metadataProvider.getCurrentLogVersion();

        var appendData = createBuffer()
                .put(random.nextAsciiStringOfLength((int) (THRESHOLD + 1)).getBytes(UTF_8));
        try {
            int appendIterations = 100;
            LogPosition firstPosition = null;
            for (int i = 0; i < appendIterations; i++) {
                var position = logService.append(
                        appendData,
                        OptionalLong.of(i + 5),
                        Optional.of(LATEST_KERNEL_VERSION.version()),
                        BASE_TX_CHECKSUM,
                        UNKNOWN_LOG_OFFSET,
                        Optional.of(LATEST_LOG_FORMAT.getVersionByte()));
                if (firstPosition == null) {
                    firstPosition = position;
                }
                appendData.rewind();
            }
            assertThat(walService.getLogFile().getLogRangeInfo().highestVersion())
                    .isGreaterThanOrEqualTo(firstPosition.getLogVersion());
            logService.restore(firstPosition);

            assertEquals(
                    firstPosition,
                    logService.append(
                            appendData,
                            OptionalLong.of(5),
                            Optional.of(LATEST_KERNEL_VERSION.version()),
                            BASE_TX_CHECKSUM,
                            UNKNOWN_LOG_OFFSET,
                            Optional.of(LATEST_LOG_FORMAT.getVersionByte())));
            assertEquals(
                    logVersionBefore, walService.getLogFile().getLogRangeInfo().highestVersion());
        } finally {
            ByteBuffers.releaseBuffer(appendData, INSTANCE);
        }
    }

    @Test
    void logFileChannelsAreNonWritable() throws IOException {
        createNodeInIsolatedTransaction("a");
        createNodeInIsolatedTransaction("b");
        createNodeInIsolatedTransaction("c");

        try (TransactionLogChannels logReaders = logService.logFilesChannels(2)) {
            List<LogChannel> logFileChannels = logReaders.getChannels();
            assertThat(logFileChannels).hasSize(1);

            LogChannel channel = logFileChannels.getFirst();
            assertEquals(2, channel.startAppendIndex());

            StoreChannel storeChannel = channel.channel();
            assertThrows(
                    UnsupportedOperationException.class,
                    () -> storeChannel.writeAll(ByteBuffers.allocate(1, ByteOrder.LITTLE_ENDIAN, INSTANCE)));
            assertThrows(
                    UnsupportedOperationException.class,
                    () -> storeChannel.writeAll(ByteBuffers.allocate(1, ByteOrder.LITTLE_ENDIAN, INSTANCE), 1));
            assertThrows(UnsupportedOperationException.class, () -> storeChannel.truncate(1));
            assertThrows(
                    UnsupportedOperationException.class,
                    () -> storeChannel.write(ByteBuffers.allocate(1, ByteOrder.LITTLE_ENDIAN, INSTANCE)));
            assertThrows(UnsupportedOperationException.class, () -> storeChannel.write(new ByteBuffer[] {}, 1, 1));
            assertThrows(UnsupportedOperationException.class, () -> storeChannel.write(new ByteBuffer[] {}));
        }
    }

    @Test
    void setsStartingTransactionIdCorrectlyForAllFiles() throws IOException {
        var propertyValue = random.nextAsciiStringOfLength((int) THRESHOLD / 2);

        int numberOfTransactions = 40;
        for (int i = 0; i < numberOfTransactions; i++) {
            createNodeInIsolatedTransaction(propertyValue);
        }

        int initialAppendIndex = 17;
        try (TransactionLogChannels logReaders = logService.logFilesChannels(initialAppendIndex)) {
            List<LogChannel> logFileChannels = logReaders.getChannels();
            assertThat(logFileChannels).hasSizeGreaterThanOrEqualTo(14);
            checkChannelsCoverage(
                    logFileChannels,
                    initialAppendIndex,
                    databaseAPI
                            .getDependencyResolver()
                            .resolveDependency(TransactionIdStore.class)
                            .getLastCommittedTransactionId());

            long prevLastTxId = -1;
            for (LogChannel logChannel : logFileChannels) {
                if (prevLastTxId != -1) {
                    assertThat(logChannel.startAppendIndex()).isEqualTo(prevLastTxId + 1);
                }
                prevLastTxId = logChannel.lastAppendIndex();
            }
        }
    }

    @Test
    void setsLastTransactionIdCorrectlyForAllFiles() throws IOException {
        var propertyValue = random.nextAsciiStringOfLength((int) THRESHOLD / 2);

        int numberOfTransactions = 40;
        for (int i = 0; i < numberOfTransactions; i++) {
            createNodeInIsolatedTransaction(propertyValue);
        }

        int initialAppendIndex = 17;
        try (TransactionLogChannels logReaders = logService.logFilesChannels(initialAppendIndex)) {
            List<LogChannel> logFileChannels = logReaders.getChannels();
            assertThat(logFileChannels).hasSizeGreaterThanOrEqualTo(14);
            checkChannelsCoverage(
                    logFileChannels,
                    initialAppendIndex,
                    databaseAPI
                            .getDependencyResolver()
                            .resolveDependency(TransactionIdStore.class)
                            .getLastCommittedTransactionId());

            long prevLastTxId = -1;
            for (LogChannel logChannel : logFileChannels) {
                if (prevLastTxId != -1) {
                    assertThat(prevLastTxId).isEqualTo(logChannel.startAppendIndex() - 1);
                }
                prevLastTxId = logChannel.lastAppendIndex();
            }
        }
    }

    @Test
    void endOffsetPositionedToEndOfFile() throws IOException {
        var propertyValue = random.nextAsciiStringOfLength((int) THRESHOLD);

        int numberOfTransactions = 30;
        for (int i = 0; i < numberOfTransactions; i++) {
            createNodeInIsolatedTransaction(propertyValue);
        }
        try (TransactionLogChannels logReaders = logService.logFilesChannels(2)) {

            var channels = logReaders.getChannels();
            var fullChannels = channels.subList(0, channels.size() - 1);

            for (LogChannel fullChannel : fullChannels) {
                assertThat(fullChannel.endOffset())
                        .isEqualTo(fullChannel.channel().size());
            }
        }
    }

    @Test
    @SkipOnSpd(notes = incompatible, reason = "Incompatible running on a cluster")
    void endOffsetPositionedToLastCommittedTransaction() throws Exception {
        createNodeInIsolatedTransaction("some prop value");
        // This test ensures that we return the use the last committed transaction, not the last closed transaction as
        // the upper bound when we retrieve channels.

        var txIsCommitted = new BinaryLatch();
        var canCloseTx = new BinaryLatch();

        try (OtherThreadExecutor e1 = new OtherThreadExecutor("tx taking a long time to apply")) {

            // lock next applying transaction on canCloseTx so that it is committed but not closed
            InjectableBeforeApplyTracers.InjectableBeforeApplyTxWriteEvent.INSTANCE.beforeStoreApply.set(() -> {
                txIsCommitted.release();
                canCloseTx.await();
            });

            var initialLastCommittedTx = metadataProvider.getLastCommittedTransactionId();
            var initialLastClosedTx = metadataProvider.getHighestGapFreeClosedTransactionId();

            var hasAppliedTx = e1.executeDontWait(() -> {
                try (var tx = databaseAPI.beginTx()) {
                    tx.createNode();
                    tx.commit();
                }
                return null;
            });

            try {
                txIsCommitted.await();

                // commit a transaction after the current opened one:
                createNodeInIsolatedTransaction("some prop value");

                // then we should have the last committed after the last closed:
                var lastCommittedTransaction = metadataProvider.getLastCommittedTransactionId();
                var lastAppendIndex = metadataProvider.getLastAppendIndex();
                var lastClosedTx = metadataProvider.getHighestGapFreeClosedTransactionId();
                assertThat(lastClosedTx).isEqualTo(initialLastClosedTx);
                assertThat(lastAppendIndex).isEqualTo(initialLastCommittedTx + 2);

                // when we get the channels starting at the last committed transaction:
                try (TransactionLogChannels logReaders = logService.logFilesChannels(lastAppendIndex)) {
                    var channels = logReaders.getChannels();
                    assertThat(channels).hasSize(1);
                    var channel = channels.getFirst();
                    // they should include only the last committed transaction (not the last closed)
                    assertThat(channel.lastAppendIndex()).isEqualTo(lastCommittedTransaction);
                    assertThat(channel.startAppendIndex()).isEqualTo(lastCommittedTransaction);
                    assertThat(channel.endOffset()).isEqualTo(getTxEndOffset(lastCommittedTransaction));
                }
            } finally {
                canCloseTx.release();
                hasAppliedTx.get(1, TimeUnit.MINUTES);
            }
        }
    }

    @Test
    void firstLogChannelIsProperlyPositionedToInitialTransaction() throws IOException {
        int numberOfTransactions = 20;
        for (int i = 0; i < numberOfTransactions; i++) {
            createNodeInIsolatedTransaction("abc");
        }

        verifyReportedPositions(2, getTxStartOffset(2));
        verifyReportedPositions(3, getTxStartOffset(3));
        verifyReportedPositions(4, getTxStartOffset(4));
        verifyReportedPositions(5, getTxStartOffset(5));
        verifyReportedPositions(15, getTxStartOffset(15));
    }

    @Test
    void setsPreviousChecksumCorrectlyForEnvelopedLogs() throws IOException {
        Path directory = testDirectory.directory("home-dir");
        try (var dbms = new TestDatabaseManagementServiceBuilder(directory)
                .setConfig(Map.of(
                        GraphDatabaseInternalSettings.latest_kernel_version,
                        KernelVersion.GLORIOUS_FUTURE.version(),
                        GraphDatabaseInternalSettings.latest_runtime_version,
                        DbmsRuntimeVersion.GLORIOUS_FUTURE.getVersion(),
                        GraphDatabaseInternalSettings.envelope_log_format_on_future,
                        true))
                .build()) {

            GraphDatabaseAPI database = (GraphDatabaseAPI) dbms.database(DEFAULT_DATABASE_NAME);
            var propertyValue = random.nextAsciiStringOfLength((int) THRESHOLD / 16);

            int numberOfTransactions = 32;
            for (int i = 0; i < numberOfTransactions; i++) {
                try (var tx = database.beginTx()) {
                    Node node = tx.createNode();
                    node.setProperty("a", propertyValue);
                    tx.commit();
                }
            }

            TransactionLogService logService =
                    database.getDependencyResolver().resolveDependency(TransactionLogService.class);
            WALService walService = database.getDependencyResolver().resolveDependency(WALService.class);
            TransactionMetadataCache metadataCache =
                    database.getDependencyResolver().resolveDependency(TransactionMetadataCache.class);
            int initialAppendIndex = 17;

            TransactionMetadataCache.TransactionMetadata transactionMetadata =
                    metadataCache.getTransactionMetadata(initialAppendIndex);
            LogPosition logPosition = transactionMetadata.startPosition();
            long logVersion = logPosition.getLogVersion();

            try (TransactionLogChannels logReaders = logService.logFilesChannels(initialAppendIndex)) {
                List<LogChannel> logFileChannels = logReaders.getChannels();
                assertThat(logFileChannels).hasSize(4);

                for (LogChannel logChannel : logFileChannels) {
                    LogHeader logHeader = walService.getLogFile().extractHeader(logVersion);
                    int expectedChecksum = logVersion != logPosition.getLogVersion()
                            ? logHeader.getPreviousLogFileChecksum()
                            : getPrevChecksumFromPosition(logPosition, walService);
                    assertThat(logChannel.previousChecksum()).isEqualTo(expectedChecksum);
                    assertThat(logChannel.kernelVersion()).isEqualTo(logHeader.getKernelVersion());
                    logVersion++;
                }
            }
        }
    }

    @Test
    void setsPreviousChecksumCorrectlyForNonEnvelopedLogs() throws IOException {
        Path directory = testDirectory.directory("home-dir2");
        try (var dbms = new TestDatabaseManagementServiceBuilder(directory)
                .setConfig(Map.of(
                        GraphDatabaseInternalSettings.latest_kernel_version,
                        LATEST_KERNEL_VERSION_WITHOUT_ENVELOPES.version(),
                        GraphDatabaseInternalSettings.latest_runtime_version,
                        LATEST_RUNTIME_VERSION_WITHOUT_ENVELOPES.getVersion(),
                        GraphDatabaseInternalSettings.allow_new_log_format_on_upgrade_or_create,
                        false,
                        GraphDatabaseInternalSettings.merged_log,
                        false))
                .build()) {

            GraphDatabaseAPI database = (GraphDatabaseAPI) dbms.database(DEFAULT_DATABASE_NAME);

            var propertyValue = random.nextAsciiStringOfLength((int) THRESHOLD / 16);

            int numberOfTransactions = 35;
            for (int i = 0; i < numberOfTransactions; i++) {
                try (var tx = database.beginTx()) {
                    Node node = tx.createNode();
                    node.setProperty("a", propertyValue);
                    tx.commit();
                }
            }

            TransactionLogService logService =
                    database.getDependencyResolver().resolveDependency(TransactionLogService.class);

            int initialAppendIndex = 17;
            try (TransactionLogChannels logReaders = logService.logFilesChannels(initialAppendIndex)) {
                List<LogChannel> logFileChannels = logReaders.getChannels();
                assertThat(logFileChannels).hasSize(3);

                boolean first = true;
                for (LogChannel logChannel : logFileChannels) {
                    // Non-enveloped format doesn't care about checksum because it doesn't use that header field
                    // For now it will send UNKNOWN when starting in the middle of a file, and BASE for any other
                    // file since it is taken from the header.
                    int expectedChecksum = first ? TransactionIdStore.UNKNOWN_TX_CHECKSUM : BASE_TX_CHECKSUM;
                    assertThat(logChannel.previousChecksum()).isEqualTo(expectedChecksum);
                    first = false;
                }
            }
        }
    }

    int getPrevChecksumFromPosition(LogPosition logPosition, WALService walService) throws IOException {
        try (ReadableLogChannel reader = walService.getLogFile().getReader(logPosition)) {
            return reader.getChecksum();
        }
    }

    @Test
    void restoreRequireNonAvailableDatabase() {
        assertThrows(IllegalStateException.class, () -> logService.restore(LogPosition.UNSPECIFIED));
    }

    @Test
    void failToRestoreWithLogPositionInHigherLogFile() {
        availabilityGuard.require(new AvailabilityRequirement("Database unavailable"));

        assertThatThrownBy(() -> logService.restore(new LogPosition(metadataProvider.getCurrentLogVersion() + 5, 100)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Log position requested for restore points to the log file that is higher than existing available highest log file. "
                                + "Requested restore position: LogPosition{logVersion=5, byteOffset=100}, current log file version: 0.");
    }

    @Test
    void failToRestoreWithLogPositionInCommittedFile() throws IOException {
        availabilityGuard.require(new AvailabilityRequirement("Database unavailable"));

        LogFile logFile = walService.getLogFile();
        logFile.rotate();

        assertThatThrownBy(() -> logService.restore(new LogPosition(metadataProvider.getCurrentLogVersion() - 1, 100)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Log position requested to be used for restore belongs to the log file that was already appended by transaction and cannot be restored.")
                .hasMessageContaining("requested restore: LogPosition{logVersion=0, byteOffset=100}");
    }

    @Test
    void failToAppendCheckpointOnAvailableDatabase() {
        assertThrows(
                IllegalStateException.class,
                () -> logService.appendCheckpoint(
                        TransactionIdStore.UNKNOWN_TRANSACTION_ID, UNKNOWN_APPEND_INDEX, "Test"));
    }

    @Test
    void checkpointAtEndOfFileWhenAppendingToLastAvailableTransaction() throws IOException {
        availabilityGuard.require(new AvailabilityRequirement("Database unavailable"));

        TransactionId lastTransactionId = metadataProvider.getLastCommittedTransaction();
        String testReason = "Should checkpoint at end of file";

        var eofPosition = findEndOfFile(lastTransactionId.id());

        logService.appendCheckpoint(lastTransactionId, lastTransactionId.appendIndex(), testReason);

        var checkpointInfo =
                walService.getCheckpointFile().findLatestCheckpoint().orElseThrow();
        assertThat(checkpointInfo.reason()).contains(testReason);

        LogTailInformation freshTail = getFreshLogTail();
        assertThat(lastTransactionId).isEqualTo(freshTail.getLastCommittedTransaction());
        assertThat(freshTail.getLastCheckPoint().orElseThrow().transactionLogPosition())
                .isEqualTo(eofPosition);
        assertThat(freshTail.getLastCheckPoint().orElseThrow()).isEqualTo(checkpointInfo);
        assertThat(freshTail.hasRecordsToRecover())
                .describedAs("There should not be any commits after the checkpoint." + freshTail)
                .isFalse();
        assertThat(freshTail.isRecoveryRequired())
                .describedAs("Recovery should not be required. " + freshTail)
                .isFalse();
    }

    @Test
    void appendCheckpointForNotTheLastAvailableTransaction() throws IOException {
        TransactionId lastTransactionId = metadataProvider.getLastCommittedTransaction();

        createNodeInIsolatedTransaction("foo");

        availabilityGuard.require(new AvailabilityRequirement("Database unavailable"));
        String testReason = "My unique last checkpoint2.";
        logService.appendCheckpoint(lastTransactionId, lastTransactionId.appendIndex(), testReason);

        var checkpointInfo =
                walService.getCheckpointFile().findLatestCheckpoint().orElseThrow();
        assertThat(checkpointInfo.reason()).contains(testReason);

        LogTailInformation freshTail = getFreshLogTail();
        assertThat(lastTransactionId).isEqualTo(freshTail.getLastCommittedTransaction());
        assertThat(freshTail.getLastCheckPoint().orElseThrow()).isEqualTo(checkpointInfo);
        assertThat(freshTail.hasRecordsToRecover())
                .describedAs("There should be new commits after the checkpoint." + freshTail)
                .isTrue();
        assertThat(freshTail.isRecoveryRequired())
                .describedAs("Recovery should be required. " + freshTail)
                .isTrue();
        assertThat(freshTail.firstAppendIndexAfterLastCheckPoint)
                .describedAs("Transaction id after should be right after checkpointed tx id.")
                .isEqualTo(lastTransactionId.id() + 1);
    }

    @Test
    void checkpointAtEndOfFileWhenLogFileIsEmpty() throws IOException {
        for (int i = 0; i < 10; i++) {
            createNodeInIsolatedTransaction("foo");
        }
        // we have some transaction that actually generated tx id
        TransactionId lastTransactionId = metadataProvider.getLastCommittedTransaction();
        LogFile logFile = walService.getLogFile();
        long logVersion = logFile.getCurrentLogVersion();
        logFile.rotate();

        // remove all old file version
        while (logFile.versionExists(logVersion)) {
            Path file = logFile.getLogFileForVersion(logVersion);
            fs.deleteFile(file);
            logVersion--;
        }

        long highestVersion = logFile.getLogRangeInfo().highestVersion();
        var eofPosition = new LogPosition(
                highestVersion,
                logFile.extractHeader(highestVersion).getStartPosition().getByteOffset());
        availabilityGuard.require(new AvailabilityRequirement("Database unavailable"));

        String testReason = "Checkpoint on empty log files should work since its full story copy.";
        logService.appendCheckpoint(lastTransactionId, lastTransactionId.appendIndex(), testReason);

        LogTailInformation freshTail = getFreshLogTail();
        assertThat(lastTransactionId).isEqualTo(freshTail.getLastCommittedTransaction());
        assertThat(freshTail.getLastCheckPoint().orElseThrow().transactionLogPosition())
                .isEqualTo(eofPosition);
        assertThat(freshTail.hasRecordsToRecover())
                .describedAs("There should not be any commits after the checkpoint." + freshTail)
                .isFalse();
        assertThat(freshTail.isRecoveryRequired())
                .describedAs("Recovery should not be required. " + freshTail)
                .isFalse();
    }

    @Test
    void checkpointAtEndOfFileWhenTransactionIsRotatedOut() throws IOException {
        for (int i = 0; i < 10; i++) {
            createNodeInIsolatedTransaction("foo");
        }
        // we have some transaction that actually generated tx id
        TransactionId lastTransactionId = metadataProvider.getLastCommittedTransaction();
        LogFile logFile = walService.getLogFile();
        long logVersion = logFile.getCurrentLogVersion();
        logFile.rotate();
        logFile.rotate();

        // remove all old file version
        while (logFile.versionExists(logVersion)) {
            Path file = logFile.getLogFileForVersion(logVersion);
            fs.deleteFile(file);
            logVersion--;
        }

        long highestVersion = logFile.getLogRangeInfo().highestVersion();
        var eofPosition = new LogPosition(
                highestVersion,
                logFile.extractHeader(highestVersion).getStartPosition().getByteOffset());

        availabilityGuard.require(new AvailabilityRequirement("Database unavailable"));

        String testReason = "Checkpoint at EOF when tx is rotated out";
        logService.appendCheckpoint(lastTransactionId, lastTransactionId.appendIndex(), testReason);

        var checkpointInfo =
                walService.getCheckpointFile().findLatestCheckpoint().orElseThrow();
        assertThat(checkpointInfo.reason()).contains(testReason);

        LogTailInformation freshTail = getFreshLogTail();
        assertThat(lastTransactionId).isEqualTo(freshTail.getLastCommittedTransaction());
        assertThat(freshTail.getLastCheckPoint().orElseThrow()).isEqualTo(checkpointInfo);
        assertThat(freshTail.getLastCheckPoint().orElseThrow().transactionLogPosition())
                .isEqualTo(eofPosition);
    }

    @Test
    void findTransactionPositionWhenInPreviousLogFile() throws IOException {
        for (int i = 0; i < 10; i++) {
            createNodeInIsolatedTransaction("foo");
        }
        // we have some transaction that actually generated tx id
        TransactionId lastTransactionId = metadataProvider.getLastCommittedTransaction();

        LogFile logFile = walService.getLogFile();
        logFile.rotate();

        for (int i = 0; i < 10; i++) {
            createNodeInIsolatedTransaction("foo");
        }

        logFile.rotate();
        var expectedPosition = findEndOfTransaction(lastTransactionId.id());

        availabilityGuard.require(new AvailabilityRequirement("Database unavailable"));

        String testReason = "Find position for tx even when it has been rotated";
        logService.appendCheckpoint(lastTransactionId, lastTransactionId.appendIndex(), testReason);

        var checkpointInfo =
                walService.getCheckpointFile().findLatestCheckpoint().orElseThrow();
        assertThat(checkpointInfo.reason()).contains(testReason);

        LogTailInformation freshTail = getFreshLogTail();
        assertThat(lastTransactionId).isEqualTo(freshTail.getLastCommittedTransaction());
        assertThat(freshTail.getLastCheckPoint().orElseThrow()).isEqualTo(checkpointInfo);
        assertThat(freshTail.getLastCheckPoint().orElseThrow().transactionLogPosition())
                .isEqualTo(expectedPosition);
    }

    @Test
    void checkpointAtEndOfFileWhenTransactionDoesntExist() throws IOException {
        for (int i = 0; i < 10; i++) {
            createNodeInIsolatedTransaction("foo");
        }
        TransactionId lastTransactionId = metadataProvider.getLastCommittedTransaction();
        var eofPosition = findEndOfFile(lastTransactionId.id());
        availabilityGuard.require(new AvailabilityRequirement("Database unavailable"));
        String testReason = "Checkpoint at end of file when tx doesn't exist";
        var transactionId = new TransactionId(789, 798, LATEST_KERNEL_VERSION, 7, 8, 9);
        logService.appendCheckpoint(transactionId, transactionId.appendIndex(), testReason);

        var checkpointInfo =
                walService.getCheckpointFile().findLatestCheckpoint().orElseThrow();
        assertThat(checkpointInfo.reason()).contains(testReason);

        LogTailInformation freshTail = getFreshLogTail();
        assertThat(freshTail.getLastCheckPoint().orElseThrow()).isEqualTo(checkpointInfo);
        assertThat(freshTail.getLastCheckPoint().orElseThrow().transactionLogPosition())
                .isEqualTo(eofPosition);
    }

    private static ByteBuffer createBuffer() {
        return ByteBuffers.allocateDirect((int) (THRESHOLD << 1), ByteOrder.LITTLE_ENDIAN, INSTANCE);
    }

    private static class BulkAppendLogRotationMonitor extends LogRotationMonitorAdapter {
        private final List<Long> versions = new CopyOnWriteArrayList<>();

        @Override
        public void finishLogRotation(
                Path logFile,
                LogType type,
                long logVersion,
                LogHeader logHeader,
                long lastAppendIndex,
                long rotationMillis,
                long millisSinceLastRotation) {
            versions.add(logVersion);
        }

        public List<Long> getObservedVersions() {
            return versions;
        }
    }

    private LogPosition findEndOfTransaction(long txId) throws IOException {
        try (var cursor = transactionStore.getCommandBatches(txId + 1)) {
            // Return end position of txId
            return cursor.position();
        }
    }

    private LogPosition findEndOfFile(long txId) throws IOException {
        try (var cursor = transactionStore.getCommandBatches(txId)) {
            while (cursor.next()) {
                // Find last command
            }
            // Return last position in file
            return cursor.position();
        }
    }

    private LogTailInformation getFreshLogTail() {
        return ((CheckpointLogFile) walService.getCheckpointFile())
                .getLogTailScanner()
                .findLogTail();
    }

    private long getTxStartOffset(long txId) throws IOException {
        try (var commandBatches = transactionStore.getCommandBatches(txId)) {
            return commandBatches.position().getByteOffset();
        }
    }

    private long getTxEndOffset(long txId) throws IOException {
        try (var commandBatches = transactionStore.getCommandBatches(txId)) {
            commandBatches.next();
            return commandBatches.position().getByteOffset();
        }
    }

    private void verifyReportedPositions(int txId, long expectedOffset) throws IOException {
        try (TransactionLogChannels logReaders = logService.logFilesChannels(txId)) {
            List<LogChannel> logFileChannels = logReaders.getChannels();
            assertThat(logFileChannels).hasSize(1);
            assertEquals(expectedOffset, logFileChannels.getFirst().channel().position());
        }
    }

    private void createNodeInIsolatedTransaction(String propertyValue) {
        try (var tx = databaseAPI.beginTx()) {
            Node node = tx.createNode();
            node.setProperty("a", propertyValue);
            tx.commit();
        }
    }

    private static void checkChannelsCoverage(
            List<LogChannel> channels, long expectedFirstIndex, long expectedLastAppendIndex) {
        LongRange totalRange = null;
        for (LogChannel channel : channels) {
            var channelRange = LongRange.range(channel.startAppendIndex(), channel.lastAppendIndex());
            if (totalRange == null) {
                totalRange = channelRange;
            } else if (channelRange == LongRange.EMPTY_RANGE) {
                // This can happen for envelope channels where one chunk can stretch over more than one file
                // The start append index is always last append from header + 1 which isn't always true for envelopes,
                // not if one chunk is continuing from a previous file.
                // But the receiver always uses the start index - 1 so it does not matter for the protocol.
                // The range (x, x-1) should therefore be accepted as long as it is not the first file
                assertThat(channel).isNotEqualTo(channels.getFirst());
                assertThat(channel.startAppendIndex()).isEqualTo(channel.lastAppendIndex() + 1);
            } else {
                totalRange = LongRange.join(totalRange, channelRange);
            }
        }

        assertEquals(expectedFirstIndex, totalRange.from());
        assertEquals(expectedLastAppendIndex, totalRange.to());
    }

    /**
     * A tracer tree capable of injecting code into the {@link TransactionWriteEvent#beginStoreApply()},
     * See {@link InjectableBeforeApplyTxWriteEvent#beforeStoreApply}
     */
    public static class InjectableBeforeApplyTracers implements Tracers {
        @Override
        public PageCacheTracer getPageCacheTracer() {
            return PageCacheTracer.NULL;
        }

        @Override
        public LockTracer getLockTracer() {
            return LockTracer.NONE;
        }

        @Override
        public DatabaseTracer getDatabaseTracer(NamedDatabaseId namedDatabaseId) {
            return new InjectableBeforeApplyDatabaseTracer();
        }

        @Override
        public VersionStorageTracer getVersionStorageTracer(NamedDatabaseId namedDatabaseId) {
            return VersionStorageTracer.NULL;
        }

        private static class InjectableBeforeApplyDatabaseTracer extends DefaultDatabaseTracer {
            public InjectableBeforeApplyDatabaseTracer() {
                super(PageCacheTracer.NULL);
            }

            @Override
            public TransactionEvent beginTransaction(CursorContext cursorContext, long transactionSequenceNumber) {
                return new InjectableBeforeApplyTransactionEvent();
            }
        }

        private static class InjectableBeforeApplyTxWriteEvent implements TransactionWriteEvent {

            public static final InjectableBeforeApplyTxWriteEvent INSTANCE = new InjectableBeforeApplyTxWriteEvent();

            private final AtomicReference<Runnable> beforeStoreApply = new AtomicReference<>();

            @Override
            public void close() {}

            @Override
            public LogAppendEvent beginLogAppend() {
                return LogAppendEvent.NULL;
            }

            @Override
            public StoreApplyEvent beginStoreApply() {
                var beforeStoreApplyRunnable = beforeStoreApply.getAndSet(null);
                if (beforeStoreApplyRunnable != null) {
                    beforeStoreApplyRunnable.run();
                }
                return StoreApplyEvent.NULL;
            }

            @Override
            public void chunkAppended(
                    int chunkNumber, long transactionSequenceNumber, long transactionId, long appendIndex) {}
        }

        private static class InjectableBeforeApplyTransactionEvent implements TransactionEvent {

            @Override
            public void setCommit(boolean commit) {}

            @Override
            public void setRollback(boolean rollback) {}

            @Override
            public TransactionWriteEvent beginCommitEvent() {
                return InjectableBeforeApplyTxWriteEvent.INSTANCE;
            }

            @Override
            public TransactionWriteEvent beginChunkWriteEvent() {
                return InjectableBeforeApplyTxWriteEvent.INSTANCE;
            }

            @Override
            public TransactionRollbackEvent beginRollback() {
                return TransactionRollbackEvent.NULL;
            }

            @Override
            public void close() {}

            @Override
            public void setTransactionWriteState(String transactionWriteState) {}

            @Override
            public void setReadOnly(boolean wasReadOnly) {}

            @Override
            public void refreshVisibilityBoundary() {}
        }
    }
}
