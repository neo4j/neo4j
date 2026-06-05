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
package org.neo4j.kernel.impl.transaction.log;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.neo4j.common.Subject.ANONYMOUS;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.KernelVersion.GLORIOUS_FUTURE;
import static org.neo4j.kernel.KernelVersion.V5_11;
import static org.neo4j.kernel.KernelVersion.V5_12;
import static org.neo4j.kernel.KernelVersion.V5_13;
import static org.neo4j.kernel.KernelVersionProviders.fixed;
import static org.neo4j.kernel.impl.api.txid.TransactionIdGenerator.EMPTY;
import static org.neo4j.kernel.impl.transaction.log.TransactionAppenderFactory.createTransactionAppender;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.DatabaseConfig;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.dbms.database.DbmsRuntimeVersion;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.BinarySupportedKernelVersions;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.impl.api.CompleteTransaction;
import org.neo4j.kernel.impl.api.TestCommand;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.rotation.monitor.LogRotationMonitorAdapter;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.DatabaseHealthEventGenerator;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Monitors;
import org.neo4j.monitoring.Panic;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.AppendIndexProvider;
import org.neo4j.storageengine.api.CommandBatch;
import org.neo4j.storageengine.api.Commitment;
import org.neo4j.storageengine.api.Leases;
import org.neo4j.storageengine.api.LogMetadataProvider;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.Race;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

@Neo4jLayoutExtension
@ExtendWith(LifeExtension.class)
class TransactionLogAppendAndRotateIT {
    private static final TestLogAppendEvent TEST_LOG_APPEND_EVENT = new TestLogAppendEvent();

    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private LifeSupport life;

    @Inject
    private DatabaseLayout databaseLayout;

    private ThreadPoolJobScheduler jobScheduler;

    @BeforeEach
    void setUp() {
        jobScheduler = new ThreadPoolJobScheduler();
    }

    @AfterEach
    void tearDown() {
        life.shutdown();
        jobScheduler.close();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldKeepTransactionsIntactWhenConcurrentlyRotationAndAppending(boolean useQueueAppender) throws Throwable {
        // GIVEN
        Setup setup = setupLogAppender(LatestVersions.LATEST_KERNEL_VERSION_PROVIDER, useQueueAppender);

        // WHEN
        Race race = new Race();
        for (int i = 0; i < 4; i++) {
            race.addContestant(() -> {
                while (!setup.end().get()) {
                    try {
                        setup.appender()
                                .register(
                                        new CompleteTransaction(
                                                sillyTransaction(1_000),
                                                NULL_CONTEXT,
                                                StoreCursors.NULL,
                                                Commitment.NO_COMMITMENT,
                                                EMPTY),
                                        TEST_LOG_APPEND_EVENT);
                    } catch (Exception e) {
                        setup.end().set(true);
                        fail(e.getMessage(), e);
                    }
                }
            });
        }
        race.addContestant(endAfterMax(250, MILLISECONDS, setup.end(), setup.monitoring()));
        race.go();

        // THEN
        assertTrue(setup.monitoring().numberOfRotations() > 0);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldRotateLogWhenSeeingNewKernelVersion(boolean useQueueAppender) throws Throwable {
        // Start on version 5_11. This should mean no rotation on 5_11 commits
        KernelVersionProvider startVersionProvider = fixed(V5_11);

        Setup setup = setupLogAppender(startVersionProvider, useQueueAppender);

        setup.appender.register(
                new CompleteTransaction(
                        txWithVersion(V5_11), NULL_CONTEXT, StoreCursors.NULL, Commitment.NO_COMMITMENT, EMPTY),
                TEST_LOG_APPEND_EVENT);

        assertThat(setup.monitoring.numberOfRotations()).isEqualTo(0);

        for (int i = 0; i < 3; i++) {
            setup.appender.register(
                    new CompleteTransaction(
                            txWithVersion(V5_12), NULL_CONTEXT, StoreCursors.NULL, Commitment.NO_COMMITMENT, EMPTY),
                    TEST_LOG_APPEND_EVENT);
        }

        assertThat(setup.monitoring.numberOfRotations()).isEqualTo(1);

        for (int i = 0; i < 3; i++) {
            setup.appender.register(
                    new CompleteTransaction(
                            txWithVersion(V5_13), NULL_CONTEXT, StoreCursors.NULL, Commitment.NO_COMMITMENT, EMPTY),
                    TEST_LOG_APPEND_EVENT);
        }

        assertThat(setup.monitoring.numberOfRotations()).isEqualTo(2);

        assertWholeTransactionsWithCorrectVersionInSpecificLogVersion(
                setup.logFiles.getLogFile(), LogVersionRepository.INITIAL_LOG_VERSION, V5_11, 1);
        assertWholeTransactionsWithCorrectVersionInSpecificLogVersion(
                setup.logFiles.getLogFile(), LogVersionRepository.INITIAL_LOG_VERSION + 1, V5_12, 3);
        assertWholeTransactionsWithCorrectVersionInSpecificLogVersion(
                setup.logFiles.getLogFile(), LogVersionRepository.INITIAL_LOG_VERSION + 2, V5_13, 3);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldRotateIfFirstCommandHasNewVersion(boolean useQueueAppender) throws Throwable {
        // Start on version 5_11. This should mean rotation on first commit
        KernelVersionProvider startVersionProvider = fixed(V5_11);

        Setup setup = setupLogAppender(startVersionProvider, useQueueAppender);

        setup.appender.register(
                new CompleteTransaction(
                        txWithVersion(V5_12), NULL_CONTEXT, StoreCursors.NULL, Commitment.NO_COMMITMENT, EMPTY),
                TEST_LOG_APPEND_EVENT);

        assertThat(setup.monitoring.numberOfRotations()).isEqualTo(1);

        int txsInFirstFile = assertWholeTransactionsIn(
                setup.logFiles.getLogFile(), LogVersionRepository.INITIAL_LOG_VERSION, (v) -> {}, true);
        assertThat(txsInFirstFile).isZero();
        assertWholeTransactionsWithCorrectVersionInSpecificLogVersion(
                setup.logFiles.getLogFile(), LogVersionRepository.INITIAL_LOG_VERSION + 1, V5_12, 1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void rotateCommandHasCorrectPositionFromNewFile(boolean useQueueAppender) throws Throwable {
        var fromVersion = V5_11;
        var toVersion = V5_12;

        KernelVersionProvider startVersionProvider = fixed(fromVersion);

        Setup setup = setupLogAppender(startVersionProvider, useQueueAppender);

        setup.appender.register(
                new CompleteTransaction(
                        txWithVersion(fromVersion), NULL_CONTEXT, StoreCursors.NULL, Commitment.NO_COMMITMENT, EMPTY),
                TEST_LOG_APPEND_EVENT);

        // position of append is in the initial file
        var noRotationStartPosition1 = setup.metadataCache
                .getTransactionMetadata(setup.appendIndexProvider.getLastAppendIndex())
                .startPosition();
        assertEquals(LogVersionRepository.INITIAL_LOG_VERSION, noRotationStartPosition1.getLogVersion());
        assertEquals(
                LogFormat.fromKernelVersion(fromVersion).getHeaderSize(), noRotationStartPosition1.getByteOffset());

        setup.appender.register(
                new CompleteTransaction(
                        txWithVersion(fromVersion), NULL_CONTEXT, StoreCursors.NULL, Commitment.NO_COMMITMENT, EMPTY),
                TEST_LOG_APPEND_EVENT);

        // position of second append is still in the initial file
        var noRotationStartPosition2 = setup.metadataCache
                .getTransactionMetadata(setup.appendIndexProvider.getLastAppendIndex())
                .startPosition();
        assertEquals(LogVersionRepository.INITIAL_LOG_VERSION, noRotationStartPosition2.getLogVersion());
        assertThat(noRotationStartPosition2.getByteOffset()).isGreaterThan(LogFormat.BIGGEST_HEADER);

        // we have not rotations yet
        assertThat(setup.monitoring.numberOfRotations()).isEqualTo(0);

        setup.appender.register(
                new CompleteTransaction(
                        txWithVersion(toVersion), NULL_CONTEXT, StoreCursors.NULL, Commitment.NO_COMMITMENT, EMPTY),
                TEST_LOG_APPEND_EVENT);

        // now we did rotation
        assertThat(setup.monitoring.numberOfRotations()).isEqualTo(1);

        // position should be from the new file
        var postRotationStartPosition = setup.metadataCache
                .getTransactionMetadata(setup.appendIndexProvider.getLastAppendIndex())
                .startPosition();
        assertEquals(LogVersionRepository.INITIAL_LOG_VERSION + 1, postRotationStartPosition.getLogVersion());
        assertEquals(LogFormat.fromKernelVersion(toVersion).getHeaderSize(), postRotationStartPosition.getByteOffset());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void rotatedFilesShouldGetTheHeaderMatchingTheVersion(boolean useQueueAppender) throws Throwable {
        // Start on version 5_11. This should mean rotation on first commit

        Setup setup = setupLogAppender(fixed(V5_11), useQueueAppender);

        setup.appender.register(
                new CompleteTransaction(
                        txWithVersion(GLORIOUS_FUTURE),
                        NULL_CONTEXT,
                        StoreCursors.NULL,
                        Commitment.NO_COMMITMENT,
                        EMPTY),
                TEST_LOG_APPEND_EVENT);

        assertThat(setup.monitoring.numberOfRotations()).isEqualTo(1);

        // Null because the format for V5_11 doesn't contain the kernelVersion
        assertLogHeaderExpectedVersion(setup.logFiles, LogVersionRepository.INITIAL_LOG_VERSION, null);
        // Null because the log format only changes on upgrade tx, not directly connected to the kernel version
        assertLogHeaderExpectedVersion(setup.logFiles, LogVersionRepository.INITIAL_LOG_VERSION + 1, null);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void rotationShouldHappenOnNewVersionEvenInBatch(boolean useQueueAppender) throws Throwable {
        Setup setup = setupLogAppender(fixed(V5_11), useQueueAppender);

        CompleteTransaction batch = new CompleteTransaction(
                txWithVersion(V5_11), NULL_CONTEXT, StoreCursors.NULL, Commitment.NO_COMMITMENT, EMPTY);
        CompleteTransaction two = new CompleteTransaction(
                txWithVersion(V5_11), NULL_CONTEXT, StoreCursors.NULL, Commitment.NO_COMMITMENT, EMPTY);
        batch.next(two);
        CompleteTransaction three = new CompleteTransaction(
                txWithVersion(GLORIOUS_FUTURE), NULL_CONTEXT, StoreCursors.NULL, Commitment.NO_COMMITMENT, EMPTY);
        two.next(three);
        three.next(new CompleteTransaction(
                txWithVersion(GLORIOUS_FUTURE), NULL_CONTEXT, StoreCursors.NULL, Commitment.NO_COMMITMENT, EMPTY));
        setup.appender.register(batch, TEST_LOG_APPEND_EVENT);

        assertThat(setup.monitoring.numberOfRotations()).isEqualTo(1);

        // Null because the format for V5_11 doesn't contain the kernelVersion
        assertLogHeaderExpectedVersion(setup.logFiles, LogVersionRepository.INITIAL_LOG_VERSION, null);
        assertWholeTransactionsWithCorrectVersionInSpecificLogVersion(
                setup.logFiles.getLogFile(), LogVersionRepository.INITIAL_LOG_VERSION, V5_11, 2);
        // Null because the log format only changes on upgrade tx, not directly connected to the kernel version
        assertLogHeaderExpectedVersion(setup.logFiles, LogVersionRepository.INITIAL_LOG_VERSION + 1, null);
        assertWholeTransactionsWithCorrectVersionInSpecificLogVersion(
                setup.logFiles.getLogFile(), LogVersionRepository.INITIAL_LOG_VERSION + 1, GLORIOUS_FUTURE, 2);
    }

    private void assertLogHeaderExpectedVersion(LogFiles logFiles, long logVersion, KernelVersion expectedVersion)
            throws IOException {
        LogHeader logHeader = LogHeaderReader.readLogHeader(
                fileSystem, logFiles.getLogFile().getLogFileForVersion(logVersion), EmptyMemoryTracker.INSTANCE);
        assertThat(logHeader.getKernelVersion()).isEqualTo(expectedVersion);
    }

    private Setup setupLogAppender(KernelVersionProvider versionProvider, boolean useQueueAppender) throws IOException {
        Monitors monitors = new Monitors();
        var storeId = new StoreId(1, 2, "engine-1", "format-1", 3, 4);
        var databaseConfig = new DatabaseConfig(Config.defaults(Map.of(
                GraphDatabaseInternalSettings.dedicated_transaction_appender,
                useQueueAppender,
                GraphDatabaseInternalSettings.latest_kernel_version,
                GLORIOUS_FUTURE.version(),
                GraphDatabaseInternalSettings.latest_runtime_version,
                DbmsRuntimeVersion.GLORIOUS_FUTURE.getVersion())));
        LogFiles logFiles = LogFilesBuilder.writeableBuilder(
                        databaseLayout,
                        fileSystem,
                        versionProvider,
                        () -> LogFormat.fromKernelVersion(versionProvider.kernelVersion()))
                .withRotationThreshold(ByteUnit.mebiBytes(1))
                .withMonitors(monitors)
                .withCommandReaderFactory(TestCommandReaderFactory.INSTANCE)
                .withStoreId(storeId)
                .withConfig(databaseConfig)
                .build();
        LogMetadataProvider logMetadataProvider = logFiles.logMetadataProvider();
        life.add(logFiles);
        final AtomicBoolean end = new AtomicBoolean();
        TestLogFileMonitor monitoring = new TestLogFileMonitor(end, 100, logFiles.getLogFile());
        monitors.addMonitorListener(monitoring);

        Panic panic = new DatabaseHealth(mock(DatabaseHealthEventGenerator.class), NullLog.getInstance());
        TransactionMetadataCache metadataCache = new TransactionMetadataCache();
        final TransactionAppender appender = life.add(createBatchAppender(
                logFiles,
                logMetadataProvider,
                panic,
                jobScheduler,
                databaseConfig,
                metadataCache,
                logMetadataProvider));
        return new Setup(end, monitoring, appender, logFiles, metadataCache, logMetadataProvider);
    }

    private record Setup(
            AtomicBoolean end,
            TestLogFileMonitor monitoring,
            TransactionAppender appender,
            LogFiles logFiles,
            TransactionMetadataCache metadataCache,
            AppendIndexProvider appendIndexProvider) {}

    private TransactionAppender createBatchAppender(
            LogFiles logFiles,
            TransactionIdStore txIdStore,
            Panic panic,
            JobScheduler jobScheduler,
            DatabaseConfig databaseConfig,
            TransactionMetadataCache metadataCache,
            AppendIndexProvider appendIndexProvider) {
        return createTransactionAppender(
                logFiles,
                txIdStore,
                appendIndexProvider,
                databaseConfig,
                panic,
                jobScheduler,
                NullLogProvider.getInstance(),
                metadataCache,
                "le db",
                false,
                false);
    }

    private static Runnable endAfterMax(
            final int time, final TimeUnit unit, final AtomicBoolean end, TestLogFileMonitor monitoring) {
        return () -> {
            while (monitoring.numberOfRotations() < 2 && !end.get()) {
                parkNanos(MILLISECONDS.toNanos(50));
            }
            long endTime = currentTimeMillis() + unit.toMillis(time);
            while (currentTimeMillis() < endTime && !end.get()) {
                parkNanos(MILLISECONDS.toNanos(50));
            }
            end.set(true);
        };
    }

    private static void assertWholeTransactionsIn(LogFile logFile, long logVersion) throws IOException {
        int transactions = assertWholeTransactionsIn(logFile, logVersion, (v) -> {}, false);
        assertTrue(transactions > 0);
    }

    private static void assertWholeTransactionsWithCorrectVersionInSpecificLogVersion(
            LogFile logFile, long logVersion, KernelVersion kernelVersion, int expectedNbrTxs) throws IOException {
        int transactions = assertWholeTransactionsIn(
                logFile,
                logVersion,
                testKernelVersion -> assertThat(testKernelVersion).isEqualTo(kernelVersion),
                true);
        assertThat(transactions).isEqualTo(expectedNbrTxs);
    }

    private static int assertWholeTransactionsIn(
            LogFile logFile, long logVersion, Consumer<KernelVersion> extraCheck, boolean onlyCheckSuppliedLogVersion)
            throws IOException {
        int transactions = 0;

        try (ReadableLogChannel reader = logFile.getReader(
                logFile.extractHeader(logVersion).getStartPosition(),
                onlyCheckSuppliedLogVersion
                        ? LogVersionBridge.NO_MORE_CHANNELS
                        : ReaderLogVersionBridge.forFile(logFile))) {
            LogEntryReader entryReader = new VersionAwareLogEntryReader(
                    TestCommandReaderFactory.INSTANCE,
                    new BinarySupportedKernelVersions(Config.defaults(Map.of(
                            GraphDatabaseInternalSettings.latest_runtime_version,
                            DbmsRuntimeVersion.GLORIOUS_FUTURE.getVersion(),
                            GraphDatabaseInternalSettings.latest_kernel_version,
                            GLORIOUS_FUTURE.version()))),
                    EmptyMemoryTracker.INSTANCE);
            LogEntry entry;
            boolean inTx = false;
            while ((entry = entryReader.readLogEntry(reader)) != null) {
                if (!inTx) // Expects start entry
                {
                    assertInstanceOf(LogEntryStart.class, entry);
                    extraCheck.accept(((LogEntryStart) entry).kernelVersion());
                    inTx = true;
                } else // Expects command/commit entry
                {
                    assertTrue(entry instanceof LogEntryCommand || entry instanceof LogEntryCommit);
                    if (entry instanceof LogEntryCommit) {
                        inTx = false;
                        transactions++;
                    }
                }
            }
            assertFalse(inTx);
        }
        return transactions;
    }

    private static CommandBatch sillyTransaction(int size) {
        List<StorageCommand> commands = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            // The actual data isn't super important
            commands.add(new TestCommand(30));
            commands.add(new TestCommand(60));
        }
        return new CompleteCommandBatch(
                commands,
                UNKNOWN_CONSENSUS_INDEX,
                0,
                0,
                0,
                0,
                Leases.NO_LEASES,
                LatestVersions.LATEST_KERNEL_VERSION,
                ANONYMOUS);
    }

    private static CommandBatch txWithVersion(KernelVersion version) {
        List<StorageCommand> commands = new ArrayList<>(1);
        commands.add(new TestCommand(50, version));
        return new CompleteCommandBatch(
                commands, UNKNOWN_CONSENSUS_INDEX, 0, 0, 0, 0, Leases.NO_LEASES, version, ANONYMOUS);
    }

    private static class TestLogFileMonitor extends LogRotationMonitorAdapter {
        private final AtomicBoolean end;
        private final int maxNumberOfRotations;
        private final LogFile logFile;
        private final AtomicInteger rotations = new AtomicInteger();

        TestLogFileMonitor(AtomicBoolean end, int maxNumberOfRotations, LogFile logFile) {
            this.end = end;
            this.maxNumberOfRotations = maxNumberOfRotations;
            this.logFile = logFile;
        }

        @Override
        public void finishLogRotation(
                Path logFile,
                LogType type,
                long logVersion,
                LogHeader logHeader,
                long lastAppendIndex,
                long rotationMillis,
                long millisSinceLastRotation) {
            try {
                assertWholeTransactionsIn(this.logFile, logVersion);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                if (rotations.getAndIncrement() > maxNumberOfRotations) {
                    end.set(true);
                }
            }
        }

        int numberOfRotations() {
            return rotations.get();
        }
    }

    private static class TestLogAppendEvent extends LogAppendEvent.Empty {
        @Override
        public void appendedBytes(long bytes) {
            assertThat(bytes).isGreaterThanOrEqualTo(0);
        }
    }
}
