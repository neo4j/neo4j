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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.common.Subject.ANONYMOUS;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.recovery.IncompleteTransactionAction.STOP;
import static org.neo4j.kernel.recovery.RecoveryStartupChecker.EMPTY_CHECKER;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.DatabaseConfig;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.kernel.impl.api.ChunkedTransactionTracker;
import org.neo4j.kernel.impl.api.CompleteTransaction;
import org.neo4j.kernel.impl.api.TestCommand;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.api.txid.IdStoreTransactionIdGenerator;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatchRepresentation;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatchRepresentation.BatchInformation;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.recovery.CorruptedLogsTruncator;
import org.neo4j.kernel.recovery.RecoveryApplier;
import org.neo4j.kernel.recovery.RecoveryMode;
import org.neo4j.kernel.recovery.RecoveryMonitor;
import org.neo4j.kernel.recovery.RecoveryPredicate;
import org.neo4j.kernel.recovery.RecoveryService;
import org.neo4j.kernel.recovery.RecoveryStartInformation;
import org.neo4j.kernel.recovery.TransactionLogsRecovery;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Monitors;
import org.neo4j.monitoring.Panic;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.AppendIndexProvider;
import org.neo4j.storageengine.api.CommandBatch;
import org.neo4j.storageengine.api.Leases;
import org.neo4j.storageengine.api.LogMetadataProvider;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.test.utils.TestDirectory;

@Neo4jLayoutExtension
class PhysicalLogicalTransactionStoreTest {
    private static final Panic DATABASE_PANIC = mock(DatabaseHealth.class);

    @Inject
    private DefaultFileSystemAbstraction fileSystem;

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private DatabaseLayout databaseLayout;

    private Path databaseDirectory;
    private final Monitors monitors = new Monitors();
    private ThreadPoolJobScheduler jobScheduler;

    @BeforeEach
    void setup() {
        jobScheduler = new ThreadPoolJobScheduler();
        databaseDirectory = testDirectory.homePath();
    }

    @AfterEach
    void tearDown() {
        jobScheduler.close();
    }

    @Test
    void extractTransactionFromLogFilesSkippingLastLogFileWithoutHeader() throws Exception {
        TransactionMetadataCache positionCache = new TransactionMetadataCache();
        Config config = Config.defaults();
        final long consensusIndex = 1;
        final long timeStarted = 12345;
        long latestCommittedTxWhenStarted = 4545;
        long timeCommitted = timeStarted + 10;

        LifeSupport life = new LifeSupport();
        final LogFiles logFiles = buildLogFiles();
        LogMetadataProvider logMetadataProvider = logFiles.logMetadataProvider();
        long initialAppendIndex = logMetadataProvider.getLastAppendIndex();
        life.add(logFiles);
        life.start();
        try {
            addATransactionAndRewind(
                    life,
                    logFiles,
                    positionCache,
                    logMetadataProvider,
                    consensusIndex,
                    timeStarted,
                    latestCommittedTxWhenStarted,
                    timeCommitted,
                    jobScheduler);
        } finally {
            life.shutdown();
        }

        // create empty transaction log file and clear transaction cache to force re-read
        LogFile logFile = logFiles.getLogFile();
        fileSystem
                .write(logFile.getLogFileForVersion(logFile.getLogRangeInfo().highestVersion() + 1))
                .close();
        positionCache.clear();

        final LogicalTransactionStore store = new PhysicalLogicalTransactionStore(
                logFiles, positionCache, TestCommandReaderFactory.INSTANCE, monitors, true, config, INSTANCE);
        verifyTransaction(
                positionCache,
                consensusIndex,
                timeStarted,
                latestCommittedTxWhenStarted,
                timeCommitted,
                store,
                initialAppendIndex);
    }

    @Test
    void shouldOpenCleanStore() throws Exception {
        // GIVEN
        LifeSupport life = new LifeSupport();
        final LogFiles logFiles = buildLogFiles();
        LogMetadataProvider logMetadataProvider = logFiles.logMetadataProvider();
        life.add(logFiles);

        life.add(createTransactionAppender(
                logMetadataProvider,
                logFiles,
                new DatabaseConfig(Config.defaults()),
                jobScheduler,
                new TransactionMetadataCache()));

        try {
            // WHEN
            life.start();
        } finally {
            life.shutdown();
        }
    }

    @Test
    void shouldOpenAndRecoverExistingData() throws Exception {
        // GIVEN
        TransactionMetadataCache positionCache = new TransactionMetadataCache();
        var contextFactory = new CursorContextFactory(new DefaultPageCacheTracer(), EMPTY_CONTEXT_SUPPLIER);
        Config config = Config.defaults();
        final long consensusIndex = 2;
        final long timeStarted = 12345;
        long latestCommittedTxWhenStarted = 4545;
        long timeCommitted = timeStarted + 10;
        LifeSupport life = new LifeSupport();
        final LogFiles logFiles = buildLogFiles();
        LogMetadataProvider logMetadataProvider = logFiles.logMetadataProvider();

        life.start();
        life.add(logFiles);
        try {
            addATransactionAndRewind(
                    life,
                    logFiles,
                    positionCache,
                    logMetadataProvider,
                    consensusIndex,
                    timeStarted,
                    latestCommittedTxWhenStarted,
                    timeCommitted,
                    jobScheduler);
        } finally {
            life.shutdown();
        }

        life = new LifeSupport();
        life.add(logFiles);
        final AtomicBoolean recoveryPerformed = new AtomicBoolean();
        FakeRecoveryVisitor visitor =
                new FakeRecoveryVisitor(consensusIndex, timeStarted, timeCommitted, latestCommittedTxWhenStarted);

        LogicalTransactionStore txStore = new PhysicalLogicalTransactionStore(
                logFiles, positionCache, TestCommandReaderFactory.INSTANCE, monitors, true, config, INSTANCE);

        life.add(createTransactionAppender(
                logMetadataProvider, logFiles, new DatabaseConfig(Config.defaults()), jobScheduler, positionCache));
        CorruptedLogsTruncator logPruner =
                new CorruptedLogsTruncator(databaseDirectory, logFiles, fileSystem, INSTANCE);
        life.add(new TransactionLogsRecovery(
                logFiles,
                LatestVersions.LATEST_KERNEL_VERSION_PROVIDER,
                LatestVersions.LATEST_LOG_FORMAT_PROVIDER,
                new TestRecoveryService(visitor, logFiles, txStore, recoveryPerformed),
                logPruner,
                new LifecycleAdapter(),
                mock(RecoveryMonitor.class),
                ProgressMonitorFactory.NONE,
                false,
                false,
                EMPTY_CHECKER,
                RecoveryPredicate.ALL,
                STOP,
                contextFactory,
                Clock.systemUTC(),
                LatestVersions.BINARY_VERSIONS,
                RecoveryMode.FULL,
                new ChunkedTransactionTracker(),
                false));

        // WHEN
        try {
            life.start();
        } finally {
            life.shutdown();
        }

        // THEN
        assertEquals(1, visitor.getVisitedTransactions());
        assertTrue(recoveryPerformed.get());
    }

    @Test
    void shouldExtractMetadataFromExistingTransaction() throws Exception {
        // GIVEN
        TransactionMetadataCache positionCache = new TransactionMetadataCache();
        Config config = Config.defaults();
        final long consensusIndex = 5;
        final long timeStarted = 12345;
        long latestCommittedTxWhenStarted = 4545;
        long timeCommitted = timeStarted + 10;

        LifeSupport life = new LifeSupport();
        final LogFiles logFiles = buildLogFiles();
        LogMetadataProvider logMetadataProvider = logFiles.logMetadataProvider();
        long initialAppendIndex = logMetadataProvider.getLastAppendIndex();
        life.start();
        life.add(logFiles);
        try {
            addATransactionAndRewind(
                    life,
                    logFiles,
                    positionCache,
                    logMetadataProvider,
                    consensusIndex,
                    timeStarted,
                    latestCommittedTxWhenStarted,
                    timeCommitted,
                    jobScheduler);
        } finally {
            life.shutdown();
        }

        life = new LifeSupport();
        life.add(logFiles);
        final LogicalTransactionStore store = new PhysicalLogicalTransactionStore(
                logFiles, positionCache, TestCommandReaderFactory.INSTANCE, monitors, true, config, INSTANCE);

        // WHEN
        life.start();
        try {
            verifyTransaction(
                    positionCache,
                    consensusIndex,
                    timeStarted,
                    latestCommittedTxWhenStarted,
                    timeCommitted,
                    store,
                    initialAppendIndex);
        } finally {
            life.shutdown();
        }
    }

    @Test
    void shouldThrowNoSuchTransactionExceptionIfLogFileIsMissing() throws Exception {
        // GIVEN
        Config config = Config.defaults();
        LogFile logFile = mock(LogFile.class);
        LogFiles logFiles = mock(LogFiles.class);
        // a missing file
        when(logFiles.getLogFile()).thenReturn(logFile);
        when(logFile.getReader(any(LogPosition.class))).thenThrow(new NoSuchFileException("mock"));
        // Which is nevertheless in the metadata cache
        TransactionMetadataCache cache = new TransactionMetadataCache();
        cache.cacheTransactionMetadata(10, new LogPosition(2, 130));

        LifeSupport life = new LifeSupport();

        final LogicalTransactionStore txStore = new PhysicalLogicalTransactionStore(
                logFiles, cache, TestCommandReaderFactory.INSTANCE, monitors, true, config, INSTANCE);

        try {
            life.start();

            // WHEN
            // we ask for that transaction and forward
            assertThrows(NoSuchLogEntryException.class, () -> txStore.getCommandBatches(10));
        } finally {
            life.shutdown();
        }
    }

    private LogFiles buildLogFiles() throws IOException {
        var storeId = new StoreId(1, 2, "engine-1", "format-1", 3, 4);
        return LogFilesBuilder.writeableBuilder(
                        databaseLayout,
                        fileSystem,
                        LatestVersions.LATEST_KERNEL_VERSION_PROVIDER,
                        LatestVersions.LATEST_LOG_FORMAT_PROVIDER)
                .withRotationThreshold(ByteUnit.mebiBytes(1))
                .withLogVersionRepository(mock(LogVersionRepository.class))
                .withCommandReaderFactory(TestCommandReaderFactory.INSTANCE)
                .withStoreId(storeId)
                .build();
    }

    private static void addATransactionAndRewind(
            LifeSupport life,
            LogFiles logFiles,
            TransactionMetadataCache positionCache,
            LogMetadataProvider logMetadataProvider,
            long consensusIndex,
            long timeStarted,
            long latestCommittedTxWhenStarted,
            long timeCommitted,
            JobScheduler jobScheduler)
            throws Exception {
        TransactionAppender appender = life.add(createTransactionAppender(
                logMetadataProvider, logFiles, new DatabaseConfig(Config.defaults()), jobScheduler, positionCache));
        CompleteCommandBatch transaction = new CompleteCommandBatch(
                singleTestCommand(),
                consensusIndex,
                timeStarted,
                latestCommittedTxWhenStarted,
                timeCommitted,
                -1,
                Leases.NO_LEASES,
                LatestVersions.LATEST_KERNEL_VERSION,
                ANONYMOUS);
        var transactionCommitment = new TransactionCommitment(logMetadataProvider);
        appender.register(
                new CompleteTransaction(
                        transaction,
                        NULL_CONTEXT,
                        StoreCursors.NULL,
                        transactionCommitment,
                        new IdStoreTransactionIdGenerator(logMetadataProvider)),
                LogAppendEvent.NULL);
    }

    private static List<StorageCommand> singleTestCommand() {
        return Collections.singletonList(new TestCommand());
    }

    private static void verifyTransaction(
            TransactionMetadataCache positionCache,
            long consensusIndex,
            long timeStarted,
            long latestCommittedTxWhenStarted,
            long timeCommitted,
            LogicalTransactionStore store,
            long initialAppendIndex)
            throws IOException {
        try (CommandBatchCursor cursor = store.getCommandBatches(initialAppendIndex + 1)) {
            boolean hasNext = cursor.next();
            assertTrue(hasNext);
            CommittedCommandBatchRepresentation commandBatch = cursor.get();
            CommandBatch transaction = commandBatch.commandBatch();
            assertEquals(consensusIndex, transaction.consensusIndex());
            assertEquals(timeStarted, transaction.getTimeStarted());
            assertEquals(timeCommitted, commandBatch.timeWritten());
            assertEquals(latestCommittedTxWhenStarted, transaction.getLatestCommittedTxWhenStarted());
        }

        positionCache.clear();
    }

    private static TransactionAppender createTransactionAppender(
            LogMetadataProvider logMetadataProvider,
            LogFiles logFiles,
            DatabaseConfig databaseConfig,
            JobScheduler jobScheduler,
            TransactionMetadataCache positionCache) {
        return TransactionAppenderFactory.createTransactionAppender(
                logFiles,
                logMetadataProvider,
                logMetadataProvider,
                databaseConfig,
                DATABASE_PANIC,
                jobScheduler,
                NullLogProvider.getInstance(),
                positionCache,
                "le db",
                false,
                false);
    }

    private static class FakeRecoveryVisitor implements RecoveryApplier {
        private final long consensusIndex;
        private final long timeStarted;
        private final long timeCommitted;
        private final long latestCommittedTxWhenStarted;
        private int visitedTransactions;

        FakeRecoveryVisitor(
                long consensusIndex, long timeStarted, long timeCommitted, long latestCommittedTxWhenStarted) {
            this.consensusIndex = consensusIndex;
            this.timeStarted = timeStarted;
            this.timeCommitted = timeCommitted;
            this.latestCommittedTxWhenStarted = latestCommittedTxWhenStarted;
        }

        @Override
        public boolean visit(CommittedCommandBatchRepresentation batch) {
            CommandBatch transaction = batch.commandBatch();
            assertEquals(consensusIndex, transaction.consensusIndex());
            assertEquals(timeStarted, transaction.getTimeStarted());
            assertEquals(timeCommitted, batch.timeWritten());
            assertEquals(latestCommittedTxWhenStarted, transaction.getLatestCommittedTxWhenStarted());
            visitedTransactions++;
            return false;
        }

        int getVisitedTransactions() {
            return visitedTransactions;
        }

        @Override
        public void close() {}
    }

    private static class TestRecoveryService implements RecoveryService {
        private final FakeRecoveryVisitor visitor;
        private final LogFiles logFiles;
        private final LogicalTransactionStore txStore;
        private final AtomicBoolean recoveryPerformed;

        TestRecoveryService(
                FakeRecoveryVisitor visitor,
                LogFiles logFiles,
                LogicalTransactionStore txStore,
                AtomicBoolean recoveryPerformed) {
            this.visitor = visitor;
            this.logFiles = logFiles;
            this.txStore = txStore;
            this.recoveryPerformed = recoveryPerformed;
        }

        @Override
        public RecoveryApplier getRecoveryApplier(
                TransactionApplicationMode mode, CursorContextFactory contextFactory, String tracerTag) {
            return mode.isReverseStep() ? mock(RecoveryApplier.class) : visitor;
        }

        @Override
        public RecoveryStartInformation getRecoveryStartInformation() throws IOException {
            LogPosition startPosition = logFiles.getLogFile().extractHeader(0).getStartPosition();
            return new RecoveryStartInformation(startPosition, startPosition, null, 1);
        }

        @Override
        public CommandBatchCursor getCommandBatches(long transactionId) throws IOException {
            return txStore.getCommandBatches(transactionId);
        }

        @Override
        public CommandBatchCursor getCommandBatches(
                LogPosition position, LogPosition maxPosition, boolean treatBrokenLastEntryAsCorruption)
                throws IOException {
            return txStore.getCommandBatches(position, maxPosition, treatBrokenLastEntryAsCorruption);
        }

        @Override
        public CommandBatchCursor getCommandBatchesInReverseOrder(LogPosition position, LogPosition maxPosition)
                throws IOException {
            return txStore.getCommandBatchesInReverseOrder(position, maxPosition);
        }

        @Override
        public void transactionsRecovered(
                BatchInformation highestTransactionHeadCommandBatch,
                AppendIndexProvider appendIndexProvider,
                LogPosition lastTransactionPosition,
                LogPosition positionAfterLastRecoveredTransaction,
                LogPosition checkpointPosition,
                RecoveryOutcome recoveryOutcome) {
            recoveryPerformed.set(true);
        }

        @Override
        public void checkMissingStoreFiles() {}

        @Override
        public void missingLogs() {
            recoveryPerformed.set(true);
        }
    }
}
