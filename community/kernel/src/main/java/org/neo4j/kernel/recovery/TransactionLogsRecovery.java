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

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.kernel.recovery.Recovery.throwUnableToCleanRecover;
import static org.neo4j.kernel.recovery.RecoveryMode.FORWARD;
import static org.neo4j.kernel.recovery.TransactionStatus.RECOVERABLE;
import static org.neo4j.storageengine.AppendIndexProvider.BASE_APPEND_INDEX;
import static org.neo4j.storageengine.api.TransactionApplicationMode.RECOVERY;
import static org.neo4j.storageengine.api.TransactionApplicationMode.REVERSE_RECOVERY;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;

import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.time.Clock;
import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.dbms.database.DatabaseStartAbortedException;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.BinarySupportedKernelVersions;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatchRepresentation;
import org.neo4j.kernel.impl.transaction.log.CommandBatchCursor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.PhysicalFlushableLogPositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.AppendIndexProvider;
import org.neo4j.time.Stopwatch;

/**
 * This is the process of doing a recovery on the transaction log and store, and is executed
 * at startup of {@link Database}.
 */
public class TransactionLogsRecovery extends LifecycleAdapter {
    private static final String REVERSE_RECOVERY_TAG = "restoreDatabase";
    private static final String RECOVERY_TAG = "recoverDatabase";
    private static final String RECOVERY_COMPLETED_TAG = "databaseRecoveryCompleted";

    private final LogFiles logFiles;
    private final KernelVersionProvider versionProvider;
    private final RecoveryService recoveryService;
    private final RecoveryMonitor monitor;
    private final CorruptedLogsTruncator logsTruncator;
    private final Lifecycle schemaLife;
    private final ProgressMonitorFactory progressMonitorFactory;
    private final boolean failOnCorruptedLogFiles;
    private final RecoveryStartupChecker recoveryStartupChecker;
    private final boolean rollbackIncompleteTransactions;
    private final CursorContextFactory contextFactory;
    private final RecoveryPredicate recoveryPredicate;
    private final Clock clock;
    private final BinarySupportedKernelVersions binarySupportedKernelVersions;
    private final RecoveryMode mode;

    private ProgressListener progressListener;

    public TransactionLogsRecovery(
            LogFiles logFiles,
            KernelVersionProvider versionProvider,
            RecoveryService recoveryService,
            CorruptedLogsTruncator logsTruncator,
            Lifecycle schemaLife,
            RecoveryMonitor monitor,
            ProgressMonitorFactory progressMonitorFactory,
            boolean failOnCorruptedLogFiles,
            RecoveryStartupChecker recoveryStartupChecker,
            RecoveryPredicate recoveryPredicate,
            boolean rollbackIncompleteTransactions,
            CursorContextFactory contextFactory,
            Clock clock,
            BinarySupportedKernelVersions binarySupportedKernelVersions,
            RecoveryMode mode) {
        this.logFiles = logFiles;
        this.versionProvider = versionProvider;
        this.recoveryService = recoveryService;
        this.monitor = monitor;
        this.logsTruncator = logsTruncator;
        this.schemaLife = schemaLife;
        this.progressMonitorFactory = progressMonitorFactory;
        this.failOnCorruptedLogFiles = failOnCorruptedLogFiles;
        this.recoveryStartupChecker = recoveryStartupChecker;
        this.rollbackIncompleteTransactions = rollbackIncompleteTransactions;
        this.contextFactory = contextFactory;
        this.recoveryPredicate = recoveryPredicate;
        this.clock = clock;
        this.binarySupportedKernelVersions = binarySupportedKernelVersions;
        this.mode = mode;
        this.progressListener = null;
    }

    @Override
    public void init() throws Exception {
        RecoveryStartInformation recoveryStartInformation = recoveryService.getRecoveryStartInformation();
        if (!recoveryStartInformation.isRecoveryRequired()) {
            schemaLife.init();
            return;
        }

        Stopwatch recoveryStartTime = Stopwatch.start();

        TransactionIdTracker transactionIdTracker = new TransactionIdTracker();
        LogPosition recoveryStartPosition = recoveryStartInformation.transactionLogPosition();

        monitor.recoveryRequired(recoveryStartInformation);

        var recoveryContextTracker =
                new RecoveryContextTracker(recoveryStartPosition, recoveryStartInformation.checkpointInfo());

        RecoveryRollbackAppendIndexProvider appendIndexProvider = null;
        boolean incompleteBatchEncountered = false;
        try {
            if (!recoveryStartInformation.missingLogs()) {
                try {
                    reverseRecovery(recoveryStartInformation, transactionIdTracker);

                    // We cannot initialise the schema (tokens, schema cache, indexing service, etc.) until we have
                    // returned
                    // the store to a consistent state.
                    // We need to be able to read the store before we can even figure out what indexes, tokens, etc. we
                    // have. Hence, we defer the initialisation
                    // of the schema life until after we've done the reverse recovery.
                    schemaLife.init();

                    boolean fullRecovery = true;
                    try (CommandBatchCursor transactionsToRecover =
                                    recoveryService.getCommandBatches(recoveryStartPosition);
                            var recoveryVisitor =
                                    recoveryService.getRecoveryApplier(RECOVERY, contextFactory, RECOVERY_TAG)) {
                        while (fullRecovery && transactionsToRecover.next()) {
                            var nextCommandBatch = transactionsToRecover.get();
                            if (!recoveryPredicate.test(nextCommandBatch)) {
                                monitor.partialRecovery(
                                        recoveryPredicate, recoveryContextTracker.getLastHighestTransactionBatchInfo());
                                fullRecovery = false;
                                if (!recoveryContextTracker.hasRecoveredBatches()) {
                                    // First transaction after checkpoint failed predicate test
                                    // we can't always load transaction before checkpoint to check what values we had
                                    // there
                                    // since those logs may be pruned,
                                    // but we will try to load first transaction before checkpoint to see if we just on
                                    // the
                                    // edge of provided criteria
                                    // and will fail otherwise.
                                    long beforeCheckpointAppendIndex =
                                            recoveryStartInformation.firstAppendIndexAfterLastCheckPoint() - 1;
                                    if (beforeCheckpointAppendIndex < BASE_APPEND_INDEX) {
                                        throw new RecoveryPredicateException(format(
                                                "Partial recovery criteria can't be satisfied. No transaction after checkpoint matching to provided "
                                                        + "criteria found and transaction before checkpoint is not valid. "
                                                        + "Append index before checkpoint: %d, criteria %s.",
                                                beforeCheckpointAppendIndex, recoveryPredicate.describe()));
                                    }
                                    try (var beforeCheckpointCursor =
                                            recoveryService.getCommandBatches(beforeCheckpointAppendIndex)) {
                                        if (beforeCheckpointCursor.next()) {
                                            CommittedCommandBatchRepresentation candidate =
                                                    beforeCheckpointCursor.get();
                                            if (!recoveryPredicate.test(candidate)) {
                                                throw new RecoveryPredicateException(format(
                                                        "Partial recovery criteria can't be satisfied. "
                                                                + "Transaction after and before checkpoint does not satisfy provided recovery criteria. "
                                                                + "Observed transaction id: %d, recovery criteria: %s.",
                                                        candidate.txId(), recoveryPredicate.describe()));
                                            }
                                            recoveryContextTracker.commitedBatch(
                                                    candidate, beforeCheckpointCursor.position());
                                        } else {
                                            throw new RecoveryPredicateException(format(
                                                    "Partial recovery criteria can't be satisfied. No transaction after checkpoint matching "
                                                            + "to provided criteria found and transaction before checkpoint not found. Recovery criteria: %s.",
                                                    recoveryPredicate.describe()));
                                        }
                                    } catch (RecoveryPredicateException re) {
                                        throw re;
                                    } catch (Exception e) {
                                        throw new RecoveryPredicateException(
                                                format(
                                                        "Partial recovery criteria can't be satisfied. No transaction after checkpoint matching "
                                                                + "to provided criteria found and fail to read transaction before checkpoint. Recovery criteria: %s.",
                                                        recoveryPredicate.describe()),
                                                e);
                                    }
                                }
                            } else {
                                recoveryStartupChecker.checkIfCanceled();
                                switch (transactionIdTracker.transactionStatus(nextCommandBatch.txId())) {
                                    case RECOVERABLE -> {
                                        recoveryVisitor.visit(nextCommandBatch);
                                        monitor.batchRecovered(nextCommandBatch);
                                    }
                                    case ROLLED_BACK -> monitor.batchApplySkipped(nextCommandBatch);
                                    case INCOMPLETE -> {
                                        monitor.batchApplySkipped(nextCommandBatch);
                                        if (!rollbackIncompleteTransactions) {
                                            fullRecovery = false;
                                            incompleteBatchEncountered = true;
                                        }
                                    }
                                }
                                if (!incompleteBatchEncountered) {
                                    recoveryContextTracker.commitedBatch(
                                            nextCommandBatch, transactionsToRecover.position());
                                }
                                reportProgress();
                            }
                        }
                        recoveryContextTracker.completeRecovery(
                                fullRecovery
                                        ? transactionsToRecover.position()
                                        : recoveryContextTracker.getLastTransactionPosition());
                    }
                } catch (Error
                        | ClosedByInterruptException
                        | DatabaseStartAbortedException
                        | RecoveryPredicateException e) {
                    // We do not want to truncate logs based on these exceptions. Since users can influence them with
                    // config
                    // changes
                    // the users are able to workaround this if truncations is really needed.
                    throw e;
                } catch (Throwable t) {
                    if (failOnCorruptedLogFiles) {
                        throwUnableToCleanRecover(t);
                    }
                    if (recoveryContextTracker.hasRecoveredBatches()) {
                        monitor.failToRecoverTransactionsAfterCommit(
                                t,
                                recoveryContextTracker.getLastHighestTransactionBatchInfo(),
                                recoveryContextTracker.getRecoveryToPosition());
                    } else {
                        monitor.failToRecoverTransactionsAfterPosition(t, recoveryStartPosition);
                    }
                }

                appendIndexProvider =
                        new RecoveryRollbackAppendIndexProvider(recoveryContextTracker.getLastBatchInfo());
                if (rollbackIncompleteTransactions) {
                    logsTruncator.truncate(
                            recoveryContextTracker.getRecoveryToPosition(), recoveryStartInformation.checkpointInfo());
                    var rollbackTransactionInfo = rollbackTransactions(
                            recoveryContextTracker.getRecoveryToPosition(),
                            transactionIdTracker,
                            appendIndexProvider,
                            monitor);
                    if (rollbackTransactionInfo != null) {
                        recoveryContextTracker.rollbackBatch(
                                rollbackTransactionInfo, rollbackTransactionInfo.position());
                    }
                }
            }
        } finally {
            closeProgress();
        }

        try (var cursorContext = contextFactory.create(RECOVERY_COMPLETED_TAG)) {
            final boolean missingLogs = recoveryStartInformation.missingLogs();
            recoveryService.transactionsRecovered(
                    recoveryContextTracker.getLastHighestTransactionBatchInfo(),
                    appendIndexProvider,
                    recoveryContextTracker.getLastTransactionPosition(),
                    recoveryContextTracker.getRecoveryToPosition(),
                    recoveryStartInformation.getCheckpointPosition(),
                    missingLogs,
                    cursorContext);
        }
        monitor.recoveryCompleted(recoveryStartTime.elapsed(MILLISECONDS), mode);
    }

    private RollbackTransactionInfo rollbackTransactions(
            LogPosition writePosition,
            TransactionIdTracker transactionTracker,
            AppendIndexProvider appendIndexProvider,
            RecoveryMonitor monitor)
            throws IOException {
        long[] notCompletedTransactions = transactionTracker.notCompletedTransactions();
        if (notCompletedTransactions.length == 0) {
            return null;
        }
        KernelVersion kernelVersion = versionProvider.kernelVersion();
        LogFile logFile = logFiles.getLogFile();
        PhysicalLogVersionedStoreChannel channel =
                logFile.createLogChannelForExistingVersion(writePosition.getLogVersion());
        LogHeader logHeader = logFile.extractHeader(writePosition.getLogVersion());
        channel.position(writePosition.getByteOffset());
        try (var writerChannel =
                new PhysicalFlushableLogPositionAwareChannel(channel, logHeader, EmptyMemoryTracker.INSTANCE)) {
            var entryWriter = new LogEntryWriter<>(writerChannel, binarySupportedKernelVersions);
            long time = clock.millis();
            CommittedCommandBatchRepresentation.BatchInformation lastBatchInfo = null;
            for (int i = 0; i < notCompletedTransactions.length; i++) {
                long notCompletedTransaction = notCompletedTransactions[i];
                long appendIndex = appendIndexProvider.nextAppendIndex();
                int checksum =
                        entryWriter.writeRollbackEntry(kernelVersion, notCompletedTransaction, appendIndex, time);
                if (i == (notCompletedTransactions.length - 1)) {
                    lastBatchInfo = new CommittedCommandBatchRepresentation.BatchInformation(
                            notCompletedTransaction,
                            kernelVersion,
                            checksum,
                            time,
                            UNKNOWN_CONSENSUS_INDEX,
                            appendIndex);
                }
                monitor.rollbackTransaction(notCompletedTransaction, appendIndex);
            }

            return new RollbackTransactionInfo(lastBatchInfo, writerChannel.getCurrentLogPosition());
        }
    }

    private void reverseRecovery(
            RecoveryStartInformation recoveryStartInformation, TransactionIdTracker transactionIdTracker)
            throws Exception {

        if (mode == FORWARD) {
            // nothing to do, update the progress to match this
            initProgressReporter(recoveryStartInformation, recoveryStartInformation.transactionLogPosition());
            return;
        }
        CommittedCommandBatchRepresentation lastReversedCommandBatch = null;

        var oldestNotVisibleTransactionLogPosition = recoveryStartInformation.oldestNotVisibleTransactionLogPosition();
        var checkpointedLogPosition = recoveryStartInformation.transactionLogPosition();

        long lowestRecoveredAppendIndex = recoveryStartInformation.firstAppendIndexAfterLastCheckPoint();
        try (var transactionsToRecover =
                        recoveryService.getCommandBatchesInReverseOrder(oldestNotVisibleTransactionLogPosition);
                var recoveryVisitor =
                        recoveryService.getRecoveryApplier(REVERSE_RECOVERY, contextFactory, REVERSE_RECOVERY_TAG)) {
            while (transactionsToRecover.next()) {
                recoveryStartupChecker.checkIfCanceled();
                CommittedCommandBatchRepresentation commandBatch = transactionsToRecover.get();
                if (lastReversedCommandBatch == null) {
                    lastReversedCommandBatch = commandBatch;
                    initProgressReporter(recoveryStartInformation, lastReversedCommandBatch, mode);
                }
                transactionIdTracker.trackBatch(commandBatch);
                // we need to unroll transactions that were never completed or located after checkpointed position
                if (shouldReverseBatch(
                        transactionIdTracker, transactionsToRecover, checkpointedLogPosition, commandBatch)) {
                    recoveryVisitor.visit(commandBatch);
                }
                lowestRecoveredAppendIndex = commandBatch.appendIndex();
                reportProgress();
            }
        }
        monitor.reverseStoreRecoveryCompleted(lowestRecoveredAppendIndex);
    }

    private static boolean shouldReverseBatch(
            TransactionIdTracker transactionIdTracker,
            CommandBatchCursor transactionsToRecover,
            LogPosition checkpointedLogPosition,
            CommittedCommandBatchRepresentation commandBatch) {
        return transactionsToRecover.position().compareTo(checkpointedLogPosition) >= 0
                || (RECOVERABLE != transactionIdTracker.transactionStatus(commandBatch.txId()));
    }

    private void initProgressReporter(
            RecoveryStartInformation recoveryStartInformation, LogPosition recoveryStartPosition) throws IOException {
        try (var transactionsToRecover = recoveryService.getCommandBatchesInReverseOrder(recoveryStartPosition)) {
            if (transactionsToRecover.next()) {
                CommittedCommandBatchRepresentation commandBatch = transactionsToRecover.get();
                initProgressReporter(recoveryStartInformation, commandBatch, mode);
            }
        }
    }

    private void initProgressReporter(
            RecoveryStartInformation recoveryStartInformation,
            CommittedCommandBatchRepresentation lastReversedBatch,
            RecoveryMode mode) {
        long numberOfBatchesToRecover = estimateNumberOfBatchesToRecover(recoveryStartInformation, lastReversedBatch);
        // In full mode we will process each transaction twice (doing reverse and direct detour) we need to
        // multiply number of transactions that we want to recover by 2 to be able to report correct progress
        progressListener = progressMonitorFactory.singlePart(
                "TransactionLogsRecovery",
                mode == RecoveryMode.FULL ? numberOfBatchesToRecover * 2 : numberOfBatchesToRecover);
    }

    private void reportProgress() {
        progressListener.add(1);
    }

    private void closeProgress() {
        if (progressListener != null) {
            progressListener.close();
        }
    }

    private static long estimateNumberOfBatchesToRecover(
            RecoveryStartInformation recoveryStartInformation,
            CommittedCommandBatchRepresentation lastReversedCommandBatch) {
        return lastReversedCommandBatch.appendIndex()
                - recoveryStartInformation.firstAppendIndexAfterLastCheckPoint()
                + 1;
    }

    @Override
    public void start() throws Exception {
        schemaLife.start();
    }

    @Override
    public void stop() throws Exception {
        schemaLife.stop();
    }

    @Override
    public void shutdown() throws Exception {
        schemaLife.shutdown();
    }

    private static class RecoveryRollbackAppendIndexProvider implements AppendIndexProvider {
        private final MutableLong rollbackIndex;

        public RecoveryRollbackAppendIndexProvider(CommittedCommandBatchRepresentation.BatchInformation lastBatchInfo) {
            this.rollbackIndex = lastBatchInfo == null
                    ? new MutableLong(BASE_APPEND_INDEX)
                    : new MutableLong(lastBatchInfo.appendIndex());
        }

        @Override
        public long nextAppendIndex() {
            return rollbackIndex.incrementAndGet();
        }

        @Override
        public long getLastAppendIndex() {
            return rollbackIndex.longValue();
        }
    }
}
