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
import static org.neo4j.kernel.recovery.IncompleteTransactionAction.ROLLBACK;
import static org.neo4j.kernel.recovery.Recovery.throwUnableToCleanRecover;
import static org.neo4j.kernel.recovery.RecoveryMode.FULL;
import static org.neo4j.storageengine.AppendIndexProvider.BASE_APPEND_INDEX;
import static org.neo4j.storageengine.AppendIndexProvider.UNKNOWN_APPEND_INDEX;
import static org.neo4j.storageengine.api.TransactionApplicationMode.MVCC_INCOMPLETE_REVERSE_RECOVERY;
import static org.neo4j.storageengine.api.TransactionApplicationMode.RECOVERY;
import static org.neo4j.storageengine.api.TransactionApplicationMode.REVERSE_RECOVERY;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_TX_SEQUENCE_NUMBER;
import static org.neo4j.wal.LogTermProvider.UNKNOWN_TERM_PROVIDER;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.time.Clock;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.dbms.database.DatabaseStartAbortedException;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.BinarySupportedKernelVersions;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.api.ChunkedTransactionTracker;
import org.neo4j.kernel.impl.api.LeaseService;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatchRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.recovery.TransactionIdTracker.PartialLastTransactionChunk;
import org.neo4j.storageengine.AppendIndexProvider;
import org.neo4j.storageengine.api.OpenTransactionMetadata;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.wal.LogFile;
import org.neo4j.wal.LogFormatVersionProvider;
import org.neo4j.wal.LogPosition;
import org.neo4j.wal.PartialRecoveryOutcome;
import org.neo4j.wal.PhysicalFlushableLogPositionAwareChannel;
import org.neo4j.wal.PhysicalLogVersionedStoreChannel;
import org.neo4j.wal.RecoveryOutcome;
import org.neo4j.wal.WALService;
import org.neo4j.wal.entry.LogEntryWriter;
import org.neo4j.wal.rotation.LogRotateEvents;
import org.neo4j.wal.rotation.LogRotation;
import org.neo4j.wal.rotation.NextLogHeader;

/**
 * This is the process of doing a recovery on the transaction log and store, and is executed
 * at startup of {@link Database}.
 */
public class TransactionLogsRecovery extends LifecycleAdapter {
    private static final String REVERSE_RECOVERY_TAG = "restoreDatabase";
    private static final String RECOVERY_TAG = "recoverDatabase";

    private final WALService walService;
    private final KernelVersionProvider versionProvider;
    private final LogFormatVersionProvider logFormatVersionProvider;
    private final RecoveryService recoveryService;
    private final RecoveryMonitor monitor;
    private final CorruptedLogsTruncator logsTruncator;
    private final Lifecycle schemaLife;
    private final ProgressMonitorFactory progressMonitorFactory;
    private final boolean failOnCorruptedLogFiles;
    private final boolean treatBrokenLastEntryAsCorruption;
    private final RecoveryStartupChecker recoveryStartupChecker;
    private final IncompleteTransactionAction incompleteTransactionAction;
    private final CursorContextFactory contextFactory;
    private final RecoveryPredicate recoveryPredicate;
    private final Clock clock;
    private final BinarySupportedKernelVersions binarySupportedKernelVersions;
    private final RecoveryMode mode;
    private final ChunkedTransactionTracker chunkedTransactionTracker;
    private final boolean parallelRecovery;

    private ProgressListener progressListener;
    private RecoveryOutcome recoveryOutcome = RecoveryOutcome.EMPTY_OUTCOME;

    public TransactionLogsRecovery(
            WALService walService,
            KernelVersionProvider versionProvider,
            LogFormatVersionProvider logFormatVersionProvider,
            RecoveryService recoveryService,
            CorruptedLogsTruncator logsTruncator,
            Lifecycle schemaLife,
            RecoveryMonitor monitor,
            ProgressMonitorFactory progressMonitorFactory,
            boolean failOnCorruptedLogFiles,
            boolean treatBrokenLastEntryAsCorruption,
            RecoveryStartupChecker recoveryStartupChecker,
            RecoveryPredicate recoveryPredicate,
            IncompleteTransactionAction incompleteTransactionAction,
            CursorContextFactory contextFactory,
            Clock clock,
            BinarySupportedKernelVersions binarySupportedKernelVersions,
            RecoveryMode mode,
            ChunkedTransactionTracker chunkedTransactionTracker,
            boolean parallelRecovery) {
        this.walService = walService;
        this.versionProvider = versionProvider;
        this.logFormatVersionProvider = logFormatVersionProvider;
        this.recoveryService = recoveryService;
        this.monitor = monitor;
        this.logsTruncator = logsTruncator;
        this.schemaLife = schemaLife;
        this.progressMonitorFactory = progressMonitorFactory;
        this.failOnCorruptedLogFiles = failOnCorruptedLogFiles;
        this.treatBrokenLastEntryAsCorruption = treatBrokenLastEntryAsCorruption;
        this.recoveryStartupChecker = recoveryStartupChecker;
        this.incompleteTransactionAction = incompleteTransactionAction;
        this.contextFactory = contextFactory;
        this.recoveryPredicate = recoveryPredicate;
        this.clock = clock;
        this.binarySupportedKernelVersions = binarySupportedKernelVersions;
        this.mode = mode;
        this.chunkedTransactionTracker = chunkedTransactionTracker;
        this.parallelRecovery = parallelRecovery;
        this.progressListener = null;
    }

    @Override
    public void init() throws Exception {
        RecoveryStartInformation recoveryStartInformation = recoveryService.getRecoveryStartInformation();
        if (!recoveryStartInformation.isRecoveryRequired()) {
            recoveryService.checkMissingStoreFiles();
            schemaLife.init();
            return;
        }

        monitor.recoveryRequired(recoveryStartInformation);
        if (recoveryStartInformation.missingLogs()) {
            recoveryService.checkMissingStoreFiles();
            recoveryService.missingLogs();
            walService.getLogFile().initializeMissingLogFile();
            return;
        }
        performRecovery(recoveryStartInformation);
    }

    private void performRecovery(RecoveryStartInformation recoveryStartInformation)
            throws DatabaseStartAbortedException, RecoveryPredicateException, IOException {
        try {
            var recoveryStartPosition = recoveryStartInformation.oldestNotVisibleTransactionLogPosition();
            var recoveryContextTracker = new RecoveryContextTracker(
                    recoveryStartPosition, recoveryStartInformation.checkpointInfo(), incompleteTransactionAction);
            var transactionIdTracker = new TransactionIdTracker(incompleteTransactionAction);
            reverseAndForwardRecovery(
                    recoveryStartInformation, transactionIdTracker, recoveryStartPosition, recoveryContextTracker);

            var appendIndexProvider =
                    new RecoveryRollbackAppendIndexProvider(recoveryContextTracker.getLastBatchInfo());
            if (ROLLBACK == incompleteTransactionAction && recoveryPredicate.changingTheLogAllowed()) {
                logsTruncator.truncate(
                        recoveryContextTracker.getRecoveryToPosition(), recoveryStartInformation.checkpointInfo());
            }

            var rollbackTransactionInfo = handleIncompleteTransactions(
                    recoveryContextTracker.getRecoveryToPosition(), transactionIdTracker, appendIndexProvider, monitor);
            if (rollbackTransactionInfo != null) {
                recoveryContextTracker.rollbackBatch(rollbackTransactionInfo, rollbackTransactionInfo.position());
            }

            recoveryOutcome = createOutcome(recoveryContextTracker, chunkedTransactionTracker);

            recoveryService.transactionsRecovered(
                    recoveryContextTracker.getLastHighestTransactionBatchInfo(),
                    recoveryContextTracker.getLastBatchInfo(),
                    appendIndexProvider,
                    recoveryContextTracker.getLastTransactionPosition(),
                    recoveryContextTracker.getRecoveryToPosition(),
                    recoveryStartInformation.getCheckpointPosition(),
                    recoveryOutcome);
            monitor.transactionsRecovered(
                    recoveryContextTracker.getLastHighestTransactionBatchInfo(),
                    appendIndexProvider,
                    recoveryContextTracker.getLastTransactionPosition(),
                    recoveryContextTracker.getRecoveryToPosition(),
                    recoveryStartInformation.getCheckpointPosition());
        } finally {
            closeProgress();
        }
    }

    private RecoveryOutcome createOutcome(
            RecoveryContextTracker recoveryContextTracker, ChunkedTransactionTracker chunkedTransactionTracker) {
        // we create outcome only if we need to apply those partial transactions
        if (incompleteTransactionAction != IncompleteTransactionAction.APPLY) {
            return RecoveryOutcome.EMPTY_OUTCOME;
        }
        var incompleteTransactions = chunkedTransactionTracker.incompleteTransactions();
        if (incompleteTransactions.isEmpty()) {
            return RecoveryOutcome.EMPTY_OUTCOME;
        }
        long[] notClosedTransactionIds = new long[incompleteTransactions.size()];
        int index = 0;
        for (ChunkedTransactionTracker.TransactionInfo transactionInfo : incompleteTransactions) {
            notClosedTransactionIds[index++] = transactionInfo.transactionId();
        }
        Arrays.sort(notClosedTransactionIds);

        var lastHighestTransactionBatchInfo = recoveryContextTracker.getLastHighestTransactionBatchInfo();
        return new PartialRecoveryOutcome(
                notClosedTransactionIds,
                new TransactionId(
                        lastHighestTransactionBatchInfo.txId(),
                        lastHighestTransactionBatchInfo.appendIndex(),
                        lastHighestTransactionBatchInfo.kernelVersion(),
                        lastHighestTransactionBatchInfo.checksum(),
                        lastHighestTransactionBatchInfo.timeWritten(),
                        lastHighestTransactionBatchInfo.consensusIndex()),
                recoveryContextTracker.gapFreeClosedTransactionInfo(),
                recoveryContextTracker.getEarliestOpenTransactionMetadata());
    }

    private void reverseAndForwardRecovery(
            RecoveryStartInformation recoveryStartInformation,
            TransactionIdTracker transactionIdTracker,
            LogPosition recoveryStartPosition,
            RecoveryContextTracker recoveryContextTracker)
            throws ClosedByInterruptException, DatabaseStartAbortedException, RecoveryPredicateException {
        try {
            if (mode == FULL) {
                reverseRecovery(recoveryStartInformation, transactionIdTracker);
            } else {
                skipReverseRecovery(recoveryStartInformation);
            }

            recoveryService.checkMissingStoreFiles();
            forwardRecovery(
                    recoveryStartInformation, transactionIdTracker, recoveryStartPosition, recoveryContextTracker);
        } catch (Error | ClosedByInterruptException | DatabaseStartAbortedException | RecoveryPredicateException e) {
            // We do not want to truncate logs based on these exceptions. Since users can influence them with
            // config changes. The users are able to workaround this if truncations is really needed.
            throw e;
        } catch (Throwable t) {
            handleUnexpectedRecoveryError(recoveryStartPosition, recoveryContextTracker, t);
        }
    }

    private void skipReverseRecovery(RecoveryStartInformation recoveryStartInformation) throws Exception {
        initProgressReporter(recoveryStartInformation, recoveryStartInformation.transactionLogPosition());
        schemaLife.init();
    }

    private void handleUnexpectedRecoveryError(
            LogPosition recoveryStartPosition, RecoveryContextTracker recoveryContextTracker, Throwable t) {
        if (failOnCorruptedLogFiles) {
            if (parallelRecovery) {
                throwUnableToDoParallelRecovery(t);
            }
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

    private static void throwUnableToDoParallelRecovery(Throwable t) {
        throw new RuntimeException(
                "Error during database recovery when running in parallel mode. You can disable parallel recovery by setting '"
                        + GraphDatabaseInternalSettings.do_parallel_recovery.name()
                        + "=false' or --parallel-recovery=false if running backup and try again.",
                t);
    }

    private void forwardRecovery(
            RecoveryStartInformation recoveryStartInformation,
            TransactionIdTracker transactionIdTracker,
            LogPosition recoveryStartPosition,
            RecoveryContextTracker recoveryContextTracker)
            throws Exception {
        try (var transactionsToRecover = recoveryService.getCommandBatches(
                        recoveryStartPosition, recoveryPredicate.maxPosition(), treatBrokenLastEntryAsCorruption);
                var recoveryVisitor = recoveryService.getRecoveryApplier(RECOVERY, contextFactory, RECOVERY_TAG)) {
            LogPosition previousBatchStatePosition = transactionsToRecover.position();
            while (transactionsToRecover.next()) {
                var nextCommandBatch = transactionsToRecover.get();
                if (!recoveryPredicate.test(nextCommandBatch)) {
                    completeAsPartialRecovery(recoveryStartInformation, recoveryContextTracker);
                    return;
                }
                recoveryStartupChecker.checkIfCanceled();
                if (processCommandBatch(
                        transactionIdTracker,
                        nextCommandBatch,
                        recoveryVisitor,
                        recoveryContextTracker,
                        previousBatchStatePosition)) {
                    recoveryContextTracker.completeRecovery(recoveryContextTracker.getLastTransactionPosition());
                    return;
                }
                recoveryContextTracker.commitedBatch(nextCommandBatch, transactionsToRecover.position());
                reportProgress();
                previousBatchStatePosition = transactionsToRecover.position();
            }
            recoveryContextTracker.completeRecovery(transactionsToRecover.position());
        }
    }

    private boolean processCommandBatch(
            TransactionIdTracker idTracker,
            CommittedCommandBatchRepresentation commandBatch,
            RecoveryApplier visitor,
            RecoveryContextTracker recoveryContextTracker,
            LogPosition previousBatchStatePosition)
            throws Exception {
        switch (idTracker.transactionStatus(commandBatch.txId())) {
            case RECOVERABLE -> {
                visitor.visit(commandBatch);
                monitor.batchRecovered(commandBatch);
            }
            case ROLLED_BACK -> monitor.batchApplySkipped(commandBatch);
            case INCOMPLETE -> {
                monitor.batchApplySkipped(commandBatch);
                return ROLLBACK != incompleteTransactionAction;
            }
            case INCOMPLETE_RECOVERABLE -> {
                recoveryContextTracker.unrecoverableBatch(new OpenTransactionMetadata(
                        commandBatch.txId(), commandBatch.appendIndex(), previousBatchStatePosition));
                visitor.visit(commandBatch);
                monitor.batchRecovered(commandBatch);
            }
        }
        return false;
    }

    private void completeAsPartialRecovery(
            RecoveryStartInformation recoveryStartInformation, RecoveryContextTracker recoveryContextTracker)
            throws RecoveryPredicateException {
        monitor.partialRecovery(recoveryPredicate, recoveryContextTracker.getLastHighestTransactionBatchInfo());
        verifyPartialRecovery(recoveryStartInformation, recoveryContextTracker);
        recoveryContextTracker.completeRecovery(recoveryContextTracker.getLastTransactionPosition());
    }

    private void verifyPartialRecovery(
            RecoveryStartInformation recoveryStartInformation, RecoveryContextTracker recoveryContextTracker)
            throws RecoveryPredicateException {
        if (recoveryContextTracker.hasRecoveredBatches()) {
            // at least one transaction is already recovered, so it exists and satisfies recovery predicate
            return;
        }
        // First transaction after checkpoint failed predicate test
        // we can't always load transaction before checkpoint to check what values we had
        // there since those logs may be pruned,
        // but we will try to load first transaction before checkpoint to see if we just on the
        // edge of provided criteria and will fail otherwise.
        long beforeCheckpointAppendIndex = recoveryStartInformation.firstAppendIndexAfterLastCheckPoint() - 1;
        if (beforeCheckpointAppendIndex < BASE_APPEND_INDEX) {
            throw new RecoveryPredicateException(format(
                    "Partial recovery criteria can't be satisfied. No transaction after checkpoint matching to provided "
                            + "criteria found and transaction before checkpoint is not valid. "
                            + "Append index before checkpoint: %d, criteria %s.",
                    beforeCheckpointAppendIndex, recoveryPredicate.describe()));
        }
        verifyCommandBatchSatisfiesPartialRecoveryPredicate(recoveryContextTracker, beforeCheckpointAppendIndex);
    }

    private void verifyCommandBatchSatisfiesPartialRecoveryPredicate(
            RecoveryContextTracker recoveryContextTracker, long appendIndex) throws RecoveryPredicateException {
        try (var commandBatches = recoveryService.getCommandBatches(appendIndex)) {
            if (!commandBatches.next()) {
                throw new RecoveryPredicateException(format(
                        "Partial recovery criteria can't be satisfied. No transaction after checkpoint matching "
                                + "to provided criteria found and transaction before checkpoint not found. Recovery criteria: %s.",
                        recoveryPredicate.describe()));
            }
            var candidate = commandBatches.get();
            if (!recoveryPredicate.test(candidate)) {
                throw new RecoveryPredicateException(format(
                        "Partial recovery criteria can't be satisfied. "
                                + "Transaction after and before checkpoint does not satisfy provided recovery criteria. "
                                + "Observed transaction id: %d, recovery criteria: %s.",
                        candidate.txId(), recoveryPredicate.describe()));
            }
            recoveryContextTracker.commitedBatch(candidate, commandBatches.position());
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

    /**
     * Depending on the incomplete transaction action, either rollback the incomplete transactions or register them as incomplete so they can be completed once online.
     * @return RollbackTransactionInfo if the incomplete transactions were rolled back, null otherwise.
     */
    private RollbackTransactionInfo handleIncompleteTransactions(
            LogPosition writePosition,
            TransactionIdTracker transactionTracker,
            AppendIndexProvider appendIndexProvider,
            RecoveryMonitor monitor)
            throws IOException {

        chunkedTransactionTracker.clear();
        long[] notCompletedTransactions = transactionTracker.notCompletedTransactions();

        if (notCompletedTransactions.length == 0) {
            return null;
        }

        KernelVersion kernelVersion = versionProvider.kernelVersion();
        if (ROLLBACK == incompleteTransactionAction) {
            LogFile logFile = walService.getLogFile();
            try (ChannelWithPartialLogRotationAbility channelAllocator = new ChannelWithPartialLogRotationAbility(
                    logFile, versionProvider, logFormatVersionProvider, logFile.rotationSize(), writePosition)) {
                PhysicalFlushableLogPositionAwareChannel writerChannel = channelAllocator.getWriterChannel();
                var entryWriter = new LogEntryWriter<>(writerChannel, binarySupportedKernelVersions);
                long time = clock.millis();
                CommittedCommandBatchRepresentation.BatchInformation lastBatchInfo = null;
                for (int i = 0; i < notCompletedTransactions.length; i++) {
                    long notCompletedTransaction = notCompletedTransactions[i];
                    long appendIndex = appendIndexProvider.nextAppendIndex();
                    var partialLastTransactionChunk =
                            transactionTracker.getPartialLastTransactionChunk(notCompletedTransaction);
                    int checksum = entryWriter.writeRollbackEntry(
                            kernelVersion,
                            notCompletedTransaction,
                            appendIndex,
                            partialLastTransactionChunk.lastSeenChunkId(),
                            time,
                            UNKNOWN_TX_SEQUENCE_NUMBER,
                            partialLastTransactionChunk.lastSeenAppendIndex(),
                            UNKNOWN_CONSENSUS_INDEX);
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

        var notCompletedTransactionChunks = transactionTracker.getPartialLastTransactionChunks();
        for (PartialLastTransactionChunk partialLastTransactionChunk : notCompletedTransactionChunks) {
            chunkedTransactionTracker.registerChunkedTransaction(
                    partialLastTransactionChunk.transactionId(),
                    partialLastTransactionChunk.earliestSeenAppendIndex(),
                    partialLastTransactionChunk.lastSeenAppendIndex(),
                    partialLastTransactionChunk.lastSeenChunkId(),
                    kernelVersion,
                    LeaseService.NO_LEASE);
        }
        return null;
    }

    private static class ChannelWithPartialLogRotationAbility implements LogRotation, Closeable {
        private final PhysicalFlushableLogPositionAwareChannel writer;
        private PhysicalLogVersionedStoreChannel channel;
        private final LogFile logFile;
        private final KernelVersionProvider versionProvider;
        private final LogFormatVersionProvider logFormatVersionProvider;
        private final long rotateAtSize;

        public ChannelWithPartialLogRotationAbility(
                LogFile logFile,
                KernelVersionProvider versionProvider,
                LogFormatVersionProvider logFormatVersionProvider,
                long rotateAtSize,
                LogPosition writePosition)
                throws IOException {
            this.logFile = logFile;
            this.versionProvider = versionProvider;
            this.logFormatVersionProvider = logFormatVersionProvider;
            this.rotateAtSize = rotateAtSize;

            channel = logFile.createLogChannelForExistingVersion(writePosition.getLogVersion());
            channel.position(writePosition.getByteOffset());
            writer = new PhysicalFlushableLogPositionAwareChannel(
                    channel,
                    logFile.extractHeader(writePosition.getLogVersion()),
                    new PhysicalFlushableLogPositionAwareChannel.VersionedPhysicalFlushableLogChannelProvider(
                            LogRotation.NO_ROTATION, DatabaseTracer.NULL, logFile.createScopedBuffer()));
        }

        public PhysicalFlushableLogPositionAwareChannel getWriterChannel() {
            return writer;
        }

        @Override
        public boolean rotateLogIfNeeded(LogRotateEvents logRotateEvents) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean locklessRotateLogIfNeeded(LogRotateEvents logRotateEvents, NextLogHeader.Builder headerBuilder) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean locklessBatchedRotateLogIfNeeded(
                LogRotateEvents logRotateEvents, NextLogHeader.Builder headerBuilder) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void rotateLogFile(LogRotateEvents logRotateEvents) {
            // rotation should only occur due to envelope file size limits
            throw new UnsupportedOperationException();
        }

        @Override
        public void locklessRotateLogFile(LogRotateEvents logRotateEvents, NextLogHeader.Builder headerBuilder)
                throws IOException {
            long newLogVersion = channel.getLogVersion() + 1;
            writer.prepareForFlush().flush();
            channel.truncate(channel.position());
            // Only the envelope channel rotates here, and it always supplies the tail of the file being closed
            NextLogHeader nextHeader = headerBuilder.build(new NextLogHeader(
                    versionProvider.kernelVersion(),
                    logFormatVersionProvider.getCurrentLogFormat(),
                    UNKNOWN_APPEND_INDEX,
                    writer.currentChecksum().orElse(BASE_TX_CHECKSUM),
                    UNKNOWN_TERM_PROVIDER.getCurrentTerm()));
            PhysicalLogVersionedStoreChannel newLog = logFile.createLogChannelForVersion(
                    newLogVersion,
                    nextHeader::lastAppendIndex,
                    nextHeader::kernelVersion,
                    nextHeader.previousChecksum(),
                    nextHeader::logFormat,
                    nextHeader::lastTerm);
            channel.close();
            channel = newLog;
            writer.setChannel(channel, logFile.extractHeader(channel.getLogVersion()));
        }

        @Override
        public long rotationSize() {
            return rotateAtSize;
        }

        @Override
        public void close() throws IOException {
            writer.close();
        }
    }

    private void reverseRecovery(
            RecoveryStartInformation recoveryStartInformation, TransactionIdTracker transactionIdTracker)
            throws Exception {

        long lowestRecoveredAppendIndex = reverseCompleteBatches(recoveryStartInformation, transactionIdTracker);

        // We cannot initialise the schema (tokens, schema cache, indexing service, etc.) until we have returned
        // the store to a consistent state.
        // We need to be able to read the store before we can even figure out what indexes, tokens, etc. we
        // have. Hence, we defer the initialisation of the schema life until after we've done the reverse recovery.
        schemaLife.init();

        reverseIncompleteBatches(
                transactionIdTracker.notCompletedTransactionAppendIndexes(), getOngoingAppendIndexesToReverse());

        monitor.reverseStoreRecoveryCompleted(lowestRecoveredAppendIndex);
    }

    private void reverseIncompleteBatches(long[] incompleteAppendIndexes, Set<Long> ongoingAppendIndexesToReverse)
            throws Exception {
        if ((incompleteAppendIndexes == null || incompleteAppendIndexes.length == 0)
                && ongoingAppendIndexesToReverse.isEmpty()) {
            return;
        }
        // Incomplete batches especially one before the last checkpoint position must also undo indexes and id
        // generators, therefore using MVCC_INCOMPLETE_REVERSE_RECOVERY applier
        try (var recoveryVisitor = recoveryService.getRecoveryApplier(
                MVCC_INCOMPLETE_REVERSE_RECOVERY, contextFactory, REVERSE_RECOVERY_TAG)) {
            for (long appendIndex : incompleteAppendIndexes) {
                ongoingAppendIndexesToReverse.remove(appendIndex);
                reverseChain(appendIndex, recoveryVisitor);
            }

            // we may already have partially applied transactions before this run so we undo those (cluster pool)
            for (Long appendIndex : ongoingAppendIndexesToReverse) {
                reverseChain(appendIndex, recoveryVisitor);
            }
        }
    }

    private void reverseChain(long nextIndexToReverse, RecoveryApplier recoveryVisitor) throws Exception {
        while (nextIndexToReverse != UNKNOWN_APPEND_INDEX) {
            nextIndexToReverse = reverseChunk(nextIndexToReverse, recoveryVisitor);
        }
    }

    private Set<Long> getOngoingAppendIndexesToReverse() {
        return chunkedTransactionTracker.incompleteTransactions().stream()
                .map(ChunkedTransactionTracker.TransactionInfo::lastBatchAppendIndex)
                .collect(Collectors.toSet());
    }

    private long reverseChunk(long appendIndex, RecoveryApplier recoveryVisitor) throws Exception {
        try (var batchToReverse = recoveryService.getCommandBatches(appendIndex)) {
            if (batchToReverse.next()) {
                recoveryStartupChecker.checkIfCanceled();
                var batch = batchToReverse.get();
                recoveryVisitor.visit(batch);
                reportProgress();
                assert batch.commandBatch().isFirst() == (batch.previousBatchAppendIndex() == UNKNOWN_APPEND_INDEX)
                        : "the first batch must not have previous batch append index but points to "
                                + batch.previousBatchAppendIndex();
                return batch.previousBatchAppendIndex();
            }
        }
        throw new Error(format(
                "Error reversing incomplete transactions Expected to find the batch with append index %d",
                appendIndex));
    }

    /**
     * Reverse complete batches and collect information about incomplete ones
     */
    private long reverseCompleteBatches(
            RecoveryStartInformation recoveryStartInformation, TransactionIdTracker transactionIdTracker)
            throws Exception {
        CommittedCommandBatchRepresentation lastReversedCommandBatch = null;

        var oldestNotVisibleTransactionLogPosition = recoveryStartInformation.oldestNotVisibleTransactionLogPosition();

        long lowestRecoveredAppendIndex = recoveryStartInformation.firstAppendIndexAfterLastCheckPoint();
        try (var transactionsToRecover = recoveryService.getCommandBatchesInReverseOrder(
                        oldestNotVisibleTransactionLogPosition, recoveryPredicate.maxPosition());
                var recoveryVisitor =
                        recoveryService.getRecoveryApplier(REVERSE_RECOVERY, contextFactory, REVERSE_RECOVERY_TAG)) {
            while (transactionsToRecover.next()) {
                recoveryStartupChecker.checkIfCanceled();
                var commandBatch = transactionsToRecover.get();
                if (lastReversedCommandBatch == null) {
                    lastReversedCommandBatch = commandBatch;
                    initProgressReporter(recoveryStartInformation, lastReversedCommandBatch, mode);
                }
                transactionIdTracker.trackBatch(commandBatch);
                // we need to unroll only transactions located after checkpointed position
                if (transactionsToRecover.position().isAfterOrSame(oldestNotVisibleTransactionLogPosition)) {
                    recoveryVisitor.visit(commandBatch);
                }
                lowestRecoveredAppendIndex = commandBatch.appendIndex();
                reportProgress();
            }
        }
        return lowestRecoveredAppendIndex;
    }

    private void initProgressReporter(
            RecoveryStartInformation recoveryStartInformation, LogPosition recoveryStartPosition) throws IOException {
        try (var transactionsToRecover = recoveryService.getCommandBatchesInReverseOrder(
                recoveryStartPosition, recoveryPredicate.maxPosition())) {
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

    public RecoveryOutcome getRecoveryOutcome() {
        return recoveryOutcome;
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
