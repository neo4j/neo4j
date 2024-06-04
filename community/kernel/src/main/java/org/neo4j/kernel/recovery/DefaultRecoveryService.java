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

import static org.neo4j.kernel.impl.transaction.log.entry.LogFormat.fromKernelVersion;
import static org.neo4j.storageengine.api.LogVersionRepository.INITIAL_LOG_VERSION;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.context.TransactionIdSnapshot;
import org.neo4j.kernel.BinarySupportedKernelVersions;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatch;
import org.neo4j.kernel.impl.transaction.log.CommandBatchCursor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.PhysicalFlushableLogPositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.checkpoint.CheckpointFile;
import org.neo4j.logging.InternalLog;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.AppendIndexProvider;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.TransactionIdStore;

public class DefaultRecoveryService implements RecoveryService {
    private final RecoveryStartInformationProvider recoveryStartInformationProvider;
    private final StorageEngine storageEngine;
    private final TransactionIdStore transactionIdStore;
    private final LogicalTransactionStore logicalTransactionStore;
    private final LogVersionRepository logVersionRepository;
    private final LogFiles logFiles;
    private final KernelVersionProvider versionProvider;
    private final InternalLog log;
    private final Clock clock;
    private final boolean doParallelRecovery;
    private final BinarySupportedKernelVersions binarySupportedKernelVersions;
    private final CursorContextFactory contextFactory;

    DefaultRecoveryService(
            StorageEngine storageEngine,
            TransactionIdStore transactionIdStore,
            LogicalTransactionStore logicalTransactionStore,
            LogVersionRepository logVersionRepository,
            LogFiles logFiles,
            KernelVersionProvider versionProvider,
            RecoveryStartInformationProvider.Monitor monitor,
            InternalLog log,
            Clock clock,
            boolean doParallelRecovery,
            BinarySupportedKernelVersions binarySupportedKernelVersions,
            CursorContextFactory contextFactory) {
        this.storageEngine = storageEngine;
        this.transactionIdStore = transactionIdStore;
        this.logicalTransactionStore = logicalTransactionStore;
        this.logVersionRepository = logVersionRepository;
        this.logFiles = logFiles;
        this.versionProvider = versionProvider;
        this.log = log;
        this.clock = clock;
        this.doParallelRecovery = doParallelRecovery;
        this.binarySupportedKernelVersions = binarySupportedKernelVersions;
        this.contextFactory = contextFactory;
        this.recoveryStartInformationProvider = new RecoveryStartInformationProvider(logFiles, monitor);
    }

    @Override
    public RecoveryStartInformation getRecoveryStartInformation() {
        return recoveryStartInformationProvider.get();
    }

    @Override
    public RecoveryApplier getRecoveryApplier(
            TransactionApplicationMode mode, CursorContextFactory contextFactory, String tracerTag) {
        if (doParallelRecovery) {
            return new ParallelRecoveryVisitor(storageEngine, mode, contextFactory, tracerTag);
        }
        return new RecoveryVisitor(storageEngine, mode, contextFactory, tracerTag);
    }

    @Override
    public RollbackTransactionInfo rollbackTransactions(
            LogPosition writePosition,
            TransactionIdTracker transactionTracker,
            CommittedCommandBatch.BatchInformation lastCommittedBatch,
            AppendIndexProvider appendIndexProvider)
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
            CommittedCommandBatch.BatchInformation lastBatchInfo = null;
            for (int i = 0; i < notCompletedTransactions.length; i++) {
                long notCompletedTransaction = notCompletedTransactions[i];
                long appendIndex = appendIndexProvider.nextAppendIndex();
                int checksum =
                        entryWriter.writeRollbackEntry(kernelVersion, notCompletedTransaction, appendIndex, time);
                if (i == (notCompletedTransactions.length - 1)) {
                    lastBatchInfo = new CommittedCommandBatch.BatchInformation(
                            notCompletedTransaction,
                            kernelVersion,
                            checksum,
                            time,
                            UNKNOWN_CONSENSUS_INDEX,
                            appendIndex);
                }
            }

            return new RollbackTransactionInfo(lastBatchInfo, writerChannel.getCurrentLogPosition());
        }
    }

    @Override
    public CommandBatchCursor getCommandBatches(long appendIndex) throws IOException {
        return logicalTransactionStore.getCommandBatches(appendIndex);
    }

    @Override
    public CommandBatchCursor getCommandBatches(LogPosition position) throws IOException {
        return logicalTransactionStore.getCommandBatches(position);
    }

    @Override
    public CommandBatchCursor getCommandBatchesInReverseOrder(LogPosition position) throws IOException {
        return logicalTransactionStore.getCommandBatchesInReverseOrder(position);
    }

    @Override
    public void transactionsRecovered(
            CommittedCommandBatch.BatchInformation highestTransactionRecoveredBatch,
            AppendIndexProvider recoverAppendIndexProvider,
            LogPosition lastRecoveredTransactionPosition,
            LogPosition positionAfterLastRecoveredTransaction,
            LogPosition checkpointPosition,
            boolean missingLogs,
            CursorContext cursorContext) {
        if (missingLogs) {
            // in case if logs are missing we need to reset position of last committed transaction since
            // this information influencing checkpoint that will be created and if we will not gonna do that
            // it will still reference old offset from logs that are gone and as result log position in checkpoint
            // record will be incorrect
            // and that can cause partial next recovery.
            var lastClosedTransaction = transactionIdStore.getLastClosedTransaction();
            var lastClosedTransactionId = lastClosedTransaction.transactionId();
            long logVersion = lastClosedTransaction.logPosition().getLogVersion();
            log.warn(
                    "Recovery detected that transaction logs were missing. "
                            + "Resetting offset of last closed transaction to point to the head of %d transaction log file.",
                    logVersion);
            transactionIdStore.resetLastClosedTransaction(
                    lastClosedTransactionId.id(),
                    lastClosedTransaction.transactionId().appendIndex(),
                    versionProvider.kernelVersion(),
                    logVersion,
                    fromKernelVersion(versionProvider.kernelVersion()).getHeaderSize(),
                    lastClosedTransactionId.checksum(),
                    lastClosedTransactionId.commitTimestamp(),
                    lastClosedTransactionId.consensusIndex());
            logVersionRepository.setCurrentLogVersion(logVersion);

            // cleanup checkpoint log files
            logVersionRepository.setCheckpointLogVersion(
                    Math.max(INITIAL_LOG_VERSION, logFiles.getCheckpointFile().getHighestLogVersion()));
            tryRemoveLegacyCheckpointFiles();

            return;
        }
        if (highestTransactionRecoveredBatch != null) {
            transactionIdStore.setLastCommittedAndClosedTransactionId(
                    highestTransactionRecoveredBatch.txId(),
                    highestTransactionRecoveredBatch.appendIndex(),
                    highestTransactionRecoveredBatch.kernelVersion(),
                    // TODO: misha this checksum is from the first batch while usually its from the last one
                    highestTransactionRecoveredBatch.checksum(),
                    highestTransactionRecoveredBatch.timeWritten(),
                    highestTransactionRecoveredBatch.consensusIndex(),
                    lastRecoveredTransactionPosition.getByteOffset(),
                    lastRecoveredTransactionPosition.getLogVersion(),
                    recoverAppendIndexProvider.getLastAppendIndex());
            var lastRecoveredTxId = highestTransactionRecoveredBatch.txId();
            // if there will be index population after that, it will have proper visibility
            contextFactory.init(() -> new TransactionIdSnapshot(lastRecoveredTxId), () -> lastRecoveredTxId);
        } else {
            // we do not have last recovered transaction but recovery was still triggered
            // this happens when we read past end of the log file or can't read it at all but recovery was enforced
            // which means that log files after last recovered position can't be trusted, and we need to reset last
            // closed tx log info
            var lastClosedTransaction = transactionIdStore.getLastClosedTransaction();
            log.warn("Recovery detected that transaction logs tail can't be trusted. "
                    + "Resetting offset of last closed transaction to point to the last recoverable log position: "
                    + positionAfterLastRecoveredTransaction);
            transactionIdStore.resetLastClosedTransaction(
                    lastClosedTransaction.transactionId().id(),
                    lastClosedTransaction.transactionId().appendIndex(),
                    versionProvider.kernelVersion(),
                    positionAfterLastRecoveredTransaction.getLogVersion(),
                    positionAfterLastRecoveredTransaction.getByteOffset(),
                    BASE_TX_CHECKSUM,
                    BASE_TX_COMMIT_TIMESTAMP,
                    UNKNOWN_CONSENSUS_INDEX);
        }

        logVersionRepository.setCurrentLogVersion(positionAfterLastRecoveredTransaction.getLogVersion());
        logVersionRepository.setCheckpointLogVersion(
                checkpointPosition == LogPosition.UNSPECIFIED
                        ? INITIAL_LOG_VERSION
                        : checkpointPosition.getLogVersion());
    }

    private void tryRemoveLegacyCheckpointFiles() {
        try {
            CheckpointFile checkpointFile = logFiles.getCheckpointFile();
            Path[] detachedCheckpointFiles = checkpointFile.getDetachedCheckpointFiles();
            for (Path obsoleteCheckpointFile : detachedCheckpointFiles) {
                FileUtils.deleteFile(obsoleteCheckpointFile);
            }
        } catch (IOException e) {
            log.error("Failed to delete legacy checkpoint files.", e);
        }
    }
}
