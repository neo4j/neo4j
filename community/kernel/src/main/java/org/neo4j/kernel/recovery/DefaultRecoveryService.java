/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.recovery;

import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_FORMAT_LOG_HEADER_SIZE;
import static org.neo4j.storageengine.api.LogVersionRepository.INITIAL_LOG_VERSION;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP;

import java.io.IOException;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.logging.InternalLog;
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
    private final InternalLog log;
    private final boolean doParallelRecovery;

    DefaultRecoveryService(
            StorageEngine storageEngine,
            TransactionIdStore transactionIdStore,
            LogicalTransactionStore logicalTransactionStore,
            LogVersionRepository logVersionRepository,
            LogFiles logFiles,
            RecoveryStartInformationProvider.Monitor monitor,
            InternalLog log,
            boolean doParallelRecovery) {
        this.storageEngine = storageEngine;
        this.transactionIdStore = transactionIdStore;
        this.logicalTransactionStore = logicalTransactionStore;
        this.logVersionRepository = logVersionRepository;
        this.log = log;
        this.doParallelRecovery = doParallelRecovery;
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
    public TransactionCursor getTransactions(long transactionId) throws IOException {
        return logicalTransactionStore.getTransactions(transactionId);
    }

    @Override
    public TransactionCursor getTransactions(LogPosition position) throws IOException {
        return logicalTransactionStore.getTransactions(position);
    }

    @Override
    public TransactionCursor getTransactionsInReverseOrder(LogPosition position) throws IOException {
        return logicalTransactionStore.getTransactionsInReverseOrder(position);
    }

    @Override
    public void transactionsRecovered(
            CommittedTransactionRepresentation lastRecoveredTransaction,
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
            var lastClosedTransactionData = transactionIdStore.getLastClosedTransaction();
            long logVersion = lastClosedTransactionData.logPosition().getLogVersion();
            log.warn(
                    "Recovery detected that transaction logs were missing. "
                            + "Resetting offset of last closed transaction to point to the head of %d transaction log file.",
                    logVersion);
            transactionIdStore.resetLastClosedTransaction(
                    lastClosedTransactionData.transactionId(),
                    logVersion,
                    CURRENT_FORMAT_LOG_HEADER_SIZE,
                    lastClosedTransactionData.checksum(),
                    lastClosedTransactionData.commitTimestamp());
            logVersionRepository.setCurrentLogVersion(logVersion);
            long checkpointLogVersion = logVersionRepository.getCheckpointLogVersion();
            if (checkpointLogVersion < 0) {
                log.warn(
                        "Recovery detected that checkpoint log version is invalid. "
                                + "Resetting version to start from the beginning. Current recorded version: %d. New version: 0.",
                        checkpointLogVersion);
                logVersionRepository.setCheckpointLogVersion(INITIAL_LOG_VERSION);
            }
            return;
        }
        if (lastRecoveredTransaction != null) {
            LogEntryCommit commitEntry = lastRecoveredTransaction.getCommitEntry();
            transactionIdStore.setLastCommittedAndClosedTransactionId(
                    commitEntry.getTxId(),
                    lastRecoveredTransaction.getChecksum(),
                    commitEntry.getTimeWritten(),
                    lastRecoveredTransactionPosition.getByteOffset(),
                    lastRecoveredTransactionPosition.getLogVersion());
        } else {
            // we do not have last recovered transaction but recovery was still triggered
            // this happens when we read past end of the log file or can't read it at all but recovery was enforced
            // which means that log files after last recovered position can't be trusted and we need to reset last
            // closed tx log info
            long lastClosedTransactionId = transactionIdStore.getLastClosedTransactionId();
            log.warn("Recovery detected that transaction logs tail can't be trusted. "
                    + "Resetting offset of last closed transaction to point to the last recoverable log position: "
                    + positionAfterLastRecoveredTransaction);
            transactionIdStore.resetLastClosedTransaction(
                    lastClosedTransactionId,
                    positionAfterLastRecoveredTransaction.getLogVersion(),
                    positionAfterLastRecoveredTransaction.getByteOffset(),
                    BASE_TX_CHECKSUM,
                    BASE_TX_COMMIT_TIMESTAMP);
        }

        logVersionRepository.setCurrentLogVersion(positionAfterLastRecoveredTransaction.getLogVersion());
        logVersionRepository.setCheckpointLogVersion(checkpointPosition.getLogVersion());
    }
}
