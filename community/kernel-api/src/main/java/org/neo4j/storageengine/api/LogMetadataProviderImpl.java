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
package org.neo4j.storageengine.api;

import static org.neo4j.kernel.impl.transaction.log.RecoveryOutcome.EMPTY_OUTCOME;

import java.util.concurrent.atomic.AtomicLong;
import org.neo4j.io.pagecache.context.TransactionIdSnapshot;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.AppendBatchInfo;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.kernel.impl.transaction.log.RecoveryOutcome;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.storageengine.util.ChunkedTransactionRegistry;
import org.neo4j.storageengine.util.HighestAppendBatch;
import org.neo4j.storageengine.util.HighestTransactionId;
import org.neo4j.util.concurrent.ArrayQueueOutOfOrderSequence;
import org.neo4j.util.concurrent.OutOfOrderSequence;
import org.neo4j.util.concurrent.OutOfOrderSequence.Meta;

public class LogMetadataProviderImpl implements LogMetadataProvider {
    private final AtomicLong logVersion;
    private final AtomicLong checkpointLogVersion;
    private final AtomicLong lastCommittingTx;

    private final HighestTransactionId highestCommittedTransaction;
    private final HighestTransactionId highestClosedTransaction;

    private final OutOfOrderSequence lastClosedTx;
    private final OutOfOrderSequence lastClosedBatch;

    private final AtomicLong appendIndex;
    private final HighestAppendBatch lastCommittedBatch;
    private final ChunkedTransactionRegistry chunkedTransactionRegistry = new ChunkedTransactionRegistry();
    private volatile long lowestAvailableCommittedTransactionId = TransactionIdStore.UNKNOWN_TX_ID;
    private volatile KernelVersion kernelVersion;
    private volatile LogFormat logFormat;

    public LogMetadataProviderImpl(
            LogTailLogVersionsMetadata logTailMetadata,
            LogFormat logFormat,
            KernelVersion kernelVersion,
            RecoveryOutcome recoveryOutcome) {
        this.checkpointLogVersion = new AtomicLong(logTailMetadata.getCheckpointLogVersion());
        this.logVersion = new AtomicLong(logTailMetadata.getLogVersion());
        this.logFormat = logFormat;
        this.kernelVersion = kernelVersion;
        AppendBatchInfo lastBatch = logTailMetadata.lastBatch();
        this.lastCommittedBatch = new HighestAppendBatch(lastBatch);
        this.appendIndex = new AtomicLong(lastBatch.appendIndex());

        if (recoveryOutcome.isEmpty()) {
            var lastCommittedTx = logTailMetadata.getLastCommittedTransaction();
            highestCommittedTransaction = new HighestTransactionId(lastCommittedTx);
            highestClosedTransaction = new HighestTransactionId(lastCommittedTx);
            lastCommittingTx = new AtomicLong(lastCommittedTx.id());
            var logPosition = logTailMetadata.getLastTransactionLogPosition();
            var initialMeta = new Meta(
                    logPosition.getLogVersion(),
                    logPosition.getByteOffset(),
                    lastCommittedTx.kernelVersion().version(),
                    lastCommittedTx.checksum(),
                    lastCommittedTx.commitTimestamp(),
                    lastCommittedTx.consensusIndex(),
                    lastCommittedTx.appendIndex());
            lastClosedTx = new ArrayQueueOutOfOrderSequence(lastCommittedTx.id(), 128, initialMeta);
            lastClosedBatch = new ArrayQueueOutOfOrderSequence(lastBatch.appendIndex(), 128, initialMeta);
            return;
        }

        TransactionId lastCommittedTransactionId = recoveryOutcome.lastCommittingTransactionId();
        highestClosedTransaction = new HighestTransactionId(lastCommittedTransactionId);
        highestCommittedTransaction = new HighestTransactionId(lastCommittedTransactionId);

        lastClosedBatch = new ArrayQueueOutOfOrderSequence(
                lastBatch.appendIndex(),
                128,
                new Meta(
                        lastBatch.logPositionAfter().getLogVersion(),
                        lastBatch.logPositionAfter().getByteOffset(),
                        kernelVersion.version(),
                        UNKNOWN_TX_CHECKSUM,
                        UNKNOWN_TX_COMMIT_TIMESTAMP,
                        UNKNOWN_CONSENSUS_INDEX,
                        lastBatch.appendIndex()));

        long[] notClosedTransactionIds = recoveryOutcome.notClosedTransactionIds();
        var numberWithMeta = recoveryOutcome.lastClosedGapFree();
        var earliestOpenTxMetadata = recoveryOutcome.earliestOpenTransaction();
        lastCommittingTx = new AtomicLong(lastCommittedTransactionId.id());
        lastClosedTx = new ArrayQueueOutOfOrderSequence(
                numberWithMeta.number(), 128, numberWithMeta.meta(), notClosedTransactionIds);

        if (earliestOpenTxMetadata != null) {
            this.chunkedTransactionRegistry.registerTransaction(
                    earliestOpenTxMetadata.txId(),
                    earliestOpenTxMetadata.appendIndex(),
                    earliestOpenTxMetadata.logPosition());
        }
    }

    public LogMetadataProviderImpl(
            LogTailLogVersionsMetadata logTailMetadata, LogFormat logFormat, KernelVersion kernelVersion) {
        this(logTailMetadata, logFormat, kernelVersion, EMPTY_OUTCOME);
    }

    public LogMetadataProviderImpl(LogTailMetadata logTailMetadata, RecoveryOutcome recoveryOutcome) {
        this(logTailMetadata, logTailMetadata.getCurrentLogFormat(), logTailMetadata.kernelVersion(), recoveryOutcome);
    }

    public LogMetadataProviderImpl(LogTailMetadata logTailMetadata) {
        this(logTailMetadata, EMPTY_OUTCOME);
    }

    @Override
    public long nextAppendIndex() {
        return appendIndex.incrementAndGet();
    }

    @Override
    public long getLastAppendIndex() {
        return appendIndex.getAcquire();
    }

    @Override
    public long getCheckpointLogVersion() {
        return checkpointLogVersion.get();
    }

    @Override
    public void setCheckpointLogVersion(long version) {
        checkpointLogVersion.set(version);
    }

    @Override
    public long incrementAndGetCheckpointLogVersion() {
        return checkpointLogVersion.incrementAndGet();
    }

    @Override
    public void setLastCommittedAndClosedTransactionId(
            long lastCommitedTxId,
            long lastClosedTxId,
            long[] notClosedTransactions,
            long transactionAppendIndex,
            KernelVersion kernelVersion,
            int checksum,
            long commitTimestamp,
            long consensusIndex,
            long byteOffset,
            long logVersion,
            long appendIndex,
            OpenTransactionMetadata earliestOpenTransactionMetadata,
            OutOfOrderSequence.NumberWithMeta lastClosedTxIdInfo) {
        this.lastCommittingTx.set(lastCommitedTxId);
        var meta = new Meta(
                logVersion,
                byteOffset,
                kernelVersion.version(),
                checksum,
                commitTimestamp,
                consensusIndex,
                transactionAppendIndex);
        lastClosedBatch.set(appendIndex, meta);

        if (lastClosedTxIdInfo != null) {
            Meta txMeta = lastClosedTxIdInfo.meta();
            lastClosedTx.set(lastClosedTxIdInfo.number(), txMeta, notClosedTransactions);
            highestClosedTransaction.set(
                    lastClosedTxIdInfo.number(),
                    txMeta.appendIndex(),
                    KernelVersion.getForVersion(txMeta.kernelVersion()),
                    txMeta.checksum(),
                    txMeta.commitTimestamp(),
                    txMeta.consensusIndex());
        } else {
            lastClosedTx.set(appendIndex, meta);
            highestClosedTransaction.set(
                    lastClosedTxId, transactionAppendIndex, kernelVersion, checksum, commitTimestamp, consensusIndex);
        }

        highestCommittedTransaction.set(
                lastClosedTxId, transactionAppendIndex, kernelVersion, checksum, commitTimestamp, consensusIndex);
        this.appendIndex.set(appendIndex);
        this.lastCommittedBatch.set(appendIndex, LogPosition.UNSPECIFIED);
        if (earliestOpenTransactionMetadata != null) {
            this.chunkedTransactionRegistry.registerTransaction(
                    earliestOpenTransactionMetadata.txId(),
                    earliestOpenTransactionMetadata.appendIndex(),
                    earliestOpenTransactionMetadata.logPosition());
        }
    }

    @Override
    public void setLastCommittedAndClosedTransactionId(
            long transactionId,
            long transactionAppendIndex,
            KernelVersion kernelVersion,
            int checksum,
            long commitTimestamp,
            long consensusIndex,
            long byteOffset,
            long logVersion,
            long appendIndex) {
        lastCommittingTx.set(transactionId);
        var meta = new Meta(
                logVersion,
                byteOffset,
                kernelVersion.version(),
                checksum,
                commitTimestamp,
                consensusIndex,
                transactionAppendIndex);
        lastClosedBatch.set(appendIndex, meta);
        lastClosedTx.set(transactionId, meta);
        highestClosedTransaction.set(
                transactionId, transactionAppendIndex, kernelVersion, checksum, commitTimestamp, consensusIndex);
        highestCommittedTransaction.set(
                transactionId, transactionAppendIndex, kernelVersion, checksum, commitTimestamp, consensusIndex);
        this.appendIndex.set(appendIndex);
        this.lastCommittedBatch.set(appendIndex, LogPosition.UNSPECIFIED);
    }

    @Override
    public long getCurrentLogVersion() {
        return logVersion.get();
    }

    @Override
    public void setCurrentLogVersion(long version) {
        logVersion.set(version);
    }

    @Override
    public long incrementAndGetVersion() {
        return logVersion.incrementAndGet();
    }

    @Override
    public long nextCommittingTransactionId() {
        return lastCommittingTx.incrementAndGet();
    }

    @Override
    public void transactionCommitted(
            long transactionId,
            long appendIndex,
            KernelVersion kernelVersion,
            int checksum,
            long commitTimestamp,
            long consensusIndex) {
        highestCommittedTransaction.offer(
                transactionId, appendIndex, kernelVersion, checksum, commitTimestamp, consensusIndex);
    }

    @Override
    public long getLastCommittedTransactionId() {
        return highestCommittedTransaction.get().id();
    }

    @Override
    public TransactionId getLastCommittedTransaction() {
        return highestCommittedTransaction.get();
    }

    @Override
    public long getHighestGapFreeClosedTransactionId() {
        return lastClosedTx.getHighestGapFreeNumber();
    }

    @Override
    public TransactionIdSnapshot getClosedTransactionSnapshot() {
        return new TransactionIdSnapshot(lastClosedTx.reverseSnapshot());
    }

    @Override
    public ClosedTransactionMetadata getHighestGapFreeClosedTransaction() {
        return new ClosedTransactionMetadata(lastClosedTx.get());
    }

    @Override
    public ClosedBatchMetadata getLastClosedBatch() {
        return new ClosedBatchMetadata(lastClosedBatch.get());
    }

    @Override
    public void transactionClosed(
            long transactionId,
            long appendIndex,
            KernelVersion kernelVersion,
            long logVersion,
            long byteOffset,
            int checksum,
            long commitTimestamp,
            long consensusIndex) {
        lastClosedTx.offer(
                transactionId,
                new Meta(
                        logVersion,
                        byteOffset,
                        kernelVersion.version(),
                        checksum,
                        commitTimestamp,
                        consensusIndex,
                        appendIndex));
        highestClosedTransaction.offer(
                transactionId, appendIndex, kernelVersion, checksum, commitTimestamp, consensusIndex);
    }

    @Override
    public void batchClosed(
            long transactionId,
            long appendIndex,
            boolean firstBatch,
            boolean lastBatch,
            KernelVersion kernelVersion,
            LogPosition logPositionAfter) {
        lastClosedBatch.offer(
                appendIndex,
                new Meta(
                        logPositionAfter.getLogVersion(),
                        logPositionAfter.getByteOffset(),
                        kernelVersion.version(),
                        UNKNOWN_TX_CHECKSUM,
                        UNKNOWN_TX_COMMIT_TIMESTAMP,
                        UNKNOWN_CONSENSUS_INDEX,
                        appendIndex));
        // only remove transaction if this is the last batch in multi batch transaction
        if (lastBatch && !firstBatch) {
            chunkedTransactionRegistry.removeTransaction(transactionId);
        }
    }

    @Override
    public void resetLastClosedTransaction(
            long transactionId,
            long appendIndex,
            KernelVersion kernelVersion,
            long logVersion,
            long byteOffset,
            int checksum,
            long commitTimestamp,
            long consensusIndex) {
        var meta = new Meta(
                logVersion,
                byteOffset,
                kernelVersion.version(),
                checksum,
                commitTimestamp,
                consensusIndex,
                appendIndex);
        lastClosedBatch.set(appendIndex, meta);
        lastClosedTx.set(transactionId, meta);
    }

    @Override
    public void appendBatch(
            long transactionId,
            long appendIndex,
            boolean firstBatch,
            boolean lastBatch,
            LogPosition logPositionBefore,
            LogPosition logPositionAfter) {
        this.lastCommittedBatch.offer(appendIndex, logPositionAfter);

        // this is the first and last batch, no need to register in progress transaction
        if (firstBatch && lastBatch) {
            return;
        }
        if (firstBatch) {
            chunkedTransactionRegistry.registerTransaction(transactionId, appendIndex, logPositionBefore);
        }
    }

    @Override
    public AppendBatchInfo getLastCommittedBatch() {
        return lastCommittedBatch.get();
    }

    @Override
    public OpenTransactionMetadata getOldestOpenTransaction() {
        return chunkedTransactionRegistry.oldestOpenTransactionMetadata();
    }

    @Override
    public TransactionId getHighestEverClosedTransaction() {
        return highestClosedTransaction.get();
    }

    @Override
    public long getLowestAvailableCommittedTransactionId() {
        return lowestAvailableCommittedTransactionId;
    }

    @Override
    public void setLowestAvailableCommittedTransactionId(long transactionId) {
        this.lowestAvailableCommittedTransactionId = transactionId;
    }

    @Override
    public void setKernelVersion(KernelVersion kernelVersion) {
        this.kernelVersion = kernelVersion;
    }

    @Override
    public KernelVersion kernelVersion() {
        return kernelVersion;
    }

    @Override
    public void setCurrentLogFormat(LogFormat logFormat) {
        this.logFormat = logFormat;
    }

    @Override
    public LogFormat getCurrentLogFormat() {
        return logFormat;
    }
}
