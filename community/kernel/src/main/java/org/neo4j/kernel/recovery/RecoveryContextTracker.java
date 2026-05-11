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

import static org.neo4j.kernel.recovery.IncompleteTransactionAction.APPLY;

import org.eclipse.collections.api.map.primitive.MutableLongLongMap;
import org.eclipse.collections.impl.factory.primitive.LongLongMaps;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatchRepresentation;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatchRepresentation.BatchInformation;
import org.neo4j.kernel.impl.transaction.log.CheckpointInfo;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.storageengine.api.OpenTransactionMetadata;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.util.concurrent.ArrayQueueOutOfOrderSequence;
import org.neo4j.util.concurrent.OutOfOrderSequence;

class RecoveryContextTracker {
    private final IncompleteTransactionAction incompleteTransactionAction;
    private BatchInformation lastHighestTransactionBatchInfo = null;
    private BatchInformation lastBatchInfo = null;
    private LogPosition recoveryToPosition;
    private LogPosition lastTransactionPosition;
    private OpenTransactionMetadata earliestOpenTransactionMetadata;
    private long recoveredBatches;
    private ArrayQueueOutOfOrderSequence closedTxTracker;
    private final MutableLongLongMap transactionIdFirstAppendIndexMap = LongLongMaps.mutable.empty();

    RecoveryContextTracker(
            LogPosition recoveryStartPosition,
            CheckpointInfo checkpointInfo,
            IncompleteTransactionAction incompleteTransactionAction) {
        this.incompleteTransactionAction = incompleteTransactionAction;
        updatePositions(recoveryStartPosition);
        initInitialInfo(checkpointInfo);
        closedTxTracker = initClosedTxTracker(checkpointInfo, incompleteTransactionAction);
    }

    private ArrayQueueOutOfOrderSequence initClosedTxTracker(
            CheckpointInfo checkpointInfo, IncompleteTransactionAction incompleteTransactionAction) {
        if (APPLY != incompleteTransactionAction || checkpointInfo == null) {
            return null;
        }
        TransactionId transactionId = checkpointInfo.transactionId();
        return new ArrayQueueOutOfOrderSequence(
                transactionId.id(),
                128,
                new OutOfOrderSequence.Meta(
                        checkpointInfo.transactionLogPosition().getLogVersion(),
                        checkpointInfo.transactionLogPosition().getByteOffset(),
                        transactionId.kernelVersion().version(),
                        transactionId.checksum(),
                        transactionId.commitTimestamp(),
                        transactionId.consensusIndex(),
                        transactionId.appendIndex()));
    }

    private void initInitialInfo(CheckpointInfo checkpointInfo) {
        if (checkpointInfo == null) {
            return;
        }
        var checkpointTransactionId = checkpointInfo.transactionId();
        var checkpointBatchInfo = new BatchInformation(checkpointTransactionId, checkpointTransactionId.appendIndex());
        var transactionId = checkpointInfo.transactionId();

        lastBatchInfo = new BatchInformation(transactionId, checkpointInfo.appendIndex());
        lastHighestTransactionBatchInfo = checkpointBatchInfo;
    }

    void commitedBatch(CommittedCommandBatchRepresentation nextCommandBatch, LogPosition position) {
        BatchInformation batchInfo = nextCommandBatch.batchInformation();
        if (updateHighestBatchInfo(nextCommandBatch.txId())) {
            lastHighestTransactionBatchInfo = batchInfo;
        }
        lastBatchInfo = batchInfo;

        offerClosedTx(nextCommandBatch, position);
        updatePositions(position);

        recoveredBatches++;
    }

    private void offerClosedTx(CommittedCommandBatchRepresentation nextCommandBatch, LogPosition position) {
        if (APPLY != incompleteTransactionAction) {
            return;
        }
        if (nextCommandBatch.commandBatch().isFirst()) {
            transactionIdFirstAppendIndexMap.put(nextCommandBatch.txId(), nextCommandBatch.appendIndex());
        }
        if (nextCommandBatch.commandBatch().isLast()) {
            long firstAppendIndex = transactionIdFirstAppendIndexMap.removeKeyIfAbsent(nextCommandBatch.txId(), -1);
            if (firstAppendIndex == -1) {
                throw new IllegalStateException(
                        "Transaction " + nextCommandBatch.txId() + " first append index is missing.");
            }
            OutOfOrderSequence.Meta meta = new OutOfOrderSequence.Meta(
                    position.getLogVersion(),
                    position.getByteOffset(),
                    nextCommandBatch.batchInformation().kernelVersion().version(),
                    nextCommandBatch.batchInformation().checksum(),
                    -1,
                    nextCommandBatch.batchInformation().consensusIndex(),
                    firstAppendIndex);
            if (closedTxTracker != null) {
                closedTxTracker.offer(nextCommandBatch.txId(), meta);
                return;
            }
            closedTxTracker = new ArrayQueueOutOfOrderSequence(nextCommandBatch.txId(), 128, meta);
        }
    }

    void rollbackBatch(RollbackTransactionInfo rollbackTransactionInfo, LogPosition position) {
        if (updateHighestBatchInfo(rollbackTransactionInfo.batchInfo().txId())) {
            lastHighestTransactionBatchInfo = rollbackTransactionInfo.batchInfo();
        }
        updatePositions(position);
    }

    private boolean updateHighestBatchInfo(long id) {
        return lastHighestTransactionBatchInfo == null || lastHighestTransactionBatchInfo.txId() < id;
    }

    void completeRecovery(LogPosition logPosition) {
        recoveryToPosition = logPosition;
    }

    private void updatePositions(LogPosition position) {
        this.recoveryToPosition = position;
        this.lastTransactionPosition = position;
    }

    BatchInformation getLastHighestTransactionBatchInfo() {
        return lastHighestTransactionBatchInfo;
    }

    public OutOfOrderSequence.NumberWithMeta gapFreeClosedTransactionInfo() {
        return closedTxTracker.get();
    }

    BatchInformation getLastBatchInfo() {
        return lastBatchInfo;
    }

    LogPosition getRecoveryToPosition() {
        return recoveryToPosition;
    }

    LogPosition getLastTransactionPosition() {
        return lastTransactionPosition;
    }

    boolean hasRecoveredBatches() {
        return recoveredBatches > 0;
    }

    public void unrecoverableBatch(OpenTransactionMetadata openTransactionMetadata) {
        if (earliestOpenTransactionMetadata != null) {
            return;
        }
        this.earliestOpenTransactionMetadata = openTransactionMetadata;
    }

    public OpenTransactionMetadata getEarliestOpenTransactionMetadata() {
        return earliestOpenTransactionMetadata;
    }
}
