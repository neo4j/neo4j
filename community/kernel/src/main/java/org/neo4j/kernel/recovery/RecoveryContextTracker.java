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

import org.neo4j.kernel.impl.transaction.CommittedCommandBatchRepresentation;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatchRepresentation.BatchInformation;
import org.neo4j.kernel.impl.transaction.log.CheckpointInfo;
import org.neo4j.kernel.impl.transaction.log.LogPosition;

class RecoveryContextTracker {
    private BatchInformation lastHighestTransactionBatchInfo = null;
    private BatchInformation lastBatchInfo = null;
    private LogPosition recoveryToPosition;
    private LogPosition lastTransactionPosition;
    private long recoveredBatches;

    RecoveryContextTracker(LogPosition recoveryStartPosition, CheckpointInfo checkpointInfo) {
        updatePositions(recoveryStartPosition);
        initInitialInfo(checkpointInfo);
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

        updatePositions(position);
        recoveredBatches++;
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
}
