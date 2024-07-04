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

import static org.neo4j.storageengine.AppendIndexProvider.UNKNOWN_APPEND_INDEX;

import org.neo4j.kernel.KernelVersion;
import org.neo4j.storageengine.api.Commitment;
import org.neo4j.storageengine.api.TransactionIdStore;

public class TransactionCommitment implements Commitment {

    private final TransactionIdStore transactionIdStore;
    private boolean committed;
    private long transactionId;
    private int checksum;
    private long consensusIndex;
    private KernelVersion kernelVersion;
    private LogPosition logPositionAfterCommit;
    private long transactionCommitTimestamp;

    private long firstAppendIndex;
    private long currentBatchAppendIndex = UNKNOWN_APPEND_INDEX;
    private long lastClosedAppendIndex = UNKNOWN_APPEND_INDEX;

    TransactionCommitment(TransactionIdStore transactionIdStore) {
        this.transactionIdStore = transactionIdStore;
    }

    @Override
    public void commit(
            long transactionId,
            long appendIndex,
            KernelVersion kernelVersion,
            LogPosition logPositionAfterCommit,
            int checksum,
            long consensusIndex) {
        this.transactionId = transactionId;
        this.kernelVersion = kernelVersion;
        this.logPositionAfterCommit = logPositionAfterCommit;
        this.checksum = checksum;
        this.consensusIndex = consensusIndex;
        this.currentBatchAppendIndex = appendIndex;
        transactionIdStore.appendBatch(appendIndex, logPositionAfterCommit);
    }

    @Override
    public void publishAsCommitted(long transactionCommitTimestamp, long firstAppendIndex) {
        this.committed = true;
        this.firstAppendIndex = firstAppendIndex;
        this.transactionCommitTimestamp = transactionCommitTimestamp;
        transactionIdStore.transactionCommitted(
                transactionId, firstAppendIndex, kernelVersion, checksum, transactionCommitTimestamp, consensusIndex);
    }

    @Override
    public void publishAsClosed() {
        if (lastClosedAppendIndex != currentBatchAppendIndex) {
            transactionIdStore.batchClosed(currentBatchAppendIndex, kernelVersion, logPositionAfterCommit);
            lastClosedAppendIndex = currentBatchAppendIndex;
        }
        if (committed) {
            transactionIdStore.transactionClosed(
                    transactionId,
                    firstAppendIndex,
                    kernelVersion,
                    logPositionAfterCommit.getLogVersion(),
                    logPositionAfterCommit.getByteOffset(),
                    checksum,
                    transactionCommitTimestamp,
                    consensusIndex);
        }
    }
}
