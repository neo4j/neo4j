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
package org.neo4j.storageengine;

import org.neo4j.io.pagecache.context.TransactionIdSnapshot;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.storageengine.api.ClosedTransactionMetadata;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.api.TransactionIdStore;

public class ReadOnlyTransactionIdStore implements TransactionIdStore {
    private final LogPosition logPosition;
    private final TransactionId lastCommittedTransaction;
    private final AppendBatchInfo lastBatch;

    public ReadOnlyTransactionIdStore(LogTailLogVersionsMetadata logTailMetadata) {
        this.lastCommittedTransaction = logTailMetadata.getLastCommittedTransaction();
        this.logPosition = logTailMetadata.getLastTransactionLogPosition();
        this.lastBatch = new AppendBatchInfo(logTailMetadata.getLastCheckpointedAppendIndex(), LogPosition.UNSPECIFIED);
    }

    @Override
    public long nextCommittingTransactionId() {
        throw new UnsupportedOperationException("Read-only transaction ID store");
    }

    @Override
    public long committingTransactionId() {
        throw new UnsupportedOperationException("Read-only transaction ID store");
    }

    @Override
    public void transactionCommitted(
            long transactionId,
            long appendIndex,
            KernelVersion kernelVersion,
            int checksum,
            long commitTimestamp,
            long consensusIndex) {
        throw new UnsupportedOperationException("Read-only transaction ID store");
    }

    @Override
    public long getLastCommittedTransactionId() {
        return lastCommittedTransaction.id();
    }

    @Override
    public TransactionId getLastCommittedTransaction() {
        return lastCommittedTransaction;
    }

    @Override
    public long getLastClosedTransactionId() {
        return lastCommittedTransaction.id();
    }

    @Override
    public TransactionIdSnapshot getClosedTransactionSnapshot() {
        return new TransactionIdSnapshot(getLastClosedTransactionId());
    }

    @Override
    public ClosedTransactionMetadata getLastClosedTransaction() {
        return new ClosedTransactionMetadata(lastCommittedTransaction, logPosition);
    }

    @Override
    public void setLastCommittedAndClosedTransactionId(
            long transactionId,
            long transactionAppendIndex,
            KernelVersion kernelVersion,
            int checksum,
            long commitTimestamp,
            long consensusIndex,
            long logByteOffset,
            long logVersion,
            long appendIndex) {
        throw new UnsupportedOperationException("Read-only transaction ID store");
    }

    @Override
    public void transactionClosed(
            long transactionId,
            long appendIndex,
            KernelVersion kernelVersion,
            long logVersion,
            long logByteOffset,
            int checksum,
            long commitTimestamp,
            long consensusIndex) {
        throw new UnsupportedOperationException("Read-only transaction ID store");
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
        throw new UnsupportedOperationException("Read-only transaction ID store");
    }

    @Override
    public void appendBatch(long appendIndex, LogPosition logPositionAfter) {}

    @Override
    public AppendBatchInfo lastBatch() {
        return lastBatch;
    }
}
