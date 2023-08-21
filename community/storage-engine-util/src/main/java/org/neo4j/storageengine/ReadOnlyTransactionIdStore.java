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
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.storageengine.api.ClosedTransactionMetadata;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.api.TransactionIdStore;

public class ReadOnlyTransactionIdStore implements TransactionIdStore {
    private final long transactionId;
    private final int transactionChecksum;
    private final long transactionCommitTimestamp;
    private final long transactionConsensusIndex;
    private final LogPosition logPosition;

    public ReadOnlyTransactionIdStore(LogTailLogVersionsMetadata logTailMetadata) {
        var lastCommittedTransaction = logTailMetadata.getLastCommittedTransaction();
        transactionId = lastCommittedTransaction.transactionId();
        transactionChecksum = lastCommittedTransaction.checksum();
        transactionCommitTimestamp = lastCommittedTransaction.commitTimestamp();
        logPosition = logTailMetadata.getLastTransactionLogPosition();
        transactionConsensusIndex = lastCommittedTransaction.consensusIndex();
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
    public void transactionCommitted(long transactionId, int checksum, long commitTimestamp, long consensusIndex) {
        throw new UnsupportedOperationException("Read-only transaction ID store");
    }

    @Override
    public long getLastCommittedTransactionId() {
        return transactionId;
    }

    @Override
    public TransactionId getLastCommittedTransaction() {
        return new TransactionId(
                transactionId, transactionChecksum, BASE_TX_COMMIT_TIMESTAMP, transactionConsensusIndex);
    }

    @Override
    public long getLastClosedTransactionId() {
        return transactionId;
    }

    @Override
    public TransactionIdSnapshot getClosedTransactionSnapshot() {
        return new TransactionIdSnapshot(transactionId);
    }

    @Override
    public ClosedTransactionMetadata getLastClosedTransaction() {
        return new ClosedTransactionMetadata(
                transactionId, logPosition, transactionChecksum, transactionCommitTimestamp, transactionConsensusIndex);
    }

    @Override
    public void setLastCommittedAndClosedTransactionId(
            long transactionId,
            int checksum,
            long commitTimestamp,
            long consensusIndex,
            long logByteOffset,
            long logVersion) {
        throw new UnsupportedOperationException("Read-only transaction ID store");
    }

    @Override
    public void transactionClosed(
            long transactionId,
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
            long logVersion,
            long byteOffset,
            int checksum,
            long commitTimestamp,
            long consensusIndex) {
        throw new UnsupportedOperationException("Read-only transaction ID store");
    }
}
