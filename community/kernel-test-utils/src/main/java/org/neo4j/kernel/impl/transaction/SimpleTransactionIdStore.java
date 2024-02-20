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
package org.neo4j.kernel.impl.transaction;

import static org.neo4j.storageengine.api.LogVersionRepository.BASE_TX_LOG_BYTE_OFFSET;
import static org.neo4j.storageengine.api.LogVersionRepository.BASE_TX_LOG_VERSION;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.neo4j.io.pagecache.context.TransactionIdSnapshot;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.storageengine.api.ClosedTransactionMetadata;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.util.concurrent.ArrayQueueOutOfOrderSequence;
import org.neo4j.util.concurrent.OutOfOrderSequence;
import org.neo4j.util.concurrent.OutOfOrderSequence.Meta;

/**
 * Simple implementation of a {@link TransactionIdStore}.
 */
public class SimpleTransactionIdStore implements TransactionIdStore {
    private final AtomicLong committingTransactionId = new AtomicLong();
    private final OutOfOrderSequence closedTransactionId =
            new ArrayQueueOutOfOrderSequence(-1, 100, OutOfOrderSequence.EMPTY_META);
    private final AtomicReference<TransactionId> committedTransactionId = new AtomicReference<>(
            new TransactionId(BASE_TX_ID, BASE_TX_CHECKSUM, BASE_TX_COMMIT_TIMESTAMP, UNKNOWN_CONSENSUS_INDEX));

    public SimpleTransactionIdStore() {
        this(
                BASE_TX_ID,
                BASE_TX_CHECKSUM,
                BASE_TX_COMMIT_TIMESTAMP,
                UNKNOWN_CONSENSUS_INDEX,
                BASE_TX_LOG_VERSION,
                BASE_TX_LOG_BYTE_OFFSET);
    }

    public SimpleTransactionIdStore(
            long previouslyCommittedTxId,
            int checksum,
            long previouslyCommittedTxCommitTimestamp,
            long previousConsensusIndex,
            long previouslyCommittedTxLogVersion,
            long previouslyCommittedTxLogByteOffset) {
        assert previouslyCommittedTxId >= BASE_TX_ID : "cannot start from a tx id less than BASE_TX_ID";
        setLastCommittedAndClosedTransactionId(
                previouslyCommittedTxId,
                checksum,
                previouslyCommittedTxCommitTimestamp,
                previousConsensusIndex,
                previouslyCommittedTxLogByteOffset,
                previouslyCommittedTxLogVersion);
    }

    @Override
    public long nextCommittingTransactionId() {
        return committingTransactionId.incrementAndGet();
    }

    @Override
    public long committingTransactionId() {
        return committingTransactionId.get();
    }

    @Override
    public synchronized void transactionCommitted(
            long transactionId, int checksum, long commitTimestamp, long consensusIndex) {
        TransactionId current = committedTransactionId.get();
        if (current == null || transactionId > current.transactionId()) {
            committedTransactionId.set(new TransactionId(transactionId, checksum, commitTimestamp, consensusIndex));
        }
    }

    @Override
    public long getLastCommittedTransactionId() {
        return committedTransactionId.get().transactionId();
    }

    @Override
    public TransactionId getLastCommittedTransaction() {
        return committedTransactionId.get();
    }

    @Override
    public long getLastClosedTransactionId() {
        return closedTransactionId.getHighestGapFreeNumber();
    }

    @Override
    public TransactionIdSnapshot getClosedTransactionSnapshot() {
        return new TransactionIdSnapshot(closedTransactionId.reverseSnapshot());
    }

    @Override
    public ClosedTransactionMetadata getLastClosedTransaction() {
        var data = closedTransactionId.get();
        return new ClosedTransactionMetadata(
                data.number(),
                new LogPosition(data.meta().logVersion(), data.meta().byteOffset()),
                data.meta().checksum(),
                data.meta().commitTimestamp(),
                data.meta().consensusIndex());
    }

    @Override
    public void setLastCommittedAndClosedTransactionId(
            long transactionId,
            int checksum,
            long commitTimestamp,
            long consensusIndex,
            long byteOffset,
            long logVersion) {
        committingTransactionId.set(transactionId);
        committedTransactionId.set(new TransactionId(transactionId, checksum, commitTimestamp, consensusIndex));
        closedTransactionId.set(
                transactionId, new Meta(logVersion, byteOffset, checksum, commitTimestamp, consensusIndex));
    }

    @Override
    public void transactionClosed(
            long transactionId,
            long logVersion,
            long byteOffset,
            int checksum,
            long commitTimestamp,
            long consensusIndex) {
        closedTransactionId.offer(
                transactionId, new Meta(logVersion, byteOffset, checksum, commitTimestamp, consensusIndex));
    }

    @Override
    public void resetLastClosedTransaction(
            long transactionId,
            long byteOffset,
            long logVersion,
            int checksum,
            long commitTimestamp,
            long consensusIndex) {
        closedTransactionId.set(
                transactionId, new Meta(logVersion, byteOffset, checksum, commitTimestamp, consensusIndex));
    }
}
