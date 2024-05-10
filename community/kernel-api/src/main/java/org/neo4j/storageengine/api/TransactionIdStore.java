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

import static org.neo4j.storageengine.AppendIndexProvider.BASE_APPEND_INDEX;
import static org.neo4j.storageengine.AppendIndexProvider.UNKNOWN_APPEND_INDEX;

import org.neo4j.io.pagecache.context.TransactionIdSnapshot;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.LogPosition;

/**
 * Keeps a latest transaction id. There's one counter for {@code committed transaction id} and one for
 * {@code closed transaction id}. The committed transaction id is for writing into a log before making
 * the changes to be made. After that the application of those transactions might be asynchronous and
 * completion of those are marked using {@link #transactionClosed(long, long, KernelVersion, long, long, int, long, long)}.
 * <p>
 * A transaction ID passes through a {@link TransactionIdStore} like this:
 * <ol>
 * <li>{@link #nextCommittingTransactionId()} is called and an id is returned to a committer.
 * At this point that id isn't visible from any getter.</li>
 * <li>{@link #transactionCommitted(long, long, KernelVersion, int, long, long)} is called with this id after the fact that the transaction
 * has been committed, i.e. written forcefully to a log. After this call the id may be visible from
 * {@link #getLastCommittedTransactionId()} if all ids before it have also been committed.</li>
 * <li>{@link #transactionClosed(long, long, KernelVersion, long, long, int, long, long)} is called with this id again, this time after all changes the
 * transaction imposes have been applied to the store.
 * </ol>
 */
public interface TransactionIdStore {
    /**
     * Tx id counting starting from this value (this value means no transaction ever committed).
     *
     * Note that a read only transaction will get txId = 0.
     */
    long BASE_TX_ID = 1;

    int BASE_TX_CHECKSUM = 0xDEAD5EED;

    /**
     * Timestamp value used initially for an empty database.
     */
    long BASE_TX_COMMIT_TIMESTAMP = 0;

    /**
     * First chunk id for chunked transaction
     */
    int BASE_CHUNK_NUMBER = 1;

    /**
     * CONSTANT FOR UNKNOWN TX CHECKSUM
     */
    int UNKNOWN_TX_CHECKSUM = 1;

    /**
     * Timestamp value used when record in the metadata store is not present and there are no transactions in logs.
     */
    long UNKNOWN_TX_COMMIT_TIMESTAMP = 1;

    /**
     * Value for unknown consensus log index.
     */
    long UNKNOWN_CONSENSUS_INDEX = -1;

    long UNKNOWN_TX_ID = BASE_TX_ID - 1;

    TransactionId UNKNOWN_TRANSACTION_ID = new TransactionId(
            UNKNOWN_TX_ID,
            UNKNOWN_APPEND_INDEX,
            KernelVersion.EARLIEST,
            UNKNOWN_TX_CHECKSUM,
            UNKNOWN_TX_COMMIT_TIMESTAMP,
            UNKNOWN_CONSENSUS_INDEX);

    TransactionId BASE_TRANSACTION_ID = emptyVersionedTransaction(KernelVersion.EARLIEST);

    static TransactionId emptyVersionedTransaction(KernelVersion version) {
        return new TransactionId(
                BASE_TX_ID,
                BASE_APPEND_INDEX,
                version,
                BASE_TX_CHECKSUM,
                BASE_TX_COMMIT_TIMESTAMP,
                UNKNOWN_CONSENSUS_INDEX);
    }

    /**
     * @return the next transaction id for a committing transaction. The transaction id is incremented
     * with each call. Ids returned from this method will not be visible from {@link #getLastCommittedTransactionId()}
     * until handed to {@link #transactionCommitted(long, long, KernelVersion, int, long, long)}.
     */
    long nextCommittingTransactionId();

    /**
     * @return the transaction id of last committing transaction.
     */
    long committingTransactionId();

    /**
     * Signals that a transaction with the given transaction id has been committed (i.e. appended to a log).
     * Calls to this method may come in out-of-transaction-id order. The highest transaction id
     * seen given to this method will be visible in {@link #getLastCommittedTransactionId()}.
     *
     * @param transactionId the applied transaction id.
     * @param appendIndex the applied transaction append index.
     * @param kernelVersion applied transaction kernel version
     * @param checksum checksum of the transaction.
     * @param commitTimestamp the timestamp of the transaction commit.
     * @param consensusIndex consensus index of the transaction.
     */
    void transactionCommitted(
            long transactionId,
            long appendIndex,
            KernelVersion kernelVersion,
            int checksum,
            long commitTimestamp,
            long consensusIndex);

    /**
     * @return highest seen {@link #transactionCommitted(long, long, KernelVersion, int, long, long)}  committed transaction id}.
     */
    long getLastCommittedTransactionId();

    /**
     * Returns transaction information about the highest committed transaction, i.e.
     * transaction id as well as checksum.
     *
     * @return {@link TransactionId} describing the last (i.e. highest) committed transaction.
     */
    TransactionId getLastCommittedTransaction();

    /**
     * @return highest seen gap-free {@link #transactionClosed(long, long, KernelVersion, long, long, int, long, long)}  closed transaction id}.
     */
    long getLastClosedTransactionId();

    /**
     * @return current snapshot of closed and visible transaction ids
     */
    TransactionIdSnapshot getClosedTransactionSnapshot();

    /**
     * Returns transaction information about the last committed transaction, i.e.
     * transaction id as well as the log position following the commit entry in the transaction log.
     *
     * @return transaction information about the last closed (highest gap-free) transaction.
     */
    ClosedTransactionMetadata getLastClosedTransaction();

    /**
     * Used by recovery, where last committed/closed transaction ids are set.
     *
     * @param transactionId transaction id that will be the last closed/committed id.
     * @param transactionAppendIndex append index to set sequence to
     * @param kernelVersion kernel version of transaction that was last closed/committed
     * @param checksum checksum of the transaction.
     * @param commitTimestamp the timestamp of the transaction commit.
     * @param consensusIndex consensus index of the transaction.
     * @param byteOffset offset in the log file where the committed entry has been written.
     * @param logVersion version of log the committed entry has been written into.
     */
    void setLastCommittedAndClosedTransactionId(
            long transactionId,
            long transactionAppendIndex,
            KernelVersion kernelVersion,
            int checksum,
            long commitTimestamp,
            long consensusIndex,
            long byteOffset,
            long logVersion,
            long logsAppendIndex);

    /**
     * Signals that a transaction with the given transaction id has been fully applied. Calls to this method
     * may come in out-of-transaction-id order.
     *
     * @param transactionId the applied transaction id.
     * @param appendIndex first append index of the closed transaction
     * @param kernelVersion the applied transaction kernel version.
     * @param logVersion version of log the committed entry has been written into.
     * @param byteOffset offset in the log file where start writing the next log entry.
     * @param checksum applied transaction checksum
     * @param commitTimestamp applied transaction commit timestamp
     * @param consensusIndex consensus index of the transaction.
     */
    void transactionClosed(
            long transactionId,
            long appendIndex,
            KernelVersion kernelVersion,
            long logVersion,
            long byteOffset,
            int checksum,
            long commitTimestamp,
            long consensusIndex);

    /**
     * Unconditionally set last closed transaction info. Should be used for cases where last closed transaction info should be
     * set or overwritten.
     *
     * @param transactionId new last closed transaction id.
     * @param appendIndex new last value of append index
     * @param kernelVersion new last closed transaction kernel version.
     * @param logVersion new last closed transaction log version.
     * @param byteOffset new last closed transaction offset.
     * @param checksum new last closed transaction checksum.
     * @param commitTimestamp new last closed transaction commit timestamp.
     * @param consensusIndex new last closed transaction consensus index.
     */
    void resetLastClosedTransaction(
            long transactionId,
            long appendIndex,
            KernelVersion kernelVersion,
            long logVersion,
            long byteOffset,
            int checksum,
            long commitTimestamp,
            long consensusIndex);

    /**
     * Signals that some new chunk of info was added to transaction logs with new provided index and position
     *
     * @param appendIndex latest append index
     * @param logPositionAfter log position after entry with provided appendIndex
     */
    void appendBatch(long appendIndex, LogPosition logPositionAfter);

    /**
     * Returns information about last encountered appended registered batch.
     * After database restart and before first applied transaction position after the batch will be UNSPECIFFIED and interested
     * parties should do lookup in their side.
     */
    AppendBatchInfo lastBatch();

    record AppendBatchInfo(long appendIndex, LogPosition logPositionAfter) {}
}
