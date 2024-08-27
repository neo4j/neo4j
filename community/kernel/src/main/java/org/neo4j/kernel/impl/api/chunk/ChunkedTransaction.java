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
package org.neo4j.kernel.impl.api.chunk;

import static org.neo4j.storageengine.AppendIndexProvider.UNKNOWN_APPEND_INDEX;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_TX_ID;

import java.util.function.LongConsumer;
import org.neo4j.common.Subject;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.api.txid.TransactionIdGenerator;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatchRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.storageengine.api.CommandBatch;
import org.neo4j.storageengine.api.Commitment;
import org.neo4j.storageengine.api.StorageEngineTransaction;
import org.neo4j.storageengine.api.cursor.StoreCursors;

public class ChunkedTransaction implements StorageEngineTransaction {

    private ChunkedCommandBatch chunk;
    private final CursorContext cursorContext;
    private final long transactionSequenceNumber;
    private final StoreCursors storeCursors;
    private final Commitment commitment;
    private final TransactionIdGenerator transactionIdGenerator;
    private boolean idGenerated;
    private long lastBatchAppendIndex = UNKNOWN_APPEND_INDEX;
    private long transactionId = UNKNOWN_TX_ID;
    private StorageEngineTransaction next;
    private long firstAppendIndex;
    private LongConsumer closedCallback;

    public ChunkedTransaction(
            CursorContext cursorContext,
            long transactionSequenceNumber,
            StoreCursors storeCursors,
            Commitment commitment,
            TransactionIdGenerator transactionIdGenerator) {
        this.cursorContext = cursorContext;
        this.transactionSequenceNumber = transactionSequenceNumber;
        this.storeCursors = storeCursors;
        this.commitment = commitment;
        this.transactionIdGenerator = transactionIdGenerator;
    }

    public ChunkedTransaction(
            CommittedCommandBatchRepresentation committedCommandBatchRepresentation,
            CursorContext cursorContext,
            StoreCursors storeCursors) {
        this(committedCommandBatchRepresentation.txId(), cursorContext, storeCursors);
        init((ChunkedCommandBatch) committedCommandBatchRepresentation.commandBatch());
    }

    public ChunkedTransaction(long transactionId, CursorContext cursorContext, StoreCursors storeCursors) {
        this(cursorContext, -1, storeCursors, Commitment.NO_COMMITMENT, TransactionIdGenerator.EXTERNAL_ID);
        this.transactionId = transactionId;
        this.idGenerated = true;
    }

    public void init(ChunkedCommandBatch chunk) {
        this.chunk = chunk;
    }

    @Override
    public Subject subject() {
        return chunk.subject();
    }

    @Override
    public long transactionId() {
        if (idGenerated) {
            return transactionId;
        }
        this.transactionId = transactionIdGenerator.nextId(transactionId);
        idGenerated = true;
        return transactionId;
    }

    @Override
    public long chunkId() {
        return chunk.chunkMetadata().chunkId();
    }

    @Override
    public long previousBatchAppendIndex() {
        return chunk.chunkMetadata().previousBatchAppendIndex();
    }

    @Override
    public CursorContext cursorContext() {
        return cursorContext;
    }

    @Override
    public StoreCursors storeCursors() {
        return storeCursors;
    }

    @Override
    public StorageEngineTransaction next() {
        return next;
    }

    @Override
    public void next(StorageEngineTransaction next) {
        this.next = next;
    }

    @Override
    public void onClose(LongConsumer closedCallback) {
        this.closedCallback = closedCallback;
    }

    @Override
    public void commit() {
        commitment.publishAsCommitedLastBatch();
        if (chunk.isLast()) {
            commitment.publishAsCommitted(chunk.chunkMetadata().chunkCommitTime(), firstAppendIndex);
        }
    }

    public long lastBatchAppendIndex() {
        return lastBatchAppendIndex;
    }

    @Override
    public CommandBatch commandBatch() {
        return chunk;
    }

    @Override
    public void batchAppended(long appendIndex, LogPosition beforeStart, LogPosition positionAfter, int checksum) {
        if (chunk.isFirst()) {
            this.cursorContext.getVersionContext().initWrite(transactionId);
            this.firstAppendIndex = appendIndex;
        }
        this.commitment.commit(
                transactionId,
                appendIndex,
                chunk.chunkMetadata().first(),
                chunk.chunkMetadata().last(),
                chunk.kernelVersion(),
                beforeStart,
                positionAfter,
                checksum,
                chunk.chunkMetadata().consensusIndex().longValue());
        chunk.setAppendIndex(appendIndex);
        lastBatchAppendIndex = appendIndex;
    }

    @Override
    public void close() {
        commitment.publishAsClosed();
        if (chunk.isLast() && closedCallback != null) {
            closedCallback.accept(transactionId);
        }
    }

    @Override
    public String toString() {
        return "ChunkedTransaction{" + "transactionSequenceNumber=" + transactionSequenceNumber + ", transactionId="
                + transactionId + ", chunkId=" + chunk.chunkMetadata().chunkId() + '}';
    }

    /**
     * While we are talking how data about chunked transactions be transferred in clusters we at least need to make sure that tx ids are aligned,
     * otherwise tx id sequences will go completely out of sync
     */
    public void updateClusteredTransactionId(long transactionId) {
        if (!idGenerated) {
            this.transactionId = transactionId;
            idGenerated = true;
        }
    }
}
