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
import static org.neo4j.storageengine.api.LogPositionMetadata.NO_METADATA;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_TX_ID;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_TX_SEQUENCE_NUMBER;

import java.util.function.LongConsumer;
import org.neo4j.common.Subject;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.api.txid.TransactionIdGenerator;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatchRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.storageengine.api.CommandBatch;
import org.neo4j.storageengine.api.Commitment;
import org.neo4j.storageengine.api.LogPositionMetadata;
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
    private LogPositionMetadata logPositionMetadata;
    private StorageEngineTransaction next;
    private long firstAppendIndex;
    private LongConsumer closedCallback;
    private boolean fillGapsOnClose = false;

    public ChunkedTransaction(
            CursorContext cursorContext,
            long transactionSequenceNumber,
            StoreCursors storeCursors,
            Commitment commitment,
            TransactionIdGenerator transactionIdGenerator) {
        this(cursorContext, transactionSequenceNumber, NO_METADATA, storeCursors, commitment, transactionIdGenerator);
    }

    public ChunkedTransaction(
            CursorContext cursorContext,
            long transactionSequenceNumber,
            LogPositionMetadata logPositionMetadata,
            StoreCursors storeCursors,
            Commitment commitment,
            TransactionIdGenerator transactionIdGenerator) {
        this.cursorContext = cursorContext;
        this.transactionSequenceNumber = transactionSequenceNumber;
        this.logPositionMetadata = logPositionMetadata;
        this.storeCursors = storeCursors;
        this.commitment = commitment;
        this.transactionIdGenerator = transactionIdGenerator;
    }

    public ChunkedTransaction(
            CommittedCommandBatchRepresentation committedCommandBatchRepresentation,
            CursorContext cursorContext,
            StoreCursors storeCursors) {
        this(committedCommandBatchRepresentation.txId(), UNKNOWN_TX_SEQUENCE_NUMBER, cursorContext, storeCursors);
        init((ChunkedCommandBatch) committedCommandBatchRepresentation.commandBatch());
    }

    public ChunkedTransaction(
            long transactionId,
            long transactionSequenceNumber,
            long lastBatchAppendIndex,
            CursorContext cursorContext,
            StoreCursors storeCursors,
            ChunkedCommandBatch chunk) {
        this(
                cursorContext,
                transactionSequenceNumber,
                storeCursors,
                Commitment.NO_COMMITMENT,
                TransactionIdGenerator.EXTERNAL_ID);
        this.transactionId = transactionId;
        this.lastBatchAppendIndex = lastBatchAppendIndex;
        this.chunk = chunk;
        this.idGenerated = true;
    }

    public ChunkedTransaction(
            long transactionId,
            long transactionSequenceNumber,
            CursorContext cursorContext,
            StoreCursors storeCursors) {
        this(
                cursorContext,
                transactionSequenceNumber,
                storeCursors,
                Commitment.NO_COMMITMENT,
                TransactionIdGenerator.EXTERNAL_ID);
        this.transactionId = transactionId;
        this.idGenerated = true;
    }

    public ChunkedTransaction(
            long transactionId,
            long firstBatchAppendIndex,
            long lastBatchAppendIndex,
            long transactionSequenceNumber,
            LogPositionMetadata logPositionMetadata,
            CursorContext cursorContext,
            StoreCursors storeCursors,
            Commitment commitment) {
        this(cursorContext, transactionSequenceNumber, storeCursors, commitment, TransactionIdGenerator.EXTERNAL_ID);
        this.transactionId = transactionId;
        this.firstAppendIndex = firstBatchAppendIndex;
        this.lastBatchAppendIndex = lastBatchAppendIndex;
        this.logPositionMetadata = logPositionMetadata;
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
        return transactionId(transactionId);
    }

    @Override
    public long transactionId(long externalId) {
        if (idGenerated) {
            if (transactionId != externalId) {
                throw new IllegalStateException(
                        "Attempted to set transaction id when a different one has already been generated.");
            }
            return transactionId;
        }
        transactionId = transactionIdGenerator.nextId(externalId);
        idGenerated = true;
        return transactionId;
    }

    @Override
    public long chunkId() {
        return chunk.chunkMetadata().chunkId();
    }

    /*
    Append index of the previous chunk, read from the current chunk's metadata. Used to write log entry headers and to walk backwards during rollback.
     */
    @Override
    public long previousBatchAppendIndex() {
        return chunk.previousBatchAppendIndex();
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
    public boolean fillGapsOnCloseIfRelevant() {
        return this.fillGapsOnClose;
    }

    @Override
    public void fillGapsOnCloseIfRelevant(boolean fillGapsOnClose) {
        this.fillGapsOnClose = fillGapsOnClose;
    }

    @Override
    public void commit() {
        commitment.publishAsCommitedLastBatch();

        if (!chunk.isFirst() && fillGapsOnClose) {
            commitment.publishEmptyAsCommitted(chunk.chunkMetadata().chunkCommitTime());
        }

        if (chunk.isLast()) {
            commitment.publishAsCommitted(chunk.chunkMetadata().chunkCommitTime(), firstAppendIndex);
        }
    }

    /*
    Append index assigned to the most recently appended chunk for this transaction.
     */
    public long lastBatchAppendIndex() {
        return lastBatchAppendIndex;
    }

    public long firstBatchAppendIndex() {
        return firstAppendIndex;
    }

    @Override
    public CommandBatch commandBatch() {
        return chunk;
    }

    @Override
    public LogPositionMetadata logPositionMetadata() {
        return logPositionMetadata;
    }

    @Override
    public void batchAppended(long appendIndex, LogPosition beforeStart, LogPosition positionAfter, int checksum) {
        var versionContext = this.cursorContext.getVersionContext();
        if (chunk.isFirst()) {
            versionContext.initWrite(transactionId);
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
        versionContext.initChunkId(chunkId());
        chunk.setAppendIndex(appendIndex);
        lastBatchAppendIndex = appendIndex;
    }

    @Override
    public void close() {
        commitment.publishAsClosed();

        if (!chunk.isFirst() && fillGapsOnClose) {
            commitment.publishEmptyAsClosed();
        }

        if (chunk.isLast() && closedCallback != null) {
            closedCallback.accept(transactionId);
        }
    }

    @Override
    public String toString() {
        return "ChunkedTransaction{" + "transactionSequenceNumber=" + transactionSequenceNumber + ", transactionId="
                + transactionId + ", chunkId=" + chunk.chunkMetadata().chunkId() + '}';
    }

    public long getTransactionSequenceNumber() {
        return transactionSequenceNumber;
    }

    /**
     * While we are talking how data about chunked transactions be transferred in clusters we at least need to make sure that tx ids are aligned,
     * otherwise tx id sequences will go completely out of sync
     */
    @Override
    public void updateClusteredInfo(long transactionId, long appendIndex, long chunkId) {
        if (!idGenerated) {
            this.transactionId = transactionId;
            this.firstAppendIndex = appendIndex;
            cursorContext.getVersionContext().initWrite(transactionId);
            idGenerated = true;
        }
        cursorContext.getVersionContext().initChunkId(chunkId);
        lastBatchAppendIndex = appendIndex;
    }
}
