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

import static org.neo4j.kernel.impl.api.TransactionToApply.TRANSACTION_ID_NOT_SPECIFIED;

import java.io.IOException;
import java.util.Iterator;
import org.neo4j.common.Subject;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.api.txid.TransactionIdGenerator;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatch;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.storageengine.api.CommandBatch;
import org.neo4j.storageengine.api.CommandBatchToApply;
import org.neo4j.storageengine.api.Commitment;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.cursor.StoreCursors;

public class ChunkedTransaction implements CommandBatchToApply {

    private CommandChunk chunk;
    // to sure for what reason we need those now here?
    private final CursorContext cursorContext;
    private final long transactionSequenceNumber;
    private final StoreCursors storeCursors;
    private final Commitment commitment;
    private final TransactionIdGenerator transactionIdGenerator;
    private boolean idGenerated;
    private LogPosition lastBatchLogPosition = LogPosition.UNSPECIFIED;
    private long transactionId = TRANSACTION_ID_NOT_SPECIFIED;
    private CommandBatchToApply next;
    private long firstAppendIndex;

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
            CommittedCommandBatch committedCommandBatch, CursorContext cursorContext, StoreCursors storeCursors) {
        this(cursorContext, -1, storeCursors, Commitment.NO_COMMITMENT, TransactionIdGenerator.EXTERNAL_ID);
        this.transactionId = committedCommandBatch.txId();
        init((CommandChunk) committedCommandBatch.commandBatch());
    }

    public void init(CommandChunk chunk) {
        this.chunk = chunk;
    }

    @Override
    public boolean accept(Visitor<StorageCommand, IOException> visitor) throws IOException {
        return chunk.accept(visitor);
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
    public LogPosition previousBatchLogPosition() {
        return chunk.chunkMetadata().previousBatchLogPosition();
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
    public CommandBatchToApply next() {
        return next;
    }

    @Override
    public void next(CommandBatchToApply next) {
        this.next = next;
    }

    @Override
    public void commit() {
        if (chunk.isLast()) {
            commitment.publishAsCommitted(chunk.chunkMetadata().chunkCommitTime(), firstAppendIndex);
        }
    }

    public LogPosition lastBatchLogPosition() {
        return lastBatchLogPosition;
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
        lastBatchLogPosition = beforeStart;
    }

    @Override
    public void close() {
        commitment.publishAsClosed();
    }

    @Override
    public Iterator<StorageCommand> iterator() {
        return chunk.iterator();
    }

    @Override
    public String toString() {
        return "ChunkedTransaction{" + "transactionSequenceNumber=" + transactionSequenceNumber + ", transactionId="
                + transactionId + '}';
    }
}
