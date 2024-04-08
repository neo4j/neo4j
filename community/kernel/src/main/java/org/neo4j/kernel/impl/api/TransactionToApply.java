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
package org.neo4j.kernel.impl.api;

import static org.neo4j.internal.helpers.Format.date;
import static org.neo4j.kernel.impl.api.txid.TransactionIdGenerator.EXTERNAL_ID;

import java.io.IOException;
import java.util.Iterator;
import java.util.function.LongConsumer;
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

/**
 * A chain of transactions to apply. Transactions form a linked list, each pointing to the {@link #next()}
 * or {@code null}. This design chosen for less garbage and convenience, i.e. that we pass in a number of transactions
 * while also expecting some results for each, and every one of those transactions back. The results are
 * written directly into each instance instead of creating another data structure which is then returned.
 * This is an internal class so even if it mixes arguments with results it's easier to work with,
 * requires less code... and less objects.
 */
public class TransactionToApply implements CommandBatchToApply {
    public static final long TRANSACTION_ID_NOT_SPECIFIED = 0;
    public static final int NOT_SPECIFIED_CHUNK_ID = 0;

    // These fields are provided by user
    private final CommandBatch commandBatch;
    private boolean idGenerated;
    private long transactionId;
    private final CursorContext cursorContext;
    private final StoreCursors storeCursors;
    private final TransactionIdGenerator transactionIdGenerator;
    private CommandBatchToApply next;

    // These fields are provided by commit process, storage engine, or recovery process
    private final Commitment commitment;
    private LongConsumer closedCallback;

    public TransactionToApply(
            CommittedCommandBatch committedCommandBatch, CursorContext cursorContext, StoreCursors storeCursors) {
        this(committedCommandBatch, cursorContext, storeCursors, Commitment.NO_COMMITMENT, EXTERNAL_ID);
    }

    public TransactionToApply(
            CommittedCommandBatch committedCommandBatch,
            CursorContext cursorContext,
            StoreCursors storeCursors,
            Commitment commitment,
            TransactionIdGenerator transactionIdGenerator) {
        this(committedCommandBatch.commandBatch(), cursorContext, storeCursors, commitment, transactionIdGenerator);
        this.transactionId = committedCommandBatch.txId();
    }

    public TransactionToApply(
            CommandBatch commandBatch,
            CursorContext cursorContext,
            StoreCursors storeCursors,
            Commitment commitment,
            TransactionIdGenerator transactionIdGenerator) {
        this.commandBatch = commandBatch;
        this.cursorContext = cursorContext;
        this.storeCursors = storeCursors;
        this.commitment = commitment;
        this.transactionIdGenerator = transactionIdGenerator;
    }

    // These methods are called by the user when building a batch
    @Override
    public void next(CommandBatchToApply next) {
        this.next = next;
    }

    @Override
    public void commit() {
        commitment.publishAsCommitted(commandBatch.getTimeCommitted());
    }

    @Override
    public long transactionId() {
        if (idGenerated) {
            return transactionId;
        }
        transactionId = transactionIdGenerator.nextId(transactionId);
        idGenerated = true;
        return transactionId;
    }

    @Override
    public long chunkId() {
        return NOT_SPECIFIED_CHUNK_ID;
    }

    @Override
    public LogPosition previousBatchLogPosition() {
        return LogPosition.UNSPECIFIED;
    }

    @Override
    public Subject subject() {
        return commandBatch.subject();
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
    public boolean accept(Visitor<StorageCommand, IOException> visitor) throws IOException {
        return commandBatch.accept(visitor);
    }

    @Override
    public CommandBatch commandBatch() {
        return commandBatch;
    }

    @Override
    public void batchAppended(LogPosition beforeCommit, LogPosition positionAfter, int checksum) {
        this.commitment.commit(
                transactionId,
                commandBatch.kernelVersion(),
                beforeCommit,
                positionAfter,
                checksum,
                commandBatch.consensusIndex());
        this.cursorContext.getVersionContext().initWrite(transactionId);
    }

    @Override
    public CommandBatchToApply next() {
        return next;
    }

    public void onClose(LongConsumer closedCallback) {
        this.closedCallback = closedCallback;
    }

    @Override
    public void close() {
        commitment.publishAsClosed();
        if (closedCallback != null) {
            closedCallback.accept(transactionId);
        }
    }

    @Override
    public String toString() {
        CommandBatch tr = this.commandBatch;
        return "Transaction #" + transactionId
                + " {started "
                + date(tr.getTimeStarted()) + ", committed "
                + date(tr.getTimeCommitted()) + ", with "
                + countCommands() + " commands in this transaction" + ", lease "
                + tr.getLeaseId() + ", latest committed transaction id when started was "
                + tr.getLatestCommittedTxWhenStarted() + ", consensusIndex: "
                + tr.consensusIndex() + "}";
    }

    private String countCommands() {
        class Counter implements Visitor<StorageCommand, IOException> {
            private int count;

            @Override
            public boolean visit(StorageCommand element) {
                count++;
                return false;
            }
        }
        try {
            Counter counter = new Counter();
            accept(counter);
            return String.valueOf(counter.count);
        } catch (Throwable e) {
            return "(unable to count: " + e.getMessage() + ")";
        }
    }

    @Override
    public Iterator<StorageCommand> iterator() {
        return commandBatch.iterator();
    }
}
