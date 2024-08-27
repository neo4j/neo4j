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

import static java.lang.String.format;
import static org.neo4j.storageengine.AppendIndexProvider.BASE_APPEND_INDEX;
import static org.neo4j.storageengine.AppendIndexProvider.UNKNOWN_APPEND_INDEX;

import org.neo4j.graphdb.TransactionRollbackException;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatchRepresentation;
import org.neo4j.kernel.impl.transaction.log.CommandBatchCursor;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.tracing.TransactionRollbackEvent;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.TransactionApplicationMode;

public final class MultiVersionTransactionRollbackProcess implements TransactionRollbackProcess {
    private final LogicalTransactionStore transactionStore;
    private final StorageEngine storageEngine;

    public MultiVersionTransactionRollbackProcess(
            LogicalTransactionStore transactionStore, StorageEngine storageEngine) {
        this.transactionStore = transactionStore;
        this.storageEngine = storageEngine;
    }

    @Override
    public void rollbackChunks(ChunkedTransaction chunkedTransaction, TransactionRollbackEvent rollbackEvent)
            throws Exception {
        long transactionIdToRollback = chunkedTransaction.transactionId();
        long chunksToRollback = chunkedTransaction.chunkId() - 1;
        int rolledbackBatches = 0;
        long nextBatchToRollbackIndex = chunkedTransaction.lastBatchAppendIndex();
        var rollbackChunkedTransaction = new ChunkedTransaction(
                transactionIdToRollback, chunkedTransaction.cursorContext(), chunkedTransaction.storeCursors());
        try (var rollbackDataEvent = rollbackEvent.beginRollbackDataEvent()) {
            while (rolledbackBatches != chunksToRollback) {
                validateBatchIndex(
                        nextBatchToRollbackIndex, chunksToRollback, rolledbackBatches, transactionIdToRollback);
                try (CommandBatchCursor commandBatches = transactionStore.getCommandBatches(nextBatchToRollbackIndex)) {
                    if (!commandBatches.next()) {
                        throw new TransactionRollbackException(format(
                                "Transaction rollback failed. Expected to rollback %d batches, but was able to undo only %d for transaction with id %d.",
                                chunksToRollback, rolledbackBatches, transactionIdToRollback));
                    }
                    CommittedCommandBatchRepresentation commandBatch = commandBatches.get();
                    if (commandBatch.txId() != transactionIdToRollback) {
                        throw new TransactionRollbackException(String.format(
                                "Transaction rollback failed. Batch with transaction id %d encountered, while it was expected to belong to transaction id %d. Batch id: %s.",
                                commandBatch.txId(), transactionIdToRollback, chunkId(commandBatch)));
                    }
                    rollbackChunkedTransaction.init((ChunkedCommandBatch) commandBatch.commandBatch());
                    storageEngine.apply(rollbackChunkedTransaction, TransactionApplicationMode.MVCC_ROLLBACK);
                    rolledbackBatches++;
                    nextBatchToRollbackIndex = commandBatch.previousBatchAppendIndex();
                }
            }
            if (nextBatchToRollbackIndex != UNKNOWN_APPEND_INDEX) {
                throw new TransactionRollbackException(String.format(
                        "Transaction rollback failed. All expected %d batches in transaction id %d were rolled back but chain claims to have more at append index: %s.",
                        chunksToRollback, transactionIdToRollback, nextBatchToRollbackIndex));
            }
            rollbackDataEvent.batchedRolledBack(chunksToRollback, transactionIdToRollback);
        }
    }

    private static void validateBatchIndex(
            long batchToRollbackIndex,
            long totalChunksToRollback,
            long rolledBackChunks,
            long transactionIdToRollback) {
        if (batchToRollbackIndex < BASE_APPEND_INDEX) {
            throw new TransactionRollbackException(String.format(
                    "Transaction rollback failed. Was able to rollback %d chunks out of %d for transaction %d until encountered incorrect batch index %d.",
                    rolledBackChunks, totalChunksToRollback, transactionIdToRollback, batchToRollbackIndex));
        }
    }

    private static String chunkId(CommittedCommandBatchRepresentation commandBatch) {
        return commandBatch.commandBatch() instanceof ChunkedCommandBatch cc
                ? String.valueOf(cc.chunkMetadata().chunkId())
                : "N/A";
    }
}
