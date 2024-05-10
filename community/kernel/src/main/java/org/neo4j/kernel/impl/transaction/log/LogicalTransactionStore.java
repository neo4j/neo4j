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

import java.io.IOException;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatch;

/**
 * Accessor of command batches and their metadata.
 */
public interface LogicalTransactionStore {
    /**
     * Acquires a {@link CommandBatchCursor cursor} which will provide {@link CommittedCommandBatch}
     * instances for committed command batches, starting from the specified {@code appendIndexToStartFrom}.
     * Command batches will be returned from the cursor in sequential order.
     *
     * @param appendIndexToStartFrom id of the first append batch that the cursor will return.
     * @return an {@link CommandBatchCursor} capable of returning {@link CommittedCommandBatch} instances
     * for committed transactions or parts of transactions, starting from the specified {@code appendIndexToStartFrom}.
     * @throws NoSuchLogEntryException if the requested index hasn't been found,
     * or if the batch has been committed, but information about it is no longer available for some reason.
     * @throws IOException if there was an I/O related error looking for the start transaction.
     */
    CommandBatchCursor getCommandBatches(long appendIndexToStartFrom) throws IOException;

    /**
     * Acquires a {@link CommandBatchCursor cursor} which will provide {@link CommittedCommandBatch}
     * instances for committed command batches, starting from the specified {@link LogPosition}.
     * This is useful for placing a cursor at a position referred to by a {@link CheckpointInfo}.
     * Command batches will be returned from the cursor in transaction-id-sequential order.
     *
     * @param position {@link LogPosition} of the first transaction that the cursor will return.
     * @return an {@link CommandBatchCursor} capable of returning {@link CommittedCommandBatch} instances
     * for committed transactions, starting from the specified {@code position}.
     * @throws NoSuchLogEntryException if the requested transaction hasn't been committed,
     * or if the transaction has been committed, but information about it is no longer available for some reason.
     * @throws IOException if there was an I/O related error looking for the start transaction.
     */
    CommandBatchCursor getCommandBatches(LogPosition position) throws IOException;

    /**
     * Acquires a {@link CommandBatchCursor cursor} which will provide {@link CommittedCommandBatch}
     * instances for committed transactions, starting from the end of the whole command batch stream
     * back to (and including) the batch at {@link LogPosition}.
     * Command batches will be returned in reverse order from the end of the batch stream.
     *
     * @param backToPosition {@link LogPosition} of the lowest (last to be returned) transaction.
     * @return an {@link CommandBatchCursor} capable of returning {@link CommittedCommandBatch} instances
     * for committed command batches in the given range in reverse order.
     * @throws IOException if there was an I/O related error looking for the start transaction.
     */
    CommandBatchCursor getCommandBatchesInReverseOrder(LogPosition backToPosition) throws IOException;
}
