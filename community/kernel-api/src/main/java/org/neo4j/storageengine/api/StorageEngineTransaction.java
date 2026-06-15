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

import java.util.function.LongConsumer;
import org.neo4j.common.Subject;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.storageengine.AppendIndexProvider;
import org.neo4j.storageengine.api.cursor.StoreCursors;

/**
 * Group of commands to apply onto {@link StorageEngine}, as well as reference to {@link #next()} group of commands.
 * The linked list will form a batch.
 */
public interface StorageEngineTransaction extends AutoCloseable {
    /**
     * @return transaction id representing this group of commands.
     */
    long transactionId();

    /**
     * @param externalId suggested id to use
     * @return transaction id representing this group of commands.
     */
    long transactionId(long externalId);

    /**
     * @return chunk id representing this group of commands
     */
    long chunkId();

    /**
     * Append index of previous command batch of the same transaction in the transaction log
     * @return append index of previous batch or {@link AppendIndexProvider#UNKNOWN_APPEND_INDEX} if no previous batch exists.
     */
    long previousBatchAppendIndex();

    /**
     * Subject that triggered the commands.
     * <p>
     * This is used for monitoring purposes, so a unit of work can be linked to its initiator.
     */
    Subject subject();

    /**
     * Page cursor tracer to trace access to underlying page cache
     * @return underlying page cursor tracer
     */
    CursorContext cursorContext();

    /**
     * Transaction store cursors to access underlying stores
     */
    StoreCursors storeCursors();

    /**
     * @return next group of commands in this batch.
     */
    StorageEngineTransaction next();

    /**
     * @param next set next group of commands in this batch.
     */
    void next(StorageEngineTransaction next);

    /**
     * Set transaction close callback
     */
    void onClose(LongConsumer closedCallback);

    /**
     * The chunks in multi-chunked transactions all share a transaction id. On merged logs, this can result in gaps in
     * our metadata store, as that txid is based on the append index of the first chunk. This method returns true if we
     * are on merged logs and therefore need to fill those gaps.
     */
    default boolean fillGapsOnCloseIfRelevant() {
        return false;
    }

    /**
     * The chunks in multi-chunked transactions all share a transaction id. On merged logs, this can result in gaps in
     * our metadata store, as that txid is based on the append index of the first chunk. This method should be called
     * with {@code true} when registering txs on merged logs.
     */
    default void fillGapsOnCloseIfRelevant(boolean fillGapsOnClose) {
        // No-op by default
    }

    void commit();

    /**
     * Commands that should be applied as part of this particular batch
     */
    CommandBatch commandBatch();

    LogPositionMetadata logPositionMetadata();

    /**
     * Invoked by commit process after this batch of commands was applied to transaction log
     *
     * @param appendIndex append index of newly added batch
     * @param beforeCommit log position before append
     * @param positionAfter log position after append
     * @param checksum checksum ot appended batch entries
     */
    void batchAppended(long appendIndex, LogPosition beforeCommit, LogPosition positionAfter, int checksum);

    /**
     * Method to update back transaction with data from clustering replication process
     * @param transactionId - transaction id that was generated for this transaction
     * @param appendIndex - append index generated for the latest chunk of this transaction
     * @param chunkId - chunk id generated for the latest chunk of this transaction
     */
    void updateClusteredInfo(long transactionId, long appendIndex, long chunkId);

    @Override
    void close();
}
