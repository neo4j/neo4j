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
package org.neo4j.counts;

import java.io.IOException;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.kernel.impl.index.schema.ConsistencyCheckable;
import org.neo4j.memory.MemoryTracker;

public interface CountsStore extends AutoCloseable, ConsistencyCheckable {

    /**
     * @param txId id of the transaction that produces the changes that are being applied.
     * @param cursorContext underlying page cursor context
     * @return an updater where count deltas are being applied onto.
     */
    CountsUpdater updater(long txId, boolean isLast, CursorContext cursorContext);

    /**
     * @param txId id of the transaction that produces the changes that are being applied.
     * @param cursorContext underlying page cursor context
     * @return an updater to process count deltas for transaction rollback
     */
    default CountsUpdater rollbackUpdater(long txId, CursorContext cursorContext) {
        return CountsUpdater.NO_OP_UPDATER;
    }

    /**
     * @param txId id of the transaction that produces the changes that are being applied.
     * @param cursorContext underlying page cursor context
     * @return an updater to process count deltas during reverse recovery
     */
    default CountsUpdater reverseRecoveryUpdater(long txId, CursorContext cursorContext) {
        return CountsUpdater.NO_OP_UPDATER;
    }

    default CountsUpdater directUpdater(CursorContext cursorContext) throws IOException {
        return updater(cursorContext.getVersionContext().committingTransactionId(), true, cursorContext);
    }

    /**
     * @param labelId node label token id to get count for.
     * @param cursorContext underlying page cursor context
     * @return the count for the label token id, i.e. number of nodes with that label.
     */
    long nodeCount(int labelId, CursorContext cursorContext);

    /**
     * @param startLabelId node label token id of start node.
     * @param typeId relationship type token id of relationship.
     * @param endLabelId node label token id of end node.
     * @param cursorContext underlying page cursor context
     * @return the count for the start/end node label and relationship type combination.
     */
    long relationshipCount(int startLabelId, int typeId, int endLabelId, CursorContext cursorContext);

    /**
     * Puts the counts store in started state, i.e. after potentially recovery has been made. Any changes
     * before this call is made are considered recovery repairs from a previous non-clean shutdown.
     * @throws IOException any type of error happening when transitioning to started state.
     */
    void start(CursorContext cursorContext, MemoryTracker memoryTracker) throws IOException;

    /**
     * Accepts a visitor observing all entries in this store.
     * @param visitor to receive the entries.
     * @param cursorContext page cache access context.
     */
    void accept(CountsVisitor visitor, CursorContext cursorContext);

    /**
     * Checkpoints changes made up until this point so that they are available even after next restart.
     *
     * @param flushEvent page file flush event
     * @param cursorContext page cache access context.
     * @throws IOException on I/O error.
     */
    void checkpoint(FileFlushEvent flushEvent, CursorContext cursorContext) throws IOException;

    @Override
    void close();
}
