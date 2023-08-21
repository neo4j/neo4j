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
package org.neo4j.internal.counts;

import java.io.IOException;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.kernel.impl.index.schema.ConsistencyCheckable;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.TransactionIdStore;

/**
 * Store for degrees of relationship chains for dense nodes. Relationship group record ID plus relationship direction forms the key for the counts.
 */
public interface RelationshipGroupDegreesStore extends AutoCloseable, ConsistencyCheckable {

    /**
     * @param txId id of the transaction that produces the changes that are being applied.
     * @param cursorContext underlying page cursor context
     * @return an updater where count deltas are being applied onto.
     */
    DegreeUpdater updater(long txId, boolean isLast, CursorContext cursorContext);

    /**
     * @param txId id of the transaction that produces the changes that are being applied.
     * @param cursorContext underlying page cursor context
     * @return an updater to process count deltas for transaction rollback
     */
    default DegreeUpdater rollbackUpdater(long txId, CursorContext cursorContext) {
        return DegreeUpdater.NO_OP_UPDATER;
    }

    /**
     * @param txId id of the transaction that produces the changes that are being applied.
     * @param cursorContext underlying page cursor context
     * @return an updater to process count deltas during reverse recovery
     */
    default DegreeUpdater reverseRecoveryUpdater(long txId, CursorContext cursorContext) {
        return DegreeUpdater.NO_OP_UPDATER;
    }

    /**
     * @param groupId the relationship group ID to look for.
     * @param direction the direction to look for.
     * @param cursorContext page cache access context.
     * @return the degree for the given groupId and direction, or {@code 0} if it wasn't found.
     */
    long degree(long groupId, RelationshipDirection direction, CursorContext cursorContext);

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
    void accept(GroupDegreeVisitor visitor, CursorContext cursorContext);

    /**
     * Checkpoints changes made up until this point so that they are available even after next restart.
     *
     * @param flushEvent page file flush event
     * @param cursorContext page cache access context.
     * @throws IOException on I/O error.
     */
    void checkpoint(FileFlushEvent flushEvent, CursorContext cursorContext) throws IOException;

    default DegreeUpdater directApply(CursorContext cursorContext) throws IOException {
        return updater(TransactionIdStore.BASE_TX_ID, true, cursorContext);
    }

    @Override
    void close();
}
