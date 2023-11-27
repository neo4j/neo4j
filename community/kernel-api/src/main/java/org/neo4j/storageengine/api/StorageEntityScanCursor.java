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

public interface StorageEntityScanCursor<S extends Scan> extends StorageEntityCursor {
    /**
     * Initializes this cursor so that it will scan over all existing entities. Each call to {@link #next()} will
     * advance the cursor so that the next entity is read.
     */
    void scan();

    /**
     * Initializes this cursor to perform batched scan.
     *
     * The provided <code>SCAN</code> can be shared among worker-threads where each thread has a separate cursor.
     * The role of <code>scan</code> is to make sure we don't get overlapping ranges.
     *
     * @param scan scan maintains state across threads so that we get exclusive ranges for each batch.
     * @param sizeHint the batch will try to read this number of entities.
     * Note: This is just a hint and the actual number of entities in a batch may be both greater or smaller than this number
     * @return <code>true</code> if there are entities to be found, otherwise <code>false</code>
     */
    boolean scanBatch(S scan, long sizeHint);

    /**
     * Initializes this cursor so that the next call to {@link #next()} will place this cursor at that entity.
     * @param reference entity to place this cursor at the next call to {@link #next()}.
     */
    void single(long reference);
}
