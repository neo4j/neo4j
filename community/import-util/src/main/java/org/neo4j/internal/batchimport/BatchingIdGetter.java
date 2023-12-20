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
package org.neo4j.internal.batchimport;

import static org.neo4j.internal.kernel.api.Read.NO_ID;

import org.eclipse.collections.api.iterator.LongIterator;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdSequence;
import org.neo4j.io.pagecache.context.CursorContext;

/**
 * Exposes batches of ids from an {@link IdGenerator} as a {@link LongIterator}.
 * It makes use of {@link IdGenerator#nextConsecutiveIdRange(int, boolean, CursorContext)} (with default batch size the number of records per page)
 * and caches that batch, exhausting it in {@link #nextId(CursorContext)} before getting next batch.
 */
public class BatchingIdGetter implements IdSequence {
    private final IdGenerator source;
    private final int batchSize;
    private long currentBatchStartId = NO_ID;
    private int currentBatchIndex;

    public BatchingIdGetter(IdGenerator idGenerator, int recordsPerPage) {
        this.source = idGenerator;
        this.batchSize = recordsPerPage;
    }

    @Override
    public long nextId(CursorContext cursorContext) {
        long id = nextIdFromCurrentBatch();
        if (id != NO_ID) {
            return id;
        }

        currentBatchStartId = source.nextConsecutiveIdRange(batchSize, false, cursorContext);
        currentBatchIndex = 0;
        return nextIdFromCurrentBatch();
    }

    private long nextIdFromCurrentBatch() {
        return currentBatchStartId == NO_ID || currentBatchIndex == batchSize
                ? NO_ID
                : currentBatchStartId + currentBatchIndex++;
    }

    public void markUnusedIdsAsDeleted(CursorContext cursorContext) {
        try (var marker = source.transactionalMarker(cursorContext)) {
            long id;
            while ((id = nextIdFromCurrentBatch()) != NO_ID) {
                marker.markDeleted(id);
            }
        }
    }
}
