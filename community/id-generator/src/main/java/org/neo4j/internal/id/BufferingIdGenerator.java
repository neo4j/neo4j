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
package org.neo4j.internal.id;

import static org.neo4j.internal.id.IdUtils.combinedIdAndNumberOfIds;

import java.util.List;
import org.neo4j.collection.trackable.HeapTrackingLongArrayList;
import org.neo4j.internal.id.range.PageIdRange;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.memory.MemoryTracker;

class BufferingIdGenerator extends IdGenerator.Delegate {

    private final BufferingIdGeneratorFactory bufferedFactory;
    private final int idTypeOrdinal;
    private final MemoryTracker memoryTracker;
    private final Runnable collector;
    private HeapTrackingLongArrayList bufferedDeletedIds;

    BufferingIdGenerator(
            BufferingIdGeneratorFactory bufferedFactory,
            IdGenerator delegate,
            int idTypeOrdinal,
            MemoryTracker memoryTracker,
            Runnable collector) {
        super(delegate);
        this.bufferedFactory = bufferedFactory;
        this.idTypeOrdinal = idTypeOrdinal;
        this.memoryTracker = memoryTracker;
        this.collector = collector;
        newFreeBuffer();
    }

    private void newFreeBuffer() {
        bufferedDeletedIds = HeapTrackingLongArrayList.newLongArrayList(10, memoryTracker);
    }

    @Override
    public PageIdRange nextPageRange(CursorContext cursorContext, int idsPerPage) {
        throw new UnsupportedOperationException("Range allocation supported only in multi versioned id allocators.");
    }

    @Override
    public void releasePageRange(PageIdRange range, CursorContext cursorContext) {
        throw new UnsupportedOperationException("Range allocation supported only in multi versioned id allocators.");
    }

    @Override
    public void close() {
        bufferedFactory.release(idType());
        delegate.close();
    }

    @Override
    public TransactionalMarker transactionalMarker(CursorContext cursorContext) {
        var actualMarker = super.transactionalMarker(cursorContext);
        if (!allocationEnabled()) {
            return actualMarker;
        }

        return new TransactionalMarker.Delegate(actualMarker) {
            @Override
            public void markDeleted(long id, int numberOfIds) {
                // Run these by the buffering too
                actual.markDeleted(id, numberOfIds);
                synchronized (BufferingIdGenerator.this) {
                    bufferedDeletedIds.add(combinedIdAndNumberOfIds(id, numberOfIds, false));
                }
                if (bufferedDeletedIds.size() > 10_000) {
                    collector.run();
                }
            }
        };
    }

    @Override
    public synchronized void clearCache(boolean allocationEnabled, CursorContext cursorContext) {
        if (!bufferedDeletedIds.isEmpty()) {
            newFreeBuffer();
        }
        delegate.clearCache(allocationEnabled, cursorContext);
    }

    synchronized void collectBufferedIds(List<BufferingIdGeneratorFactory.IdBuffer> idBuffers) {
        if (!bufferedDeletedIds.isEmpty()) {
            idBuffers.add(new BufferingIdGeneratorFactory.IdBuffer(idTypeOrdinal, bufferedDeletedIds));
            newFreeBuffer();
        }
    }
}
