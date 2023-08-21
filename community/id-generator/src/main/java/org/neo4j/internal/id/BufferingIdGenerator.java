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
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.neo4j.collection.trackable.HeapTrackingLongArrayList;
import org.neo4j.internal.id.range.PageIdRange;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.memory.MemoryTracker;

class BufferingIdGenerator extends IdGenerator.Delegate {
    private static final int MAX_BUFFERED_RANGES = 1000;
    private final ConcurrentLinkedQueue<PageIdRange> rangeCache = new ConcurrentLinkedQueue<>();

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
        var range = rangeCache.poll();
        if (range != null) {
            return range;
        }

        return delegate.nextPageRange(cursorContext, idsPerPage);
    }

    @Override
    public void releasePageRange(PageIdRange range, CursorContext cursorContext) {
        if (rangeCache.size() < MAX_BUFFERED_RANGES && range.hasNext()) {
            rangeCache.add(range);
        } else {
            delegate.releasePageRange(range, cursorContext);
        }
    }

    @Override
    public void close() {
        releaseRanges();
        bufferedFactory.release(idType());
        delegate.close();
    }

    @Override
    public TransactionalMarker transactionalMarker(CursorContext cursorContext) {
        return new TransactionalMarker.Delegate(super.transactionalMarker(cursorContext)) {
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

    public void releaseRanges() {
        if (!rangeCache.isEmpty()) {
            rangeCache.forEach(pageIdRange -> delegate.releasePageRange(pageIdRange, NULL_CONTEXT));
            rangeCache.clear();
        }
    }

    synchronized void collectBufferedIds(List<BufferingIdGeneratorFactory.IdBuffer> idBuffers) {
        if (!bufferedDeletedIds.isEmpty()) {
            idBuffers.add(new BufferingIdGeneratorFactory.IdBuffer(idTypeOrdinal, bufferedDeletedIds));
            newFreeBuffer();
        }
    }
}
