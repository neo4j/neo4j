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
package org.neo4j.io.pagecache.tracing.linear;

import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.tracing.PinEvent;
import org.neo4j.io.pagecache.tracing.cursor.CursorStatisticSnapshot;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

/**
 * Tracer for page cache cursor events that add all of them to event history tracer that can build proper linear
 * history across all tracers.
 * Only use this for debugging internal data race bugs and the like, in the page cache.
 *
 * @see HEvents
 * @see LinearHistoryPageCacheTracer
 */
public class LinearHistoryPageCursorTracer implements PageCursorTracer {
    private final LinearHistoryTracer tracer;
    private final String tag;

    LinearHistoryPageCursorTracer(LinearHistoryTracer tracer, String tag) {
        this.tracer = tracer;
        this.tag = tag;
    }

    @Override
    public long faults() {
        return 0;
    }

    @Override
    public long failedFaults() {
        return 0;
    }

    @Override
    public long noFaults() {
        return 0;
    }

    @Override
    public long vectoredFaults() {
        return 0;
    }

    @Override
    public long failedVectoredFaults() {
        return 0;
    }

    @Override
    public long noPinFaults() {
        return 0;
    }

    @Override
    public long pins() {
        return 0;
    }

    @Override
    public long unpins() {
        return 0;
    }

    @Override
    public long hits() {
        return 0;
    }

    @Override
    public long bytesRead() {
        return 0;
    }

    @Override
    public long evictions() {
        return 0;
    }

    @Override
    public long evictionFlushes() {
        return 0;
    }

    @Override
    public long evictionExceptions() {
        return 0;
    }

    @Override
    public long bytesWritten() {
        return 0;
    }

    @Override
    public long flushes() {
        return 0;
    }

    @Override
    public long merges() {
        return 0;
    }

    @Override
    public long snapshotsLoaded() {
        return 0;
    }

    @Override
    public double hitRatio() {
        return 0d;
    }

    @Override
    public long copiedPages() {
        return 0;
    }

    @Override
    public PinEvent beginPin(boolean writeLock, long filePageId, PageSwapper swapper) {
        return tracer.add(new HEvents.PinHEvent(tracer, writeLock, filePageId, swapper));
    }

    @Override
    public void unpin(long filePageId, PageSwapper swapper) {}

    @Override
    public void reportEvents() {
        // nothing to do
    }

    @Override
    public String getTag() {
        return tag;
    }

    @Override
    public void openCursor() {}

    @Override
    public void closeCursor() {}

    @Override
    public long openedCursors() {
        return 0;
    }

    @Override
    public long closedCursors() {
        return 0;
    }

    @Override
    public void merge(CursorStatisticSnapshot statisticSnapshot) {}

    @Override
    public void pageCopied(long pageRef, long version) {}
}
