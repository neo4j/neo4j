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
package org.neo4j.io.pagecache.tracing.recording;

import java.util.Objects;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.tracing.EvictionEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.PinEvent;
import org.neo4j.io.pagecache.tracing.PinPageFaultEvent;
import org.neo4j.io.pagecache.tracing.cursor.CursorStatisticSnapshot;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

/**
 * Recording tracer of page cursor events.
 * Records and counts number of {@link Pin} and {@link Fault} events.
 * Propagate those counters to global page cache tracer during event reporting.
 */
public class RecordingPageCursorTracer extends RecordingTracer implements PageCursorTracer {
    private int pins;
    private int faults;
    private final PageCacheTracer tracer;
    private final String tag;

    public RecordingPageCursorTracer(PageCacheTracer tracer, String tag) {
        super(Pin.class, Fault.class);
        this.tracer = tracer;
        this.tag = tag;
    }

    @SafeVarargs
    public RecordingPageCursorTracer(PageCacheTracer tracer, String tag, Class<? extends Event>... eventTypesToTrace) {
        super(eventTypesToTrace);
        this.tracer = tracer;
        this.tag = tag;
    }

    @Override
    public long faults() {
        return faults;
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
        return pins;
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
    public PinEvent beginPin(boolean writeLock, final long filePageId, PageSwapper swapper) {
        return new PinEvent() {
            private boolean hit = true;

            @Override
            public void setCachePageId(long cachePageId) {}

            @Override
            public PinPageFaultEvent beginPageFault(long filePageId, PageSwapper swapper) {
                hit = false;
                return new PinPageFaultEvent() {
                    @Override
                    public void addBytesRead(long bytes) {}

                    @Override
                    public void close() {
                        pageFaulted(filePageId, swapper);
                    }

                    @Override
                    public void setException(Throwable throwable) {}

                    @Override
                    public void freeListSize(int freeListSize) {}

                    @Override
                    public EvictionEvent beginEviction(long cachePageId) {
                        return EvictionEvent.NULL;
                    }

                    @Override
                    public void setCachePageId(long cachePageId) {}
                };
            }

            @Override
            public void hit() {}

            @Override
            public void noFault() {}

            @Override
            public void snapshotsLoaded(int oldSnapshotsLoaded) {}

            @Override
            public void close() {
                pinned(filePageId, swapper, hit);
            }
        };
    }

    @Override
    public void unpin(long filePageId, PageSwapper swapper) {}

    @Override
    public void reportEvents() {
        Objects.requireNonNull(tracer);
        tracer.pins(pins);
        tracer.faults(faults);
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
    public void pageCopied(long pageRef, long version) {}

    @Override
    public void merge(CursorStatisticSnapshot statisticSnapshot) {}

    private void pageFaulted(long filePageId, PageSwapper swapper) {
        faults++;
        record(new Fault(swapper, filePageId));
    }

    private void pinned(long filePageId, PageSwapper swapper, boolean hit) {
        pins++;
        record(new Pin(swapper, filePageId, hit));
    }

    public static class Fault extends Event {
        private Fault(PageSwapper io, long pageId) {
            super(io, pageId);
        }
    }

    public static class Pin extends Event {
        private final boolean hit;

        private Pin(PageSwapper io, long pageId, boolean hit) {
            super(io, pageId);
            this.hit = hit;
        }

        @Override
        public String toString() {
            return String.format("%s{io=%s, pageId=%s,hit=%s}", getClass().getSimpleName(), io, pageId, hit);
        }
    }
}
