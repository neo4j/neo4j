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
package org.neo4j.io.pagecache.tracing.cursor;

import static org.neo4j.util.FeatureToggles.flag;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.internal.helpers.MathUtil;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.tracing.EvictionEvent;
import org.neo4j.io.pagecache.tracing.FlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageFileSwapperTracer;
import org.neo4j.io.pagecache.tracing.PageReferenceTranslator;
import org.neo4j.io.pagecache.tracing.PinEvent;
import org.neo4j.io.pagecache.tracing.PinPageFaultEvent;
import org.neo4j.io.pagecache.tracing.VectoredPageFaultEvent;

public class DefaultPageCursorTracer implements PageCursorTracer {
    /**
     * On encountering a mismatching counts check error in a test this debugging is useful to trace down which exact pin it's about.
     * Just flip DEBUG_PINS = true.
     */
    private static final boolean DEBUG_PINS = false;

    private static final ConcurrentMap<Pair<Path, Long>, Exception> PIN_DEBUG_MAP =
            DEBUG_PINS ? new ConcurrentHashMap<>() : null;

    private static final boolean CHECK_REPORTED_COUNTERS =
            flag(DefaultPageCursorTracer.class, "CHECK_REPORTED_COUNTERS", false);

    private long pins;
    private long unpins;
    private long hits;
    private long faults;
    private long noFaults;
    private long failedFaults;
    private long vectoredFaults;
    private long failedVectoredFaults;
    private long noPinFaults;
    private long bytesRead;
    private long bytesWritten;
    private long evictions;
    private long evictionFlushes;
    private long evictionExceptions;
    private long flushes;
    private long merges;
    private long snapshotsLoaded;
    private long copiesCreated;
    private long openedCursors;
    private long closedCursors;

    private final DefaultPinEvent pinTracingEvent = new DefaultPinEvent();
    private final PageFaultEvictionEvent evictionEvent = new PageFaultEvictionEvent();
    private final DefaultPinPageFaultEvent pageFaultEvent = new DefaultPinPageFaultEvent();
    private final DefaultVectoredPageFaultEvent vectoredPageFaultEvent = new DefaultVectoredPageFaultEvent();
    private final DefaultFlushEvent flushEvent = new DefaultFlushEvent();

    private final PageCacheTracer pageCacheTracer;
    private final String tag;
    private boolean ignoreCounterCheck;

    public DefaultPageCursorTracer(PageCacheTracer pageCacheTracer, String tag) {
        this.pageCacheTracer = pageCacheTracer;
        this.tag = tag;
    }

    @Override
    public String getTag() {
        return tag;
    }

    @Override
    public void openCursor() {
        openedCursors += 1;
    }

    @Override
    public long openedCursors() {
        return openedCursors;
    }

    @Override
    public void closeCursor() {
        closedCursors += 1;
    }

    @Override
    public long closedCursors() {
        return closedCursors;
    }

    @Override
    public void merge(CursorStatisticSnapshot statisticSnapshot) {
        this.pins += statisticSnapshot.pins();
        this.unpins += statisticSnapshot.unpins();
        this.hits += statisticSnapshot.hits();
        this.faults += statisticSnapshot.faults();
        this.noFaults += statisticSnapshot.noFaults();
        this.failedFaults += statisticSnapshot.failedFaults();
        this.vectoredFaults += statisticSnapshot.vectoredFaults();
        this.failedVectoredFaults += statisticSnapshot.failedVectoredFaults();
        this.noPinFaults += statisticSnapshot.noPinFaults();
        this.bytesRead += statisticSnapshot.bytesRead();
        this.bytesWritten += statisticSnapshot.bytesWritten();
        this.evictions += statisticSnapshot.evictions();
        this.evictionFlushes += statisticSnapshot.evictionFlushes();
        this.evictionExceptions += statisticSnapshot.evictionExceptions();
        this.flushes += statisticSnapshot.flushes();
        this.merges += statisticSnapshot.merges();
        this.snapshotsLoaded += statisticSnapshot.snapshotsLoaded();
        this.copiesCreated += statisticSnapshot.copiedPages();
        this.openedCursors += statisticSnapshot.openedCursors();
        this.closedCursors += statisticSnapshot.closedCursors();
    }

    @Override
    public void pageCopied(long pageRef, long version) {
        copiesCreated++;
    }

    // When updating reporting here please check if that affects any reporting on additional available tracers
    @Override
    public void reportEvents() {
        if (CHECK_REPORTED_COUNTERS && !ignoreCounterCheck) {
            checkCounters();
        }
        if (pins > 0) {
            pageCacheTracer.pins(pins);
        }
        if (unpins > 0) {
            pageCacheTracer.unpins(unpins);
        }
        if (hits > 0) {
            pageCacheTracer.hits(hits);
        }
        if (faults > 0) {
            pageCacheTracer.faults(faults);
        }
        if (noFaults > 0) {
            pageCacheTracer.noFaults(noFaults);
        }
        if (failedFaults > 0) {
            pageCacheTracer.failedFaults(failedFaults);
        }
        if (vectoredFaults > 0) {
            pageCacheTracer.vectoredFaults(vectoredFaults);
        }
        if (failedVectoredFaults > 0) {
            pageCacheTracer.failedVectoredFaults(vectoredFaults);
        }
        if (noPinFaults > 0) {
            pageCacheTracer.noPinFaults(noPinFaults);
        }
        if (bytesRead > 0) {
            pageCacheTracer.bytesRead(bytesRead);
        }
        if (evictions > 0) {
            pageCacheTracer.evictions(evictions);
            // all evictions counted by PageCursorTracer are cooperative
            pageCacheTracer.cooperativeEvictions(evictions);
        }
        if (evictionFlushes > 0) {
            pageCacheTracer.cooperativeEvictionFlushes(evictionFlushes);
        }
        if (evictionExceptions > 0) {
            pageCacheTracer.evictionExceptions(evictionExceptions);
        }
        if (bytesWritten > 0) {
            pageCacheTracer.bytesWritten(bytesWritten);
        }
        if (flushes > 0) {
            pageCacheTracer.flushes(flushes);
        }
        if (merges > 0) {
            pageCacheTracer.merges(merges);
        }
        if (snapshotsLoaded > 0) {
            pageCacheTracer.snapshotsLoaded(snapshotsLoaded);
        }
        if (copiesCreated > 0) {
            pageCacheTracer.pagesCopied(copiesCreated);
        }
        if (openedCursors > 0) {
            pageCacheTracer.openedCursors(openedCursors);
        }
        if (closedCursors > 0) {
            pageCacheTracer.closedCursors(closedCursors);
        }
        reset();
    }

    private void checkCounters() {
        boolean pinsInvariant = pins == hits + faults + noFaults - noPinFaults;
        boolean unpinsInvariant = unpins == hits + faults - failedFaults - noPinFaults;
        if (!(pinsInvariant && unpinsInvariant)) {
            throw new RuntimeException("Mismatch cursor counters. " + this);
        }
    }

    @Override
    public String toString() {
        return "PageCursorTracer{" + "pins=" + pins + ", unpins=" + unpins + ", hits=" + hits + ", faults=" + faults
                + ", noFaults=" + noFaults + ", failedFaults=" + failedFaults
                + ", vectoredFaults=" + vectoredFaults + ", failedVectoredFaults=" + failedVectoredFaults
                + ", noPinFaults=" + noPinFaults
                + ", bytesRead=" + bytesRead + ", bytesWritten="
                + bytesWritten + ", evictions=" + evictions + ", evictionExceptions=" + evictionExceptions
                + ", flushes=" + flushes + ", merges="
                + merges + ", tag='" + tag + '\'' + (DEBUG_PINS ? ", current (yet unpinned) pins:" + currentPins() : "")
                + '}';
    }

    private String currentPins() {
        assert DEBUG_PINS;
        ByteArrayOutputStream byteArrayOut = new ByteArrayOutputStream();
        try (PrintStream out = new PrintStream(byteArrayOut)) {
            PIN_DEBUG_MAP.forEach((pin, stackTrace) -> {
                out.println();
                stackTrace.printStackTrace(out);
            });
        }
        return byteArrayOut.toString();
    }

    protected void reset() {
        pins = 0;
        unpins = 0;
        hits = 0;
        faults = 0;
        noFaults = 0;
        failedFaults = 0;
        vectoredFaults = 0;
        failedVectoredFaults = 0;
        noPinFaults = 0;
        bytesRead = 0;
        bytesWritten = 0;
        evictions = 0;
        evictionFlushes = 0;
        evictionExceptions = 0;
        flushes = 0;
        merges = 0;
        snapshotsLoaded = 0;
        copiesCreated = 0;
        openedCursors = 0;
        closedCursors = 0;
    }

    @Override
    public long faults() {
        return faults;
    }

    @Override
    public long failedFaults() {
        return failedFaults;
    }

    @Override
    public long noFaults() {
        return noFaults;
    }

    @Override
    public long vectoredFaults() {
        return vectoredFaults;
    }

    @Override
    public long failedVectoredFaults() {
        return failedVectoredFaults;
    }

    @Override
    public long noPinFaults() {
        return noPinFaults;
    }

    @Override
    public long pins() {
        return pins;
    }

    @Override
    public long unpins() {
        return unpins;
    }

    @Override
    public long hits() {
        return hits;
    }

    @Override
    public long bytesRead() {
        return bytesRead;
    }

    @Override
    public long evictions() {
        return evictions;
    }

    @Override
    public long evictionFlushes() {
        return evictionFlushes;
    }

    @Override
    public long evictionExceptions() {
        return evictionExceptions;
    }

    @Override
    public long bytesWritten() {
        return bytesWritten;
    }

    @Override
    public long flushes() {
        return flushes;
    }

    @Override
    public long merges() {
        return merges;
    }

    @Override
    public long snapshotsLoaded() {
        return snapshotsLoaded;
    }

    @Override
    public double hitRatio() {
        return MathUtil.portion(hits(), faults());
    }

    @Override
    public long copiedPages() {
        return copiesCreated;
    }

    @Override
    public PinEvent beginPin(boolean writeLock, long filePageId, PageSwapper swapper) {
        if (DEBUG_PINS) {
            PIN_DEBUG_MAP.put(Pair.of(swapper.path(), filePageId), new Exception());
        }
        pinTracingEvent.swapperTracer = swapper.fileSwapperTracer();
        return pinTracingEvent;
    }

    @Override
    public void unpin(long filePageId, PageSwapper swapper) {
        unpins++;
        swapper.fileSwapperTracer().unpins(1);
        if (DEBUG_PINS) {
            PIN_DEBUG_MAP.remove(Pair.of(swapper.path(), filePageId));
        }
    }

    @Override
    public VectoredPageFaultEvent beginVectoredPageFault(PageSwapper pageSwapper) {
        vectoredPageFaultEvent.swapperTracer = pageSwapper.fileSwapperTracer();
        return vectoredPageFaultEvent;
    }

    public void setIgnoreCounterCheck(boolean ignoreCounterCheck) {
        this.ignoreCounterCheck = ignoreCounterCheck;
    }

    private class DefaultPinEvent implements PinEvent {
        private PageFileSwapperTracer swapperTracer;

        @Override
        public void setCachePageId(long cachePageId) {}

        @Override
        public PinPageFaultEvent beginPageFault(long filePageId, PageSwapper pageSwapper) {
            pageFaultEvent.swapperTracer = pageSwapper.fileSwapperTracer();
            return pageFaultEvent;
        }

        @Override
        public void hit() {
            hits++;
            swapperTracer.hits(1);
        }

        @Override
        public void noFault() {
            noFaults++;
            swapperTracer.noFaults(1);
        }

        @Override
        public void close() {
            pins++;
            swapperTracer.pins(1);
        }

        @Override
        public void snapshotsLoaded(int oldSnapshotsLoaded) {
            snapshotsLoaded += oldSnapshotsLoaded;
        }
    }

    private class DefaultPinPageFaultEvent implements PinPageFaultEvent {
        private PageFileSwapperTracer swapperTracer;

        @Override
        public void addBytesRead(long bytes) {
            bytesRead += bytes;
            swapperTracer.bytesRead(bytesRead);
        }

        @Override
        public void close() {
            faults++;
            swapperTracer.faults(1);
        }

        @Override
        public void setException(Throwable throwable) {
            failedFaults++;
            swapperTracer.failedFaults(1);
        }

        @Override
        public void freeListSize(int listSize) {}

        @Override
        public EvictionEvent beginEviction(long cachePageId) {
            return evictionEvent;
        }

        @Override
        public void setCachePageId(long cachePageId) {}
    }

    private class DefaultVectoredPageFaultEvent implements VectoredPageFaultEvent {
        private PageFileSwapperTracer swapperTracer;

        @Override
        public void addBytesRead(long bytes) {
            bytesRead += bytes;
            swapperTracer.bytesRead(bytesRead);
        }

        @Override
        public void close() {
            vectoredFaults++;
            swapperTracer.vectoredFaults(1);
        }

        @Override
        public void setException(Throwable throwable) {
            failedVectoredFaults++;
            swapperTracer.failedVectoredFaults(1);
        }

        @Override
        public void freeListSize(int listSize) {}

        @Override
        public EvictionEvent beginEviction(long cachePageId) {
            return evictionEvent;
        }

        @Override
        public void addPagesFaulted(int numberOfPages, long[] pageRefs, PageReferenceTranslator referenceTranslator) {
            // we track page faults happened as part of vectored fault separately at cursor level, so we can check
            // pin-hit-fault invariants
            noPinFaults += numberOfPages;
            faults += numberOfPages;
            swapperTracer.faults(numberOfPages);
        }
    }

    private class DefaultFlushEvent implements FlushEvent {
        private PageFileSwapperTracer swapperTracer;

        @Override
        public void addBytesWritten(long bytes) {
            bytesWritten += bytes;
            swapperTracer.bytesWritten(bytesWritten);
        }

        @Override
        public void close() {}

        @Override
        public void setException(IOException exception) {}

        @Override
        public void addPagesFlushed(int flushedPages) {
            flushes += flushedPages;
            swapperTracer.flushes(flushedPages);
        }

        @Override
        public void addEvictionFlushedPages(int pageCount) {
            evictionFlushes += pageCount;
            addPagesFlushed(pageCount);
        }

        @Override
        public void addPagesMerged(int pagesMerged) {
            merges += pagesMerged;
            swapperTracer.merges(pagesMerged);
        }
    }

    private class PageFaultEvictionEvent implements EvictionEvent {
        private PageFileSwapperTracer swapperTracer;

        @Override
        public void setFilePageId(long filePageId) {}

        @Override
        public void setSwapper(PageSwapper swapper) {
            swapperTracer = swapper.fileSwapperTracer();
        }

        @Override
        public FlushEvent beginFlush(
                long pageRef, PageSwapper swapper, PageReferenceTranslator pageReferenceTranslator) {
            flushEvent.swapperTracer = swapper.fileSwapperTracer();
            return flushEvent;
        }

        @Override
        public void setException(IOException exception) {
            evictionExceptions++;
            swapperTracer.evictionExceptions(1);
        }

        @Override
        public void close() {
            evictions++;
            // it can be the case that we fail to do eviction since file was not there anymore, but we still were the
            // one who actually cleared page binding
            if (swapperTracer != null) {
                swapperTracer.evictions(1);
            }
        }
    }
}
