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
package org.neo4j.io.pagecache.tracing;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.neo4j.internal.helpers.MathUtil;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.muninn.swapper.PageSwapper;
import org.neo4j.io.pagecache.tracing.async.AsyncEvictionCompletion;
import org.neo4j.io.pagecache.tracing.async.AsyncEvictionEvent;
import org.neo4j.io.pagecache.tracing.async.AsyncEvictionFailure;
import org.neo4j.io.pagecache.tracing.async.AsyncFlushCompletion;
import org.neo4j.io.pagecache.tracing.async.AsyncFlushFailure;
import org.neo4j.io.pagecache.tracing.async.SubmitEvent;
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

/**
 * The default PageCacheTracer implementation, that just increments counters.
 */
public class DefaultPageCacheTracer implements PageCacheTracer {
    protected final LongAdder faults = new LongAdder();
    protected final LongAdder noFaults = new LongAdder();
    protected final LongAdder failedFaults = new LongAdder();
    protected final LongAdder vectoredFaults = new LongAdder();
    protected final LongAdder failedVectoredFaults = new LongAdder();
    protected final LongAdder noPinFaults = new LongAdder();
    protected final LongAdder evictions = new LongAdder();
    protected final LongAdder cooperativeEvictions = new LongAdder();
    protected final LongAdder pins = new LongAdder();
    protected final LongAdder unpins = new LongAdder();
    protected final LongAdder hits = new LongAdder();
    protected final LongAdder flushes = new LongAdder();
    protected final LongAdder evictionFlushes = new LongAdder();
    protected final LongAdder cooperativeEvictionFlushes = new LongAdder();
    protected final LongAdder merges = new LongAdder();
    protected final LongAdder bytesRead = new LongAdder();
    protected final LongAdder bytesWritten = new LongAdder();
    protected final LongAdder bytesTruncated = new LongAdder();

    protected final LongAdder filesMapped = new LongAdder();
    protected final LongAdder filesUnmapped = new LongAdder();
    protected final LongAdder fileTruncations = new LongAdder();

    protected final LongAdder asyncIoSubmit = new LongAdder();
    protected final LongAdder asyncIoCompleted = new LongAdder();
    protected final LongAdder asyncIoFailed = new LongAdder();

    protected final LongAdder evictionExceptions = new LongAdder();
    protected final LongAdder iopqPerformed = new LongAdder();
    protected final LongAdder globalLimitTimes = new LongAdder();
    protected final LongAdder globalLimitedMillis = new LongAdder();
    protected final LongAdder openedCursors = new LongAdder();
    protected final LongAdder closedCursors = new LongAdder();
    protected final LongAdder copiedPages = new LongAdder();
    protected final LongAdder prefetchedPages = new LongAdder();
    protected final LongAdder prefetchedPagesWithFaults = new LongAdder();
    protected final LongAdder snapshotsLoaded = new LongAdder();
    protected final LongAdder segmentsCreated = new LongAdder();
    protected final LongAdder segmentsLoaded = new LongAdder();
    protected final LongAdder segmentsUnloaded = new LongAdder();
    protected final LongAdder segmentsDeleted = new LongAdder();
    protected final AtomicLong maxPages = new AtomicLong();

    private final boolean tracePageFileIndividually;

    private final EvictionEvent evictionEvent = new PageCacheEvictionEvent();
    private final EvictionRunEvent evictionRunEvent = new DefaultEvictionRunEvent();
    private final AsyncEvictionEvent asyncEvictionEvent = new AsyncPageCacheEvictionEvent();
    private final AsyncEvictionCompletionEvent asyncEvictionCompletion = new AsyncEvictionCompletionEvent();
    private final AsyncEvictionFailure asyncEvictionFailure = new AsyncEvictionFailureEvent();
    private final AsyncPageCacheSubmitEvent asyncPageCacheSubmitEvent = new AsyncPageCacheSubmitEvent();
    private final DatabaseFlushEvent databaseFlushEvent = new DatabaseFlushEvent(
            new DefaultPageCacheFileFlushEvent(), new DefaultAsyncFlushCompletion(), new DefaultAsyncFlushFailure());

    public DefaultPageCacheTracer() {
        this(false);
    }

    public DefaultPageCacheTracer(boolean tracePageFileIndividually) {
        this.tracePageFileIndividually = tracePageFileIndividually;
    }

    @Override
    public PageFileSwapperTracer createFileSwapperTracer() {
        return tracePageFileIndividually ? new DefaultPageFileSwapperTracer() : PageFileSwapperTracer.NULL;
    }

    @Override
    public PageCursorTracer createPageCursorTracer(String tag) {
        return new DefaultPageCursorTracer(this, tag);
    }

    @Override
    public void mappedFile(int swapperId, PagedFile mappedFile) {
        filesMapped.increment();
    }

    @Override
    public void unmappedFile(int swapperId, PagedFile mappedFile) {
        filesUnmapped.increment();
    }

    @Override
    public EvictionRunEvent beginPageEvictions(int pageCountToEvict) {
        return evictionRunEvent;
    }

    @Override
    public EvictionRunEvent beginEviction() {
        return evictionRunEvent;
    }

    @Override
    public AsyncEvictionCompletion asyncEvictionCompletion() {
        return asyncEvictionCompletion;
    }

    @Override
    public AsyncEvictionFailure asyncEvictionFailure() {
        return asyncEvictionFailure;
    }

    @Override
    public FileFlushEvent beginFileFlush(PageSwapper swapper) {
        return new DefaultPageCacheFileFlushEvent();
    }

    @Override
    public FileFlushEvent beginFileFlush() {
        return new DefaultPageCacheFileFlushEvent();
    }

    @Override
    public DatabaseFlushEvent beginDatabaseFlush() {
        return databaseFlushEvent;
    }

    @Override
    public long faults() {
        return faults.sum();
    }

    @Override
    public long failedFaults() {
        return failedFaults.sum();
    }

    @Override
    public long noFaults() {
        return noFaults.sum();
    }

    @Override
    public long vectoredFaults() {
        return vectoredFaults.sum();
    }

    @Override
    public long failedVectoredFaults() {
        return failedVectoredFaults.sum();
    }

    @Override
    public long noPinFaults() {
        return noPinFaults.sum();
    }

    @Override
    public long evictions() {
        return evictions.sum();
    }

    @Override
    public long asyncIoSubmitted() {
        return asyncIoSubmit.sum();
    }

    @Override
    public long asyncIoCompleted() {
        return asyncIoCompleted.sum();
    }

    @Override
    public long asyncIoFailed() {
        return asyncIoFailed.sum();
    }

    @Override
    public long cooperativeEvictions() {
        return cooperativeEvictions.sum();
    }

    @Override
    public long pins() {
        return pins.sum();
    }

    @Override
    public long unpins() {
        return unpins.sum();
    }

    @Override
    public long hits() {
        return hits.sum();
    }

    @Override
    public long flushes() {
        return flushes.sum();
    }

    @Override
    public long evictionFlushes() {
        return evictionFlushes.sum();
    }

    @Override
    public long cooperativeEvictionFlushes() {
        return cooperativeEvictionFlushes.sum();
    }

    @Override
    public long merges() {
        return merges.sum();
    }

    @Override
    public long bytesRead() {
        return bytesRead.sum();
    }

    @Override
    public long bytesWritten() {
        return bytesWritten.sum();
    }

    @Override
    public long bytesTruncated() {
        return bytesTruncated.sum();
    }

    @Override
    public long filesMapped() {
        return filesMapped.sum();
    }

    @Override
    public long filesUnmapped() {
        return filesUnmapped.sum();
    }

    @Override
    public long filesTruncated() {
        return fileTruncations.sum();
    }

    @Override
    public long evictionExceptions() {
        return evictionExceptions.sum();
    }

    @Override
    public double hitRatio() {
        return MathUtil.portion(hits(), faults() - noPinFaults());
    }

    @Override
    public long maxPages() {
        return maxPages.get();
    }

    @Override
    public long iopqPerformed() {
        return iopqPerformed.sum();
    }

    @Override
    public long ioLimitedTimes() {
        return globalLimitTimes.sum();
    }

    @Override
    public long ioLimitedMillis() {
        return globalLimitedMillis.sum();
    }

    @Override
    public long openedCursors() {
        return openedCursors.sum();
    }

    @Override
    public long closedCursors() {
        return closedCursors.sum();
    }

    @Override
    public long copiedPages() {
        return copiedPages.sum();
    }

    @Override
    public long snapshotsLoaded() {
        return snapshotsLoaded.sum();
    }

    @Override
    public long prefetchedPages() {
        return prefetchedPages.sum();
    }

    @Override
    public long prefetchedPagesWithFaults() {
        return prefetchedPagesWithFaults.sum();
    }

    @Override
    public long segmentsCreated() {
        return segmentsCreated.sum();
    }

    @Override
    public long segmentsLoaded() {
        return segmentsLoaded.sum();
    }

    @Override
    public long segmentsUnloaded() {
        return segmentsUnloaded.sum();
    }

    @Override
    public long segmentsDeleted() {
        return segmentsDeleted.sum();
    }

    @Override
    public SegmentEvent createSegment(Path basePath, int segmentIndex) {
        return new CountingSegmentEvent(segmentsCreated);
    }

    @Override
    public SegmentEvent loadSegment(Path basePath, int segmentIndex) {
        return new CountingSegmentEvent(segmentsLoaded);
    }

    @Override
    public SegmentEvent unloadSegment(Path basePath, int segmentIndex) {
        return new CountingSegmentEvent(segmentsUnloaded);
    }

    @Override
    public SegmentEvent deleteSegment(Path basePath, int segmentIndex) {
        return new CountingSegmentEvent(segmentsDeleted);
    }

    private static final class CountingSegmentEvent implements SegmentEvent {
        private final LongAdder counter;

        CountingSegmentEvent(LongAdder counter) {
            this.counter = counter;
        }

        @Override
        public void close() {
            counter.increment();
        }
    }

    @Override
    public void iopq(long iopq) {
        iopqPerformed.add(iopq);
    }

    @Override
    public void limitIO(long millis) {
        globalLimitTimes.increment();
        globalLimitedMillis.add(millis);
    }

    @Override
    public void asyncIoSubmitted(long asyncIoSubmitted) {
        asyncIoSubmit.add(asyncIoSubmitted);
    }

    @Override
    public void asyncIoCompleted(long asyncIoCompleted) {
        this.asyncIoCompleted.add(asyncIoCompleted);
    }

    @Override
    public void asyncIoFailed(long asyncIoFailed) {
        this.asyncIoFailed.add(asyncIoFailed);
    }

    @Override
    public void pagesCopied(long copiesCreated) {
        copiedPages.add(copiesCreated);
    }

    @Override
    public void pagesPrefetched(long count) {
        prefetchedPages.add(count);
    }

    @Override
    public void pagesPrefetchedWithFaults(long count) {
        prefetchedPagesWithFaults.add(count);
    }

    @Override
    public void filesTruncated(long truncatedFiles) {
        this.fileTruncations.add(truncatedFiles);
    }

    @Override
    public void bytesTruncated(long bytesTruncated) {
        this.bytesTruncated.add(bytesTruncated);
    }

    @Override
    public void openedCursors(long openedCursors) {
        this.openedCursors.add(openedCursors);
    }

    @Override
    public void closedCursors(long closedCursors) {
        this.closedCursors.add(closedCursors);
    }

    @Override
    public void failedUnmap(String reason) {}

    @Override
    public void pins(long pins) {
        this.pins.add(pins);
    }

    @Override
    public void unpins(long unpins) {
        this.unpins.add(unpins);
    }

    @Override
    public void hits(long hits) {
        this.hits.add(hits);
    }

    @Override
    public void faults(long faults) {
        this.faults.add(faults);
    }

    @Override
    public void snapshotsLoaded(long snapshotsLoaded) {
        this.snapshotsLoaded.add(snapshotsLoaded);
    }

    @Override
    public void noFaults(long noFaults) {
        this.noFaults.add(noFaults);
    }

    @Override
    public void failedFaults(long failedFaults) {
        this.failedFaults.add(failedFaults);
    }

    @Override
    public void vectoredFaults(long faults) {
        this.vectoredFaults.add(faults);
    }

    @Override
    public void failedVectoredFaults(long failedFaults) {
        this.failedVectoredFaults.add(failedFaults);
    }

    @Override
    public void noPinFaults(long faults) {
        this.noPinFaults.add(faults);
    }

    @Override
    public void bytesRead(long bytesRead) {
        this.bytesRead.add(bytesRead);
    }

    @Override
    public void evictions(long evictions) {
        this.evictions.add(evictions);
    }

    @Override
    public void cooperativeEvictions(long evictions) {
        this.cooperativeEvictions.add(evictions);
    }

    @Override
    public void cooperativeEvictionFlushes(long evictionFlushes) {
        cooperativeEvictionFlushes.add(evictionFlushes);
    }

    @Override
    public void evictionExceptions(long evictionExceptions) {
        this.evictionExceptions.add(evictionExceptions);
    }

    @Override
    public void bytesWritten(long bytesWritten) {
        this.bytesWritten.add(bytesWritten);
    }

    @Override
    public void flushes(long flushes) {
        this.flushes.add(flushes);
    }

    @Override
    public void merges(long merges) {
        this.merges.add(merges);
    }

    @Override
    public void maxPages(long maxPages, long pageSize) {
        this.maxPages.set(maxPages);
    }

    private class AsyncEvictionCompletionEvent implements AsyncEvictionCompletion {

        @Override
        public void addBytesWritten(int bytes, PageFileSwapperTracer swapperTracer) {
            bytesWritten.add(bytes);
            swapperTracer.bytesWritten(bytes);
        }

        @Override
        public void addPagesCompleted(int pageCount, PageFileSwapperTracer swapperTracer) {
            asyncIoCompleted.add(pageCount);
            flushes.add(pageCount);
            swapperTracer.flushes(pageCount);
            evictions.add(pageCount);
            evictionFlushes.add(pageCount);
        }

        @Override
        public void close() {}

        @Override
        public void freeListSize(int size) {}
    }

    private class AsyncEvictionFailureEvent implements AsyncEvictionFailure {

        @Override
        public void close() {
            evictions.increment();
            asyncIoFailed.increment();
            evictionExceptions.increment();
        }
    }

    private class PageCacheFlushEvent implements FlushEvent {
        private PageFileSwapperTracer swapperTracer;
        private long pagesFlushed;
        private final LongAdder localBytesWritten = new LongAdder();

        @Override
        public void addBytesWritten(long bytes) {
            bytesWritten.add(bytes);
            localBytesWritten.add(bytes);
            swapperTracer.bytesWritten(bytes);
        }

        public void reset() {
            pagesFlushed = 0;
        }

        @Override
        public void close() {}

        @Override
        public void setException(IOException exception) {}

        @Override
        public void addPagesFlushed(int pageCount) {
            pagesFlushed += pageCount;
            flushes.add(pageCount);
            swapperTracer.flushes(pageCount);
        }

        @Override
        public void addEvictionFlushedPages(int pageCount) {
            evictionFlushes.add(pageCount);
            this.addPagesFlushed(pageCount);
        }

        @Override
        public void addPagesMerged(int pagesMerged) {
            merges.add(pagesMerged);
            swapperTracer.merges(pagesMerged);
        }

        public long getLocalBytesWritten() {
            return localBytesWritten.longValue();
        }

        public long getPagesFlushed() {
            return pagesFlushed;
        }
    }

    private class DefaultEvictionRunEvent implements EvictionRunEvent {

        @Override
        public void freeListSize(int size) {}

        @Override
        public EvictionEvent beginEviction(long cachePageId) {
            return evictionEvent;
        }

        @Override
        public AsyncEvictionEvent beginAsyncEviction(long cachePageId) {
            return asyncEvictionEvent;
        }

        @Override
        public void close() {}
    }

    public class DefaultPageCacheFileFlushEvent implements FileFlushEvent {

        private final PageCacheFlushEvent flushEvent = new PageCacheFlushEvent();
        private long ioPerformed;
        private long limitTimes;
        private long limitMillis;

        @Override
        public void reset() {
            ioPerformed = 0;
            limitTimes = 0;
            limitMillis = 0;
            flushEvent.reset();
        }

        @Override
        public FlushEvent beginFlush(
                long[] pageRefs,
                PageSwapper swapper,
                PageReferenceTranslator pageReferenceTranslator,
                int pagesToFlush,
                int mergedPages) {
            flushEvent.swapperTracer = swapper.fileSwapperTracer();
            return flushEvent;
        }

        @Override
        public SubmitEvent beginAsyncSubmit() {
            return asyncPageCacheSubmitEvent;
        }

        @Override
        public FlushEvent beginFlush(
                long pageRef, PageSwapper swapper, PageReferenceTranslator pageReferenceTranslator) {
            flushEvent.swapperTracer = swapper.fileSwapperTracer();
            return flushEvent;
        }

        @Override
        public void startFlush(int[][] translationTable) {}

        @Override
        public ChunkEvent startChunk(int[] chunk) {
            return ChunkEvent.NULL;
        }

        @Override
        public void throttle(long recentlyCompletedIOs, long millis) {
            limitTimes++;
            limitMillis += millis;
            globalLimitTimes.add(1);
            globalLimitedMillis.add(millis);
        }

        @Override
        public void reportIO(int completedIOs) {
            ioPerformed += completedIOs;
            iopqPerformed.add(completedIOs);
        }

        @Override
        public long localBytesWritten() {
            return flushEvent.getLocalBytesWritten()
                    + databaseFlushEvent.asyncFlushCompletion().getLocalBytesWritten();
        }

        @Override
        public long ioPerformed() {
            return ioPerformed;
        }

        @Override
        public long limitedNumberOfTimes() {
            return limitTimes;
        }

        @Override
        public long limitedMillis() {
            return limitMillis;
        }

        @Override
        public long pagesFlushed() {
            return flushEvent.getPagesFlushed();
        }

        @Override
        public void close() {}
    }

    private class PageCacheEvictionEvent implements EvictionEvent {

        private PageFileSwapperTracer swapperTracer;
        private final PageCacheFlushEvent flushEvent = new PageCacheFlushEvent();

        @Override
        public void setFilePageId(long filePageId) {}

        @Override
        public void setSwapper(PageSwapper swapper) {
            this.swapperTracer = swapper.fileSwapperTracer();
        }

        @Override
        public FlushEvent beginFlush(
                long pageRef, PageSwapper swapper, PageReferenceTranslator pageReferenceTranslator) {
            flushEvent.swapperTracer = swapper.fileSwapperTracer();
            return flushEvent;
        }

        @Override
        public void setException(IOException exception) {
            evictionExceptions.increment();
            swapperTracer.evictionExceptions(1);
        }

        @Override
        public void close() {
            evictions.increment();
            if (swapperTracer != null) {
                swapperTracer.evictions(1);
            }
        }
    }

    private class AsyncPageCacheSubmitEvent implements SubmitEvent {
        @Override
        public void addSubmittedPages(int pageCount) {
            asyncIoSubmit.add(pageCount);
        }

        @Override
        public void addPagesMerged(int pageCount) {
            merges.add(pageCount);
        }

        @Override
        public void setException(Exception e) {}

        @Override
        public void close() {}
    }

    private class AsyncPageCacheEvictionEvent implements AsyncEvictionEvent {
        private PageFileSwapperTracer swapperTracer;

        @Override
        public void setFilePageId(long filePageId) {}

        @Override
        public void setSwapper(PageSwapper swapper) {
            this.swapperTracer = swapper.fileSwapperTracer();
        }

        @Override
        public void setException(Exception exception) {
            evictionExceptions.increment();
            swapperTracer.evictionExceptions(1);
        }

        @Override
        public SubmitEvent beginAsyncSubmit(
                long pageRef, PageSwapper swapper, PageReferenceTranslator pageReferenceTranslator) {
            return asyncPageCacheSubmitEvent;
        }

        @Override
        public void evicted() {
            evictions.increment();
        }

        @Override
        public void close() {}
    }

    private class DefaultAsyncFlushCompletion implements AsyncFlushCompletion {

        private long pagesFlushed;
        private long ioPerformed;
        private final LongAdder localBytesWritten = new LongAdder();

        @Override
        public void addBytesWritten(int bytes) {
            bytesWritten.add(bytes);
            localBytesWritten.add(bytes);
        }

        @Override
        public void addPagesCompleted(int pageCount) {
            asyncIoCompleted.add(pageCount);
            pagesFlushed += pageCount;
            flushes.add(pageCount);
        }

        @Override
        public void reportIO(int completedIOs) {
            ioPerformed += completedIOs;
            iopqPerformed.add(completedIOs);
        }

        @Override
        public void reset() {
            pagesFlushed = 0;
            ioPerformed = 0;
            localBytesWritten.reset();
        }

        @Override
        public long pagesFlushed() {
            return pagesFlushed;
        }

        @Override
        public long ioPerformed() {
            return ioPerformed;
        }

        @Override
        public long getLocalBytesWritten() {
            return localBytesWritten.longValue();
        }

        @Override
        public void close() {}
    }

    private class DefaultAsyncFlushFailure implements AsyncFlushFailure {
        private long iosPerformed;

        @Override
        public void addPagesFailed(int pageCount) {
            asyncIoFailed.add(pageCount);
        }

        @Override
        public void reportIO(int completedIOs) {
            iosPerformed += completedIOs;
            iopqPerformed.add(completedIOs);
        }

        @Override
        public void reset() {
            this.iosPerformed = 0;
        }

        @Override
        public long ioPerformed() {
            return iosPerformed;
        }

        @Override
        public void close() {}
    }
}
