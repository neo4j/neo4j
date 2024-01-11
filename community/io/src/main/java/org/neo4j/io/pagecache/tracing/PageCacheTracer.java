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

import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.monitoring.PageCacheCounters;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

/**
 * A PageCacheTracer receives a steady stream of events and data about what
 * whole page cache is doing. Implementations of this interface should be as
 * efficient as possible, lest they severely slow down the page cache.
 */
public interface PageCacheTracer extends PageCacheCounters {
    /**
     * A PageCacheTracer that does nothing other than return the NULL variants of the companion interfaces.
     */
    PageCacheTracer NULL = new PageCacheTracer() {
        @Override
        public PageFileSwapperTracer createFileSwapperTracer() {
            return PageFileSwapperTracer.NULL;
        }

        @Override
        public PageCursorTracer createPageCursorTracer(String tag) {
            return PageCursorTracer.NULL;
        }

        @Override
        public void mappedFile(int swapperId, PagedFile pagedFile) {}

        @Override
        public void unmappedFile(int swapperId, PagedFile pagedFile) {}

        @Override
        public EvictionRunEvent beginPageEvictions(int pageCountToEvict) {
            return EvictionRunEvent.NULL;
        }

        @Override
        public EvictionRunEvent beginEviction() {
            return EvictionRunEvent.NULL;
        }

        @Override
        public FileFlushEvent beginFileFlush(PageSwapper swapper) {
            return FileFlushEvent.NULL;
        }

        @Override
        public FileFlushEvent beginFileFlush() {
            return FileFlushEvent.NULL;
        }

        @Override
        public DatabaseFlushEvent beginDatabaseFlush() {
            return DatabaseFlushEvent.NULL;
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
        public long evictions() {
            return 0;
        }

        @Override
        public long cooperativeEvictions() {
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
        public long flushes() {
            return 0;
        }

        @Override
        public long evictionFlushes() {
            return 0;
        }

        @Override
        public long cooperativeEvictionFlushes() {
            return 0;
        }

        @Override
        public long merges() {
            return 0;
        }

        @Override
        public long bytesRead() {
            return 0;
        }

        @Override
        public long bytesWritten() {
            return 0;
        }

        @Override
        public long bytesTruncated() {
            return 0;
        }

        @Override
        public long filesMapped() {
            return 0;
        }

        @Override
        public long filesUnmapped() {
            return 0;
        }

        @Override
        public long filesTruncated() {
            return 0;
        }

        @Override
        public long evictionExceptions() {
            return 0;
        }

        @Override
        public double hitRatio() {
            return 0d;
        }

        @Override
        public double usageRatio() {
            return 0d;
        }

        @Override
        public long maxPages() {
            return 0;
        }

        @Override
        public long iopqPerformed() {
            return 0;
        }

        @Override
        public long ioLimitedTimes() {
            return 0;
        }

        @Override
        public long ioLimitedMillis() {
            return 0;
        }

        @Override
        public long openedCursors() {
            return 0;
        }

        @Override
        public long closedCursors() {
            return 0;
        }

        @Override
        public long copiedPages() {
            return 0;
        }

        @Override
        public long snapshotsLoaded() {
            return 0;
        }

        @Override
        public void pins(long pins) {}

        @Override
        public void unpins(long unpins) {}

        @Override
        public void hits(long hits) {}

        @Override
        public void faults(long faults) {}

        @Override
        public void noFaults(long noFaults) {}

        @Override
        public void failedFaults(long failedFaults) {}

        @Override
        public void vectoredFaults(long faults) {}

        @Override
        public void failedVectoredFaults(long failedFaults) {}

        @Override
        public void noPinFaults(long faults) {}

        @Override
        public void bytesRead(long bytesRead) {}

        @Override
        public void evictions(long evictions) {}

        @Override
        public void cooperativeEvictions(long evictions) {}

        @Override
        public void cooperativeEvictionFlushes(long evictionFlushes) {}

        @Override
        public void evictionExceptions(long evictionExceptions) {}

        @Override
        public void bytesWritten(long bytesWritten) {}

        @Override
        public void flushes(long flushes) {}

        @Override
        public void merges(long merges) {}

        @Override
        public void snapshotsLoaded(long snapshotsLoaded) {}

        @Override
        public void maxPages(long maxPages, long pageSize) {}

        @Override
        public void iopq(long iopq) {}

        @Override
        public void limitIO(long millis) {}

        @Override
        public void pagesCopied(long copiesCreated) {}

        @Override
        public void filesTruncated(long truncatedFiles) {}

        @Override
        public void bytesTruncated(long bytesTruncated) {}

        @Override
        public void openedCursors(long openedCursors) {}

        @Override
        public void closedCursors(long closedCursors) {}

        @Override
        public String toString() {
            return PageCacheTracer.class.getName() + ".NULL";
        }
    };

    /**
     * Create page file tracer for underlying page file.
     * Every new mapped page file will call this method to create a new instance of tracer.
     *
     * @return page file tracer
     */
    PageFileSwapperTracer createFileSwapperTracer();

    /**
     * Create page cursor tracer for underlying page cache with a specific tag.
     * @param tag specific tag of underlying cursor tracer
     * @return page cursor tracer.
     */
    PageCursorTracer createPageCursorTracer(String tag);

    /**
     * The given file has been mapped, where no existing mapping for that file existed.
     */
    void mappedFile(int swapperId, PagedFile pagedFile);

    /**
     * The last reference to the given file has been unmapped.
     */
    void unmappedFile(int swapperId, PagedFile pagedFile);

    /**
     * A background eviction has begun. Called from the background eviction thread.
     *
     * The method returns an EvictionRunEvent to represent the event of this eviction run.
     **/
    EvictionRunEvent beginPageEvictions(int pageCountToEvict);

    /**
     * Start of vacuum eviction event to cleanup unknown number of pages for obsolete swappers
     * @return an EvictionRunEvent to represent the event of this eviction run.
     */
    EvictionRunEvent beginEviction();

    /**
     * A PagedFile wants to flush all its bound pages.
     */
    FileFlushEvent beginFileFlush(PageSwapper swapper);

    /**
     * The PageCache wants to flush file bound pages.
     */
    FileFlushEvent beginFileFlush();

    /**
     * Start database flush event
     */
    DatabaseFlushEvent beginDatabaseFlush();

    /**
     * Report number of observed pins
     * @param pins number of pins
     */
    void pins(long pins);

    /**
     * Report number of observed unpins
     * @param unpins number of unpins
     */
    void unpins(long unpins);

    /**
     * Report number of observer hits
     * @param hits number of hits
     */
    void hits(long hits);

    /**
     * Report number of observed fault attempts
     * @param faults number of fault attempts
     */
    void faults(long faults);

    /**
     * Report number of observed no-faults
     * @param noFaults number of no-faults
     */
    void noFaults(long noFaults);

    /**
     * Report number of observed failed faults
     * @param failedFaults number of failed faults
     */
    void failedFaults(long failedFaults);

    /**
     * Report number of observed vectored fault attempts
     * @param faults number of vectored fault attempts
     */
    void vectoredFaults(long faults);

    /**
     * Report number of observed failed vectored faults
     * @param failedFaults number of failed vectored faults
     */
    void failedVectoredFaults(long failedFaults);

    /**
     * Report number of observed faults not caused by pins
     * @param faults number faults
     */
    void noPinFaults(long faults);

    /**
     * Report number of bytes read
     * @param bytesRead number of read bytes
     */
    void bytesRead(long bytesRead);

    /**
     * Report number of observed evictions
     * @param evictions number of evictions
     */
    void evictions(long evictions);

    /**
     * Report number of observed cooperative evictions
     * @param evictions number of cooperative evictions
     */
    void cooperativeEvictions(long evictions);

    /**
     * Report number of flushes caused by cooperative evictions
     * @param evictionFlushes number of cooperative eviction flushes
     */
    void cooperativeEvictionFlushes(long evictionFlushes);

    /**
     * Report number of eviction exceptions
     * @param evictionExceptions number of eviction exceptions
     */
    void evictionExceptions(long evictionExceptions);

    /**
     * Report number of bytes written
     * @param bytesWritten number of written bytes
     */
    void bytesWritten(long bytesWritten);

    /**
     * Report number of flushes
     * @param flushes number of flushes
     */
    void flushes(long flushes);

    /**
     * Report number of merges
     * @param merges number of merges
     */
    void merges(long merges);

    /**
     * Sets the number of available pages.
     * @param maxPages the total number of available pages.
     * @param pageSize size of page
     */
    void maxPages(long maxPages, long pageSize);

    /**
     * Report number of performed iopq.
     * @param iopq number of performed io operations per quantum of time.
     */
    void iopq(long iopq);

    /**
     * Report io throttling by io limiter.
     * @param millis number of millisecond io should be blocked.
     */
    void limitIO(long millis);

    /**
     * report number of loaded page old snapshots
     */
    void snapshotsLoaded(long snapshotsLoaded);

    /**
     * report number of copied pages
     */
    void pagesCopied(long copiesCreated);

    /**
     * report number of truncated files
     */
    void filesTruncated(long truncatedFiles);

    /**
     * report number of truncated bytes
     */
    void bytesTruncated(long bytesTruncated);

    /**
     * report number of opened page cache cursors.
     */
    void openedCursors(long openedCursors);

    /**
     * report number of closed page cache cursors.
     */
    void closedCursors(long closedCursors);
}
