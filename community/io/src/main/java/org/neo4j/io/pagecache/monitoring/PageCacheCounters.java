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
package org.neo4j.io.pagecache.monitoring;

/**
 * The PageCacheCounters exposes internal counters from the page cache.
 * The data for these counters is sourced through the PageCacheTracer API.
 */
public interface PageCacheCounters {
    /**
     * @return The number of page faults observed thus far, both failed and successful.
     */
    long faults();

    /**
     * @return The number of failed page faults observed thus far.
     */
    long failedFaults();

    /**
     * @return The number of no-faults observed thus far.
     */
    long noFaults();

    /**
     * @return The number of vectored faults observed thus far, both failed and successful.
     * One vectored fault can represent faults of many pages.
     */
    long vectoredFaults();

    /**
     * @return The number of failed vectored faults observed thus far.
     */
    long failedVectoredFaults();

    /**
     * @return The number of page faults not caused by pins
     */
    long noPinFaults();

    /**
     * @return The number of page evictions observed thus far.
     */
    long evictions();

    /**
     * @return The number of cooperative page evictions observed thus far.
     */
    long cooperativeEvictions();

    /**
     * @return The number of page pins observed thus far.
     */
    long pins();

    /**
     * @return The number of page unpins observed thus far.
     */
    long unpins();

    /**
     * @return The number of page cache hits so far.
     */
    long hits();

    /**
     * @return The number of page flushes observed thus far.
     */
    long flushes();

    /**
     * @return Number of flushes performed by page evictions
     */
    long evictionFlushes();

    /**
     * @return Number of flushes performed by cooperation evictions
     */
    long cooperativeEvictionFlushes();

    /**
     * @return The number of page merges observed so far
     */
    long merges();

    /**
     * @return The sum total of bytes read in through page faults thus far.
     */
    long bytesRead();

    /**
     * @return The sum total of bytes written through flushes thus far.
     */
    long bytesWritten();

    /**
     * @return The sum total of bytes truncated through truncate thus far.
     */
    long bytesTruncated();

    /**
     * @return The number of file mappings observed thus far.
     */
    long filesMapped();

    /**
     * @return The number of file unmappings observed thus far.
     */
    long filesUnmapped();

    /**
     * @return The number truncate operations so far.
     */
    long filesTruncated();

    /**
     * @return The number of page evictions that have thrown exceptions thus far.
     */
    long evictionExceptions();

    /**
     * @return The cache hit ratio observed thus far.
     */
    double hitRatio();

    /**
     * @return The current usage ration of number of used pages to the total number of pages or {@code 0} if it cannot
     * be determined.
     */
    double usageRatio();

    /**
     * Return max number of page cache pages. 0 if number is not available.
     */
    long maxPages();

    /**
     * @return The number of IOPQ performed thus far.
     */
    long iopqPerformed();

    /**
     * @return The number of times page cache io was throttled by io limiter thus far.
     */
    long ioLimitedTimes();

    /**
     * @return The number of millis page cache io was throttled by io limiter thus far.
     */
    long ioLimitedMillis();

    /**
     * @return Total number of opened page cache cursors.
     */
    long openedCursors();

    /**
     *
     * @return Total number of closed page cache cursors.
     */
    long closedCursors();

    long copiedPages();

    long snapshotsLoaded();
}
