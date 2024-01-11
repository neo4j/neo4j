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

import org.neo4j.io.pagecache.context.CursorContext;

/**
 * Expose information from page cache about particular page cursor events.
 *
 * Exposed counters are relevant only for particular cursor and do not represent global page cache wide picture.
 *
 * @see PageCursorCounters
 * @see CursorContext
 */
public interface PageCursorCounters {
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
     * One vectored fault can represent faults of many pages, those pages reported as part of regular faults.
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
     * @return The number of page pins observed thus far.
     */
    long pins();

    /**
     * @return The number of page unpins observed thus far.
     */
    long unpins();

    /**
     * @return The number of page hits observed so far.
     */
    long hits();

    /**
     * @return The sum total of bytes read in through page faults thus far.
     */
    long bytesRead();

    /**
     * @return The number of page evictions observed thus far.
     */
    long evictions();

    /**
     * @return The number of page flushes caused by evictions.
     */
    long evictionFlushes();

    /**
     * @return The number of page evictions that have thrown exceptions thus far.
     */
    long evictionExceptions();

    /**
     * @return The sum total of bytes written through flushes thus far.
     */
    long bytesWritten();

    /**
     * @return The number of page flushes observed thus far.
     */
    long flushes();

    /**
     * @return The number of page merges observed thus far.
     */
    long merges();

    /**
     * @return The hit ratio observed thus far.
     */
    double hitRatio();

    long snapshotsLoaded();

    long copiedPages();
}
