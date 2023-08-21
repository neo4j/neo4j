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

/**
 * Please note that this event is not thread safe since we only can have one database flush in a time.
 */
public class DatabaseFlushEvent implements AutoCloseablePageCacheTracerEvent {

    public static final DatabaseFlushEvent NULL = new DatabaseFlushEvent(FileFlushEvent.NULL) {};

    private long pagesFlushed;
    private long ioPerformed;
    private long ioLimit;
    private long timesLimited;
    private long millisLimited;
    private final FileFlushEvent flushEvent;

    public DatabaseFlushEvent(FileFlushEvent flushEvent) {
        this.flushEvent = flushEvent;
    }

    public void reset() {
        pagesFlushed = 0;
        ioPerformed = 0;
        timesLimited = 0;
        millisLimited = 0;
        ioLimit = 0;
        flushEvent.reset();
    }

    @Override
    public void close() {
        pagesFlushed += flushEvent.pagesFlushed();
        ioPerformed += flushEvent.ioPerformed();
        timesLimited += flushEvent.limitedNumberOfTimes();
        millisLimited += flushEvent.limitedMillis();
    }

    public FileFlushEvent beginFileFlush(PageSwapper swapper) {
        return flushEvent;
    }

    public FileFlushEvent beginFileFlush() {
        return flushEvent;
    }

    public FileFlushEvent getFlushEvent() {
        return flushEvent;
    }

    public long pagesFlushed() {
        return pagesFlushed;
    }

    public long ioPerformed() {
        return ioPerformed;
    }

    public long getIoLimit() {
        return ioLimit;
    }

    public long getTimesLimited() {
        return timesLimited;
    }

    public long getMillisLimited() {
        return millisLimited;
    }

    public void ioControllerLimit(long configuredLimit) {
        ioLimit = configuredLimit;
    }
}
