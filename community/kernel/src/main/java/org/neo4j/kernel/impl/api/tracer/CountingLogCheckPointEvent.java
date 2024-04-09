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
package org.neo4j.kernel.impl.api.tracer;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.neo4j.io.pagecache.tracing.DatabaseFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceWaitEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogRotateEvent;

/**
 * Log checkpoint event that counts number of checkpoint that occurred and amount of time elapsed
 * for all of them and for the last one.
 */
class CountingLogCheckPointEvent implements LogCheckPointEvent {
    private final AtomicLong checkpointCounter = new AtomicLong();
    private final AtomicLong accumulatedCheckpointTotalTimeMillis = new AtomicLong();
    private final long maxPages;
    private final LongAdder appendedBytes;
    private final CountingLogRotateEvent countingLogRotateEvent;
    private volatile LastCheckpointInfo lastCheckpointInfo = new LastCheckpointInfo(0, 0, 0, 0, 0, 0);
    private final DatabaseFlushEvent databaseFlushEvent;

    CountingLogCheckPointEvent(
            PageCacheTracer pageCacheTracer, LongAdder appendedBytes, CountingLogRotateEvent countingLogRotateEvent) {
        this.maxPages = pageCacheTracer.maxPages();
        this.appendedBytes = appendedBytes;
        this.countingLogRotateEvent = countingLogRotateEvent;
        this.databaseFlushEvent = pageCacheTracer.beginDatabaseFlush();
    }

    @Override
    public void checkpointCompleted(long checkpointMillis) {
        checkpointCounter.incrementAndGet();
        accumulatedCheckpointTotalTimeMillis.addAndGet(checkpointMillis);
        lastCheckpointInfo = new LastCheckpointInfo(
                checkpointMillis,
                databaseFlushEvent.pagesFlushed(),
                databaseFlushEvent.ioPerformed(),
                databaseFlushEvent.getIoLimit(),
                databaseFlushEvent.getTimesLimited(),
                databaseFlushEvent.getMillisLimited());
    }

    @Override
    public void close() {
        // empty
    }

    @Override
    public void appendedBytes(long bytes) {
        appendedBytes.add(bytes);
    }

    @Override
    public DatabaseFlushEvent beginDatabaseFlush() {
        databaseFlushEvent.reset();
        return databaseFlushEvent;
    }

    @Override
    public long getPagesFlushed() {
        return lastCheckpointInfo.pagesFlushed();
    }

    @Override
    public long getIOsPerformed() {
        return lastCheckpointInfo.performedIO();
    }

    @Override
    public long getTimesPaused() {
        return lastCheckpointInfo.timesPaused();
    }

    @Override
    public long getMillisPaused() {
        return lastCheckpointInfo.millisPaused();
    }

    @Override
    public long getConfiguredIOLimit() {
        return lastCheckpointInfo.ioLimit();
    }

    @Override
    public double flushRatio() {
        if (maxPages == 0) {
            return 0;
        }
        return ((double) lastCheckpointInfo.pagesFlushed()) / maxPages;
    }

    @Override
    public LogForceWaitEvent beginLogForceWait() {
        return LogForceWaitEvent.NULL;
    }

    @Override
    public LogForceEvent beginLogForce() {
        return LogForceEvent.NULL;
    }

    long numberOfCheckPoints() {
        return checkpointCounter.get();
    }

    long checkPointAccumulatedTotalTimeMillis() {
        return accumulatedCheckpointTotalTimeMillis.get();
    }

    long lastCheckpointTimeMillis() {
        return lastCheckpointInfo.timeMillis();
    }

    @Override
    public LogRotateEvent beginLogRotate() {
        return countingLogRotateEvent;
    }

    public long flushedBytes() {
        return databaseFlushEvent.getFlushEvent().localBytesWritten();
    }

    private record LastCheckpointInfo(
            long timeMillis, long pagesFlushed, long performedIO, long ioLimit, long timesPaused, long millisPaused) {}
}
