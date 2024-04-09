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
package org.neo4j.kernel.impl.transaction.tracing;

import org.neo4j.io.pagecache.tracing.DatabaseFlushEvent;

/**
 * Represents the process of appending a check point to the transaction log.
 */
public interface LogCheckPointEvent extends LogForceEvents, LogRotateEvents, AutoCloseable {
    LogCheckPointEvent NULL = new LogCheckPointEvent() {
        @Override
        public LogRotateEvent beginLogRotate() {
            return LogRotateEvent.NULL;
        }

        @Override
        public LogForceWaitEvent beginLogForceWait() {
            return LogForceWaitEvent.NULL;
        }

        @Override
        public LogForceEvent beginLogForce() {
            return LogForceEvent.NULL;
        }

        @Override
        public void checkpointCompleted(long checkpointMillis) {}

        @Override
        public void close() {}

        @Override
        public void appendedBytes(long bytes) {}

        @Override
        public DatabaseFlushEvent beginDatabaseFlush() {
            return DatabaseFlushEvent.NULL;
        }

        @Override
        public long getPagesFlushed() {
            return 0;
        }

        @Override
        public long getIOsPerformed() {
            return 0;
        }

        @Override
        public long getTimesPaused() {
            return 0;
        }

        @Override
        public long getMillisPaused() {
            return 0;
        }

        @Override
        public long getConfiguredIOLimit() {
            return 0;
        }

        @Override
        public double flushRatio() {
            return 0;
        }
    };

    /**
     * Notify about completion of checkpoint that took {@code checkpointMillis} to complete
     * @param checkpointMillis checkpoint duration
     */
    void checkpointCompleted(long checkpointMillis);

    /**
     * Marks the end of the check pointing process.
     */
    @Override
    void close();

    /**
     * Notify how many bytes were appended.
     * @param bytes number of bytes appended.
     */
    void appendedBytes(long bytes);

    /**
     * Start database flush as part of checkpoint
     */
    DatabaseFlushEvent beginDatabaseFlush();

    /**
     * Number of pages flushed by the last checkpoint event. 0 if no checkpoints were performed yet.
     */
    long getPagesFlushed();

    /**
     * Number of IOs performed by the last checkpoint event. 0 if no checkpoints were performed yet.
     */
    long getIOsPerformed();

    /**
     * Number of times the last checkpoint event was paused by io controller.
     */
    long getTimesPaused();

    /**
     * Number of milliseconds the last checkpoint event was paused by io controller.
     */
    long getMillisPaused();

    /**
     * Last observed io limit during the last checkpoint event. 0 if no checkpoints were performed yet and -1 if IO was not limited.
     */
    long getConfiguredIOLimit();

    /**
     * Ratio of flushed pages to total available pages in page cache
     */
    double flushRatio();
}
