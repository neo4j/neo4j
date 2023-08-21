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
 * Begin a mass-flushing of file pages.
 */
public interface FileFlushEvent extends AutoCloseablePageCacheTracerEvent {

    FileFlushEvent NULL = new FileFlushEvent() {

        @Override
        public FlushEvent beginFlush(
                long[] pageRefs,
                PageSwapper swapper,
                PageReferenceTranslator pageReferenceTranslator,
                int pagesToFlush,
                int mergedPages) {
            return FlushEvent.NULL;
        }

        @Override
        public FlushEvent beginFlush(
                long pageRef, PageSwapper swapper, PageReferenceTranslator pageReferenceTranslator) {
            return FlushEvent.NULL;
        }

        @Override
        public void startFlush(int[][] translationTable) {}

        @Override
        public void reset() {}

        @Override
        public long ioPerformed() {
            return 0;
        }

        @Override
        public long limitedNumberOfTimes() {
            return 0;
        }

        @Override
        public long limitedMillis() {
            return 0;
        }

        @Override
        public long pagesFlushed() {
            return 0;
        }

        @Override
        public ChunkEvent startChunk(int[] chunk) {
            return ChunkEvent.NULL;
        }

        @Override
        public void throttle(long recentlyCompletedIOs, long millis) {}

        @Override
        public void reportIO(int completedIOs) {}

        @Override
        public long localBytesWritten() {
            return 0;
        }

        @Override
        public void close() {}
    };

    /**
     * Begin flushing the given pages.
     */
    FlushEvent beginFlush(
            long[] pageRefs,
            PageSwapper swapper,
            PageReferenceTranslator pageReferenceTranslator,
            int pagesToFlush,
            int mergedPages);

    /**
     * Begin flushing the given single page.
     */
    FlushEvent beginFlush(long pageRef, PageSwapper swapper, PageReferenceTranslator pageReferenceTranslator);

    /**
     * Start flushing of given translation table
     * @param translationTable table we flush
     */
    void startFlush(int[][] translationTable);

    /**
     * Reset internal state of the event
     */
    void reset();

    /**
     * Number of IOs (number of buffers flushed) by this event after last {@link #reset()}
     */
    long ioPerformed();

    /**
     * Number of times flush was limited by io controller since last {@link #reset()}
     */
    long limitedNumberOfTimes();

    /**
     * Number of milliseconds flush event was paused by io controller since last {@link #reset()}
     */
    long limitedMillis();

    /**
     * Number of pages flushed by this event after last {@link #reset()}
     */
    long pagesFlushed();

    /**
     * Start flushing of given chunk
     * @param chunk chunk we start flushing
     */
    ChunkEvent startChunk(int[] chunk);

    /**
     * Throttle this flush event
     *
     * @param recentlyCompletedIOs number of recently completed io
     * @param millis               millis to throttle this flush event
     */
    void throttle(long recentlyCompletedIOs, long millis);

    /**
     * Report number of completed io operations by this flush event
     * @param completedIOs number of completed io operations
     */
    void reportIO(int completedIOs);

    /**
     * Report number of bytes written by this particular flush event.
     */
    long localBytesWritten();

    /**
     * Event generated during translation table chunk flushing from memory to backing file
     */
    class ChunkEvent {
        public static final ChunkEvent NULL = new ChunkEvent();

        protected ChunkEvent() {}

        public void chunkFlushed(
                long notModifiedPages, long flushPerChunk, long buffersPerChunk, long mergesPerChunk) {}
    }
}
