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

/**
 * Begin flushing modifications from an in-memory page to the backing file.
 */
public interface FlushEvent extends AutoCloseablePageCacheTracerEvent {
    /**
     * A FlushEvent implementation that does nothing.
     */
    FlushEvent NULL = new FlushEvent() {
        @Override
        public void addBytesWritten(long bytes) {}

        @Override
        public void setException(IOException exception) {}

        @Override
        public void addPagesFlushed(int pageCount) {}

        @Override
        public void addEvictionFlushedPages(int pageCount) {}

        @Override
        public void addPagesMerged(int pagesMerged) {}

        @Override
        public void close() {}
    };

    /**
     * Add up a number of bytes that has been written to the file.
     */
    void addBytesWritten(long bytes);

    /**
     * Add up a number of pages that has been flushed.
     */
    void addPagesFlushed(int pageCount);

    /**
     * Add up a number of pages that has been flushed by background evictor.
     */
    void addEvictionFlushedPages(int pageCount);

    /**
     * Record number of pages that were merged together into single flushed buffer.
     * @param pagesMerged number of merged pages
     */
    void addPagesMerged(int pagesMerged);

    /**
     * The page flush threw the given exception.
     */
    void setException(IOException exception);

    /**
     * The page flush has completed.
     */
    @Override
    void close();
}
