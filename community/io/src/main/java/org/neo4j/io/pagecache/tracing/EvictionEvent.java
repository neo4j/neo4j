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
import org.neo4j.io.pagecache.PageSwapper;

/**
 * The eviction of a page has begun.
 */
public interface EvictionEvent extends AutoCloseablePageCacheTracerEvent {
    /**
     * An EvictionEvent that does nothing other than return the FlushEventOpportunity.NULL.
     */
    EvictionEvent NULL = new EvictionEvent() {
        @Override
        public void setFilePageId(long filePageId) {}

        @Override
        public void setSwapper(PageSwapper swapper) {}

        @Override
        public FlushEvent beginFlush(
                long pageRef, PageSwapper swapper, PageReferenceTranslator pageReferenceTranslator) {
            return FlushEvent.NULL;
        }

        @Override
        public void setException(IOException exception) {}

        @Override
        public void close() {}
    };

    /**
     * The file page id the evicted page was bound to.
     */
    void setFilePageId(long filePageId);

    /**
     * The swapper the evicted page was bound to.
     */
    void setSwapper(PageSwapper swapper);

    /**
     * Begin flushing the given page.
     */
    FlushEvent beginFlush(long pageRef, PageSwapper swapper, PageReferenceTranslator pageReferenceTranslator);

    /**
     * Indicates that the eviction caused an exception to be thrown.
     * This can happen if some kind of IO error occurs.
     */
    void setException(IOException exception);
}
