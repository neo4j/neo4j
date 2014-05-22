/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.io.pagecache.impl.standard;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.io.pagecache.PageLock;
import org.neo4j.io.pagecache.impl.common.Page;

public interface PageTable
{
    /**
     * Load a new page into the table. This does not guarantee avoiding duplicate
     * pages loaded into the cache, it is up to the callee to ensure pages do not get
     * duplicated into the table.
     *
     * The page returned is pre-locked with the lock specified in the call.
     */
    PinnablePage load( PageIO io, long pageId, PageLock lock ) throws IOException;

    /** Flush all dirty pages. */
    void flush() throws IOException;

    /** Flush all dirty pages backed by the specified io. */
    void flush( PageIO io ) throws IOException;

    interface PageIO
    {
        void read( long pageId, ByteBuffer into ) throws IOException;
        void write( long pageId, ByteBuffer from ) throws IOException;

        /**
         * Notification that a page has been evicted, used to clean up state in structures
         * outside the page table.
         */
        void evicted( long pageId );
    }

    interface PinnablePage extends Page
    {
        /**
         * Pin the given page, so that it will not get evicted before unpin has been called.
         *
         * Returns true if this page still represent the given assertPageId in the context of the
         * given assertIO. In other words, it will return true if the page has not been replaced.
         * Otherwise it returns false.
         *
         * If the page is currently pinned for reading or writing, and the given PageLock is EXCLUSIVE, the pin will block.
         * If the page is currently pinned for writing, and the given PageLock is SHARED, the pin will block.
         */
        boolean pin( PageIO assertIO, long assertPageId, PageLock lock );

        /**
         * Unpin the page.
         *
         * This does not verify that the page actually is pinned, it is up to the client to
         * ensure that this method does not get called without an accompanying #pin() call
         * beforehand.
         */
        void unpin( PageLock lock );
    }
}
