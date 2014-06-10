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
package org.neo4j.io.pagecache;

import java.io.IOException;

public interface PagedFile
{
    /**
     * Pin the pages with a shared lock.
     */
    public static final int PF_SHARED_LOCK = 1;
    /**
     * Pin the pages with an exclusive lock.
     */
    public static final int PF_EXCLUSIVE_LOCK = 1 << 1;
    /**
     * Disallow pinning and navigating to pages outside the range of the underlying file.
     */
    public static final int PF_NO_GROW = 1 << 2;
    /**
     * Read-ahead hint for sequential forward scanning.
     */
    public static final int PF_READ_AHEAD = 1 << 3; // TBD
    /**
     * Do not load in the page if it is not loaded already. Only useful with exclusive
     * locking when you want to overwrite the whole page anyway.
     */
    public static final int PF_NO_FAULT = 1 << 4; // TBD



    // TODO remove pin
    void pin( PageCursor cursor, PageLock lock, long pageId ) throws IOException;

    // TODO remove unpin
    void unpin( PageCursor cursor );

    PageCursor io( long pageId, int pf_flags, PageIO pageIO, long io_context, long io_flags ) throws IOException;



    int pageSize();

    void close() throws IOException;

    int numberOfCachedPages();

    /** Flush all dirty pages into the file channel, and force the file channel to disk. */
    void flush() throws IOException;

    /** Force all changes to this file handle down to disk. Does not flush dirty pages. */
    void force() throws IOException;
}
