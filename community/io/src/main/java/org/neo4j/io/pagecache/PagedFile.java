/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

/**
 * The representation of a file that has been mapped into the associated page
 * cache.
 */
public interface PagedFile extends AutoCloseable
{
    /**
     * Pin the pages with a shared lock.
     *
     * This implies {@link org.neo4j.io.pagecache.PagedFile#PF_NO_GROW}, since
     * pages under shared locks cannot be safely written to anyway, so there's
     * no point in trying to go beyond the end of the file.
     *
     * This cannot be combined with PF_EXCLUSIVE_LOCK.
     */
    int PF_SHARED_LOCK = 1;
    /**
     * Pin the pages with an exclusive lock.
     *
     * This will mark the pages as dirty, and caused them to be flushed, if
     * they are evicted.
     *
     * This cannot be combined with PF_SHARED_LOCK.
     */
    int PF_EXCLUSIVE_LOCK = 1 << 1;
    /**
     * Disallow pinning and navigating to pages outside the range of the
     * underlying file.
     */
    int PF_NO_GROW = 1 << 2;
    /**
     * Read-ahead hint for sequential forward scanning.
     */
    int PF_READ_AHEAD = 1 << 3; // TBD
    /**
     * Do not load in the page if it is not loaded already. Only useful with
     * exclusive locking when you want to overwrite the whole page anyway.
     */
    int PF_NO_FAULT = 1 << 4; // TBD
    /**
     * Do not update page access statistics.
     */
    int PF_TRANSIENT = 1 << 5; // TBD

    /**
     * Initiate an IO interaction with the contents of the paged file.
     *
     * <p>The basic structure of an interaction looks like this:
     *
     * <pre><code>
     *     try ( PageCursor cursor = pagedFile.io( startingPageId, intentFlags ) )
     *     {
     *         if ( cursor.next() )
     *         {
     *             do
     *             {
     *                 // perform read or write operations on the page
     *             }
     *             while ( cursor.shouldRetry() );
     *         }
     *     }
     * </code></pre>
     *
     * {@link org.neo4j.io.pagecache.PageCursor PageCursors} are
     * <code>AutoCloseable</code>, so interacting with them using
     * <code>try-with-resources</code> is recommended.
     *
     * <p>The returned PageCursor is initially not bound, so
     * {@link PageCursor#next() next} must be called on it before it can be
     * used.
     *
     * <p>The first <code>next</code> call will advance the cursor to the
     * initial page, as given by the <code>pageId</code> parameter.
     * Until then, the cursor won't be bound to any page, the
     * {@link PageCursor#getCurrentPageId()} method will return the
     * {@link org.neo4j.io.pagecache.PageCursor#UNBOUND_PAGE_ID} constant, and
     * attempts at reading from or writing to the cursor will throw a
     * NullPointerException.
     *
     * <p>After the <code>next</code> call, if it returns <code>true</code>,
     * the cursor will be bound to a page, and the get and put methods will
     * access that page.
     *
     * <p>After a call to {@link PageCursor#rewind()}, the cursor will return
     * to its initial state.
     *
     * <p>The <code>pf_flags</code> argument expresses the intent of the IO
     * operation.
     * It is a bitmap that combines various <code>PF_*</code> constants.
     * You must always specify your desired locking behaviour, with either
     * {@link org.neo4j.io.pagecache.PagedFile#PF_EXCLUSIVE_LOCK} or
     * {@link org.neo4j.io.pagecache.PagedFile#PF_SHARED_LOCK}.
     * The two locking modes cannot be combined, but other intents can be
     * combined with them.
     * For instance, if you want to write to a page, but also make sure that
     * you don't write beyond the end of the file, then you can express your
     * intent with <code>PF_EXCLUSIVE_LOCK | PF_NO_GROW</code> – note how the
     * flags are combined with a bitwise-OR operator.
     *
     *
     *
     * @param pageId The initial file-page-id, that the cursor will be bound to
     *               after the first call to <code>next</code>.
     * @param pf_flags A bitmap of <code>PF_*</code> constants composed with
     *                 the bitwise-OR operator, that expresses the desired
     *                 locking behaviour, and other hints.
     * @return A PageCursor in its initial unbound state.
     * Never <code>null</code>.
     * @throws IOException if there was an error accessing the underlying file.
     */
    PageCursor io( long pageId, int pf_flags ) throws IOException;

    /**
     * Get the size of the file-pages, in bytes.
     */
    int pageSize();

    /**
     * Flush all dirty pages into the file channel, and force the file channel to disk.
     *
     * Note: Flushing has to take locks on pages, so you cannot call flush
     * while you have pages pinned.
     */
    void flushAndForce() throws IOException;

    /**
     * Get the file-page-id of the last page in the file.
     *
     * This will return -1 if the file is completely empty.
     */
    long getLastPageId() throws IOException;

    /**
     * Release a handle to a paged file.
     *
     * If this is the last handle to the file, it will be flushed and closed.
     *
     * @see AutoCloseable#close()
     * @throws IOException instead of the Exception superclass as defined in AutoCloseable, if .
     */
    void close() throws IOException;
}
