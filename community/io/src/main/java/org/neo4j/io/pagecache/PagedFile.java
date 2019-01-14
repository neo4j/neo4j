/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import java.io.File;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.OpenOption;

/**
 * The representation of a file that has been mapped into the associated page cache.
 */
public interface PagedFile extends AutoCloseable
{
    /**
     * Pin the pages with a shared lock.
     * <p>
     * This implies {@link org.neo4j.io.pagecache.PagedFile#PF_NO_GROW}, since
     * pages under read locks cannot be safely written to anyway, so there's
     * no point in trying to go beyond the end of the file.
     * <p>
     * This cannot be combined with {@link #PF_SHARED_WRITE_LOCK}.
     */
    int PF_SHARED_READ_LOCK = 1;
    /**
     * Pin the pages with a shared write lock.
     * <p>
     * This will mark the pages as dirty, and caused them to be flushed, if they are evicted.
     * <p>
     * Note that write locks are <em>not</em> exclusive. You must use other means to coordinate access to the data on
     * the pages. The write lock only means that the page will not be concurrently evicted.
     * <p>
     * Note also that write locks exclude eviction. So since we can assume that write locks never make conflicting
     * modifications (higher level locks should ensure this), it is safe to perform page writes without a
     * {@link PageCursor#shouldRetry() shouldRetry} loop. The {@code shouldRetry} method on write locking cursors
     * always returns {@code false}.
     * <p>
     * This cannot be combined with {@link #PF_SHARED_READ_LOCK}.
     */
    int PF_SHARED_WRITE_LOCK = 1 << 1;
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
     * Do not load in the page if it is not loaded already. The methods {@link PageCursor#next()} and
     * {@link PageCursor#next(long)} will always return {@code true} for pages that are within the range of the file,
     * but the {@link PageCursor#getCurrentPageId()} will return {@link PageCursor#UNBOUND_PAGE_ID} for pages that are
     * not in-memory. The current page id <em>must</em> be checked on every {@link PageCursor#shouldRetry()} loop
     * iteration, in case it (for a read cursor) was evicted concurrently with the page access.
     * <p>
     * {@link #PF_NO_FAULT} implies {@link #PF_NO_GROW}, since a page fault is necessary to be able to extend a file.
     */
    int PF_NO_FAULT = 1 << 4;
    /**
     * Do not update page access statistics.
     */
    int PF_TRANSIENT = 1 << 5; // TBD
    /**
     * Flush pages more aggressively, after they have been dirtied by a write cursor.
     */
    int PF_EAGER_FLUSH = 1 << 6;

    /**
     * Initiate an IO interaction with the contents of the paged file.
     * <p>
     * The basic structure of an interaction looks like this:
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
     * {@link org.neo4j.io.pagecache.PageCursor PageCursors} are {@link AutoCloseable}, so interacting with them
     * using <em>try-with-resources</em> is recommended.
     * <p>
     * The returned PageCursor is initially not bound, so {@link PageCursor#next() next} must be called on it before it
     * can be used.
     * <p>
     * The first {@code next} call will advance the cursor to the initial page, as given by the {@code pageId}
     * parameter. Until then, the cursor won't be bound to any page, the {@link PageCursor#getCurrentPageId()} method
     * will return the {@link org.neo4j.io.pagecache.PageCursor#UNBOUND_PAGE_ID} constant, and attempts at reading from
     * or writing to the cursor will throw a {@link NullPointerException}.
     * <p>
     * After the {@code next} call, if it returns {@code true}, the cursor will be bound to a page, and the get and put
     * methods will access that page.
     * <p>
     * After a call to {@link PageCursor#rewind()}, the cursor will return to its initial state.
     * <p>
     * The {@code pf_flags} argument expresses the intent of the IO operation. It is a bitmap that combines various
     * {@code PF_*} constants. You must always specify your desired locking behaviour, with either
     * {@link org.neo4j.io.pagecache.PagedFile#PF_SHARED_WRITE_LOCK} or
     * {@link org.neo4j.io.pagecache.PagedFile#PF_SHARED_READ_LOCK}.
     * <p>
     * The two locking modes cannot be combined, but other intents can be combined with them. For instance, if you want
     * to write to a page, but also make sure that you don't write beyond the end of the file, then you can express
     * your intent with {@code PF_SHARED_WRITE_LOCK | PF_NO_GROW} – note how the flags are combined with a bitwise-OR
     * operator.
     * Arithmetic addition can also be used, but might not make it as clear that we are dealing with a bit-set.
     *
     * @param pageId The initial file-page-id, that the cursor will be bound to
     * after the first call to <code>next</code>.
     * @param pf_flags A bitmap of <code>PF_*</code> constants composed with
     * the bitwise-OR operator, that expresses the desired
     * locking behaviour, and other hints.
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
     * Size of file, in bytes.
     */
    long fileSize() throws IOException;

    /**
     * Get the filename that is mapped by this {@code PagedFile}.
     */
    File file();

    /**
     * Flush all dirty pages into the file channel, and force the file channel to disk.
     */
    void flushAndForce() throws IOException;

    /**
     * Flush all dirty pages into the file channel, and force the file channel to disk, but limit the rate of IO as
     * advised by the given IOPSLimiter.
     *
     * @param limiter The {@link IOLimiter} that determines if pauses or sleeps should be injected into the flushing
     * process to keep the IO rate down.
     */
    void flushAndForce( IOLimiter limiter ) throws IOException;

    /**
     * Get the file-page-id of the last page in the file.
     * <p>
     * This will return <em>a negative number</em> (not necessarily -1) if the file is completely empty.
     *
     * @throws IllegalStateException if this file has been unmapped
     */
    long getLastPageId() throws IOException;

    /**
     * Release a handle to a paged file.
     * <p>
     * If this is the last handle to the file, it will be flushed and closed.
     * <p>
     * Note that this operation assumes that there are no write page cursors open on the paged file. If there are, then
     * their writes may be lost, as they might miss the last flush that can happen on their data.
     *
     * @throws IOException instead of the Exception superclass as defined in AutoCloseable, if .
     * @see AutoCloseable#close()
     */
    @Override
    void close() throws IOException;

    /**
     * Open a {@link ReadableByteChannel} view of this PagedFile.
     * <p>
     * The channel will read the file sequentially from the beginning till the end.
     * Seeking is not supported.
     * <p>
     * The channel is not thread-safe.
     *
     * @return A channel for reading the paged file.
     */
    ReadableByteChannel openReadableByteChannel() throws IOException;

    /**
     * Open a {@link WritableByteChannel} view of this PagedFile.
     * <p>
     * The channel will write to the file sequentially from the beginning of the file, overwriting whatever is there
     * already. If the amount of new data is less than the amount of existing data, then the old data will still be
     * present at the far end of the file. Thus, this function works neither like opening a file for writing, nor like
     * appending to a file.
     * <p>
     * If this is undesired, then the file can be mapped with {@link java.nio.file.StandardOpenOption#TRUNCATE_EXISTING}
     * to remove the existing data before writing to the file.
     * <p>
     * The channel is not thread-safe.
     *
     * @return A channel for writing to the paged file.
     * @see PageCache#map(File, int, OpenOption...)
     */
    WritableByteChannel openWritableByteChannel() throws IOException;
}
