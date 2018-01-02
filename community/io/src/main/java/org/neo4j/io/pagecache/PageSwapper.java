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

import java.io.File;
import java.io.IOException;

/**
 * <strong>Implementation note:</strong> These methods must NEVER swallow a thread-interrupt.
 * If the thread is interrupted when these methods are called, or gets interrupted while they are
 * executing, then they must either throw an InterruptedException, or leave the interrupted-status
 * flag alone.
 */
public interface PageSwapper
{
    /**
     * Read the page with the given filePageId, from the concrete file on the
     * file system, into the given page.
     *
     * Returns the number of bytes read in from the file. May be zero if the
     * requested page was beyond the end of the file. If less than the file
     * page size, then the rest of the page will contain zeros.
     *
     * Note: It is possible for the channel to be asynchronously closed while
     * this operation is taking place. For instance, if the current thread is
     * interrupted. If this happens, then the implementation must reopen the
     * channel and the operation must be retried.
     */
    long read( long filePageId, Page page ) throws IOException;

    /**
     * Read pages from the file into the given pages, starting from the given startFilePageId.
     *
     * Returns the number of bytes read in from the file. May be zero if the
     * requested startFilePageId was beyond the end of the file. If the file does not have enough data
     * to fill up all the buffer space represented by the pages, then the remaining buffer space will be
     * filled with zero bytes.
     *
     * The contents of the pages should be considered to be garbage if the operation throws an exception,
     * since the constituent reads can be reordered, and no zeroing will take place.
     *
     * Note: It is possible for the channel to be asynchronously closed while
     * this operation is taking place. For instance, if the current thread is
     * interrupted. If this happens, then the implementation must reopen the
     * channel and the operation must be retried.
     */
    long read( long startFilePageId, Page[] pages, int arrayOffset, int length ) throws IOException;

    /**
     * Write the contents of the given page, to the concrete file on the file
     * system, at the located indicated by the given filePageId.
     *
     * Returns the number of bytes written to the file.
     *
     * Note: It is possible for the channel to be asynchronously closed while
     * this operation is taking place. For instance, if the current thread is
     * interrupted. If this happens, then implementation must reopen the
     * channel and the operation must be retried.
     */
    long write( long filePageId, Page page ) throws IOException;

    /**
     * Write the contents of the given pages, to the concrete file on the file system,
     * starting at the location of the given startFilePageId.
     *
     * If an exception is thrown, then some of the data may have been written, and some might not.
     * The writes may reorder and tear, so no guarantee can be made about what has been written and what has not, if
     * an exception is thrown. Therefor, the entire write operation should be retried, in the case of failure, or the
     * data should be rewritten through other means.
     *
     * Returns the number of bytes written to the file.
     *
     * Note: It is possible for the channel to be asynchronously closed while
     * this operation is taking place. For instance, if the current thread is
     * interrupted. If this happens, then implementation must reopen the
     * channel and the operation must be retried.
     */
    long write( long startFilePageId, Page[] pages, int arrayOffset, int length ) throws IOException;

    /**
     * Notification that a page has been evicted, used to clean up state in structures
     * outside the page table.
     */
    void evicted( long pageId, Page page );

    /**
     * Get the file that this PageSwapper represents.
     */
    File file();

    /**
     * Close and release all resources associated with the file underlying this
     * PageSwapper.
     */
    void close() throws IOException;

    /**
     * Forces all writes done by this PageSwapper to the underlying storage device, such that the writes are durable
     * when this call returns.
     *
     * This method has no effect if the {@link PageSwapperFactory#syncDevice()} method forces the writes for all
     * non-closed PageSwappers created through the given <code>PageSwapperFactory</code>.
     * The {@link PageCache#flushAndForce()} method will first call <code>force</code> on the PageSwappers for all
     * mapped files, then call <code>syncDevice</code> on the PageSwapperFactory. This way, the writes are always made
     * durable regardless of which method that does the forcing.
     */
    void force() throws IOException;

    /**
     * Get the filePageId of the last page in the concrete file.
     */
    long getLastPageId() throws IOException;

    /**
     * Truncate the file represented by this PageSwapper, so the size of the file is zero and
     * {@link #getLastPageId()} returns -1.
     *
     * Truncation may occur concurrently with writes, in which case both operations will appear to be atomic, such that
     * either the write happens before the truncation and is lost, or the file is truncated and the write then extends
     * the file with any zero padding and the written data.
     *
     * @throws IOException
     */
    void truncate() throws IOException;
}
