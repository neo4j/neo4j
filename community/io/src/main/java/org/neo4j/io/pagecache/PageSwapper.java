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
     * This should be implemented using the
     * {@link Page#swapIn(org.neo4j.io.fs.StoreChannel, long, int)} method.
     */
    void read( long filePageId, Page page ) throws IOException;

    /**
     * Write the contents of the given page, to the concrete file on the file
     * system, at the located indicated by the given filePageId.
     *
     * This should be implemented using the
     * {@link Page#swapOut(org.neo4j.io.fs.StoreChannel, long, int)} method.
     */
    void write( long filePageId, Page page ) throws IOException;

    /**
     * Notification that a page has been evicted, used to clean up state in structures
     * outside the page table.
     */
    void evicted( long pageId, Page page );

    /**
     * Get the human-readable name for the file that this PageSwapper represents.
     */
    String fileName();

    /**
     * Close and release all resources associated with the file underlying this
     * PageSwapper.
     */
    void close() throws IOException;

    /**
     * Synchronise all writes done by this PageSwapper, with the underlying
     * storage device.
     */
    void force() throws IOException;

    /**
     * Get the filePageId of the last page in the concrete file.
     */
    long getLastPageId() throws IOException;
}
