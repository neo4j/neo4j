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

import org.neo4j.io.fs.StoreChannel;

/**
 * A page in the page cache. Always represents a concrete page in memory, and may
 * represent a particular page in a file, if that file-page has been swapped into the
 * page.
 */
public interface Page
{
    /**
     * Get the signed byte at the given offset into the page.
     *
     * May throw an AssertionError or a RuntimeException if the read is out of bounds.
     */
    byte getByte( int offset );

    /**
     * Set the signed byte at the given offset into the page.
     *
     * May throw an AssertionError or a RuntimeException if the write is out of bounds.
     */
    void putByte( byte value, int offset );

    /**
     * Fill the given array with bytes by reading from the page, beginning at the given
     * offset in the page.
     *
     * May throw an AssertionError or a RuntimeException if the read is out of bounds.
     * If an exception is thrown, then the array may have been partially filled with
     * the requested bytes, if the offset starts within the page.
     */
    void getBytes( byte[] data, int offset );

    /**
     * Write out all the bytes of the given array into the page, beginning at the given
     * offset in the page.
     *
     * May throw an AssertionError or a RuntimeException if the write is out of bounds.
     * If an exception is thrown, then some of the bytes from the given array may have
     * been written to the page, if the offset starts within the page.
     */
    void putBytes( byte[] data, int offset );

    /**
     * Get the signed short at the given offset into the page.
     *
     * May throw an AssertionError or a RuntimeException if the read is out of bounds.
     */
    short getShort( int offset );

    /**
     * Set the signed short at the given offset into the page.
     *
     * May throw an AssertionError or a RuntimeException if the write is out of bounds.
     */
    void putShort( short value, int offset );

    /**
     * Get the signed int at the given offset into the page.
     *
     * May throw an AssertionError or a RuntimeException if the read is out of bounds.
     */
    int getInt( int offset );

    /**
     * Set the signed int at the given offset into the page.
     *
     * May throw an AssertionError or a RuntimeException if the write is out of bounds.
     */
    void putInt( int value, int offset );

    /**
     * Get the signed long at the given offset into the page.
     *
     * May throw an AssertionError or a RuntimeException if the read is out of bounds.
     */
    long getLong( int offset );

    /**
     * Set the signed long at the given offset into the page.
     *
     * May throw an AssertionError or a RuntimeException if the write is out of bounds.
     */
    void putLong( long value, int offset );

    /**
     * Swap a file-page into memory.
     *
     * The file-page location is given by the offset into the file represented by the StoreChannel, and the length.
     *
     * The contents of the page in the file is unchanged by this operation.
     *
     * May throw an AssertionError or a RuntimeException if the given length is greater than the cache-page size.
     *
     * @throws IOException If the file could not be read from.
     *                     For instance, if the file has been closed, moved or deleted,
     *                     or if the requested range of data goes beyond the length of the file.
     *                     The possible causes of an IOException is platform dependent.
     *                     If the channel has been closed, then it must be
     *                     reopened and the swapIn operation must be retried.
     */
    void swapIn( StoreChannel channel, long offset, int length ) throws IOException;

    /**
     * Swap the page out to storage.
     *
     * The contents of the current page is written to the file represented by the given StoreChannel, at the
     * location expressed by the given offset into the file, and the length.
     *
     * The contents of the page in memory are unchanged by this operation.
     *
     * May throw an AssertionError or a RuntimeException if the length is greater than the cache-page size.
     *
     * If the offset is greater than the length of the file, the space in between will be filled with null-bytes.
     *
     * Note: This only performs the write on the OS level.
     * No forcing of the StoreChannel is implied.
     *
     * @throws IOException If the file could not be written to.
     *                     For instance, if the storage device is out of space,
     *                     or the file has been moved or deleted.
     *                     The possible causes of an IOException is platform dependent.
     *                     If the channel has been closed, then it must be
     *                     reopened and the swapIn operation must be retried.
     */
    void swapOut( StoreChannel channel, long offset, int length ) throws IOException;
}
