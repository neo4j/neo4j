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
 * A PageCursor is returned from {@link org.neo4j.io.pagecache.PagedFile#io(long, int)},
 * and is used to scan through pages and process them in a consistent and safe fashion.
 * <p>
 * A page must be processed in the following manner:
 * <pre><code>
 *     try ( PageCursor cursor = pagedFile.io( pageId, pf_flags ) )
 *     {
 *         // Use 'if' for processing a single page,
 *         // use 'while' for scanning through pages:
 *         if ( cursor.next() )
 *         {
 *             do
 *             {
 *                 processPage( cursor );
 *             } while ( cursor.shouldRetry() );
 *             // Any finalising, non-repeatable post-processing
 *             // goes here.
 *         }
 *     }
 *     catch ( IOException e )
 *     {
 *         // handle the error, somehow
 *     }
 * </code></pre>
 * <p>There are a couple of things to this pattern that are worth noting:
 * <ul>
 *     <li>We use a try-with-resources clause to make sure that the resources
 *     associated with the PageCursor are always released properly.
 *     </li>
 *     <li>We use an if-clause for the next() call if we are only processing
 *     a single page, to make sure that the page exist and is accessible to us.
 *     </li>
 *     <li>We use a while-clause for next() if we are scanning through pages.
 *     </li>
 *     <li>We do our processing of the page in a do-while-retry loop, to
 *     make sure that we processed a page that was in a consistent state.
 *     </li>
 * </ul>
 * You can alternatively use the {@link #next(long)} method, to navigate the
 * pages you need in a non-linear fashion.
 */
public interface PageCursor extends AutoCloseable
{
    long UNBOUND_PAGE_ID = -1;
    int UNBOUND_PAGE_SIZE = -1;

    /**
     * Get the signed byte at the current page offset, and then increment the offset by one.
     *
     * @throws IndexOutOfBoundsException
     * if the current offset is not within the page bounds.
     */
    byte getByte();

    /**
     * Get the signed byte at the given offset into the page.
     * Leaves the current page offset unchanged.
     *
     * @throws IndexOutOfBoundsException
     * if the given offset is not within the page bounds.
     */
    byte getByte( int offset );

    /**
     * Set the signed byte at the current offset into the page, and then increment the offset by one.
     *
     * @throws IndexOutOfBoundsException
     * if the current offset is not within the page bounds.
     */
    void putByte( byte value );

    /**
     * Set the signed byte at the given offset into the page.
     * Leaves the current page offset unchanged.
     *
     * @throws IndexOutOfBoundsException
     * if the given offset is not within the page bounds.
     */
    void putByte( int offset, byte value );

    /**
     * Get the signed long at the current page offset, and then increment the offset by one.
     *
     * @throws IndexOutOfBoundsException
     * if the current offset is not within the page bounds.
     */
    long getLong();

    /**
     * Get the signed long at the given offset into the page.
     * Leaves the current page offset unchanged.
     *
     * @throws IndexOutOfBoundsException
     * if the given offset is not within the page bounds.
     */
    long getLong( int offset );

    /**
     * Set the signed long at the current offset into the page, and then increment the offset by one.
     *
     * @throws IndexOutOfBoundsException
     * if the current offset is not within the page bounds.
     */
    void putLong( long value );

    /**
     * Set the signed long at the given offset into the page.
     * Leaves the current page offset unchanged.
     *
     * @throws IndexOutOfBoundsException
     * if the given offset is not within the page bounds.
     */
    void putLong( int offset, long value );

    /**
     * Get the signed int at the current page offset, and then increment the offset by one.
     *
     * @throws IndexOutOfBoundsException
     * if the current offset is not within the page bounds.
     */
    int getInt();

    /**
     * Get the signed int at the given offset into the page.
     * Leaves the current page offset unchanged.
     *
     * @throws IndexOutOfBoundsException
     * if the given offset is not within the page bounds.
     */
    int getInt( int offset );

    /**
     * Set the signed int at the current offset into the page, and then increment the offset by one.
     *
     * @throws IndexOutOfBoundsException
     * if the current offset is not within the page bounds.
     */
    void putInt( int value );

    /**
     * Set the signed int at the given offset into the page.
     * Leaves the current page offset unchanged.
     *
     * @throws IndexOutOfBoundsException
     * if the given offset is not within the page bounds.
     */
    void putInt( int offset, int value );

    /**
     * Get the unsigned int at the current page offset, and then increment the offset by one.
     *
     * @throws IndexOutOfBoundsException
     * if the current offset is not within the page bounds.
     */
    long getUnsignedInt();

    /**
     * Get the unsigned int at the given offset into the page.
     * Leaves the current page offset unchanged.
     *
     * @throws IndexOutOfBoundsException
     * if the given offset is not within the page bounds.
     */
    long getUnsignedInt( int offset );

    /**
     * Fill the given array with bytes from the page, beginning at the current offset into the page,
     * and then increment the current offset by the length of the array.
     *
     * @throws IndexOutOfBoundsException
     * if the current offset plus the length of the array reaches beyond the end of the page.
     */
    void getBytes( byte[] data );

    /**
     * Read the given length of bytes from the page into the given array, starting from the current offset into the
     * page, and writing from the given array offset, and then increment the current offset by the length argument.
     *
     * @throws IndexOutOfBoundsException
     * if the current offset plus the length reaches beyond the end of the page.
     */
    void getBytes( byte[] data, int arrayOffset, int length );

    /**
     * Write out all the bytes of the given array into the page, beginning at the current offset into the page,
     * and then increment the current offset by the length of the array.
     *
     * @throws IndexOutOfBoundsException
     * if the current offset plus the length of the array reaches beyond the end of the page.
     */
    void putBytes( byte[] data );

    /**
     * Write out the given length of bytes from the given offset into the the given array of bytes, into the page,
     * beginning at the current offset into the page, and then increment the current offset by the length argument.
     *
     * @throws IndexOutOfBoundsException
     * if the current offset plus the length reaches beyond the end of the page.
     */
    void putBytes( byte[] data, int arrayOffset, int length );

    /**
     * Get the signed short at the current page offset, and then increment the offset by one.
     *
     * @throws IndexOutOfBoundsException
     * if the current offset is not within the page bounds.
     */
    short getShort();

    /**
     * Get the signed short at the given offset into the page.
     * Leaves the current page offset unchanged.
     *
     * @throws IndexOutOfBoundsException
     * if the given offset is not within the page bounds.
     */
    short getShort( int offset );

    /**
     * Set the signed short at the current offset into the page, and then increment the offset by one.
     *
     * @throws IndexOutOfBoundsException
     * if the current offset is not within the page bounds.
     */
    void putShort( short value );

    /**
     * Set the signed short at the given offset into the page.
     * Leaves the current page offset unchanged.
     *
     * @throws IndexOutOfBoundsException
     * if the given offset is not within the page bounds.
     */
    void putShort( int offset, short value );

    /**
     * Set the current offset into the page, for interacting with the page through the read and write methods that do
     * not take a specific offset as an argument.
     */
    void setOffset( int offset );

    /**
     * Get the current offset into the page, which is the location on the page where the next interaction would take
     * place through the read and write methods that do not take a specific offset as an argument.
     */
    int getOffset();

    /**
     * Get the file page id that the cursor is currently positioned at, or
     * UNBOUND_PAGE_ID if next() has not yet been called on this cursor, or returned false.
     * A call to rewind() will make the current page id unbound as well, until
     * next() is called.
     */
    long getCurrentPageId();

    /**
     * Get the file page size of the page that the cursor is currently positioned at,
     * or UNBOUND_PAGE_SIZE if next() has not yet been called on this cursor, or returned false.
     * A call to rewind() will make the current page unbound as well, until next() is called.
     */
    int getCurrentPageSize();

    /**
     * Get the file the cursor is currently bound to, or {@code null} if next() has not yet been called on this
     * cursor, or returned false.
     * A call to rewind() will make the cursor unbound as well, until next() is called.
     */
    File getCurrentFile();

    /**
     * Rewinds the cursor to its initial condition, as if freshly returned from
     * an equivalent io() call. In other words, the next call to next() will
     * move the cursor to the starting page that was specified in the io() that
     * produced the cursor.
     */
    void rewind();

    /**
     * Moves the cursor to the next page, if any, and returns true when it is
     * ready to be processed. Returns false if there are no more pages to be
     * processed. For instance, if the cursor was requested with PF_NO_GROW
     * and the page most recently processed was the last page in the file.
     * <p>
     * <strong>NOTE: When using read locks, read operations can be inconsistent
     * and may return completely random data. The data returned from a
     * read-locked page cursor should not be interpreted until after
     * {@link #shouldRetry()} has returned {@code false}.</strong>
     * Not interpreting the data also implies that you cannot throw exceptions
     * from data validation errors until after {@link #shouldRetry()} has told
     * you that your read was consistent.
     */
    boolean next() throws IOException;

    /**
     * Moves the cursor to the page specified by the given pageId, if any,
     * and returns true when it is ready to be processed. Returns false if
     * for instance, the cursor was requested with PF_NO_GROW and the page
     * most recently processed was the last page in the file.
     * <p>
     * <strong>NOTE: When using read locks, read operations can be inconsistent
     * and may return completely random data. The data returned from a
     * read-locked page cursor should not be interpreted until after
     * {@link #shouldRetry()} has returned {@code false}.</strong>
     * Not interpreting the data also implies that you cannot throw exceptions
     * from data validation errors until after {@link #shouldRetry()} has told
     * you that your read was consistent.
     */
    boolean next( long pageId ) throws IOException;

    /**
     * Relinquishes all resources associated with this cursor, including the
     * cursor itself. The cursor cannot be used after this call.
     * @see AutoCloseable#close()
     */
    void close();

    /**
     * Returns true if the page has entered an inconsistent state since the
     * last call to next() or shouldRetry().
     * If this method returns true, the in-page offset of the cursor will be
     * reset to zero.
     *
     * @throws IOException If the page was evicted while doing IO, the cursor will have
     *                     to do a page fault to get the page back.
     *                     This may throw an IOException.
     */
    boolean shouldRetry() throws IOException;
}
