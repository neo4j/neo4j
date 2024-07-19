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
package org.neo4j.io.pagecache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.impl.FileIsNotMappedException;

/**
 * A PageCursor is returned from {@link PagedFile#io(long, int, CursorContext)},
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
 * <li>We use a try-with-resources clause to make sure that the resources associated with the PageCursor are always
 * released properly.
 * </li>
 * <li>We use an if-clause for the next() call if we are only processing a single page, to make sure that the page
 * exist and is accessible to us.
 * </li>
 * <li>We use a while-clause for next() if we are scanning through pages.
 * </li>
 * <li>We do our processing of the page in a do-while-retry loop, to make sure that we processed a page that was in a
 * consistent state.
 * </li>
 * </ul>
 * You can alternatively use the {@link #next(long)} method, to navigate the pages you need in an arbitrary order.
 */
public abstract class PageCursor implements AutoCloseable {
    public static final long UNBOUND_PAGE_ID = -1;

    /**
     * Get the signed byte at the current page offset, and then increment the offset by one.
     */
    public abstract byte getByte();

    /**
     * Get the signed byte at the given offset into the page.
     * Leaves the current page offset unchanged.
     */
    public abstract byte getByte(int offset);

    /**
     * Set the signed byte at the current offset into the page, and then increment the offset by one.
     */
    public abstract void putByte(byte value);

    /**
     * Set the signed byte at the given offset into the page.
     * Leaves the current page offset unchanged.
     */
    public abstract void putByte(int offset, byte value);

    /**
     * Get the signed long at the current page offset, and then increment the offset by one.
     */
    public abstract long getLong();

    /**
     * Get the signed long at the given offset into the page.
     * Leaves the current page offset unchanged.
     */
    public abstract long getLong(int offset);

    /**
     * Set the signed long at the current offset into the page, and then increment the offset by one.
     */
    public abstract void putLong(long value);

    /**
     * Set the signed long at the given offset into the page.
     * Leaves the current page offset unchanged.
     */
    public abstract void putLong(int offset, long value);

    /**
     * Get the signed int at the current page offset, and then increment the offset by one.
     */
    public abstract int getInt();

    /**
     * Get the signed int at the given offset into the page.
     * Leaves the current page offset unchanged.
     */
    public abstract int getInt(int offset);

    /**
     * Set the signed int at the current offset into the page, and then increment the offset by one.
     */
    public abstract void putInt(int value);

    /**
     * Set the signed int at the given offset into the page.
     * Leaves the current page offset unchanged.
     */
    public abstract void putInt(int offset, int value);

    /**
     * Fill the given array with bytes from the page, beginning at the current offset into the page,
     * and then increment the current offset by the length of the array.
     */
    public abstract void getBytes(byte[] data);

    /**
     * Read the given length of bytes from the page into the given array, starting from the current offset into the
     * page, and writing from the given array offset, and then increment the current offset by the length argument.
     */
    public abstract void getBytes(byte[] data, int arrayOffset, int length);

    /**
     * Write out all the bytes of the given array into the page, beginning at the current offset into the page,
     * and then increment the current offset by the length of the array.
     */
    public abstract void putBytes(byte[] data);

    /**
     * Write out the given length of bytes from the given offset into the the given array of bytes, into the page,
     * beginning at the current offset into the page, and then increment the current offset by the length argument.
     */
    public abstract void putBytes(byte[] data, int arrayOffset, int length);

    /**
     * Set the given number of bytes to the given value, beginning at current offset into the page.
     */
    public abstract void putBytes(int bytes, byte value);

    /**
     * Get the signed short at the current page offset, and then increment the offset by one.
     */
    public abstract short getShort();

    /**
     * Get the signed short at the given offset into the page.
     * Leaves the current page offset unchanged.
     */
    public abstract short getShort(int offset);

    /**
     * Set the signed short at the current offset into the page, and then increment the offset by one.
     */
    public abstract void putShort(short value);

    /**
     * Set the signed short at the given offset into the page.
     * Leaves the current page offset unchanged.
     */
    public abstract void putShort(int offset, short value);

    /**
     * Set the current offset into the page, for interacting with the page through the read and write methods that do
     * not take a specific offset as an argument.
     */
    public abstract void setOffset(int offset);

    /**
     * Get the current offset into the page, which is the location on the page where the next interaction would take
     * place through the read and write methods that do not take a specific offset as an argument.
     */
    public abstract int getOffset();

    /**
     * Mark the current offset. Only one offset can be marked at any time.
     */
    public abstract void mark();

    /**
     * Set the offset to the marked offset. This does not modify the value of the mark.
     */
    public abstract void setOffsetToMark();

    /**
     * Get the file page id that the cursor is currently positioned at, or
     * UNBOUND_PAGE_ID if next() has not yet been called on this cursor, or returned false.
     * A call to rewind() will make the current page id unbound as well, until
     * next() is called.
     */
    public abstract long getCurrentPageId();

    /**
     * Get the file the cursor is currently bound to, or {@code null} if next() has not yet been called on this
     * cursor, or returned false.
     * A call to rewind() will make the cursor unbound as well, until next() is called.
     */
    public abstract Path getCurrentFile();

    /**
     * Get the page file the cursor is currently bound to, or {@code null} if next() has not yet been called on this
     * cursor, or returned false.
     */
    public abstract PagedFile getPagedFile();

    /**
     * Get page cursor page file if cursor is still open or null otherwise
     * @return cursor page file or null if closed
     */
    public abstract Path getRawCurrentFile();

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
    public abstract boolean next() throws IOException;

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
    public abstract boolean next(long pageId) throws IOException;

    /**
     * Relinquishes all resources associated with this cursor, including the
     * cursor itself, and any linked cursors opened through it. The cursor cannot be used after this call.
     *
     * @see AutoCloseable#close()
     */
    @Override
    public abstract void close();

    /**
     * Returns true if the page has entered an inconsistent state since the last call to next() or shouldRetry().
     * <p>
     * If this method returns true, the in-page offset of the cursor will be reset to zero.
     * <p>
     * Note that {@link PagedFile#PF_SHARED_WRITE_LOCK write locked} cursors never conflict with each other, nor with
     * eviction, and thus technically don't require a {@code shouldRetry} check. This method always returns
     * {@code false} for write-locking cursors.
     * <p>
     * Cursors that are {@link PagedFile#PF_SHARED_READ_LOCK read locked} must <em>always</em> perform their reads in a
     * {@code shouldRetry} loop, and avoid interpreting the data they read until {@code shouldRetry} has confirmed that
     * the reads were consistent.
     *
     * @throws IOException If the page was evicted while doing IO, the cursor will have
     * to do a page fault to get the page back.
     * This may throw an IOException.
     * @throws FileIsNotMappedException if page file was unmapped while doing cursor operations.
     */
    public abstract boolean shouldRetry() throws IOException;

    /**
     * Copy the current cursor page, to the given target cursor page.
     * Always copy full page including any reserved bytes.
     * <p>
     * <strong>Note</strong> that {@code copyPage} is only guaranteed to work when both target and source cursor are from
     * the <em>same</em> page cache implementation. Using wrappers, delegates or mixing cursor implementations may
     * produce unspecified errors.
     */
    public abstract void copyPage(PageCursor targetCursor);

    /**
     * Copy the specified number of bytes from the given offset of this page, to the given offset of the target page.
     * <p>
     * If the length reaches beyond the end of either cursor, then only as many bytes as are available in this cursor,
     * or can fit in the target cursor, are actually copied.
     * <p>
     * <strong>Note</strong> that {@code copyTo} is only guaranteed to work when both target and source cursor are from
     * the <em>same</em> page cache implementation. Using wrappers, delegates or mixing cursor implementations may
     * produce unspecified errors.
     *
     * @param sourceOffset The offset into this page to copy from.
     * @param targetCursor The cursor the data will be copied to.
     * @param targetOffset The offset into the target cursor to copy to.
     * @param lengthInBytes The number of bytes to copy.
     * @return The number of bytes actually copied.
     */
    public abstract int copyTo(int sourceOffset, PageCursor targetCursor, int targetOffset, int lengthInBytes);

    /**
     * Copy bytes from the specified offset in this page, into the given buffer, until either the limit of the buffer
     * is reached, or the end of the page is reached. The actual number of bytes copied is returned.
     *
     * @param sourceOffset The offset into this page to copy from.
     * @param targetBuffer The buffer the data will be copied to.
     * @return The number of bytes actually copied.
     */
    public abstract int copyTo(int sourceOffset, ByteBuffer targetBuffer);

    /**
     * Copy bytes from the given buffer at specified offset in this page, until either the limit of the buffer
     * is reached, or the end of the page is reached. The actual number of bytes copied is returned.
     *
     * @param sourceBuffer The buffer the data will be copied from.
     * @param targetOffset The offset into this page to copy to.
     * @return The number of bytes actually copied.
     */
    public abstract int copyFrom(ByteBuffer sourceBuffer, int targetOffset);

    /**
     * Shift the specified number of bytes starting from given offset the specified number of bytes to the left or
     * right. The area
     * left behind after the shift is not padded and thus is left with garbage.
     * <p>
     * Out of bounds flag is raised if either start or end of either source range or target range fall outside end of
     * this cursor
     * or if length is negative.
     *
     * @param sourceOffset The offset into this page to start moving from.
     * @param length The number of bytes to move.
     * @param shift How many steps, in terms of number of bytes, to shift. Can be both positive and negative.
     */
    public abstract void shiftBytes(int sourceOffset, int length, int shift);

    /**
     * Discern whether an out-of-bounds access has occurred since the last call to {@link #next()} or
     * {@link #next(long)}, or since the last call to {@link #shouldRetry()} that returned {@code true}, or since the
     * last call to this method.
     *
     * @return {@code true} if an access was out of bounds.
     */
    public abstract boolean checkAndClearBoundsFlag();

    /**
     * Check if a cursor error has been set on this or any linked cursor, and if so, remove it from the cursor
     * and throw it as a {@link CursorException}.
     */
    public abstract void checkAndClearCursorException() throws CursorException;

    /**
     * Set an error condition on the cursor with the given message.
     * <p>
     * This will make calls to {@link #checkAndClearCursorException()} throw a {@link CursorException} with the given
     * message, unless the error has gotten cleared by a {@link #shouldRetry()} call that returned {@code true},
     * a call to {@link #next()} or {@link #next(long)}, or the cursor is closed.
     *
     * @param message The message of the {@link CursorException} that {@link #checkAndClearCursorException()} will
     * throw.
     */
    public abstract void setCursorException(String message);

    /**
     * Unconditionally clear any error condition that has been set on this or any linked cursor, without throwing an
     * exception.
     */
    public abstract void clearCursorException();

    /**
     * Open a new page cursor with the same pf_flags as this cursor,
     * as if calling the {@link PagedFile#io(long, int, CursorContext)}
     * on the relevant paged file. This cursor will then also delegate to the linked cursor when checking
     * {@link #shouldRetry()} and {@link #checkAndClearBoundsFlag()}.
     * <p>
     * Closing a cursor also closes any linked cursor.
     * Opening a linked cursor on a cursor that already has an opened linked cursor is not possible.
     * Close the older linked cursor, or create linked cursor from the older linked cursor.
     *
     * @param pageId The page id that the linked cursor will be placed at after its first call to {@link #next()}.
     * @return A cursor that is linked with this cursor.
     */
    public abstract PageCursor openLinkedCursor(long pageId) throws IOException;

    /**
     * Sets all bytes in this page to zero, as if this page was newly allocated at the end of the file.
     */
    public abstract void zapPage();

    /**
     * @return {@code true} if this page cursor was opened with {@link PagedFile#PF_SHARED_WRITE_LOCK},
     * {@code false} otherwise.
     */
    public abstract boolean isWriteLocked();

    /**
     * Mark current page with provided horizon.
     * @param horizon MVCC page horizon.
     */
    public abstract void setPageHorizon(long horizon);

    /**
     * Unpins cursor releasing any locks that it was holding on the page that the cursor is currently positioned at.
     * It clears the page reference making all access to the cursor be out of bounds until the next call to {@link #next()} or {@link #next(long)}
     * This method doesn't affect where subsequent call to {@link #next()} will position the cursor.
     * This doesn't affect linked cursors if any.
     */
    public abstract void unpin();

    /**
     * @return the byte order used to read and write.
     */
    public abstract ByteOrder getByteOrder();
}
