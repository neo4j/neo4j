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
package org.neo4j.kernel.api.impl.index.storage.paged;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

public class PagedIndexInput extends IndexInput implements RandomAccessInput
{
    /**
     * Fast bitwise modulo the page size: (x & moduloPageSize) == (x % pageSize).
     * Before this change, calculating where to position the cursor was 50% of the overhead of this class.
     */
    private final int moduloPageSize;

    /**
     * Fast bitwise division by page size: (x >> divideByPageSize) == (x / pageSize)
     */
    private final int divideByPageSize;

    /**
     * Tracks open resources across cloned inputs, see {@link InputResources}.
     */
    final InputResources resources;

    /** Star of the show */
    PageCursor cursor;

    /**
     * The logical size of this input. Same as file size if this is a root
     * input, but can be anything <= file size if this is a clone
     */
    private final long length;

    /** Fast access to pagedFile.pageSize() */
    private final int pageSize;

    /**
     * Position in the underlying file that this input starts at. For root
     * inputs this will be 0, for slices and clones of slices it will be >= 0.
     */
    private final long startPosition;

    /**
     * Last page this input can access - last actual page for roots,
     * any page for clones
     */
    private final long endPageId;

    /**
     * Final offset in last page this input can access - last actual
     * offset for root, any offset for clones
     */
    private final int endPageOffset;

    private long currentPageId;
    private int currentPageOffset;

    PagedIndexInput( String resourceDescription, PagedFile pagedFile, long startPosition, long size ) throws IOException
    {
        this( new InputResources.RootInputResources( pagedFile ), resourceDescription, startPosition,
                pagedFile.pageSize(), size );
    }

    PagedIndexInput( InputResources resources, String resourceDescription, long startPosition, int pageSize, long size )
            throws IOException
    {
        super( resourceDescription );

        if ( Integer.bitCount( pageSize ) != 1 )
        {
            throw new AssertionError(
                    String.format( "Only power-of-two page sizes are supported, got %d.", pageSize ) );
        }

        this.moduloPageSize = pageSize - 1;
        this.divideByPageSize = Integer.numberOfTrailingZeros( pageSize );
        this.pageSize = pageSize;
        this.length = size;
        this.startPosition = startPosition;
        this.currentPageId = pageId( 0 );
        this.currentPageOffset = pageOffset( 0 );
        this.endPageId = pageId( startPosition + size );
        this.endPageOffset = lastPageOffset( startPosition + size, pageSize );

        this.resources = resources;
        this.cursor = resources.openCursor( currentPageId, this );
    }

    private static int lastPageOffset( long fileSize, int pageSize )
    {
        if ( fileSize == 0 )
        {
            return 0;
        }
        int size = (int) (fileSize % pageSize);
        return size == 0 ? pageSize : size;
    }

    private long pageId( long position )
    {
        return (startPosition + position) >> divideByPageSize;
    }

    private int pageOffset( long position )
    {
        return (int) ((startPosition + position) & moduloPageSize);
    }

    @Override
    public final byte readByte() throws IOException
    {
        byte val = readByte( currentPageId, currentPageOffset );
        incrementPageOffset( 1 );
        return val;
    }

    @Override
    public byte readByte( long pos ) throws IOException
    {
        return readByte( pageId( pos ), pageOffset( pos ) );
    }

    private byte readByte( long pageId, int pageOffset ) throws IOException
    {
        moveCursorTo( pageId, pageOffset );

        byte val;
        do
        {
            val = cursor.getByte( pageOffset );
        }
        while ( cursor.shouldRetry() );

        if ( cursor.checkAndClearBoundsFlag() )
        {
            throw newEOFException( "Cursor bounds check failed" );
        }

        return val;
    }

    @Override
    public short readShort() throws IOException
    {
        short val = readShort( currentPageId, currentPageOffset );
        incrementPageOffset( 2 );
        return val;
    }

    @Override
    public short readShort( long pos ) throws IOException
    {
        return readShort( pageId( pos ), pageOffset( pos ) );
    }

    private short readShort( long pageId, int pageOffset ) throws IOException
    {
        // Cross-page read?
        if ( pageSize - pageOffset < 2 )
        {
            byte byte1 = readByte( pageId, pageOffset );
            byte byte2 = readByte( pageId + 1, 0 );
            return (short) (((byte1 & 0xFF) << 8) | (byte2 & 0xFF));
        }

        // Regular read
        moveCursorTo( pageId, pageOffset );

        short val;
        do
        {
            val = cursor.getShort( pageOffset );
        }
        while ( cursor.shouldRetry() );

        if ( cursor.checkAndClearBoundsFlag() )
        {
            throw newEOFException( "Cursor bounds check failed" );
        }

        return val;
    }

    @Override
    public int readInt() throws IOException
    {
        int val = readInt( currentPageId, currentPageOffset );
        incrementPageOffset( 4 );
        return val;
    }

    @Override
    public int readInt( long pos ) throws IOException
    {
        long pageId = (startPosition + pos) >> divideByPageSize;
        int pageOffset = (int) ((startPosition + pos) & moduloPageSize);
        return readInt( pageId, pageOffset );
    }

    private int readInt( long pageId, int pageOffset ) throws IOException
    {
        // 1. Optimistic fast path, relies on bounds check at checkAndClearBoundsFlag
        moveCursorTo( pageId, pageOffset );

        int val;
        do
        {
            // This may totally fail, since we may be reading an int that's split
            // across multiple pages - meaning this call is reading more bytes than
            // what fits in current page. If that happens, checkAndClearBoundsFlag
            // saves us, and we fall back to the slow path readPageCrossingInt
            val = cursor.getInt( pageOffset );
        }
        while ( cursor.shouldRetry() );

        // Our savior!
        if ( !cursor.checkAndClearBoundsFlag() )
        {
            // Fast path worked
            return val;
        }

        // 2. Slow path
        return readPageCrossingInt( pageId, pageOffset );
    }

    private int readPageCrossingInt( long pageId, int pageOffset ) throws IOException
    {
        // Note: This relies on callers already having called moveCursorTo( pageId, pageOffset )!

        // This beast is to avoid calling shouldRetry() four times to read a cross-boundary integer;
        // this optimization improved performance ~20% for for memory-resident indexes in
        // FindNodeNonUnique.countNodesWithLabelKeyValueWhenSelectivityLow.
        // This optimization also being used by readLong(..), since it converts to two readInt(..) on page-cross.
        byte b1, b2, b3, b4;
        int bytesLeftInPage = pageSize - pageOffset;
        if ( bytesLeftInPage == 1 )
        {
            do
            {
                b1 = cursor.getByte( pageOffset );
            }
            while ( cursor.shouldRetry() );

            moveCursorTo( pageId + 1, 0 );
            do
            {
                b2 = cursor.getByte( 0 );
                b3 = cursor.getByte( 1 );
                b4 = cursor.getByte( 2 );
            }
            while ( cursor.shouldRetry() );
        }
        else if ( bytesLeftInPage == 2 )
        {
            do
            {
                b1 = cursor.getByte( pageOffset );
                b2 = cursor.getByte( pageOffset + 1 );
            }
            while ( cursor.shouldRetry() );

            moveCursorTo( pageId + 1, 0 );
            do
            {
                b3 = cursor.getByte( 0 );
                b4 = cursor.getByte( 1 );
            }
            while ( cursor.shouldRetry() );
        }
        else
        {
            do
            {
                b1 = cursor.getByte( pageOffset );
                b2 = cursor.getByte( pageOffset + 1 );
                b3 = cursor.getByte( pageOffset + 2 );
            }
            while ( cursor.shouldRetry() );

            moveCursorTo( pageId + 1, 0 );
            do
            {
                b4 = cursor.getByte( 0 );
            }
            while ( cursor.shouldRetry() );
        }

        if ( cursor.checkAndClearBoundsFlag() )
        {
            throw newEOFException( "Cursor bounds check failed" );
        }

        return ((b1 & 0xFF) << 24) | ((b2 & 0xFF) << 16) | ((b3 & 0xFF) << 8) | (b4 & 0xFF);
    }

    @Override
    public long readLong() throws IOException
    {
        long val = readLong( currentPageId, currentPageOffset );
        incrementPageOffset( 8 );
        return val;
    }

    @Override
    public long readLong( long pos ) throws IOException
    {
        return readLong( pageId( pos ), pageOffset( pos ) );
    }

    private long readLong( long pageId, int pageOffset ) throws IOException
    {
        // Cross-page read?
        if ( pageSize - pageOffset < 8 )
        {
            // Delegate to readInt, which delegates to readPageCrossingInt
            int secondPageOffset = (pageOffset + 4) & moduloPageSize;
            long secondPage = secondPageOffset < pageOffset ? pageId + 1 : pageId;

            int int1 = readInt( pageId, pageOffset );
            int int2 = readInt( secondPage, secondPageOffset );
            return (((long) int1) << 32) | (int2 & 0xFFFFFFFFL);
        }

        // Regular read
        moveCursorTo( pageId, pageOffset );

        long val;
        do
        {
            val = cursor.getLong( pageOffset );
        }
        while ( cursor.shouldRetry() );

        if ( cursor.checkAndClearBoundsFlag() )
        {
            throw newEOFException( "Cursor bounds check failed" );
        }

        return val;
    }

    @Override
    public final void readBytes( byte[] b, int offset, int len ) throws IOException
    {
        int bytesRead = 0;

        while ( bytesRead < len )
        {
            moveCursorTo( currentPageId, currentPageOffset );

            int toRead = Math.min( len - bytesRead, pageSize - currentPageOffset );
            do
            {
                cursor.setOffset( currentPageOffset );
                cursor.getBytes( b, offset + bytesRead, toRead );
            }
            while ( cursor.shouldRetry() );

            if ( cursor.checkAndClearBoundsFlag() )
            {
                throw newEOFException( "Cursor bounds check failed" );
            }

            incrementPageOffset( toRead );
            bytesRead += toRead;
        }
    }

    @Override
    public void seek( long pos ) throws IOException
    {
        long newPageId = pageId( pos );
        int newOffset = pageOffset( pos );

        if ( isEOF( newPageId, newOffset ) )
        {
            throw newSeekEOFException( pos, newPageId, newOffset );
        }

        currentPageId = newPageId;
        currentPageOffset = newOffset;
    }

    @Override
    public void skipBytes( long numBytes ) throws IOException
    {
        long overflowingOffset = currentPageOffset + numBytes;
        long newPageId = currentPageId + overflowingOffset >> divideByPageSize;
        int newOffset = (int) (overflowingOffset & moduloPageSize);

        if ( isEOF( newPageId, newOffset ) )
        {
            throw newSkipBytesEOFException( newPageId, newOffset );
        }

        currentPageId = newPageId;
        currentPageOffset = newOffset;
    }

    /**
     * @return the logical position of this slice, 0 being start of slice
     */
    @Override
    public long getFilePointer()
    {
        return currentPageId * pageSize + currentPageOffset - startPosition;
    }

    @Override
    public final long length()
    {
        return length;
    }

    @Override
    public final PagedIndexInput clone()
    {
        PagedIndexInput slice = slice( toString(), 0, length() );
        slice.currentPageId = currentPageId;
        slice.currentPageOffset = currentPageOffset;
        return slice;
    }

    /**
     * Creates a slice of this index input, with the given description, offset,
     * and length. The slice is seeked to the beginning.
     */
    @Override
    public final PagedIndexInput slice( String desc, long offset, long length )
    {
        // Implementation notes:
        // This is used for both slice() and clone()
        // - Lucene does not close these child inputs
        // - Lucene may create infinitely many of these, dumping them to be GCd as it goes
        // - Lucene will access the clones concurrently with each other and their root
        if ( offset < 0 || length < 0 || offset + length > this.length )
        {
            throw new IllegalArgumentException(
                    "slice() " + desc + " out of bounds: offset=" + offset + ",length=" + length + ",fileLength=" +
                            this.length + ": " + this );
        }

        try
        {
            return new PagedIndexInput( resources.cloneResources(), desc, startPosition + offset, pageSize, length );
        }
        catch ( IOException e )
        {
            // Can't throw checked IOException due to the Lucene API
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public void close() throws IOException
    {
        resources.close( this );
    }

    private void incrementPageOffset( int byOffset )
    {
        currentPageOffset += byOffset;
        if ( currentPageOffset >= pageSize )
        {
            currentPageId += 1;
            currentPageOffset -= pageSize;
            assert currentPageOffset <= pageSize : "Should never read past two page boundaries";
        }
    }

    private void moveCursorTo( long pageId, int offsetInPage ) throws IOException
    {
        if ( isEOF( pageId, offsetInPage ) )
        {
            throw newCursorMoveEOFException( pageId, offsetInPage );
        }

        if ( !cursor.next( pageId ) )
        {
            throw newCursorMoveEOFException( pageId, offsetInPage );
        }
    }

    private boolean isEOF( long pageId, int offsetInPage )
    {
        return pageId >= endPageId && (pageId != endPageId || offsetInPage >= endPageOffset);
    }

    private EOFException newCursorMoveEOFException( long pageId, int offsetInPage )
    {
        // These are extracted from hot methods to keep bytecode down, to help the JIT
        return newEOFException( String.format( "EOF at { pageId: %d, offsetInPage: %d } %s", pageId, offsetInPage,
                describeCurrentPosition() ) );
    }

    private EOFException newSeekEOFException( long pos, long newPageId, int newOffset )
    {
        // These are extracted from hot methods to keep bytecode down, to help the JIT
        return newEOFException(
                String.format( "seek EOF check failed for { pos: %d, pageId: %d, offsetInPage: %d }", pos, newPageId,
                        newOffset ) );
    }

    private EOFException newSkipBytesEOFException( long newPageId, int newOffset )
    {
        // These are extracted from hot methods to keep bytecode down, to help the JIT
        return newEOFException(
                String.format( "skipBytes EOF check failed for { pageId: %d, offsetInPage: %d }", newPageId,
                        newOffset ) );
    }

    private EOFException newEOFException( String message )
    {
        return new EOFException( String.format( "Input [%s] read past EOF. Please ensure you are using the latest " +
                        "version of Neo4j and if you are, file a bug report including this message. Details: %s " + "%s", this,
                message, describeCurrentPosition() ) );
    }

    private String describeCurrentPosition()
    {
        return String.format(
                "{ startPosition: %d, currentPageId: %d,  currentPageOffset: %d, endPageId: %d, endPageOffset: %d, " +
                        "length: %d, pageSize: %d }", startPosition, currentPageId, currentPageOffset, endPageId,
                endPageOffset, length, pageSize );
    }
}
