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
package org.neo4j.io.pagecache.impl;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.io.pagecache.CursorException;
import org.neo4j.io.pagecache.PageCursor;

/**
 * A CompositePageCursor is a seamless view over parts of two other PageCursors.
 * @see #compose(PageCursor, int, PageCursor, int)
 */
public class CompositePageCursor extends PageCursor
{
    private final PageCursor first;
    private final int firstBaseOffset;
    private final int firstLength;
    private final PageCursor second;
    private final int secondBaseOffset;
    private final int secondLength;
    private int offset;
    private PageCursor byteAccessCursor;
    private boolean outOfBounds;

    // Constructed with static factory methods.
    private CompositePageCursor(
            PageCursor first, int firstBaseOffset, int firstLength,
            PageCursor second, int secondBaseOffset, int secondLength )
    {
        this.first = first;
        this.firstBaseOffset = firstBaseOffset;
        this.firstLength = firstLength;
        this.second = second;
        this.secondBaseOffset = secondBaseOffset;
        this.secondLength = secondLength;
        byteAccessCursor = new DelegatingPageCursor( this )
        {
            private int offset;
            @Override
            public int getInt()
            {
                int a = getByte( offset ) & 0xFF;
                int b = getByte( offset + 1 ) & 0xFF;
                int c = getByte( offset + 2 ) & 0xFF;
                int d = getByte( offset + 3 ) & 0xFF;
                int v = (a << 24) | (b << 16) | (c << 8) | d;
                return v;
            }

            @Override
            public int getInt( int offset )
            {
                this.offset = offset;
                return getInt();
            }

            @Override
            public short getShort()
            {
                int a = getByte( offset ) & 0xFF;
                int b = getByte( offset + 1 ) & 0xFF;
                int v = (a << 8) | b;
                return (short) v;
            }

            @Override
            public short getShort( int offset )
            {
                this.offset = offset;
                return getShort();
            }

            @Override
            public long getLong()
            {
                long a = getByte( offset ) & 0xFF;
                long b = getByte( offset + 1 ) & 0xFF;
                long c = getByte( offset + 2 ) & 0xFF;
                long d = getByte( offset + 3 ) & 0xFF;
                long e = getByte( offset + 4 ) & 0xFF;
                long f = getByte( offset + 5 ) & 0xFF;
                long g = getByte( offset + 6 ) & 0xFF;
                long h = getByte( offset + 7 ) & 0xFF;
                long v = (a << 56) | (b << 48) | (c << 40) | (d << 32) | (e << 24) | (f << 16) | (g << 8) | h;
                return v;
            }

            @Override
            public long getLong( int offset )
            {
                this.offset = offset;
                return getLong();
            }

            @Override
            public void getBytes( byte[] data )
            {
                for ( int i = 0; i < data.length; i++ )
                {
                    data[i] = getByte( offset + i );
                }
            }

            @Override
            public void putInt( int value )
            {
                putByte( offset    , (byte)  (value >> 24) );
                putByte( offset + 1, (byte) ((value >> 16) & 0xFF) );
                putByte( offset + 2, (byte) ((value >>  8) & 0xFF) );
                putByte( offset + 3, (byte) (value & 0xFF) );
            }

            @Override
            public void putInt( int offset, int value )
            {
                this.offset = offset;
                putInt( value );
            }

            @Override
            public void putShort( short value )
            {
                putByte( offset    , (byte)  (value >>  8) );
                putByte( offset + 1, (byte) (value & 0xFF) );
            }

            @Override
            public void putShort( int offset, short value )
            {
                this.offset = offset;
                putShort( value );
            }

            @Override
            public void putLong( long value )
            {
                putByte( offset    , (byte)  (value >> 56) );
                putByte( offset + 1, (byte) ((value >> 48) & 0xFF) );
                putByte( offset + 2, (byte) ((value >> 40) & 0xFF) );
                putByte( offset + 3, (byte) ((value >> 32) & 0xFF) );
                putByte( offset + 4, (byte) ((value >> 24) & 0xFF) );
                putByte( offset + 5, (byte) ((value >> 16) & 0xFF) );
                putByte( offset + 6, (byte) ((value >>  8) & 0xFF) );
                putByte( offset + 7, (byte) (value & 0xFF) );
            }

            @Override
            public void putLong( int offset, long value )
            {
                this.offset = offset;
                putLong( value );
            }

            @Override
            public void putBytes( byte[] data )
            {
                for ( int i = 0; i < data.length; i++ )
                {
                    putByte( offset + i, data[i] );
                }
            }

            @Override
            public void setOffset( int offset )
            {
                this.offset = offset;
            }
        };
    }

    private PageCursor cursor( int width )
    {
        return cursor( offset, width );
    }

    private PageCursor cursor( int offset, int width )
    {
        outOfBounds |= offset + width > firstLength + secondLength;
        if ( offset < firstLength )
        {
            return offset + width <= firstLength ? first : byteCursor( offset );
        }
        return second;

    }

    private PageCursor byteCursor( int offset )
    {
        byteAccessCursor.setOffset( offset );
        return byteAccessCursor;
    }

    private int relative( int offset )
    {
        return offset < firstLength ? firstBaseOffset + offset : secondBaseOffset + (offset - firstLength);
    }

    @Override
    public byte getByte()
    {
        PageCursor cursor = cursor( Byte.BYTES );
        byte b = cursor.getByte();
        offset++;
        return b;
    }

    @Override
    public byte getByte( int offset )
    {
        return cursor( offset, Byte.BYTES ).getByte( relative( offset ) );
    }

    @Override
    public void putByte( byte value )
    {
        PageCursor cursor = cursor( Byte.BYTES );
        cursor.putByte( value );
        offset++;
    }

    @Override
    public void putByte( int offset, byte value )
    {
        cursor( offset, Byte.BYTES ).putByte( relative( offset ), value );
    }

    @Override
    public long getLong()
    {
        long l = cursor( Long.BYTES ).getLong();
        offset += Long.BYTES;
        return l;
    }

    @Override
    public long getLong( int offset )
    {
        return cursor( offset, Long.BYTES ).getLong( relative( offset ) );
    }

    @Override
    public void putLong( long value )
    {
        cursor( Long.BYTES ).putLong( value );
        offset += Long.BYTES;
    }

    @Override
    public void putLong( int offset, long value )
    {
        cursor( offset, Long.BYTES ).putLong( relative( offset ), value );
    }

    @Override
    public int getInt()
    {
        int i = cursor( Integer.BYTES ).getInt();
        offset += Integer.BYTES;
        return i;
    }

    @Override
    public int getInt( int offset )
    {
        return cursor( offset, Integer.BYTES ).getInt( relative( offset ) );
    }

    @Override
    public void putInt( int value )
    {
        PageCursor cursor = cursor( Integer.BYTES );
        cursor.putInt( value );
        offset += Integer.BYTES;
    }

    @Override
    public void putInt( int offset, int value )
    {
        cursor( offset, Integer.BYTES ).putInt( relative( offset ), value );
    }

    @Override
    public void getBytes( byte[] data )
    {
        cursor( data.length ).getBytes( data );
        offset += data.length;
    }

    @Override
    public void getBytes( byte[] data, int arrayOffset, int length )
    {
        throw new UnsupportedOperationException( "Composite page cursor does not yet support this operation" );
    }

    @Override
    public void putBytes( byte[] data )
    {
        cursor( data.length ).putBytes( data );
        offset += data.length;
    }

    @Override
    public void putBytes( byte[] data, int arrayOffset, int length )
    {
        throw new UnsupportedOperationException( "Composite page cursor does not yet support this operation" );
    }

    @Override
    public void putBytes( int bytes, byte value )
    {
        throw new UnsupportedOperationException( "Composite page cursor does not yet support this operation" );
    }

    @Override
    public short getShort()
    {
        short s = cursor( Short.BYTES ).getShort();
        offset += Short.BYTES;
        return s;
    }

    @Override
    public short getShort( int offset )
    {
        return cursor( offset, Short.BYTES ).getShort( relative( offset ) );
    }

    @Override
    public void putShort( short value )
    {
        cursor( Short.BYTES ).putShort( value );
        offset += Short.BYTES;
    }

    @Override
    public void putShort( int offset, short value )
    {
        cursor( offset, Short.BYTES ).putShort( relative( offset ), value );
    }

    @Override
    public void setOffset( int offset )
    {
        if ( offset < firstLength )
        {
            first.setOffset( firstBaseOffset + offset );
            second.setOffset( secondBaseOffset );
        }
        else
        {
            first.setOffset( firstBaseOffset + firstLength );
            second.setOffset( secondBaseOffset + (offset - firstLength) );
        }
        this.offset = offset;
    }

    @Override
    public int getOffset()
    {
        return offset;
    }

    @Override
    public long getCurrentPageId()
    {
        return cursor( 0 ).getCurrentPageId();
    }

    @Override
    public int getCurrentPageSize()
    {
        throw new UnsupportedOperationException( "Getting current page size is not supported on compose PageCursor" );
    }

    @Override
    public File getCurrentFile()
    {
        return null;
    }

    @Override
    public void rewind()
    {
        first.setOffset( firstBaseOffset );
        second.setOffset( secondBaseOffset );
        offset = 0;
    }

    @Override
    public boolean next()
    {
        throw unsupportedNext();
    }

    private UnsupportedOperationException unsupportedNext()
    {
        return new UnsupportedOperationException(
                "Composite cursor does not support next operation. Please operate directly on underlying cursors." );
    }

    @Override
    public boolean next( long pageId )
    {
        throw unsupportedNext();
    }

    @Override
    public void close()
    {
        first.close();
        second.close();
    }

    @Override
    public boolean shouldRetry() throws IOException
    {
        boolean needsRetry = first.shouldRetry() | second.shouldRetry();
        if ( needsRetry )
        {
            rewind();
            checkAndClearBoundsFlag();
        }
        return needsRetry;
    }

    @Override
    public int copyTo( int sourceOffset, PageCursor targetCursor, int targetOffset, int lengthInBytes )
    {
        throw new UnsupportedOperationException( "Composite cursor does not support copyTo functionality." );
    }

    @Override
    public int copyTo( int sourceOffset, ByteBuffer targetBuffer )
    {
        throw new UnsupportedOperationException( "Composite cursor does not support copyTo functionality." );
    }

    @Override
    public void shiftBytes( int sourceOffset, int length, int shift )
    {
        throw new UnsupportedOperationException( "Composite cursor does not support shiftBytes functionality... yet." );
    }

    @Override
    public boolean checkAndClearBoundsFlag()
    {
        boolean bounds = outOfBounds | first.checkAndClearBoundsFlag() | second.checkAndClearBoundsFlag();
        outOfBounds = false;
        return bounds;
    }

    @Override
    public void checkAndClearCursorException() throws CursorException
    {
        first.checkAndClearCursorException();
        second.checkAndClearCursorException();
    }

    @Override
    public void raiseOutOfBounds()
    {
        outOfBounds = true;
    }

    @Override
    public void setCursorException( String message )
    {
        cursor( 0 ).setCursorException( message );
    }

    @Override
    public void clearCursorException()
    {
        first.clearCursorException();
        second.clearCursorException();
    }

    @Override
    public PageCursor openLinkedCursor( long pageId )
    {
        throw new UnsupportedOperationException( "Linked cursors are not supported for composite cursors" );
    }

    @Override
    public void zapPage()
    {
        first.zapPage();
        second.zapPage();
    }

    @Override
    public boolean isWriteLocked()
    {
        return first.isWriteLocked() && second.isWriteLocked();
    }

    /**
     * Build a CompositePageCursor that is a view of the first page cursor from its current offset through the given
     * length, concatenated with the second cursor from its current offset through the given length. The offsets are
     * changed as part of accessing the underlying cursors through the composite cursor. However, the size and position
     * of the view does NOT change if the offsets of the underlying cursors are changed after constructing the composite
     * cursor.
     * <p>
     * Not all cursor operations are supported on composite cursors, notably {@link #next()} and {@link #next(long)} are
     * not supported.
     * Most things work as you would expect, though. For instance, {@link #shouldRetry()} will delegate the check to
     * both of the underlying cursors, as will {@link #checkAndClearBoundsFlag()}.
     * <p>
     * The composite cursor also has its own bounds flag built in, which will be raised if an access is made outside of
     * the composite view.
     *
     * <pre>
     *     offset      first length            offset     second length
     *        │              │                    │              │
     * ┌──────▼──────────────▼────────┐   ┌───────▼──────────────▼───────┐
     * │         first cursor         │   │        second cursor         │
     * └──────▼──────────────▼────────┘   └───────▼──────────────▼───────┘
     *        └──────────┐   └──────────┐┌────────┘     ┌────────┘
     *                   ▼──────────────▼▼──────────────▼
     *                   │       composite cursor       │
     *                   └──────────────────────────────┘
     *             offset = 0, page size = first length + second length
     * </pre>
     * @param first The cursor that will form the first part of this composite cursor, from its current offset.
     * @param firstLength The number of bytes from the first cursor that will participate in the composite view.
     * @param second The cursor that will form the second part of this composite cursor, from its current offset.
     * @param secondLength The number of bytes from the second cursor that will participate in the composite view.
     * @return A cursor that is a composed view of the given parts of the two given cursors.
     */
    public static PageCursor compose( PageCursor first, int firstLength, PageCursor second, int secondLength )
    {
        return new CompositePageCursor( first, first.getOffset(), firstLength, second, second.getOffset(), secondLength );
    }
}
