/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A ByteBuffer that is lazily copied on clear. Keeps a duplication counter and
 * if asked to be cleared with duplicates open, it creates a new backing store
 * instead of clearing the current, leaving users of duplicated buffers
 * unbothered.
 * 
 * ByteBuffer hierarchy's constructors are all package private, so we cannot
 * extend. Instead we present ourselves as a wrapper that delegates all calls to
 * the wrapped reference but returns itself. Provided that the backing buffer is
 * not tampered with, holders of the result of duplicate should be safe from
 * clear() calls from users of the original.
 */
class CloseableByteBuffer
{
    private volatile ByteBuffer delegate;

    private final AtomicInteger dupCount;

    private CloseableByteBuffer( ByteBuffer delegate )
    {
        this.delegate = delegate;
        this.dupCount = new AtomicInteger( 0 );
    }

    public final void close()
    {
        dupCount.decrementAndGet();
    }

    public static CloseableByteBuffer wrap( ByteBuffer delegate )
    {
        return new CloseableByteBuffer( delegate );
    }

    public final int capacity()
    {
        return delegate.capacity();
    }

    public final int position()
    {
        return delegate.position();
    }

    public final Buffer position( int newPosition )
    {
        return delegate.position( newPosition );
    }

    public final int limit()
    {
        return delegate.limit();
    }

    public final CloseableByteBuffer limit( int newLimit )
    {
        delegate.limit( newLimit );
        return this;
    }

    public final CloseableByteBuffer mark()
    {
        delegate.mark();
        return this;
    }

    public final CloseableByteBuffer reset()
    {
        delegate.reset();
        return this;
    }

    public final CloseableByteBuffer clear()
    {
        if ( dupCount.get() > 0 )
        {
            delegate = ByteBuffer.allocate( capacity() );
        }
        else
        {
            delegate.clear();
        }
        return this;
    }

    public final CloseableByteBuffer flip()
    {
        delegate.flip();
        return this;
    }

    public final CloseableByteBuffer rewind()
    {
        delegate.rewind();
        return this;
    }

    public final int remaining()
    {
        return delegate.remaining();
    }

    public final boolean hasRemaining()
    {
        return delegate.hasRemaining();
    }

    public boolean isReadOnly()
    {
        return delegate.isReadOnly();
    }

    public CloseableByteBuffer slice()
    {
        delegate.slice();
        return this;
    }

    public CloseableByteBuffer duplicate()
    {
        dupCount.incrementAndGet();
        return CloseableByteBuffer.wrap( delegate.duplicate() );
    }

    public CharBuffer asCharBuffer()
    {
        return delegate.asCharBuffer();
    }

    ByteBuffer getDelegate()
    {
        return delegate;
    }

    public byte get()
    {
        return delegate.get();
    }

    public CloseableByteBuffer put( byte b )
    {
        delegate.put( b );
        return this;
    }

    public byte get( int index )
    {
        return delegate.get( index );
    }

    public CloseableByteBuffer put( int index, byte b )
    {
        delegate.put( index, b );
        return this;
    }

    public CloseableByteBuffer get( byte[] dst, int offset, int length )
    {
        delegate.get( dst, offset, length );
        return this;
    }

    public CloseableByteBuffer get( byte[] dst )
    {
        delegate.get( dst );
        return this;
    }

    public CloseableByteBuffer put( ByteBuffer src )
    {
        delegate.put( src );
        return this;
    }

    public CloseableByteBuffer put( byte[] src, int offset, int length )
    {
        delegate.put( src, offset, length );
        return this;
    }

    public final CloseableByteBuffer put( byte[] src )
    {
        delegate.put( src );
        return this;
    }

    public final boolean hasArray()
    {
        return delegate.hasArray();
    }

    public final byte[] array()
    {
        return delegate.array();
    }

    public final int arrayOffset()
    {
        return delegate.arrayOffset();
    }

    public CloseableByteBuffer compact()
    {
        delegate.compact();
        return this;
    }

    public boolean isDirect()
    {
        return delegate.isDirect();
    }

    public String toString()
    {
        return delegate.toString();
    }

    public int hashCode()
    {
        return delegate.hashCode();
    }

    public boolean equals( Object ob )
    {
        return delegate.equals( ob );
    }

    public int compareTo( ByteBuffer that )
    {
        return delegate.compareTo( that );
    }

    public final ByteOrder order()
    {
        return delegate.order();
    }

    public final CloseableByteBuffer order( ByteOrder bo )
    {
        delegate.order( bo );
        return this;
    }

    public char getChar()
    {
        return delegate.getChar();
    }

    public CloseableByteBuffer putChar( char value )
    {
        delegate.putChar( value );
        return this;
    }

    public char getChar( int index )
    {
        return delegate.getChar( index );
    }

    public CloseableByteBuffer putChar( int index, char value )
    {
        delegate.putChar( index, value );
        return this;
    }

    public short getShort()
    {
        return delegate.getShort();
    }

    public CloseableByteBuffer putShort( short value )
    {
        delegate.putShort( value );
        return this;
    }

    public short getShort( int index )
    {
        return delegate.getShort( index );
    }

    public CloseableByteBuffer putShort( int index, short value )
    {
        delegate.putShort( index, value );
        return this;
    }

    public int getInt()
    {
        return delegate.getInt();
    }

    public CloseableByteBuffer putInt( int value )
    {
        delegate.putInt( value );
        return this;
    }

    public int getInt( int index )
    {
        return delegate.getInt( index );
    }

    public CloseableByteBuffer putInt( int index, int value )
    {
        delegate.putInt( index, value );
        return this;
    }

    public long getLong()
    {
        return delegate.getLong();
    }

    public CloseableByteBuffer putLong( long value )
    {
        delegate.putLong( value );
        return this;
    }

    public long getLong( int index )
    {
        return delegate.getLong( index );
    }

    public CloseableByteBuffer putLong( int index, long value )
    {
        delegate.putLong( index, value );
        return this;
    }

    public float getFloat()
    {
        return delegate.getFloat();
    }

    public CloseableByteBuffer putFloat( float value )
    {
        delegate.putFloat( value );
        return this;
    }

    public float getFloat( int index )
    {
        return delegate.getFloat( index );
    }

    public CloseableByteBuffer putFloat( int index, float value )
    {
        delegate.putFloat( index, value );
        return this;
    }

    public double getDouble()
    {
        return delegate.getDouble();
    }

    public CloseableByteBuffer putDouble( double value )
    {
        delegate.putDouble( value );
        return this;
    }

    public double getDouble( int index )
    {
        return delegate.getDouble( index );
    }

    public CloseableByteBuffer putDouble( int index, double value )
    {
        delegate.putDouble( index, value );
        return this;
    }
}
