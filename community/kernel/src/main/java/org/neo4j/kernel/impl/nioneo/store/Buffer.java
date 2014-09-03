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
package org.neo4j.kernel.impl.nioneo.store;

import java.nio.ByteBuffer;

import sun.nio.ch.DirectBuffer;

/**
 * Wraps a <CODE>ByteBuffer</CODE> and is tied to a {@link PersistenceWindow}.
 * Using the {@link #setOffset(int)} method one can offset the buffer (within
 * the limits of the persistence window.
 * <p>
 * All the <CODE>put</CODE> and <CODE>get</CODE> methods of this class works
 * the same way as in <CODE>ByteBuffer</CODE>.
 *
 * @see ByteBuffer, PersistenceWindow
 */
public class Buffer
{
    private final ByteBuffer buf;
    private final PersistenceWindow persistenceWindow;

    public Buffer( PersistenceWindow persistenceWindow, ByteBuffer buf )
    {
        this.persistenceWindow = persistenceWindow;
        if ( buf == null )
        {
            throw new IllegalArgumentException( "null buf" );
        }
        this.buf = buf;
    }

    /**
     * Returns the underlying byte buffer.
     *
     * @return The byte buffer wrapped by this buffer
     */
    public ByteBuffer getBuffer()
    {
        assertOpen();
        return buf;
    }

    public void reset()
    {
        assertOpen();
        buf.clear();
    }

    /**
     * Sets the offset from persistence window position in the underlying byte
     * buffer.
     *
     * @param offset
     *            The new offset to set
     * @return This buffer
     */
    public Buffer setOffset( int offset )
    {
        assertOpen();
        try
        {
            buf.position( offset );
        }
        catch ( java.lang.IllegalArgumentException e )
        {
            throw new IllegalArgumentException( "Illegal offset " + offset +
                    " for buffer:" + buf, e );
        }
        return this;
    }

    /**
     * Returns the offset of this buffer.
     *
     * @return The offset
     */
    public int getOffset()
    {
        assertOpen();
        return buf.position();
    }

    /**
     * Puts a <CODE>byte</CODE> into the underlying buffer.
     *
     * @param b
     *            The <CODE>byte</CODE> that will be written
     * @return This buffer
     */
    public Buffer put( byte b )
    {
        assertOpen();
        buf.put( b );
        return this;
    }

    /**
     * Puts a <CODE>int</CODE> into the underlying buffer.
     *
     * @param i
     *            The <CODE>int</CODE> that will be written
     * @return This buffer
     */
    public Buffer putInt( int i )
    {
        assertOpen();
        buf.putInt( i );
        return this;
    }

    /**
     * Puts a <CODE>long</CODE> into the underlying buffer.
     *
     * @param l
     *            The <CODE>long</CODE> that will be written
     * @return This buffer
     */
    public Buffer putLong( long l )
    {
        assertOpen();
        buf.putLong( l );
        return this;
    }

    /**
     * Reads and returns a <CODE>byte</CODE> from the underlying buffer.
     *
     * @return The <CODE>byte</CODE> value at the current position/offset
     */
    public byte get()
    {
        assertOpen();
        return buf.get();
    }

    /**
     * Reads and returns a <CODE>int</CODE> from the underlying buffer.
     *
     * @return The <CODE>int</CODE> value at the current position/offset
     */
    public int getInt()
    {
        assertOpen();
        return buf.getInt();
    }

    public long getUnsignedInt()
    {
        assertOpen();
        return buf.getInt()&0xFFFFFFFFL;
    }

    /**
     * Reads and returns a <CODE>long</CODE> from the underlying buffer.
     *
     * @return The <CODE>long</CODE> value at the current position/offset
     */
    public long getLong()
    {
        assertOpen();
        return buf.getLong();
    }

    /**
     * Puts a <CODE>byte array</CODE> into the underlying buffer.
     *
     * @param src
     *            The <CODE>byte array</CODE> that will be written
     * @return This buffer
     */
    public Buffer put( byte src[] )
    {
        assertOpen();
        buf.put( src );
        return this;
    }

    public Buffer put( char src[] )
    {
        assertOpen();
        int oldPos = buf.position();
        buf.asCharBuffer().put( src );
        buf.position( oldPos + src.length * 2 );
        return this;
    }

    /**
     * Puts a <CODE>byte array</CODE> into the underlying buffer starting from
     * <CODE>offset</CODE> in the array and writing <CODE>length</CODE>
     * values.
     *
     * @param src
     *            The <CODE>byte array</CODE> to write values from
     * @param offset
     *            The offset in the <CODE>byte array</CODE>
     * @param length
     *            The number of bytes to write
     * @return This buffer
     */
    public Buffer put( byte src[], int offset, int length )
    {
        assertOpen();
        buf.put( src, offset, length );
        return this;
    }

    /**
     * Reads <CODE>byte array length</CODE> bytes into the
     * <CODE>byte array</CODE> from the underlying buffer.
     *
     * @param dst
     *            The byte array to read values into
     * @return This buffer
     */
    public Buffer get( byte dst[] )
    {
        assertOpen();
        buf.get( dst );
        return this;
    }

    public Buffer get( char dst[] )
    {
        assertOpen();
        buf.asCharBuffer().get( dst );
        return this;
    }

    public synchronized void close()
    {
        if(buf.limit() == 0)
        {
            throw new IllegalStateException( "This buffer is already closed, it cannot be closed twice." );
        }
        buf.limit( 0 );
        if(buf instanceof DirectBuffer)
        {
            ((DirectBuffer)buf).cleaner().clean();
        }
    }

    private void assertOpen()
    {
        assert buf.limit() > 0 : "This buffer is closed, it is not legal to access it anymore.";
    }

    @Override
    public String toString()
    {
        return "Buffer[[" + buf.position() + "," + buf.capacity() + "]," +
            persistenceWindow + "]";
    }
}
