/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.io.pagecache.impl;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.Page;

/** A page backed by a simple byte buffer. */
public class ByteBufferPage implements Page
{
    protected ByteBuffer buffer;

    public ByteBufferPage( ByteBuffer buffer )
    {
        this.buffer = buffer;
    }

    public byte getByte( int offset )
    {
        return buffer.get( offset );
    }

    public long getLong( int offset )
    {
        return buffer.getLong( offset );
    }

    public void putLong( long value, int offset )
    {
        buffer.putLong( offset, value );
    }

    public int getInt( int offset )
    {
        return buffer.getInt(offset);
    }

    public void putInt( int value, int offset )
    {
        buffer.putInt( offset, value );
    }

    public void getBytes( byte[] data, int offset )
    {
        for (int i = 0; i < data.length; i++)
        {
            data[i] = getByte( i + offset );
        }
    }

    public void putBytes( byte[] data, int offset )
    {
        for (int i = 0; i < data.length; i++)
        {
            putByte( data[i], offset + i );
        }
    }

    public void putByte( byte value, int offset )
    {
        buffer.put( offset, value );
    }

    public short getShort( int offset )
    {
        return buffer.getShort( offset );
    }

    public void putShort( short value, int offset )
    {
        buffer.putShort( offset, value );
    }

    @Override
    public int swapIn( StoreChannel channel, long offset, int length ) throws IOException
    {
        buffer.clear();
        buffer.limit( length );
        int bytesRead = 0;
        int read;
        do {
            read = channel.read( buffer, offset + bytesRead );
        } while ( read != -1 && (bytesRead += read) < length );

        // zero-fill the rest
        while ( buffer.position() < buffer.limit() )
        {
            buffer.put( (byte) 0 );
        }
        return bytesRead;
    }

    @Override
    public void swapOut( StoreChannel channel, long offset, int length ) throws IOException
    {
        // We duplicate the buffer here, so that our thread gets its own
        // position and limit to play with.
        // This is important because swapping out, unlike swapping in,
        // can happen concurrently to the same page.
        ByteBuffer duplicate = buffer.duplicate();
        duplicate.position( 0 );
        duplicate.limit( length );
        channel.writeAll( duplicate, offset );
    }
}
