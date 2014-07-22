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
package org.neo4j.io.pagecache.impl.common;

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

    @Override
    public byte getByte( int offset )
    {
        return buffer.get( offset );
    }

    @Override
    public long getLong( int offset )
    {
        return buffer.getLong( offset );
    }

    @Override
    public void putLong( long value, int offset )
    {
        buffer.putLong( offset, value );
    }

    @Override
    public int getInt( int offset )
    {
        return buffer.getInt(offset);
    }

    @Override
    public void putInt( int value, int offset )
    {
        buffer.putInt( offset, value );
    }

    @Override
    public void getBytes( byte[] data, int offset )
    {
        for (int i = 0; i < data.length; i++)
        {
            data[i] = getByte( i + offset );
        }
    }

    @Override
    public void putBytes( byte[] data, int offset )
    {
        for (int i = 0; i < data.length; i++)
        {
            putByte( data[i], offset + i );
        }
    }

    @Override
    public void putByte( byte value, int offset )
    {
        buffer.put( offset, value );
    }

    @Override
    public short getShort( int offset )
    {
        return buffer.getShort( offset );
    }

    @Override
    public void putShort( short value, int offset )
    {
        buffer.putShort( offset, value );
    }

    @Override
    public void swapIn( StoreChannel channel, long offset, int length ) throws IOException
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
    }

    @Override
    public void swapOut( StoreChannel channel, long offset, int length ) throws IOException
    {
        buffer.position( 0 );
        channel.writeAll( buffer, offset );
    }
}
