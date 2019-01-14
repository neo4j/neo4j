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
package org.neo4j.bolt.v1.packstream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;

/**
 * An {@link PackInput} implementation that reads from an input channel into an internal buffer.
 */
public class BufferedChannelInput implements PackInput
{
    private final ByteBuffer buffer;
    private ReadableByteChannel channel;

    public BufferedChannelInput( int bufferCapacity )
    {
        this.buffer = ByteBuffer.allocate( bufferCapacity ).order( ByteOrder.BIG_ENDIAN );
    }

    public BufferedChannelInput reset( ReadableByteChannel ch )
    {
        this.channel = ch;
        this.buffer.position( 0 );
        this.buffer.limit( 0 );
        return this;
    }

    private boolean attempt( int numBytes ) throws IOException
    {
        if ( buffer.remaining() >= numBytes )
        {
            return true;
        }

        if ( buffer.remaining() > 0 )
        {
            // If there is data remaining in the buffer, shift that remaining data to the beginning of the buffer.
            buffer.compact();
        }
        else
        {
            buffer.clear();
        }

        int count;
        do
        {
            count = channel.read( buffer );
        }
        while ( count >= 0 && (buffer.position() < numBytes && buffer.remaining() != 0) );

        buffer.flip();
        return buffer.remaining() >= numBytes;
    }

    @Override
    public byte readByte() throws IOException
    {
        ensure( 1 );
        return buffer.get();
    }

    @Override
    public short readShort() throws IOException
    {
        ensure( 2 );
        return buffer.getShort();
    }

    @Override
    public int readInt() throws IOException
    {
        ensure( 4 );
        return buffer.getInt();
    }

    @Override
    public long readLong() throws IOException
    {
        ensure( 8 );
        return buffer.getLong();
    }

    @Override
    public double readDouble() throws IOException
    {
        ensure( 8 );
        return buffer.getDouble();
    }

    @Override
    public PackInput readBytes( byte[] into, int index, int toRead ) throws IOException
    {
        int endIndex = index + toRead;
        while ( index < endIndex )
        {
            toRead = Math.min( buffer.remaining(), endIndex - index );
            buffer.get( into, index, toRead );
            index += toRead;
            if ( buffer.remaining() == 0 && index < endIndex )
            {
                attempt( endIndex - index );
                if ( buffer.remaining() == 0 )
                {
                    throw new PackStream.EndOfStream( "Expected " + (endIndex - index) + " bytes available, " +
                                                      "but no more bytes accessible from underlying stream." );
                }
            }
        }
        return this;
    }

    @Override
    public byte peekByte() throws IOException
    {
        ensure( 1 );
        return buffer.get( buffer.position() );
    }

    private void ensure( int numBytes ) throws IOException
    {
        if ( !attempt( numBytes ) )
        {
            throw new PackStream.EndOfStream( "Unexpected end of stream while trying to read " + numBytes + " bytes." );
        }
    }
}
