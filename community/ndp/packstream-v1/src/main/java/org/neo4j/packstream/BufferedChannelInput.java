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
package org.neo4j.packstream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;

public class BufferedChannelInput implements PackInput
{
    private final ByteBuffer buffer;
    private ReadableByteChannel channel;

    public BufferedChannelInput( int bufferCapacity )
    {
        this.buffer = ByteBuffer.allocateDirect( bufferCapacity ).order( ByteOrder.BIG_ENDIAN );
    }

    public BufferedChannelInput reset( ReadableByteChannel ch )
    {
        this.channel = ch;
        this.buffer.position(0);
        this.buffer.limit(0);
        return this;
    }

    @Override
    public PackInput ensure( int numBytes ) throws IOException
    {
        if(!attempt( numBytes ))
        {
            throw new PackStream.EndOfStream( "Unexpected end of stream while trying to read " + numBytes + " bytes." );
        }
        return this;
    }

    @Override
    public PackInput attemptUpTo( int numBytes ) throws IOException
    {
        attempt( Math.min(numBytes, buffer.capacity()) );
        return this;
    }

    @Override
    public boolean attempt( int numBytes ) throws IOException
    {
        if(remaining() >= numBytes)
        {
            return true;
        }

        if(buffer.remaining() > 0)
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
        } while( count >= 0 && (buffer.position() < numBytes && buffer.remaining() != 0));

        buffer.flip();
        return buffer.remaining() >= numBytes;
    }

    @Override
    public byte get()
    {
        return buffer.get();
    }

    @Override
    public int remaining()
    {
        return buffer.remaining();
    }

    @Override
    public short getShort()
    {
        return buffer.getShort();
    }

    @Override
    public int getInt()
    {
        return buffer.getInt();
    }

    @Override
    public long getLong()
    {
        return buffer.getLong();
    }

    @Override
    public double getDouble()
    {
        return buffer.getDouble();
    }

    @Override
    public PackInput get( byte[] into, int offset, int toRead )
    {
        buffer.get(into, offset, toRead);
        return this;
    }

    @Override
    public byte peek()
    {
        return buffer.get(buffer.position());
    }
}
