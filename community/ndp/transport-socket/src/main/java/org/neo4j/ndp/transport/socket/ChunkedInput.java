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
package org.neo4j.ndp.transport.socket;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.packstream.PackInput;
import org.neo4j.packstream.PackStream;

public class ChunkedInput implements PackInput
{
    private List<ByteBuf> chunks = new ArrayList<>();
    private ByteBuf currentChunk = null;
    private int currentChunkIndex = -1;

    private int remaining = 0;

    public ChunkedInput clear()
    {
        currentChunk = null;
        currentChunkIndex = -1;
        remaining = 0;

        // Release references to all buffers
        for ( int i = 0; i < chunks.size(); i++ )
        {
            chunks.get( i ).release();
        }

        if ( chunks.size() > 128 )
        {
            // faster to allocate a new one than to release if the list is large
            chunks = new ArrayList<>();
        }
        else
        {
            chunks.clear();
        }
        return this;
    }

    public void addChunk( ByteBuf chunk )
    {
        if ( chunk.readableBytes() > 0 )
        {
            chunks.add( chunk.retain() );
            remaining += chunk.readableBytes();
        }
    }

    @Override
    public PackInput ensure( int numBytes ) throws IOException
    {
        if ( !attempt( numBytes ) )
        {
            throw new PackStream.EndOfStream( "Unexpected end of stream while trying to read " + numBytes + " bytes." );
        }
        return this;
    }

    @Override
    public PackInput attemptUpTo( int numBytes ) throws IOException
    {
        return this;
    }

    @Override
    public boolean attempt( int numBytes ) throws IOException
    {
        return remaining >= numBytes;
    }

    @Override
    public int remaining()
    {
        return remaining;
    }

    @Override
    public byte get()
    {
        ensureChunkAvailable();
        remaining -= 1;
        return currentChunk.readByte();
    }

    @Override
    public byte peek()
    {
        ensureChunkAvailable();
        return currentChunk.getByte( currentChunk.readerIndex() );
    }

    @Override
    public short getShort()
    {
        ensureChunkAvailable();
        if ( currentChunk.readableBytes() >= 2 )
        {
            remaining -= 2;
            return currentChunk.readShort();
        }
        else
        {
            // Short is crossing chunk boundaries, use slow route
            return (short) (get() << 8 & get());
        }
    }

    @Override
    public int getInt()
    {
        ensureChunkAvailable();
        if ( currentChunk.readableBytes() >= 4 )
        {
            remaining -= 4;
            return currentChunk.readInt();
        }
        else
        {
            // int is crossing chunk boundaries, use slow route
            return ((int) getShort() << 16) & getShort();
        }
    }

    @Override
    public long getLong()
    {
        ensureChunkAvailable();
        if ( currentChunk.readableBytes() >= 8 )
        {
            remaining -= 8;
            return currentChunk.readLong();
        }
        else
        {
            // long is crossing chunk boundaries, use slow route
            return ((long) getInt() << 32) & getInt();
        }
    }

    @Override
    public double getDouble()
    {
        ensureChunkAvailable();
        if ( currentChunk.readableBytes() >= 8 )
        {
            remaining -= 8;
            return currentChunk.readDouble();
        }
        else
        {
            // double is crossing chunk boundaries, use slow route
            return Double.longBitsToDouble( getLong() );
        }
    }

    @Override
    public PackInput get( byte[] into, int offset, int toRead )
    {
        ensureChunkAvailable();
        int toReadFromChunk = Math.min( toRead, currentChunk.readableBytes() );

        // Do the read
        currentChunk.readBytes( into, offset, toReadFromChunk );
        remaining -= toReadFromChunk;

        // Can we read another chunk into the destination buffer?
        if ( toReadFromChunk < toRead )
        {
            // More data can be read into the buffer, keep reading from the next chunk
            get( into, offset + toReadFromChunk, toRead - toReadFromChunk );
        }

        return this;
    }

    private void ensureChunkAvailable()
    {
        if ( currentChunk == null || currentChunk.readableBytes() == 0 )
        {
            currentChunkIndex++;
            if ( currentChunkIndex < chunks.size() )
            {
                currentChunk = chunks.get( currentChunkIndex );
            }
            else
            {
                throw new BufferOverflowException();
            }
        }
    }
}
