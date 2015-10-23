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
package org.neo4j.bolt.v1.transport;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.bolt.v1.messaging.BoltIOException;
import org.neo4j.bolt.v1.packstream.PackInput;
import org.neo4j.bolt.v1.packstream.PackStream;
import org.neo4j.kernel.api.exceptions.Status;

/**
 * A {@link PackInput} that accepts data fragments and exposes them as a continuous stream to {@link PackStream}.
 */
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

    public void append( ByteBuf chunk )
    {
        if ( chunk.readableBytes() > 0 )
        {
            chunks.add( chunk.retain() );
            remaining += chunk.readableBytes();
        }
    }

    @Override
    public boolean hasMoreData() throws IOException
    {
        return remaining > 0;
    }

    @Override
    public byte peekByte() throws IOException
    {
        ensureChunkAvailable();
        return currentChunk.getByte( currentChunk.readerIndex() );
    }

    @Override
    public byte readByte() throws IOException
    {
        ensure( 1 );
        remaining -= 1;
        return currentChunk.readByte();
    }

    @Override
    public short readShort() throws IOException
    {
        ensure( 2 );
        if ( currentChunk.readableBytes() >= 2 )
        {
            remaining -= 2;
            return currentChunk.readShort();
        }
        else
        {
            // Short is crossing chunk boundaries, use slow route
            short higher = (short) ((short)readByte() << 8);
            short lower = (short) (0x00FF & readByte());
            return (short) (higher | lower);
        }
    }

    @Override
    public int readInt() throws IOException
    {
        ensure( 4 );
        if ( currentChunk.readableBytes() >= 4 )
        {
            remaining -= 4;
            return currentChunk.readInt();
        }
        else
        {
            // int is crossing chunk boundaries, use slow route
            int higher = (int) readShort() << 16;
            int lower = 0x0000FFFF & readShort();
            return higher | lower;
        }
    }

    @Override
    public long readLong() throws IOException
    {
        ensure( 8 );
        if ( currentChunk.readableBytes() >= 8 )
        {
            remaining -= 8;
            return currentChunk.readLong();
        }
        else
        {
            // long is crossing chunk boundaries, use slow route
            long higher = (long) readInt() << 32;
            long lower = 0x00000000FFFFFFFFL & readInt();
            return higher | lower;
        }
    }

    @Override
    public double readDouble() throws IOException
    {
        ensure( 8 );
        if ( currentChunk.readableBytes() >= 8 )
        {
            remaining -= 8;
            return currentChunk.readDouble();
        }
        else
        {
            // double is crossing chunk boundaries, use slow route
            return Double.longBitsToDouble( readLong() );
        }
    }

    @Override
    public PackInput readBytes( byte[] into, int offset, int toRead ) throws IOException
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
            readBytes( into, offset + toReadFromChunk, toRead - toReadFromChunk );
        }

        return this;
    }

    private void ensure( int numBytes ) throws IOException
    {
        ensureChunkAvailable();
        if ( remaining < numBytes )
        {
            throw new BoltIOException( Status.Request.InvalidFormat, "Unable to deserialize request, message " +
                                                                    "boundary found before message ended. This " +
                                                                    "indicates a serialization or framing " +
                                                                    "problem with your client driver." );
        }
    }

    private void ensureChunkAvailable() throws IOException
    {
        while ( currentChunk == null || currentChunk.readableBytes() == 0 )
        {
            currentChunkIndex++;
            if ( currentChunkIndex < chunks.size() )
            {
                currentChunk = chunks.get( currentChunkIndex );
            }
            else
            {
                throw new BoltIOException( Status.Request.InvalidFormat, "Unable to deserialize request, message " +
                                                                        "boundary found before message ended. This " +
                                                                        "indicates a serialization or framing " +
                                                                        "problem with your client driver." );
            }
        }
    }

    public void close()
    {
        clear();
    }
}
