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
package org.neo4j.kernel.impl.util;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;

/**
 * WritableByteChannel which is backed by a ByteBuffer. If data cannot fit
 * the buffer will be expanded on the fly.
 * <p>
 * Instances can be reused by calling {@link #reset()}. The internal buffer will not be reset
 * to the initial size, but will retain whatever size it had before resetting.
 */
public class ByteBufferWritableByteChannel
        implements WritableByteChannel
{
    private boolean closed = false;

    private ByteBuffer buffer;

    public ByteBufferWritableByteChannel()
    {
        this( 1024 );
    }

    public ByteBufferWritableByteChannel( int initialSize )
    {
        buffer = ByteBuffer.allocate( initialSize );
    }

    public void reset()
    {
        buffer.clear();
        closed = false;
    }

    public ByteBuffer getBuffer()
    {
        buffer.flip();
        return buffer;
    }

    @Override
    public int write( ByteBuffer src ) throws IOException
    {
        if ( closed )
        {
            throw new ClosedChannelException();
        }

        int remaining = src.remaining();

        try
        {
            buffer.put( src );
        }
        catch ( BufferOverflowException e )
        {
            // Increase internal buffer size and try again
            ByteBuffer newBuffer = ByteBuffer.allocate( buffer.capacity() + src.remaining() );
            newBuffer.put( buffer );
            return write( src );
        }

        return remaining;
    }

    @Override
    public boolean isOpen()
    {
        return !closed;
    }

    @Override
    public void close() throws IOException
    {
        closed = true;
    }
}
