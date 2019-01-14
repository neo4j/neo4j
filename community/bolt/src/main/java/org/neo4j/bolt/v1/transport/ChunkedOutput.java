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
package org.neo4j.bolt.v1.transport;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import org.neo4j.bolt.transport.TransportThrottleException;
import org.neo4j.bolt.transport.TransportThrottleGroup;
import org.neo4j.bolt.v1.messaging.BoltIOException;
import org.neo4j.bolt.v1.packstream.PackOutput;
import org.neo4j.bolt.v1.packstream.PackOutputClosedException;
import org.neo4j.bolt.v1.packstream.PackStream;
import org.neo4j.kernel.api.exceptions.Status;

/**
 * A target output for {@link PackStream} which breaks the data into a continuous stream of chunks before pushing them into a netty
 * channel.
 */
public class ChunkedOutput implements PackOutput
{
    private static final int DEFAULT_BUFFER_SIZE = 8192;

    public static final int CHUNK_HEADER_SIZE = 2;
    public static final int MESSAGE_BOUNDARY = 0;

    private static final int MAX_CHUNK_SIZE = Short.MAX_VALUE / 2;
    private static final int NO_MESSAGE = -1;

    private final Channel channel;
    private final int maxBufferSize;
    private final int maxChunkSize;
    private final TransportThrottleGroup throttleGroup;

    private ByteBuf buffer;
    private int currentChunkStartIndex;
    private boolean closed;

    /** Are currently in the middle of writing a chunk? */
    private boolean chunkOpen;
    private int currentMessageStartIndex = NO_MESSAGE;

    public ChunkedOutput( Channel ch, TransportThrottleGroup throttleGroup )
    {
        this( ch, DEFAULT_BUFFER_SIZE, throttleGroup );
    }

    public ChunkedOutput( Channel ch, int bufferSize, TransportThrottleGroup throttleGroup )
    {
        this( ch, bufferSize, MAX_CHUNK_SIZE, throttleGroup );
    }

    public ChunkedOutput( Channel channel, int maxBufferSize, int maxChunkSize, TransportThrottleGroup throttleGroup )
    {
        this.channel = Objects.requireNonNull( channel );
        this.maxBufferSize = maxBufferSize;
        this.maxChunkSize = maxChunkSize;
        this.buffer = allocateBuffer();
        this.throttleGroup = Objects.requireNonNull( throttleGroup );
    }

    @Override
    public synchronized void beginMessage()
    {
        if ( currentMessageStartIndex != NO_MESSAGE )
        {
            throw new IllegalStateException( "Message has already been started, index: " + currentMessageStartIndex );
        }

        currentMessageStartIndex = buffer.writerIndex();
    }

    @Override
    public synchronized void messageSucceeded() throws IOException
    {
        assertMessageStarted();
        currentMessageStartIndex = NO_MESSAGE;

        closeChunkIfOpen();
        buffer.writeShort( MESSAGE_BOUNDARY );

        if ( buffer.readableBytes() >= maxBufferSize )
        {
            flush();
        }
        chunkOpen = false;
    }

    @Override
    public synchronized void messageFailed() throws IOException
    {
        assertMessageStarted();
        int writerIndex = currentMessageStartIndex;
        currentMessageStartIndex = NO_MESSAGE;

        // truncate the buffer to remove all data written by an unfinished message
        buffer.capacity( writerIndex );
        chunkOpen = false;
    }

    //Flush can be called from a separate thread, we therefor need to synchronize
    //on everything that touches the buffer
    @Override
    public synchronized PackOutput flush() throws IOException
    {
        if ( buffer != null && buffer.readableBytes() > 0 )
        {
            closeChunkIfOpen();

            // check for and apply write throttles
            try
            {
                throttleGroup.writeThrottle().acquire( channel );
            }
            catch ( TransportThrottleException ex )
            {
                throw new BoltIOException( Status.Request.InvalidUsage, ex.getMessage(), ex );
            }

            // Local copy and clear the buffer field. This ensures that the buffer is not re-released if the flush call fails
            ByteBuf out = this.buffer;
            this.buffer = null;

            channel.writeAndFlush( out, channel.voidPromise() );

            buffer = allocateBuffer();
        }
        return this;
    }

    @Override
    public synchronized PackOutput writeByte( byte value ) throws IOException
    {
        ensure( 1 );
        buffer.writeByte( value );
        return this;
    }

    @Override
    public synchronized PackOutput writeShort( short value ) throws IOException
    {
        ensure( 2 );
        buffer.writeShort( value );
        return this;
    }

    @Override
    public synchronized PackOutput writeInt( int value ) throws IOException
    {
        ensure( 4 );
        buffer.writeInt( value );
        return this;
    }

    @Override
    public synchronized PackOutput writeLong( long value ) throws IOException
    {
        ensure( 8 );
        buffer.writeLong( value );
        return this;
    }

    @Override
    public synchronized PackOutput writeDouble( double value ) throws IOException
    {
        ensure( 8 );
        buffer.writeDouble( value );
        return this;
    }

    @Override
    public synchronized PackOutput writeBytes( ByteBuffer data ) throws IOException
    {
        while ( data.remaining() > 0 )
        {
            // Ensure there is an open chunk, and that it has at least one byte of space left
            ensure( 1 );

            int oldLimit = data.limit();
            data.limit( data.position() + Math.min( availableBytesInCurrentChunk(), data.remaining() ) );
            buffer.writeBytes( data );
            data.limit( oldLimit );
        }
        return this;
    }

    @Override
    public PackOutput writeBytes( byte[] data, int offset, int length ) throws IOException
    {
        if ( offset + length > data.length )
        {
            throw new IOException( "Asked to write " + length + " bytes, but there is only " + (data.length - offset) + " bytes available in data provided." );
        }
        return writeBytes( ByteBuffer.wrap( data, offset, length ) );
    }

    public synchronized void close()
    {
        if ( buffer != null )
        {
            try
            {
                flush();
            }
            catch ( IOException ignore )
            {
            }
            finally
            {
                closed = true;
                buffer.release();
                buffer = null;
            }
        }
    }

    private void ensure( int numberOfBytes ) throws IOException
    {
        assertOpen();
        assertMessageStarted();

        if ( chunkOpen )
        {
            int targetChunkSize = currentChunkBodySize() + numberOfBytes + CHUNK_HEADER_SIZE;
            if ( targetChunkSize > maxChunkSize )
            {
                closeChunkIfOpen();
                startNewChunk();
            }
        }
        else
        {
            startNewChunk();
        }
    }

    private void startNewChunk()
    {
        currentChunkStartIndex = buffer.writerIndex();

        // write empty chunk header
        buffer.writeShort( 0 );
        chunkOpen = true;
    }

    private void closeChunkIfOpen()
    {
        if ( chunkOpen )
        {
            int chunkBodySize = currentChunkBodySize();
            buffer.setShort( currentChunkStartIndex, chunkBodySize );
            chunkOpen = false;
        }
    }

    private int availableBytesInCurrentChunk()
    {
        return maxChunkSize - currentChunkBodySize() - CHUNK_HEADER_SIZE;
    }

    private int currentChunkBodySize()
    {
        return buffer.writerIndex() - (currentChunkStartIndex + CHUNK_HEADER_SIZE);
    }

    private ByteBuf allocateBuffer()
    {
        return channel.alloc().buffer( maxBufferSize );
    }

    private void assertMessageStarted()
    {
        if ( currentMessageStartIndex == NO_MESSAGE )
        {
            throw new IllegalStateException( "Message has not been started" );
        }
    }

    private void assertOpen() throws PackOutputClosedException
    {
        if ( closed )
        {
            throw new PackOutputClosedException(
                    String.format( "Network channel towards %s is closed. Client has probably been stopped.", channel.remoteAddress() ),
                    String.format( "%s", channel.remoteAddress() ) );
        }
    }
}
