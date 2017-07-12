/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import io.netty.channel.Channel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.bolt.v1.messaging.BoltResponseMessageBoundaryHook;
import org.neo4j.bolt.v1.packstream.PackOutput;
import org.neo4j.bolt.v1.packstream.PackOutputClosedException;
import org.neo4j.bolt.v1.packstream.PackStream;

import static java.lang.Math.max;

/**
 * A target output for {@link PackStream} which breaks the data into a continuous stream of chunks before pushing them into a netty
 * channel.
 */
public class ChunkedOutput implements PackOutput, BoltResponseMessageBoundaryHook
{
    public static final int CHUNK_HEADER_SIZE = 2;
    public static final int MESSAGE_BOUNDARY = 0;

    private final int bufferSize;
    private final int maxChunkSize;
    private final AtomicBoolean closed = new AtomicBoolean( false );

    private ByteBuf buffer;
    private Channel channel;
    private int currentChunkHeaderOffset;

    /** Are currently in the middle of writing a chunk? */
    private boolean chunkOpen;

    public ChunkedOutput( Channel ch, int bufferSize )
    {
        this.channel = ch;
        this.bufferSize = max( 16, bufferSize );
        this.maxChunkSize = this.bufferSize - CHUNK_HEADER_SIZE;
        this.buffer = channel.alloc().buffer( this.bufferSize, this.bufferSize );
    }

    //Flush can be called from a separate thread, we therefor need to synchronize
    //on everything that touches the buffer
    @Override
    public synchronized PackOutput flush() throws IOException
    {
        if ( buffer != null && buffer.readableBytes() > 0 )
        {
            closeChunkIfOpen();

            // Local copy and clear the buffer field. This ensures that the buffer is not re-released if the flush call fails
            ByteBuf out = this.buffer;
            this.buffer = null;

            channel.writeAndFlush( out, channel.voidPromise() );

            newBuffer();
        }
        return this;
    }

    @Override
    public synchronized PackOutput writeByte( byte value ) throws IOException
    {
        ensure(1);
        buffer.writeByte( value );
        return this;
    }

    @Override
    public synchronized PackOutput writeShort( short value ) throws IOException
    {
        ensure(2);
        buffer.writeShort( value );
        return this;
    }

    @Override
    public synchronized PackOutput writeInt( int value ) throws IOException
    {
        ensure(4);
        buffer.writeInt( value );
        return this;
    }

    @Override
    public synchronized PackOutput writeLong( long value ) throws IOException
    {
        ensure(8);
        buffer.writeLong( value );
        return this;
    }

    @Override
    public synchronized PackOutput writeDouble( double value ) throws IOException
    {
        ensure(8);
        buffer.writeDouble( value );
        return this;
    }

    @Override
    public PackOutput writeBytes( ByteBuffer data ) throws IOException
    {
        // TODO: If data is larger than our chunk size or so, we're very likely better off just passing this ByteBuffer
        // on rather than doing the copy here
        // TODO: *however* note that we need some way to find out when the data has been written (and thus the buffer
        // can be re-used) if we take that approach
        // See the comment in #newBuffer for an approach that would allow that
        while ( data.remaining() > 0 )
        {
            // Ensure there is an open chunk, and that it has at least one byte of space left
            ensure( 1 );

            int oldLimit = data.limit();
            synchronized ( this )
            {
                data.limit( data.position() + Math.min( buffer.writableBytes(), data.remaining() ) );

                buffer.writeBytes( data );
            }
            data.limit( oldLimit );
        }
        return this;
    }

    @Override
    public PackOutput writeBytes( byte[] data, int offset, int length ) throws IOException
    {
        if ( offset + length > data.length )
        {
            throw new IOException( "Asked to write " + length + " bytes, but there is only " +
                                   ( data.length - offset ) + " bytes available in data provided." );
        }
        return writeBytes( ByteBuffer.wrap( data, offset, length ) );
    }

    //must be called from within a synchronized block
    private void ensure( int size ) throws IOException
    {
        assert size <= maxChunkSize : size + " > " + maxChunkSize;
        if ( closed.get() )
        {
            throw new PackOutputClosedException( "Unable to write to the closed output channel" );
        }
        int toWriteSize = chunkOpen ? size : size + CHUNK_HEADER_SIZE;
        synchronized ( this )
        {
            if ( buffer.writableBytes() < toWriteSize )
            {
                flush();
            }

            if ( !chunkOpen )
            {
                currentChunkHeaderOffset = buffer.writerIndex();
                buffer.writerIndex( buffer.writerIndex() + CHUNK_HEADER_SIZE );
                chunkOpen = true;
            }
        }
    }

    private synchronized void closeChunkIfOpen()
    {
        if ( chunkOpen )
        {
            int chunkSize = buffer.writerIndex() - (currentChunkHeaderOffset + CHUNK_HEADER_SIZE);
            buffer.setShort( currentChunkHeaderOffset, chunkSize );
            chunkOpen = false;
        }
    }

    //must be called from within a synchronized block
    private void newBuffer()
    {
        // Assumption: We're using nettys buffer pooling here
        // If we wanted to, we can optimize this further and restrict memory usage by using our own ByteBuf impl.
        // Each Output instance would have, say, 3 buffers that it rotates. Fill one up, send it to be async flushed,
        // fill the next one up, etc. When release is called by Netty, push buffer back onto our local stack. That
        // way there are no global data structures for managing memory, no fragmentation and a fixed amount of
        // RAM per session used.
        buffer = channel.alloc().buffer( bufferSize, bufferSize );
        chunkOpen = false;
    }

    public synchronized void close()
    {
        if ( buffer != null )
        {
            try
            {
                flush();
            }
            catch ( IOException e )
            {
                //
            }
            finally
            {
                closed.set( true );
                buffer.release();
                buffer = null;
            }
        }
    }

    @Override
    public synchronized void onMessageComplete() throws IOException
    {
        closeChunkIfOpen();

        // Ensure there's space to write the message boundary
        if ( buffer.writableBytes() < CHUNK_HEADER_SIZE )
        {
            flush();
        }

        // Write message boundary
        buffer.writeShort( MESSAGE_BOUNDARY );

        // Mark us as not currently in a chunk
        chunkOpen = false;
    }
}
