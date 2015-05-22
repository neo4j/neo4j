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
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

import java.io.IOException;

import org.neo4j.packstream.PackOutput;

public class ChunkedOutput implements PackOutput
{
    public static final int CHUNK_HEADER_SIZE = 2;
    public static final int MESSAGE_BOUNDARY = 0;

    private final int bufferSize;
    private final int maxChunkSize;

    private ByteBuf buffer;
    private Channel channel;
    private int currentChunkHeaderOffset;

    /** Are currently in the middle of writing a chunk? */
    private boolean chunkOpen = false;

    private Runnable onMessageComplete = new Runnable()
    {
        @Override
        public void run()
        {
            try
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
            catch ( IOException e )
            {
                // TODO: Don't use runnable here then, use something that can throw this IOException
                throw new RuntimeException( e );
            }

        }
    };

    public ChunkedOutput( Channel ch, int bufferSize )
    {
        this.channel = ch;
        this.bufferSize = bufferSize;
        this.maxChunkSize = bufferSize - CHUNK_HEADER_SIZE;
        this.buffer = channel.alloc().buffer( bufferSize, bufferSize );
    }

    @Override
    public PackOutput flush() throws IOException
    {
        if ( buffer.readableBytes() > 0 )
        {
            closeChunkIfOpen();
            channel.writeAndFlush( buffer, channel.voidPromise() );
            newBuffer();
        }
        return this;
    }

    @Override
    public PackOutput writeByte( byte value ) throws IOException
    {
        ensure(1);
        buffer.writeByte( value );
        return this;
    }

    @Override
    public PackOutput writeShort( short value ) throws IOException
    {
        ensure(2);
        buffer.writeShort( value );
        return this;
    }

    @Override
    public PackOutput writeInt( int value ) throws IOException
    {
        ensure(4);
        buffer.writeInt( value );
        return this;
    }

    @Override
    public PackOutput writeLong( long value ) throws IOException
    {
        ensure(8);
        buffer.writeLong( value );
        return this;
    }

    @Override
    public PackOutput writeDouble( double value ) throws IOException
    {
        ensure(8);
        buffer.writeDouble( value );
        return this;
    }

    @Override
    public PackOutput writeBytes( byte[] data, int offset, int length ) throws IOException
    {
        int index = 0;
        while ( index < length )
        {
            // Ensure there is an open chunk, and that it has at least one byte of space left
            ensure(1);

            // Write as much as we can into the current chunk
            int amountToWrite = Math.min( buffer.writableBytes(), length - index );

            buffer.writeBytes( data, offset + index, amountToWrite );
            index += amountToWrite;
        }
        return this;
    }

    private void ensure( int size ) throws IOException
    {
        assert size <= maxChunkSize : size + " > " + maxChunkSize;

        if ( buffer.writableBytes() < size )
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

    private void closeChunkIfOpen()
    {
        if ( chunkOpen )
        {
            int chunkSize = buffer.writerIndex() - (currentChunkHeaderOffset + CHUNK_HEADER_SIZE);
            buffer.setShort( currentChunkHeaderOffset, chunkSize );
            chunkOpen = false;
        }
    }

    private void newBuffer()
    {
        // Assumption: We're using nettys buffer pooling here
        buffer = channel.alloc().buffer( bufferSize, bufferSize );
        chunkOpen = false;
    }

    public Runnable messageBoundaryHook()
    {
        return onMessageComplete;
    }

    public void close()
    {
        if(buffer != null)
        {
            buffer.release();
            buffer = null;
        }
    }
}
