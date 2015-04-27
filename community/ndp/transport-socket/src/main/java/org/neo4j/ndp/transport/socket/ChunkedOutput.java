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
    private ChannelHandlerContext channel;
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

    public ChunkedOutput()
    {
        this( 8192 );
    }

    public ChunkedOutput( int bufferSize )
    {
        this.bufferSize = bufferSize;
        this.maxChunkSize = bufferSize - CHUNK_HEADER_SIZE;
    }

    @Override
    public PackOutput ensure( int size ) throws IOException
    {
        assert size <= maxChunkSize : size + " > " + maxChunkSize;
        if ( buffer == null )
        {
            newBuffer();
        }
        else if ( buffer.writableBytes() < size )
        {
            flush();
        }

        if ( !chunkOpen )
        {
            currentChunkHeaderOffset = buffer.writerIndex();
            buffer.writerIndex( buffer.writerIndex() + CHUNK_HEADER_SIZE );
            chunkOpen = true;
        }

        return this;
    }

    @Override
    public PackOutput flush() throws IOException
    {
        if ( buffer != null && buffer.readableBytes() > 0 )
        {
            closeChunkIfOpen();
            channel.writeAndFlush( buffer );
            newBuffer();
        }
        return this;
    }

    @Override
    public PackOutput put( byte value )
    {
        assert chunkOpen;
        buffer.writeByte( value );
        return this;
    }

    @Override
    public PackOutput putShort( short value )
    {
        assert chunkOpen;
        buffer.writeShort( value );
        return this;
    }

    @Override
    public PackOutput putInt( int value )
    {
        assert chunkOpen;
        buffer.writeInt( value );
        return this;
    }

    @Override
    public PackOutput putLong( long value )
    {
        assert chunkOpen;
        buffer.writeLong( value );
        return this;
    }

    @Override
    public PackOutput putDouble( double value )
    {
        assert chunkOpen;
        buffer.writeDouble( value );
        return this;
    }

    @Override
    public PackOutput put( byte[] data, int offset, int length ) throws IOException
    {
        int index = 0;
        while ( index < length )
        {
            int amountToWrite = Math.min( buffer == null ? maxChunkSize : buffer.writableBytes() - CHUNK_HEADER_SIZE,
                    length - index );
            ensure( amountToWrite );

            buffer.writeBytes( data, offset + index, amountToWrite );
            index += amountToWrite;

            if ( buffer.writableBytes() == 0 )
            {
                flush();
            }
        }
        return this;
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

    public void setTargetChannel( ChannelHandlerContext channel )
    {
        this.channel = channel;
    }
}
