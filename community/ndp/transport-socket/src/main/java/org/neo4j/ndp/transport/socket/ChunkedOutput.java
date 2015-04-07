/**
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
    private final int bufferSize;
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
                closeCurrentChunk();

                // Write message boundary
                ensure( CHUNK_HEADER_SIZE );
                putShort( (short) 0 );

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
    }

    @Override
    public PackOutput ensure( int size ) throws IOException
    {
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
        if ( chunkOpen )
        {
            closeCurrentChunk();
        }

        channel.writeAndFlush( buffer );
        newBuffer();

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
            int amountToWrite = Math.min( buffer.writableBytes(), length - index );
            ensure( amountToWrite );

            buffer.writeBytes( data, offset, amountToWrite );
            index += amountToWrite;

            if ( buffer.writableBytes() == 0 )
            {
                flush();
            }
        }
        return this;
    }

    private void closeCurrentChunk()
    {
        int chunkSize = buffer.writerIndex() - (currentChunkHeaderOffset + CHUNK_HEADER_SIZE);
        buffer.setShort( currentChunkHeaderOffset, chunkSize );
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
