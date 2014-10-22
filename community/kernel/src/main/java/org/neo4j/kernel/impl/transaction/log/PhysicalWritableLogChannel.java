/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.log;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.function.Factory;
import org.neo4j.kernel.impl.util.SimplePool;

import static java.lang.Math.min;

import static org.neo4j.helpers.Format.KB;

public class PhysicalWritableLogChannel implements WritableLogChannel
{
    private class Buffer implements WriteFuture
    {
        private final ByteBuffer buffer = ByteBuffer.allocate( 512*KB );

        @Override
        public void write() throws IOException
        {
            try
            {
                buffer.flip();
                if ( buffer.hasRemaining() )
                {
                    channel.write( buffer );
                }
                buffer.clear();
            }
            finally
            {
                buffers.release( this );
            }
        }
    }

    private LogVersionedStoreChannel channel;
    private final SimplePool<Buffer> buffers = new SimplePool<>( 2, new Factory<Buffer>()
    {
        @Override
        public Buffer newInstance()
        {
            return new Buffer();
        }
    } );
    private Buffer currentBuffer;
    private ByteBuffer currentByteBuffer;

    public PhysicalWritableLogChannel( LogVersionedStoreChannel channel )
    {
        this.channel = channel;
        acquireNewBuffer();
    }

    private void acquireNewBuffer()
    {
        currentBuffer = buffers.acquire();
        currentByteBuffer = currentBuffer.buffer;
    }

    @Override
    public void force() throws IOException
    {
        channel.force( false );
    }

    void setChannel( LogVersionedStoreChannel channel )
    {
        this.channel = channel;
    }

    /**
     * Assume some kind of external synchronization.
     */
    @Override
    public WriteFuture switchBuffer()
    {
        try
        {
            // Return the current buffer which is also a WriteBuffer (no garbage created)
            return currentBuffer;
        }
        finally
        {
            // And as part of returning it, also acquire a new one
            acquireNewBuffer();
        }
    }

    @Override
    public WritableLogChannel put( byte value ) throws IOException
    {
        bufferWithGuaranteedSpace( 1 ).put( value );
        return this;
    }

    @Override
    public WritableLogChannel putShort( short value ) throws IOException
    {
        bufferWithGuaranteedSpace( 2 ).putShort( value );
        return this;
    }

    @Override
    public WritableLogChannel putInt( int value ) throws IOException
    {
        bufferWithGuaranteedSpace( 4 ).putInt( value );
        return this;
    }

    @Override
    public WritableLogChannel putLong( long value ) throws IOException
    {
        bufferWithGuaranteedSpace( 8 ).putLong( value );
        return this;
    }

    @Override
    public WritableLogChannel putFloat( float value ) throws IOException
    {
        bufferWithGuaranteedSpace( 4 ).putFloat( value );
        return this;
    }

    @Override
    public WritableLogChannel putDouble( double value ) throws IOException
    {
        bufferWithGuaranteedSpace( 8 ).putDouble( value );
        return this;
    }

    @Override
    public WritableLogChannel put( byte[] value, int length ) throws IOException
    {
        int offset = 0;
        while ( offset < length )
        {
            int chunkSize = min( length, currentByteBuffer.capacity() >> 1 );
            bufferWithGuaranteedSpace( chunkSize ).put( value, offset, chunkSize );
            offset += chunkSize;
        }
        return this;
    }

    @Override
    public LogPositionMarker getCurrentPosition( LogPositionMarker positionMarker ) throws IOException
    {
        positionMarker.mark( channel.getVersion(), channel.position() + currentByteBuffer.position() );
        return positionMarker;
    }

    private ByteBuffer bufferWithGuaranteedSpace( int spaceInBytes ) throws IOException
    {
        assert spaceInBytes < currentByteBuffer.capacity();
        if ( currentByteBuffer.remaining() < spaceInBytes )
        {
            switchBuffer().write();
        }
        return currentByteBuffer;
    }

    @Override
    public void close() throws IOException
    {
        switchBuffer().write();
    }
}
