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
package org.neo4j.kernel.impl.transaction.log;

import java.io.IOException;
import java.nio.ByteBuffer;

import static java.lang.Math.min;

import static org.neo4j.helpers.Format.KB;

public class PhysicalWritableLogChannel implements WritableLogChannel
{
    private LogVersionedStoreChannel channel;
    private final ByteBuffer buffer = ByteBuffer.allocate( 512*KB );

    public PhysicalWritableLogChannel( LogVersionedStoreChannel channel )
    {
        this.channel = channel;
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
    public void emptyBufferIntoChannelAndClearIt() throws IOException
    {
        buffer.flip();
        channel.write( buffer );
        buffer.clear();
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
            int chunkSize = min( length - offset, buffer.capacity() >> 1 );
            bufferWithGuaranteedSpace( chunkSize ).put( value, offset, chunkSize );

            offset += chunkSize;
        }
        return this;
    }

    @Override
    public LogPositionMarker getCurrentPosition( LogPositionMarker positionMarker ) throws IOException
    {
        positionMarker.mark( channel.getVersion(), channel.position() + buffer.position() );
        return positionMarker;
    }

    private ByteBuffer bufferWithGuaranteedSpace( int spaceInBytes ) throws IOException
    {
        assert spaceInBytes < buffer.capacity();
        if ( buffer.remaining() < spaceInBytes )
        {
            emptyBufferIntoChannelAndClearIt();
        }
        return buffer;
    }

    @Override
    public void close() throws IOException
    {
        emptyBufferIntoChannelAndClearIt();
    }
}
