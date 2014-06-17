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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.IOException;
import java.nio.ByteBuffer;

import static java.lang.Math.min;

public class PhysicalWritableLogChannel implements WritableLogChannel
{
    private VersionedStoreChannel channel;
    private final ByteBuffer buffer = ByteBuffer.allocate( 4*1024 );

    public PhysicalWritableLogChannel( VersionedStoreChannel channel )
    {
        this.channel = channel;
    }

    @Override
    public void force() throws IOException
    {
        emptyBufferIntoChannelAndClearIt();
        channel.force( false );
    }

    void setChannel( VersionedStoreChannel channel )
    {
        this.channel = channel;
    }

    private void emptyBufferIntoChannelAndClearIt() throws IOException
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
            int chunkSize = min( length, buffer.capacity() >> 1 );
            bufferWithGuaranteedSpace( chunkSize ).put( value, offset, chunkSize );
            offset += chunkSize;
        }
        return this;
    }

    @Override
    public WritableLogChannel put( char[] value, int length ) throws IOException
    {
        // TODO 2.2-future do this better
        for ( char ch : value )
        {
            bufferWithGuaranteedSpace( 2 ).putChar( ch );
        }
        return this;
    }

    @Override
    public LogPosition getCurrentPosition() throws IOException
    {
        return channel.getCurrentPosition();
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
        channel.close();
    }
}
