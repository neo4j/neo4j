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
package org.neo4j.packstream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

public class BufferedChannelOutput implements PackOutput
{
    private final ByteBuffer buffer;
    private WritableByteChannel channel;

    public BufferedChannelOutput( int bufferSize )
    {
        this.buffer = ByteBuffer.allocateDirect( bufferSize ).order( ByteOrder.BIG_ENDIAN );
    }

    public BufferedChannelOutput( WritableByteChannel channel )
    {
        this( channel, 1024 );
    }

    public BufferedChannelOutput( WritableByteChannel channel, int bufferSize )
    {
        this( bufferSize );
        reset( channel );
    }

    public void reset( WritableByteChannel channel )
    {
        this.channel = channel;
    }

    @Override
    public BufferedChannelOutput flush() throws IOException
    {
        buffer.flip();
        do { channel.write( buffer ); } while ( buffer.remaining() > 0 );
        buffer.clear();
        return this;
    }

    @Override
    public PackOutput writeBytes( byte[] data, int offset, int length ) throws IOException
    {
        if( offset + length > data.length )
        {
            throw new IOException( "Asked to write " + length + " bytes, but there is only " +
                                   ( data.length - offset ) + " bytes available in data provided." );
        }

        int index = 0;
        while ( index < length )
        {
            if ( buffer.remaining() == 0 )
            {
                flush();
            }

            int amountToWrite = Math.min( buffer.remaining(), length - index );

            buffer.put( data, offset + index, amountToWrite );
            index += amountToWrite;
        }
        return this;
    }

    @Override
    public PackOutput writeByte( byte value ) throws IOException
    {
        ensure( 1 );
        buffer.put( value );
        return this;
    }

    @Override
    public PackOutput writeShort( short value ) throws IOException
    {
        ensure( 2 );
        buffer.putShort( value );
        return this;
    }

    @Override
    public PackOutput writeInt( int value ) throws IOException
    {
        ensure( 4 );
        buffer.putInt( value );
        return this;
    }

    @Override
    public PackOutput writeLong( long value ) throws IOException
    {
        ensure( 8 );
        buffer.putLong( value );
        return this;
    }

    @Override
    public PackOutput writeDouble( double value ) throws IOException
    {
        ensure( 8 );
        buffer.putDouble( value );
        return this;
    }

    private void ensure( int size ) throws IOException
    {
        if ( buffer.remaining() < size )
        {
            flush();
        }
    }
}
