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

import org.neo4j.kernel.impl.nioneo.store.StoreChannel;

public class InMemoryLogChannel implements WritableLogChannel, ReadableLogChannel
{
    private byte[] bytes = new byte[1000];
    private ByteBuffer asWriter = ByteBuffer.wrap( bytes );
    private ByteBuffer asReader = ByteBuffer.wrap( bytes );
    private ByteBuffer bufferForConversions = ByteBuffer.wrap( new byte[100] );

    public void reset()
    {
        asWriter.clear();
        asReader.clear();
    }

    private void ensureArrayCapacityPlus( int plus )
    {
        while ( asWriter.remaining() < plus )
        {
            byte[] tmp = bytes;
            bytes = new byte[bytes.length*2];
            System.arraycopy( tmp, 0, bytes, 0, tmp.length );
            asWriter = duplicateByteBufferMetadata( asWriter, tmp );
            asReader = duplicateByteBufferMetadata( asReader, tmp );
        }
    }

    private ByteBuffer duplicateByteBufferMetadata( ByteBuffer source, byte[] array )
    {
        int position = source.position();
        int limit = source.limit();
        ByteBuffer result = ByteBuffer.wrap( array );
        result.limit( limit );
        result.position( position );
        return result;
    }

    @Override
    public InMemoryLogChannel put( byte b ) throws IOException
    {
        ensureArrayCapacityPlus( 1 );
        asWriter.put( b );
        return this;
    }

    @Override
    public InMemoryLogChannel putShort( short s ) throws IOException
    {
        ensureArrayCapacityPlus( 2 );
        asWriter.putShort( s );
        return this;
    }

    @Override
    public InMemoryLogChannel putInt( int i ) throws IOException
    {
        ensureArrayCapacityPlus( 4 );
        asWriter.putInt( i );
        return this;
    }

    @Override
    public InMemoryLogChannel putLong( long l ) throws IOException
    {
        ensureArrayCapacityPlus( 8 );
        asWriter.putLong( l );
        return this;
    }

    @Override
    public InMemoryLogChannel putFloat( float f ) throws IOException
    {
        ensureArrayCapacityPlus( 4 );
        asWriter.putFloat( f );
        return this;
    }

    @Override
    public InMemoryLogChannel putDouble( double d ) throws IOException
    {
        ensureArrayCapacityPlus( 8 );
        asWriter.putDouble( d );
        return this;
    }

    @Override
    public InMemoryLogChannel put( byte[] bytes, int length ) throws IOException
    {
        ensureArrayCapacityPlus( length );
        asWriter.put( bytes, 0, length );
        return this;
    }

    @Override
    public InMemoryLogChannel put( char[] chars, int length ) throws IOException
    {
        ensureConversionBufferCapacity( length*2 );
        for ( int i = 0; i < length; i++ )
        {
            asWriter.putChar( chars[i] );
        }
        return this;
    }

    private void ensureConversionBufferCapacity( int length )
    {
        if ( bufferForConversions.capacity() < length )
        {
            bufferForConversions = ByteBuffer.wrap( new byte[length*2] );
        }
    }

    @Override
    public void force() throws IOException
    {
    }

    public StoreChannel getFileChannel()
    {
        throw new UnsupportedOperationException();
    }

    public boolean isOpen()
    {
        return true;
    }

    @Override
    public void close() throws IOException
    {
    }
//
//    public int read( ByteBuffer dst ) throws IOException
//    {
//        if ( readIndex >= writeIndex )
//        {
//            return -1;
//        }
//
//        int actualLengthToRead = Math.min( dst.limit(), writeIndex-readIndex );
//        try
//        {
//            dst.put( bytes, readIndex, actualLengthToRead );
//            return actualLengthToRead;
//        }
//        finally
//        {
//            readIndex += actualLengthToRead;
//        }
//    }

    @Override
    public byte get()
    {
        return asReader.get();
    }

    @Override
    public short getShort()
    {
        return asReader.getShort();
    }

    @Override
    public int getInt()
    {
        return asReader.getInt();
    }

    @Override
    public long getLong()
    {
        return asReader.getLong();
    }

    @Override
    public float getFloat()
    {
        return asReader.getFloat();
    }

    @Override
    public double getDouble()
    {
        return asReader.getDouble();
    }

    @Override
    public void get( byte[] bytes, int length )
    {
        asReader.get( bytes, 0, length );
    }

    @Override
    public void get( char[] chars, int length ) throws IOException
    {
        asReader.asCharBuffer().get( chars, 0, length );
    }

    @Override
    public LogPosition getCurrentPosition()
    {
        throw new UnsupportedOperationException( "Please implement" );
    }
}
