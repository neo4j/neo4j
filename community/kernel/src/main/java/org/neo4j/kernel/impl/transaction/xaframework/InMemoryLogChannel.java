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
import java.util.Arrays;

import org.neo4j.kernel.impl.nioneo.store.StoreChannel;

public class InMemoryLogChannel implements WritableLogChannel, ReadableLogChannel
{
    private byte[] bytes = new byte[1000];
    private ByteBuffer asWriter = ByteBuffer.wrap( bytes );
    private ByteBuffer asReader = ByteBuffer.wrap( bytes );

    public void reset()
    {
        asWriter.clear();
        asReader.clear();
        Arrays.fill( bytes, (byte) 0 );
    }

    @Override
    public InMemoryLogChannel put( byte b ) throws IOException
    {
        asWriter.put( b );
        System.out.println("Put byte " + b );
        return this;
    }

    @Override
    public InMemoryLogChannel putShort( short s ) throws IOException
    {
        asWriter.putShort( s );
        System.out.println("Put short " + s );
        return this;
    }

    @Override
    public InMemoryLogChannel putInt( int i ) throws IOException
    {
        asWriter.putInt( i );
        System.out.println("Put int " + i );
        return this;
    }

    @Override
    public InMemoryLogChannel putLong( long l ) throws IOException
    {
        asWriter.putLong( l );
        System.out.println("Put long " + l );
        return this;
    }

    @Override
    public InMemoryLogChannel putFloat( float f ) throws IOException
    {
        asWriter.putFloat( f );
        return this;
    }

    @Override
    public InMemoryLogChannel putDouble( double d ) throws IOException
    {
        asWriter.putDouble( d );
        return this;
    }

    @Override
    public InMemoryLogChannel put( byte[] bytes, int length ) throws IOException
    {
        asWriter.put( bytes, 0, length );
        System.out.println("Put array " + length );
        return this;
    }

    @Override
    public InMemoryLogChannel put( char[] chars, int length ) throws IOException
    {
        for ( int i = 0; i < length; i++ )
        {
            asWriter.putChar( chars[i] );
        }
        return this;
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

    @Override
    public boolean hasMoreData()
    {
        return asReader.hasRemaining();
    }

    @Override
    public byte get() throws ReadPastEndException
    {
        ensureAvailableToRead( 1 );
        byte b = asReader.get();
        System.out.println("read byte " + b );
        return b;
    }

    @Override
    public short getShort() throws ReadPastEndException
    {
        ensureAvailableToRead( 2 );
        short short1 = asReader.getShort();
        System.out.println("read short " + short1 );
        return short1;
    }

    @Override
    public int getInt() throws ReadPastEndException
    {
        ensureAvailableToRead( 4 );
        int int1 = asReader.getInt();
        System.out.println("read int " + int1 );
        return int1;
    }

    @Override
    public long getLong() throws ReadPastEndException
    {
        ensureAvailableToRead( 8 );
        long long1 = asReader.getLong();
        System.out.println("read long " + long1 );
        return long1;
    }

    @Override
    public float getFloat() throws ReadPastEndException
    {
        ensureAvailableToRead( 4 );
        return asReader.getFloat();
    }

    @Override
    public double getDouble() throws ReadPastEndException
    {
        ensureAvailableToRead( 8 );
        return asReader.getDouble();
    }

    @Override
    public void get( byte[] bytes, int length ) throws ReadPastEndException
    {
        ensureAvailableToRead( length );
        asReader.get( bytes, 0, length );
        System.out.println("read array " + Arrays.toString( bytes ));
    }

    @Override
    public void get( char[] chars, int length ) throws IOException
    {
        ensureAvailableToRead( length * 2 );
        for ( int i = 0; i < length; i++)
        {            
            chars[i] = asReader.getChar();
        }
    }

    private void ensureAvailableToRead( int i ) throws ReadPastEndException
    {
        if ( asReader.remaining() < i )
        {
            throw new ReadPastEndException();
        }
    }
    
    @Override
    public LogPosition getCurrentPosition()
    {
        // Hmm, this would be for the writer.
        return new LogPosition( 0, asWriter.position() );
    }

    public void positionWriter( int position )
    {
        asWriter.position( position );
    }

    public void positionReader( int position )
    {
        asReader.position( position );
    }

    public int readerPosition()
    {
        return asReader.position();
    }

    public int writerPosition()
    {
        return asWriter.position();
    }

    public void truncateTo( int bytesSuccessfullyWritten )
    {
        asReader.limit( bytesSuccessfullyWritten );
    }
}
