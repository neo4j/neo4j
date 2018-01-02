/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.neo4j.io.fs.StoreChannel;

public class InMemoryLogChannel implements WritableLogChannel, ReadableLogChannel
{
    private final byte[] bytes;
    private final ByteBuffer asWriter;
    private final ByteBuffer asReader;

    public InMemoryLogChannel()
    {
        this(1000);
    }

    public InMemoryLogChannel( int bufferSize )
    {
        bytes = new byte[bufferSize];
        asWriter = ByteBuffer.wrap( bytes );
        asReader = ByteBuffer.wrap( bytes );
    }

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
        return this;
    }

    @Override
    public InMemoryLogChannel putShort( short s ) throws IOException
    {
        asWriter.putShort( s );
        return this;
    }

    @Override
    public InMemoryLogChannel putInt( int i ) throws IOException
    {
        asWriter.putInt( i );
        return this;
    }

    @Override
    public InMemoryLogChannel putLong( long l ) throws IOException
    {
        asWriter.putLong( l );
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
        return this;
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
    public Flushable emptyBufferIntoChannelAndClearIt()
    {
        return NO_OP_FLUSHABLE;
    }

    @Override
    public byte get() throws ReadPastEndException
    {
        ensureAvailableToRead( 1 );
        return asReader.get();
    }

    @Override
    public short getShort() throws ReadPastEndException
    {
        ensureAvailableToRead( 2 );
        return asReader.getShort();
    }

    @Override
    public int getInt() throws ReadPastEndException
    {
        ensureAvailableToRead( 4 );
        return asReader.getInt();
    }

    @Override
    public long getLong() throws ReadPastEndException
    {
        ensureAvailableToRead( 8 );
        return asReader.getLong();
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
    }

    private void ensureAvailableToRead( int i ) throws ReadPastEndException
    {
        if ( asReader.remaining() < i || asReader.position() + i > asWriter.position() )
        {
            throw ReadPastEndException.INSTANCE;
        }
    }

    @Override
    public LogPositionMarker getCurrentPosition( LogPositionMarker positionMarker )
    {
        // Hmm, this would be for the writer.
        positionMarker.mark( 0, asWriter.position() );
        return positionMarker;
    }

    public int positionWriter( int position )
    {
        int previous = asWriter.position();
        asWriter.position( position );
        return previous;
    }

    public int positionReader( int position )
    {
        int previous = asReader.position();
        asReader.position( position );
        return previous;
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

    public int capacity()
    {
        return bytes.length;
    }

    public int availableBytesToRead()
    {
        return asReader.remaining();
    }

    public int availableBytesToWrite()
    {
        return asWriter.remaining();
    }
    private static final Flushable NO_OP_FLUSHABLE = new Flushable()
    {
        @Override
        public void flush() throws IOException
        {
        }
    };
}
