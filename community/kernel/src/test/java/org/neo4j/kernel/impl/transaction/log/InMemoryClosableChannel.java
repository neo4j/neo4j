/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import java.io.Closeable;
import java.io.Flushable;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.neo4j.storageengine.api.ReadPastEndException;

import static java.lang.Math.toIntExact;

/**
 * Implementation of {@link ReadableClosablePositionAwareChannel} operating over a {@code byte[]} in memory.
 */
public class InMemoryClosableChannel implements ReadableClosablePositionAwareChannel, FlushablePositionAwareChannel
{
    private final byte[] bytes;
    private final Reader reader;
    private final Writer writer;

    public InMemoryClosableChannel()
    {
        this( 1000 );
    }

    public InMemoryClosableChannel( byte[] bytes, boolean append )
    {
        this.bytes = bytes;
        ByteBuffer writeBuffer = ByteBuffer.wrap( this.bytes );
        ByteBuffer readBuffer = ByteBuffer.wrap( this.bytes );
        if ( append )
        {
            writeBuffer.position( bytes.length );
        }
        this.writer = new Writer( writeBuffer );
        this.reader = new Reader( readBuffer );
    }

    public InMemoryClosableChannel( int bufferSize )
    {
        this( new byte[bufferSize], false );
    }

    public void reset()
    {
        writer.clear();
        reader.clear();
        Arrays.fill( bytes, (byte) 0 );
    }

    public Reader reader()
    {
        return reader;
    }

    public Writer writer()
    {
        return writer;
    }

    @Override
    public InMemoryClosableChannel put( byte b )
    {
        writer.put( b );
        return this;
    }

    @Override
    public InMemoryClosableChannel putShort( short s )
    {
        writer.putShort( s );
        return this;
    }

    @Override
    public InMemoryClosableChannel putInt( int i )
    {
        writer.putInt( i );
        return this;
    }

    @Override
    public InMemoryClosableChannel putLong( long l )
    {
        writer.putLong( l );
        return this;
    }

    @Override
    public InMemoryClosableChannel putFloat( float f )
    {
        writer.putFloat( f );
        return this;
    }

    @Override
    public InMemoryClosableChannel putDouble( double d )
    {
        writer.putDouble( d );
        return this;
    }

    @Override
    public InMemoryClosableChannel put( byte[] bytes, int length )
    {
        writer.put( bytes, length );
        return this;
    }

    public boolean isOpen()
    {
        return true;
    }

    @Override
    public void close()
    {
        reader.close();
        writer.close();
    }

    @Override
    public Flushable prepareForFlush()
    {
        return NO_OP_FLUSHABLE;
    }

    @Override
    public byte get() throws ReadPastEndException
    {
        return reader.get();
    }

    @Override
    public short getShort() throws ReadPastEndException
    {
        return reader.getShort();
    }

    @Override
    public int getInt() throws ReadPastEndException
    {
        return reader.getInt();
    }

    @Override
    public long getLong() throws ReadPastEndException
    {
        return reader.getLong();
    }

    @Override
    public float getFloat() throws ReadPastEndException
    {
        return reader.getFloat();
    }

    @Override
    public double getDouble() throws ReadPastEndException
    {
        return reader.getDouble();
    }

    @Override
    public void get( byte[] bytes, int length ) throws ReadPastEndException
    {
        reader.get( bytes, length );
    }

    @Override
    public LogPositionMarker getCurrentPosition( LogPositionMarker positionMarker )
    {
        // Hmm, this would be for the writer.
        return writer.getCurrentPosition( positionMarker );
    }

    public int positionWriter( int position )
    {
        int previous = writer.position();
        writer.position( position );
        return previous;
    }

    public int positionReader( int position )
    {
        int previous = reader.position();
        reader.position( position );
        return previous;
    }

    public int readerPosition()
    {
        return reader.position();
    }

    public int writerPosition()
    {
        return writer.position();
    }

    public void truncateTo( int offset )
    {
        reader.limit( offset );
    }

    public int capacity()
    {
        return bytes.length;
    }

    public int availableBytesToRead()
    {
        return reader.remaining();
    }

    public int availableBytesToWrite()
    {
        return writer.remaining();
    }

    private static final Flushable NO_OP_FLUSHABLE = () ->
    {
    };

    class ByteBufferBase implements PositionAwareChannel, Closeable
    {
        protected final ByteBuffer buffer;

        ByteBufferBase( ByteBuffer buffer )
        {
            this.buffer = buffer;
        }

        void clear()
        {
            buffer.clear();
        }

        int position()
        {
            return buffer.position();
        }

        void position( int position )
        {
            buffer.position( position );
        }

        int remaining()
        {
            return buffer.remaining();
        }

        void limit( int offset )
        {
            buffer.limit( offset );
        }

        @Override
        public void close()
        {
        }

        @Override
        public LogPositionMarker getCurrentPosition( LogPositionMarker positionMarker )
        {
            positionMarker.mark( 0, buffer.position() );
            return positionMarker;
        }
    }

    public class Reader extends ByteBufferBase implements ReadableClosablePositionAwareChannel, PositionableChannel
    {
        Reader( ByteBuffer buffer )
        {
            super( buffer );
        }

        @Override
        public byte get() throws ReadPastEndException
        {
            ensureAvailableToRead( 1 );
            return buffer.get();
        }

        @Override
        public short getShort() throws ReadPastEndException
        {
            ensureAvailableToRead( 2 );
            return buffer.getShort();
        }

        @Override
        public int getInt() throws ReadPastEndException
        {
            ensureAvailableToRead( 4 );
            return buffer.getInt();
        }

        @Override
        public long getLong() throws ReadPastEndException
        {
            ensureAvailableToRead( 8 );
            return buffer.getLong();
        }

        @Override
        public float getFloat() throws ReadPastEndException
        {
            ensureAvailableToRead( 4 );
            return buffer.getFloat();
        }

        @Override
        public double getDouble() throws ReadPastEndException
        {
            ensureAvailableToRead( 8 );
            return buffer.getDouble();
        }

        @Override
        public void get( byte[] bytes, int length ) throws ReadPastEndException
        {
            ensureAvailableToRead( length );
            buffer.get( bytes, 0, length );
        }

        private void ensureAvailableToRead( int i ) throws ReadPastEndException
        {
            if ( remaining() < i || position() + i > writer.position() )
            {
                throw ReadPastEndException.INSTANCE;
            }
        }

        @Override
        public void setCurrentPosition( long byteOffset )
        {
            buffer.position( toIntExact( byteOffset ) );
        }
    }

    public class Writer extends ByteBufferBase implements FlushablePositionAwareChannel
    {
        Writer( ByteBuffer buffer )
        {
            super( buffer );
        }

        @Override
        public Writer put( byte b )
        {
            buffer.put( b );
            return this;
        }

        @Override
        public Writer putShort( short s )
        {
            buffer.putShort( s );
            return this;
        }

        @Override
        public Writer putInt( int i )
        {
            buffer.putInt( i );
            return this;
        }

        @Override
        public Writer putLong( long l )
        {
            buffer.putLong( l );
            return this;
        }

        @Override
        public Writer putFloat( float f )
        {
            buffer.putFloat( f );
            return this;
        }

        @Override
        public Writer putDouble( double d )
        {
            buffer.putDouble( d );
            return this;
        }

        @Override
        public Writer put( byte[] bytes, int length )
        {
            buffer.put( bytes, 0, length );
            return this;
        }

        @Override
        public Flushable prepareForFlush()
        {
            return NO_OP_FLUSHABLE;
        }
    }
}
