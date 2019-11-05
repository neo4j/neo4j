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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.Checksum;

import org.neo4j.io.fs.ChecksumMismatchException;
import org.neo4j.io.fs.PositionableChannel;
import org.neo4j.io.fs.ReadPastEndException;

import static java.lang.Math.toIntExact;

/**
 * Implementation of {@link ReadableClosablePositionAwareChannel} operating over a {@code byte[]} in memory.
 */
public class InMemoryClosableChannel implements ReadableClosablePositionAwareChecksumChannel, FlushablePositionAwareChecksumChannel
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
    public int endChecksumAndValidate() throws IOException
    {
        return reader.endChecksumAndValidate();
    }

    @Override
    public LogPositionMarker getCurrentPosition( LogPositionMarker positionMarker )
    {
        // Hmm, this would be for the writer.
        return writer.getCurrentPosition( positionMarker );
    }

    @Override
    public int putChecksum()
    {
        return writer.putChecksum();
    }

    @Override
    public void beginChecksum()
    {
        reader.beginChecksum();
        writer.beginChecksum();
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

    static class ByteBufferBase implements PositionAwareChannel, Closeable
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

    public class Reader extends ByteBufferBase implements ReadableClosablePositionAwareChecksumChannel, PositionableChannel
    {
        private final Checksum checksum = CHECKSUM_FACTORY.get();

        Reader( ByteBuffer buffer )
        {
            super( buffer );
        }

        @Override
        public byte get() throws ReadPastEndException
        {
            ensureAvailableToRead( Byte.BYTES );
            updateCrc( Byte.BYTES );
            return buffer.get();
        }

        @Override
        public short getShort() throws ReadPastEndException
        {
            ensureAvailableToRead( Short.BYTES );
            updateCrc( Short.BYTES );
            return buffer.getShort();
        }

        @Override
        public int getInt() throws ReadPastEndException
        {
            ensureAvailableToRead( Integer.BYTES );
            updateCrc( Integer.BYTES );
            return buffer.getInt();
        }

        @Override
        public long getLong() throws ReadPastEndException
        {
            ensureAvailableToRead( Long.BYTES );
            updateCrc( Long.BYTES );
            return buffer.getLong();
        }

        @Override
        public float getFloat() throws ReadPastEndException
        {
            ensureAvailableToRead( Float.BYTES );
            updateCrc( Float.BYTES );
            return buffer.getFloat();
        }

        @Override
        public double getDouble() throws ReadPastEndException
        {
            ensureAvailableToRead( Double.BYTES );
            updateCrc( Double.BYTES );
            return buffer.getDouble();
        }

        @Override
        public void get( byte[] bytes, int length ) throws ReadPastEndException
        {
            ensureAvailableToRead( length );
            buffer.get( bytes, 0, length );
            checksum.update( bytes, 0, length );
        }

        @Override
        public int endChecksumAndValidate() throws ReadPastEndException
        {
            ensureAvailableToRead( Integer.BYTES );
            int checksum = (int) this.checksum.getValue();
            int storedChecksum = buffer.getInt();
            if ( checksum != storedChecksum )
            {
                throw new ChecksumMismatchException( storedChecksum, checksum );
            }
            beginChecksum();

            return checksum;
        }

        @Override
        public void beginChecksum()
        {
            checksum.reset();
        }

        @Override
        public void setCurrentPosition( long byteOffset )
        {
            buffer.position( toIntExact( byteOffset ) );
            beginChecksum();
        }

        private void ensureAvailableToRead( int i ) throws ReadPastEndException
        {
            if ( remaining() < i || position() + i > writer.position() )
            {
                throw ReadPastEndException.INSTANCE;
            }
        }

        private void updateCrc( int size )
        {
            checksum.update( buffer.array(), buffer.position(), size );
        }
    }

    public class Writer extends ByteBufferBase implements FlushablePositionAwareChecksumChannel
    {
        private final Checksum checksum = CHECKSUM_FACTORY.get();

        Writer( ByteBuffer buffer )
        {
            super( buffer );
        }

        @Override
        public Writer put( byte b )
        {
            buffer.put( b );
            updateCrc( Byte.BYTES );
            return this;
        }

        @Override
        public Writer putShort( short s )
        {
            buffer.putShort( s );
            updateCrc( Short.BYTES );
            return this;
        }

        @Override
        public Writer putInt( int i )
        {
            buffer.putInt( i );
            updateCrc( Integer.BYTES );
            return this;
        }

        @Override
        public Writer putLong( long l )
        {
            buffer.putLong( l );
            updateCrc( Long.BYTES );
            return this;
        }

        @Override
        public Writer putFloat( float f )
        {
            buffer.putFloat( f );
            updateCrc( Float.BYTES );
            return this;
        }

        @Override
        public Writer putDouble( double d )
        {
            buffer.putDouble( d );
            updateCrc( Double.BYTES );
            return this;
        }

        @Override
        public Writer put( byte[] bytes, int length )
        {
            buffer.put( bytes, 0, length );
            checksum.update( bytes, 0, length );
            return this;
        }

        @Override
        public Flushable prepareForFlush()
        {
            return NO_OP_FLUSHABLE;
        }

        @Override
        public int putChecksum()
        {
            int checksum = (int) this.checksum.getValue();
            buffer.putInt( checksum );
            return checksum;
        }

        @Override
        public void beginChecksum()
        {
            checksum.reset();
        }

        private void updateCrc( int size )
        {
            checksum.update( buffer.array(), buffer.position() - size, size );
        }
    }
}
