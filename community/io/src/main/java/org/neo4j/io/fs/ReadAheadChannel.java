/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.io.fs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.zip.Checksum;

import org.neo4j.io.memory.ScopedBuffer;

import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.fs.ChecksumWriter.CHECKSUM_FACTORY;
import static org.neo4j.io.fs.PhysicalFlushableChecksumChannel.DISABLE_WAL_CHECKSUM;

/**
 * A buffering implementation of {@link ReadableChannel}. This class also allows subclasses to read content
 * spanning more than one file, by properly implementing {@link #next(StoreChannel)}.
 * @param <T> The type of StoreChannel wrapped
 */
public class ReadAheadChannel<T extends StoreChannel> implements ReadableChecksumChannel, PositionableChannel
{
    public static final int DEFAULT_READ_AHEAD_SIZE = toIntExact( kibiBytes( 4 ) );
    private ScopedBuffer scopedBuffer;

    protected T channel;
    private final ByteBuffer aheadBuffer;
    private final int readAheadSize;
    private final Checksum checksum;
    private final ByteBuffer checksumView;

    public ReadAheadChannel( T channel, ByteBuffer byteBuffer )
    {
        requireNonNull( channel );
        requireNonNull( byteBuffer );
        this.aheadBuffer = byteBuffer;
        this.aheadBuffer.position( aheadBuffer.capacity() );
        this.channel = channel;
        this.readAheadSize = aheadBuffer.capacity();
        this.checksumView = aheadBuffer.duplicate();
        this.checksum = CHECKSUM_FACTORY.get();
    }

    public ReadAheadChannel( T channel, ScopedBuffer scopedBuffer )
    {
        this( channel, scopedBuffer.getBuffer() );
        this.scopedBuffer = scopedBuffer;
    }

    /**
     * This is the position within the buffered stream (and not the
     * underlying channel, which will generally be further ahead).
     *
     * @return The position within the buffered stream.
     * @throws IOException on I/O error.
     */
    public long position() throws IOException
    {
        return channel.position() - aheadBuffer.remaining();
    }

    @Override
    public byte get() throws IOException
    {
        ensureDataExists( 1 );
        return aheadBuffer.get();
    }

    @Override
    public short getShort() throws IOException
    {
        ensureDataExists( Short.BYTES );
        return aheadBuffer.getShort();
    }

    @Override
    public int getInt() throws IOException
    {
        ensureDataExists( Integer.BYTES );
        return aheadBuffer.getInt();
    }

    @Override
    public long getLong() throws IOException
    {
        ensureDataExists( Long.BYTES );
        return aheadBuffer.getLong();
    }

    @Override
    public float getFloat() throws IOException
    {
        ensureDataExists( Float.BYTES );
        return aheadBuffer.getFloat();
    }

    @Override
    public double getDouble() throws IOException
    {
        ensureDataExists( Double.BYTES );
        return aheadBuffer.getDouble();
    }

    @Override
    public void get( byte[] bytes, int length ) throws IOException
    {
        assert length <= bytes.length;

        int bytesGotten = 0;
        while ( bytesGotten < length )
        {   // get max 1024 bytes at the time, so that ensureDataExists functions as it should
            int chunkSize = min( readAheadSize >> 2, length - bytesGotten );
            ensureDataExists( chunkSize );
            aheadBuffer.get( bytes, bytesGotten, chunkSize );
            bytesGotten += chunkSize;
        }
    }

    @Override
    public int endChecksumAndValidate() throws IOException
    {
        ensureDataExists( 4 );

        if ( DISABLE_WAL_CHECKSUM )
        {
            aheadBuffer.getInt();
            return 0xDEAD5EED;
        }

        // Consume remaining bytes
        checksumView.limit( aheadBuffer.position() );
        checksum.update( checksumView );

        // Validate checksum
        int calculatedChecksum = (int) checksum.getValue();
        int checksum = aheadBuffer.getInt();
        if ( calculatedChecksum != checksum )
        {
            throw new ChecksumMismatchException( checksum, calculatedChecksum );
        }
        beginChecksum();

        return calculatedChecksum;
    }

    @Override
    public void beginChecksum()
    {
        if ( DISABLE_WAL_CHECKSUM )
        {
            return;
        }
        checksum.reset();
        checksumView.limit( checksumView.capacity() );
        checksumView.position( aheadBuffer.position() );
    }

    @Override
    public void close() throws IOException
    {
        if ( channel != null )
        {
            channel.close();
            channel = null;
        }
        if ( scopedBuffer != null )
        {
            scopedBuffer.close();
        }
    }

    private void ensureDataExists( int requestedNumberOfBytes ) throws IOException
    {
        if ( !channel.isOpen() )
        {
            throw new ClosedChannelException();
        }

        int remaining = aheadBuffer.remaining();
        if ( remaining >= requestedNumberOfBytes )
        {
            return;
        }

        // Update checksum with consumed bytes
        if ( !DISABLE_WAL_CHECKSUM )
        {
            checksumView.limit( aheadBuffer.position() );
            checksum.update( checksumView );
            checksumView.limit( checksumView.capacity() );
            checksumView.position( 0 );
        }

        // We ran out, try to read some more
        // start by copying the remaining bytes to the beginning
        aheadBuffer.compact();

        while ( aheadBuffer.position() < aheadBuffer.capacity() )
        {   // read from the current channel to try and fill the buffer
            int read = channel.read( aheadBuffer );
            if ( read == -1 )
            {
                // current channel ran out...
                if ( aheadBuffer.position() >= requestedNumberOfBytes )
                {   // ...although we have satisfied the request
                    break;
                }

                // ... we need to read even further, into the next version
                T nextChannel = next( channel );
                assert nextChannel != null;
                if ( nextChannel == channel )
                {
                    // no more channels so we cannot satisfy the requested number of bytes
                    aheadBuffer.flip();
                    throw ReadPastEndException.INSTANCE;
                }
                channel = nextChannel;
            }
        }
        // prepare for reading
        aheadBuffer.flip();
    }

    /**
     * Hook for allowing subclasses to read content spanning a sequence of files. This method is called when the current
     * file channel is exhausted and a new channel is required for reading. The default implementation returns the
     * argument, which is the condition for indicating no more content, resulting in a {@link ReadPastEndException} being
     * thrown.
     * @param channel The channel that has just been exhausted.
     * @throws IOException on I/O error.
     */
    protected T next( T channel ) throws IOException
    {
        return channel;
    }

    @Override
    public void setCurrentPosition( long byteOffset ) throws IOException
    {
        long positionRelativeToAheadBuffer = byteOffset - (channel.position() - aheadBuffer.limit());
        if ( positionRelativeToAheadBuffer >= aheadBuffer.limit() || positionRelativeToAheadBuffer < 0 )
        {
            // Beyond what we currently have buffered
            aheadBuffer.position( aheadBuffer.limit() );
            channel.position( byteOffset );
        }
        else
        {
            aheadBuffer.position( toIntExact( positionRelativeToAheadBuffer ) );
        }

        // After repositioning we need to reset checksum calculations
        beginChecksum();
    }
}
