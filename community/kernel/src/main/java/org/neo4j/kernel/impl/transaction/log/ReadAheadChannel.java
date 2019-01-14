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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.io.fs.StoreChannel;
import org.neo4j.storageengine.api.ReadPastEndException;

import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.lang.System.arraycopy;

import static org.neo4j.io.ByteUnit.kibiBytes;

/**
 * A buffering implementation of {@link ReadableClosableChannel}. This class also allows subclasses to read content
 * spanning more than one file, by properly implementing {@link #next(StoreChannel)}.
 * @param <T> The type of StoreChannel wrapped
 */
public class ReadAheadChannel<T extends StoreChannel> implements ReadableClosableChannel, PositionableChannel
{
    public static final int DEFAULT_READ_AHEAD_SIZE = toIntExact( kibiBytes( 4 ) );

    protected T channel;
    private final ByteBuffer aheadBuffer;
    private final int readAheadSize;

    public ReadAheadChannel( T channel )
    {
        this( channel, DEFAULT_READ_AHEAD_SIZE );
    }

    public ReadAheadChannel( T channel, int readAheadSize )
    {
        this.aheadBuffer = ByteBuffer.allocate( readAheadSize );
        this.aheadBuffer.position( aheadBuffer.capacity() );
        this.channel = channel;
        this.readAheadSize = readAheadSize;
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
        ensureDataExists( 2 );
        return aheadBuffer.getShort();
    }

    @Override
    public int getInt() throws IOException
    {
        ensureDataExists( 4 );
        return aheadBuffer.getInt();
    }

    @Override
    public long getLong() throws IOException
    {
        ensureDataExists( 8 );
        return aheadBuffer.getLong();
    }

    @Override
    public float getFloat() throws IOException
    {
        ensureDataExists( 4 );
        return aheadBuffer.getFloat();
    }

    @Override
    public double getDouble() throws IOException
    {
        ensureDataExists( 8 );
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
    public void close() throws IOException
    {
        channel.close();
    }

    private void ensureDataExists( int requestedNumberOfBytes ) throws IOException
    {
        int remaining = aheadBuffer.remaining();
        if ( remaining >= requestedNumberOfBytes )
        {
            return;
        }

        // We ran out, try to read some more
        // start by copying the remaining bytes to the beginning
        compactToBeginningOfBuffer( remaining );

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

    /*
     * Moves bytes between aheadBuffer.position() and aheadBuffer.capacity() to the beginning of aheadBuffer. At the
     * end of this call the aheadBuffer is positioned in end of that moved content.
     * This is to be used in preparation of reading more content in from the channel without having exhausted all
     * previous bytes.
     */
    private void compactToBeginningOfBuffer( int remaining )
    {
        arraycopy( aheadBuffer.array(), aheadBuffer.position(), aheadBuffer.array(), 0, remaining );
        aheadBuffer.clear();
        aheadBuffer.position( remaining );
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
    }
}
