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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.io.fs.StoreChannel;

import static java.lang.Math.min;
import static java.lang.System.arraycopy;

/**
 * Basically a sequence of {@link StoreChannel channels} seamlessly seen as one.
 */
public class ReadAheadLogChannel implements ReadableVersionableLogChannel
{
    public static final int DEFAULT_READ_AHEAD_SIZE = 1024*4;

    private final ByteBuffer aheadBuffer;
    private LogVersionedStoreChannel channel;
    private final LogVersionBridge bridge;
    private final int readAheadSize;

    public ReadAheadLogChannel( LogVersionedStoreChannel startingChannel, LogVersionBridge bridge, int readAheadSize )
    {
        this.channel = startingChannel;
        this.bridge = bridge;
        this.readAheadSize = readAheadSize;
        this.aheadBuffer = ByteBuffer.allocate( readAheadSize );
        aheadBuffer.position( aheadBuffer.capacity() );
    }

    @Override
    public long getVersion()
    {
        return channel.getVersion();
    }

    @Override
    public byte getLogFormatVersion()
    {
        return channel.getLogFormatVersion();
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
            int chunkSize = min( readAheadSize >> 2, (length-bytesGotten) );
            ensureDataExists( chunkSize );
            aheadBuffer.get( bytes, bytesGotten, chunkSize );
            bytesGotten += chunkSize;
        }
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
        arraycopy( aheadBuffer.array(), aheadBuffer.position(), aheadBuffer.array(), 0, remaining );
        aheadBuffer.clear();

        // fill the buffer (preferably to the brim)
        aheadBuffer.position( remaining );
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
                LogVersionedStoreChannel nextChannel = bridge.next( channel );
                assert nextChannel != null;
                if ( nextChannel == channel )
                {
                    // no more channels so we cannot satisfy the requested number of bytes
                    throw ReadPastEndException.INSTANCE;
                }
                channel = nextChannel;
            }
        }
        // prepare for reading
        aheadBuffer.flip();
    }

    @Override
    public void close() throws IOException
    {
        channel.close();
    }

    @Override
    public LogPositionMarker getCurrentPosition( LogPositionMarker positionMarker ) throws IOException
    {
        positionMarker.mark( channel.getVersion(), channel.position()-aheadBuffer.remaining() );
        return positionMarker;
    }
}
