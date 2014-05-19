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

import static java.lang.Math.min;
import static java.lang.System.arraycopy;

/**
 * Basically a sequence of {@link StoreChannel channels} seamlessly seen as one.
 */
public class ReadAheadLogChannel implements ReadableLogChannel
{
    public static final int DEFAULT_READ_AHEAD_SIZE = 1024*4;

    private final byte[] backingArray;
    private final ByteBuffer aheadBuffer;
    private VersionedStoreChannel channel;
    private final LogVersionBridge channelBridge;
    private final int readAheadSize;

    public ReadAheadLogChannel( VersionedStoreChannel startingChannel, LogVersionBridge channelBridge,
            int readAheadSize )
    {
        this.channel = startingChannel;
        this.channelBridge = channelBridge;
        this.readAheadSize = readAheadSize;
        this.backingArray = new byte[readAheadSize];
        this.aheadBuffer = ByteBuffer.wrap( backingArray );
        aheadBuffer.position( aheadBuffer.capacity() );
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

    @Override
    public void get( char[] chars, int length ) throws IOException
    {
        assert length <= chars.length;

        int charsGotten = 0;
        while ( charsGotten < length )
        {   // get max 1024 bytes at the time, so that ensureDataExists functions as it should
            int chunkSize = min( readAheadSize >> 3, (length-charsGotten) );
            ensureDataExists( chunkSize );
            aheadBuffer.asCharBuffer().get( chars, charsGotten, (length - charsGotten) );
            charsGotten += chunkSize;
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
            {   // current channel ran out, try the next channel
                VersionedStoreChannel nextChannel = channelBridge.next( channel );
                assert nextChannel != null;
                if ( nextChannel == channel )
                {   // no more channels...
                    if ( aheadBuffer.position() >= requestedNumberOfBytes )
                    {   // ...although we have read enough to satisfy the requested number of bytes
                        break;
                    }

                    // ...so we cannot satisfy the requested number of bytes
                    throw new ReadPastEndException();
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
    public LogPosition getCurrentPosition() throws IOException
    {
        return channel.getCurrentPosition();
    }
}
