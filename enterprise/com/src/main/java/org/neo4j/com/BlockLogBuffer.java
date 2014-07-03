/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.com;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import org.jboss.netty.buffer.ChannelBuffer;

import org.neo4j.kernel.impl.transaction.xaframework.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.xaframework.WritableLogChannel;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;

/**
 * Implementation of a LogBuffer over a ChannelBuffer. Maintains a byte buffer
 * of content which is flushed to the underlying channel when a maximum size is
 * reached. It is supposed to be used with {@link BlockLogReader}.
 * <p>
 * Every chunk is exactly 256 bytes in length, except for the last one which can
 * be anything greater than one and up to 256. This is signaled via the first
 * byte which is 0 for every non-last chunk and the actual number of bytes for
 * the last one (always > 0).
 */
public class BlockLogBuffer implements WritableLogChannel
{
    // First byte of every chunk that is not the last one
    static final byte FULL_BLOCK_AND_MORE = 0;
    static final int MAX_SIZE = 256; /* soft limit, incl. header */
    static final int DATA_SIZE = MAX_SIZE-1;

    private final ChannelBuffer target;
    private final ByteCounterMonitor monitor;
    // MAX_SIZE can be overcome by one primitive put(), the largest is 8 bytes
    private final byte[] byteArray = new byte[MAX_SIZE + 8/*largest atom*/];
    private final ByteBuffer byteBuffer = ByteBuffer.wrap( byteArray );

    public BlockLogBuffer( ChannelBuffer target, ByteCounterMonitor monitor )
    {
        this.target = target;
        this.monitor = monitor;
        clearInternalBuffer();
    }

    private void clearInternalBuffer()
    {
        byteBuffer.clear();
        // reserve space for size - assume we are going to fill the buffer
        byteBuffer.put( FULL_BLOCK_AND_MORE );
    }

    /**
     * If the position of the byteBuffer is larger than MAX_SIZE then
     * MAX_SIZE bytes are flushed to the underlying channel. The remaining
     * bytes (1 up to and including 8 - see the byteArray field initializer)
     * are moved over at the beginning of the cleared buffer.
     *
     * @return the buffer
     */
    private WritableLogChannel checkFlush()
    {
        if ( byteBuffer.position() > MAX_SIZE )
        {
            target.writeBytes( byteArray, 0, MAX_SIZE );
            monitor.bytesWritten( MAX_SIZE );
            int pos = byteBuffer.position();
            clearInternalBuffer();
            byteBuffer.put( byteArray, MAX_SIZE, pos - MAX_SIZE );
        }
        return this;
    }

    @Override
    public WritableLogChannel put( byte b ) throws IOException
    {
        byteBuffer.put( b );
        return checkFlush();
    }

    @Override
    public WritableLogChannel putShort( short s ) throws IOException
    {
        byteBuffer.putShort( s );
        return checkFlush();
    }

    @Override
    public WritableLogChannel putInt( int i ) throws IOException
    {
        byteBuffer.putInt( i );
        return checkFlush();
    }

    @Override
    public WritableLogChannel putLong( long l ) throws IOException
    {
        byteBuffer.putLong( l );
        return checkFlush();
    }

    @Override
    public WritableLogChannel putFloat( float f ) throws IOException
    {
        byteBuffer.putFloat( f );
        return checkFlush();
    }

    @Override
    public WritableLogChannel putDouble( double d ) throws IOException
    {
        byteBuffer.putDouble( d );
        return checkFlush();
    }

    @Override
    public WritableLogChannel put( byte[] bytes, int length ) throws IOException
    {
        for ( int pos = 0; pos < length; )
        {
            int toWrite = Math.min( byteBuffer.remaining(), length - pos );
            byteBuffer.put( bytes, pos, toWrite );
            checkFlush();
            pos += toWrite;
        }
        return this;
    }

    @Override
    public void force() throws IOException
    {
        // Do nothing
    }

    @Override
    public void getCurrentPosition( LogPositionMarker positionMarker ) throws IOException
    {
        positionMarker.unspecified();
    }

    @Override
    public ReadableByteChannel getReadableChannel()
    {
        return getFileChannel();
    }

    /**
     * Signals the end of use for this buffer over this channel - first byte of
     * the chunk is set to the position of the buffer ( != 0, instead of
     * FULL_BLOCK_AND_MORE) and it is written to the channel.
     */
    @Override
    public void close()
    {
        assert byteBuffer.position() > 1 : "buffer should contain more than the header";
        assert byteBuffer.position() <= MAX_SIZE : "buffer should not be over full";
        long howManyBytesToWrite = byteBuffer.position();
        byteBuffer.put( 0, (byte) ( byteBuffer.position() - 1 ) );
        byteBuffer.flip();
        target.writeBytes( byteBuffer );
        monitor.bytesWritten( howManyBytesToWrite );
        clearInternalBuffer();
    }

    public int write( ReadableByteChannel data ) throws IOException
    {
        int result = 0;
        int bytesRead = 0;
        while ( (bytesRead = data.read( byteBuffer )) >= 0 )
        {
            checkFlush();
            result += bytesRead;
        }
        return result;
    }
}
