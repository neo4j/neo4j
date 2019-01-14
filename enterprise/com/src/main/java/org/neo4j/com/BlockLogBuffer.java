/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.com;

import org.jboss.netty.buffer.ChannelBuffer;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import org.neo4j.kernel.monitoring.ByteCounterMonitor;

/**
 * Implementation of a LogBuffer over a ChannelBuffer. Maintains a byte buffer
 * of content which is flushed to the underlying channel when a maximum size is
 * reached. It is supposed to be used with {@link BlockLogReader}.
 * <p>
 * Every chunk is exactly 256 bytes in length, except for the last one which can
 * be anything greater than one and up to 256. This is signaled via the first
 * byte which is 0 for every non-last chunk and the actual number of bytes for
 * the last one (always &gt; 0).
 */
public class BlockLogBuffer implements Closeable
{
    // First byte of every chunk that is not the last one
    static final byte FULL_BLOCK_AND_MORE = 0;
    static final int MAX_SIZE = 256; /* soft limit, incl. header */
    static final int DATA_SIZE = MAX_SIZE - 1;

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
    private BlockLogBuffer checkFlush()
    {
        if ( byteBuffer.position() > MAX_SIZE )
        {
            flush();
        }
        return this;
    }

    private void flush()
    {
        int howManyBytesToWrite = MAX_SIZE;
        target.writeBytes( byteArray, 0, howManyBytesToWrite );
        monitor.bytesWritten( howManyBytesToWrite );
        int pos = byteBuffer.position();
        clearInternalBuffer();
        byteBuffer.put( byteArray, howManyBytesToWrite, pos - howManyBytesToWrite );
    }

    public BlockLogBuffer put( byte b )
    {
        byteBuffer.put( b );
        return checkFlush();
    }

    public BlockLogBuffer putShort( short s )
    {
        byteBuffer.putShort( s );
        return checkFlush();
    }

    public BlockLogBuffer putInt( int i )
    {
        byteBuffer.putInt( i );
        return checkFlush();
    }

    public BlockLogBuffer putLong( long l )
    {
        byteBuffer.putLong( l );
        return checkFlush();
    }

    public BlockLogBuffer putFloat( float f )
    {
        byteBuffer.putFloat( f );
        return checkFlush();
    }

    public BlockLogBuffer putDouble( double d )
    {
        byteBuffer.putDouble( d );
        return checkFlush();
    }

    public BlockLogBuffer put( byte[] bytes, int length )
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
        int bytesRead;
        while ( (bytesRead = data.read( byteBuffer )) >= 0 )
        {
            checkFlush();
            result += bytesRead;
        }
        return result;
    }
}
