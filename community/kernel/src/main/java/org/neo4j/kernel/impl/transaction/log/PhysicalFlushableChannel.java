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

import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;

import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.StoreChannel;

import static java.lang.Math.min;

/**
 * The main implementation of {@link FlushableChannel}. This class provides buffering over a simple {@link StoreChannel}
 * and, as a side effect, allows control of the flushing of that buffer to disk.
 */
public class PhysicalFlushableChannel implements FlushableChannel
{
    public static final int DEFAULT_BUFFER_SIZE = (int) ByteUnit.kibiBytes( 512 );

    private volatile boolean closed;

    protected final ByteBuffer buffer;
    protected StoreChannel channel;

    public PhysicalFlushableChannel( StoreChannel channel )
    {
        this( channel, DEFAULT_BUFFER_SIZE );
    }

    public PhysicalFlushableChannel( StoreChannel channel, int bufferSize )
    {
        this.channel = channel;
        this.buffer = ByteBuffer.allocate( bufferSize );
    }

    void setChannel( LogVersionedStoreChannel channel )
    {
        this.channel = channel;
    }

    /**
     * External synchronization between this method and close is required so that they aren't called concurrently.
     * Currently that's done by acquiring the PhysicalLogFile monitor.
     */
    @Override
    public Flushable prepareForFlush() throws IOException
    {
        buffer.flip();
        StoreChannel channel = this.channel;
        try
        {
            channel.writeAll( buffer );
        }
        catch ( ClosedChannelException e )
        {
            handleClosedChannelException( e );
        }
        buffer.clear();
        return channel;
    }

    private void handleClosedChannelException( ClosedChannelException e ) throws ClosedChannelException
    {
        // We don't want to check the closed flag every time we empty, instead we can avoid unnecessary the
        // volatile read and catch ClosedChannelException where we see if the channel being closed was
        // deliberate or not. If it was deliberately closed then throw IllegalStateException instead so
        // that callers won't treat this as a kernel panic.
        if ( closed )
        {
            throw new IllegalStateException( "This log channel has been closed", e );
        }

        // OK, this channel was closed without us really knowing about it, throw exception as is.
        throw e;
    }

    @Override
    public FlushableChannel put( byte value ) throws IOException
    {
        bufferWithGuaranteedSpace( 1 ).put( value );
        return this;
    }

    @Override
    public FlushableChannel putShort( short value ) throws IOException
    {
        bufferWithGuaranteedSpace( 2 ).putShort( value );
        return this;
    }

    @Override
    public FlushableChannel putInt( int value ) throws IOException
    {
        bufferWithGuaranteedSpace( 4 ).putInt( value );
        return this;
    }

    @Override
    public FlushableChannel putLong( long value ) throws IOException
    {
        bufferWithGuaranteedSpace( 8 ).putLong( value );
        return this;
    }

    @Override
    public FlushableChannel putFloat( float value ) throws IOException
    {
        bufferWithGuaranteedSpace( 4 ).putFloat( value );
        return this;
    }

    @Override
    public FlushableChannel putDouble( double value ) throws IOException
    {
        bufferWithGuaranteedSpace( 8 ).putDouble( value );
        return this;
    }

    @Override
    public FlushableChannel put( byte[] value, int length ) throws IOException
    {
        int offset = 0;
        while ( offset < length )
        {
            int chunkSize = min( length - offset, buffer.capacity() >> 1 );
            bufferWithGuaranteedSpace( chunkSize ).put( value, offset, chunkSize );

            offset += chunkSize;
        }
        return this;
    }

    private ByteBuffer bufferWithGuaranteedSpace( int spaceInBytes ) throws IOException
    {
        assert spaceInBytes < buffer.capacity();
        if ( buffer.remaining() < spaceInBytes )
        {
            prepareForFlush();
        }
        return buffer;
    }

    /**
     * External synchronization between this method and emptyBufferIntoChannelAndClearIt is required so that they
     * aren't called concurrently. Currently that's done by acquiring the PhysicalLogFile monitor.
     */
    @Override
    public void close() throws IOException
    {
        prepareForFlush().flush();
        closed = true;
        channel.close();
    }

    /**
     * @return the position of the channel, also taking into account buffer position.
     * @throws IOException if underlying channel throws {@link IOException}.
     */
    public long position() throws IOException
    {
        return channel.position() + buffer.position();
    }

    /**
     * Sets position of this channel to the new {@code position}. This works only if the underlying channel
     * supports positioning.
     *
     * @param position new position (byte offset) to set as new current position.
     * @throws IOException if underlying channel throws {@link IOException}.
     */
    public void position( long position ) throws IOException
    {
        // Currently we take the pessimistic approach of flushing (doesn't imply forcing) buffer to
        // channel before moving to a new position. This works in all cases, but there could be
        // made an optimization where we could see that we're moving within the current buffer range
        // and if so skip flushing and simply move the cursor in the buffer.
        prepareForFlush().flush();
        channel.position( position );
    }
}
