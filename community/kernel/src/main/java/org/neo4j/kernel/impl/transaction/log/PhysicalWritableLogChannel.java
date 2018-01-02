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
import java.nio.channels.ClosedChannelException;

import org.neo4j.io.ByteUnit;

import static java.lang.Math.min;

public class PhysicalWritableLogChannel implements WritableLogChannel
{
    private final ByteBuffer buffer;

    private volatile boolean closed;
    private LogVersionedStoreChannel channel;

    public PhysicalWritableLogChannel( LogVersionedStoreChannel channel )
    {
        this( channel, (int) ByteUnit.kibiBytes( 512 ) );
    }

    public PhysicalWritableLogChannel( LogVersionedStoreChannel channel, int bufferSize )
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
    public Flushable emptyBufferIntoChannelAndClearIt() throws IOException
    {
        buffer.flip();
        LogVersionedStoreChannel channel = this.channel;
        try
        {
            channel.write( buffer );
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
    public WritableLogChannel put( byte value ) throws IOException
    {
        bufferWithGuaranteedSpace( 1 ).put( value );
        return this;
    }

    @Override
    public WritableLogChannel putShort( short value ) throws IOException
    {
        bufferWithGuaranteedSpace( 2 ).putShort( value );
        return this;
    }

    @Override
    public WritableLogChannel putInt( int value ) throws IOException
    {
        bufferWithGuaranteedSpace( 4 ).putInt( value );
        return this;
    }

    @Override
    public WritableLogChannel putLong( long value ) throws IOException
    {
        bufferWithGuaranteedSpace( 8 ).putLong( value );
        return this;
    }

    @Override
    public WritableLogChannel putFloat( float value ) throws IOException
    {
        bufferWithGuaranteedSpace( 4 ).putFloat( value );
        return this;
    }

    @Override
    public WritableLogChannel putDouble( double value ) throws IOException
    {
        bufferWithGuaranteedSpace( 8 ).putDouble( value );
        return this;
    }

    @Override
    public WritableLogChannel put( byte[] value, int length ) throws IOException
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

    @Override
    public LogPositionMarker getCurrentPosition( LogPositionMarker positionMarker ) throws IOException
    {
        positionMarker.mark( channel.getVersion(), channel.position() + buffer.position() );
        return positionMarker;
    }

    private ByteBuffer bufferWithGuaranteedSpace( int spaceInBytes ) throws IOException
    {
        assert spaceInBytes < buffer.capacity();
        if ( buffer.remaining() < spaceInBytes )
        {
            emptyBufferIntoChannelAndClearIt();
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
        emptyBufferIntoChannelAndClearIt();
        closed = true;
    }
}
