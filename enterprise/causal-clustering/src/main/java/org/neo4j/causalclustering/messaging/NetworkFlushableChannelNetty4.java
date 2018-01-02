/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.messaging;

import io.netty.buffer.ByteBuf;

import java.io.Flushable;

import org.neo4j.kernel.impl.transaction.log.FlushableChannel;

import static java.lang.String.format;
import static org.neo4j.io.ByteUnit.mebiBytes;

public class NetworkFlushableChannelNetty4 implements FlushableChannel
{
    /**
     * This implementation puts an upper limit to the size of the state serialized in the buffer. The default
     * value for that should be sufficient for all replicated state except for transactions, the size of which
     * is unbounded.
     */
    private static final long DEFAULT_SIZE_LIMIT = mebiBytes( 2 );

    private final ByteBuf delegate;
    private final int initialWriterIndex;

    private final long sizeLimit;

    public NetworkFlushableChannelNetty4( ByteBuf delegate )
    {
        this( delegate, DEFAULT_SIZE_LIMIT );
    }

    public NetworkFlushableChannelNetty4( ByteBuf delegate, long sizeLimit )
    {
        this.delegate = delegate;
        this.initialWriterIndex = delegate.writerIndex();
        this.sizeLimit = sizeLimit;
    }

    @Override
    public Flushable prepareForFlush()
    {
        return null;
    }

    @Override
    public FlushableChannel put( byte value ) throws MessageTooBigException
    {
        checkSize( Byte.BYTES );
        delegate.writeByte( value );
        return this;
    }

    @Override
    public FlushableChannel putShort( short value ) throws MessageTooBigException
    {
        checkSize( Short.BYTES );
        delegate.writeShort( value );
        return this;
    }

    @Override
    public FlushableChannel putInt( int value ) throws MessageTooBigException
    {
        checkSize( Integer.BYTES );
        delegate.writeInt( value );
        return this;
    }

    @Override
    public FlushableChannel putLong( long value ) throws MessageTooBigException
    {
        checkSize( Long.BYTES );
        delegate.writeLong( value );
        return this;
    }

    @Override
    public FlushableChannel putFloat( float value ) throws MessageTooBigException
    {
        checkSize( Float.BYTES );
        delegate.writeFloat( value );
        return this;
    }

    @Override
    public FlushableChannel putDouble( double value ) throws MessageTooBigException
    {
        checkSize( Double.BYTES );
        delegate.writeDouble( value );
        return this;
    }

    @Override
    public FlushableChannel put( byte[] value, int length ) throws MessageTooBigException
    {
        checkSize( length );
        delegate.writeBytes( value, 0, length );
        return this;
    }

    @Override
    public void close()
    {
    }

    private void checkSize( int additional ) throws MessageTooBigException
    {
        int writtenSoFar = delegate.writerIndex() - initialWriterIndex;
        int countToCheck = writtenSoFar + additional;
        if ( countToCheck > sizeLimit )
        {
            throw new MessageTooBigException( format(
                    "Size limit exceeded. Limit is %d, wanted to write %d with the writer index at %d (started at %d), written so far %d",
                    sizeLimit, additional, delegate.writerIndex(), initialWriterIndex, writtenSoFar ) );
        }
    }
}
