/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.io.Flushable;
import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import org.neo4j.kernel.impl.transaction.log.FlushableChannel;

public class NetworkFlushableChannel implements Flushable, FlushableChannel
{
    private final ChannelBuffer delegate;

    public NetworkFlushableChannel( ChannelBuffer delegate )
    {
        this.delegate = delegate;
    }

    @Override
    public void flush() throws IOException
    {
    }

    @Override
    public FlushableChannel put( byte value ) throws IOException
    {
        delegate.writeByte( value );
        return this;
    }

    @Override
    public FlushableChannel putShort( short value ) throws IOException
    {
        delegate.writeShort( value );
        return this;
    }

    @Override
    public FlushableChannel putInt( int value ) throws IOException
    {
        delegate.writeInt( value );
        return this;
    }

    @Override
    public FlushableChannel putLong( long value ) throws IOException
    {
        delegate.writeLong( value );
        return this;
    }

    @Override
    public FlushableChannel putFloat( float value ) throws IOException
    {
        delegate.writeFloat( value );
        return this;
    }

    @Override
    public FlushableChannel putDouble( double value ) throws IOException
    {
        delegate.writeDouble( value );
        return this;
    }

    @Override
    public FlushableChannel put( byte[] value, int length ) throws IOException
    {
        delegate.writeBytes( value, 0, length );
        return this;
    }

    @Override
    public void close() throws IOException
    {
    }

    @Override
    public Flushable prepareForFlush()
    {
        return this;
    }
}
