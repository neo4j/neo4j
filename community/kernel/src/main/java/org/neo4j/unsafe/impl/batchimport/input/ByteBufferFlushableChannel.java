/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.input;

import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.kernel.impl.transaction.log.FlushableChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableClosableChannel;

/**
 * TODO a bit overkill to wrap a byte[] -> {@link ByteBuffer} -> {@link ReadableClosableChannel}?
 */
public class ByteBufferFlushableChannel implements FlushableChannel, Flushable
{
    private final ByteBuffer buffer;

    public ByteBufferFlushableChannel( ByteBuffer buffer )
    {
        this.buffer = buffer;
    }

    @Override
    public Flushable prepareForFlush() throws IOException
    {
        return this;
    }

    @Override
    public FlushableChannel put( byte value ) throws IOException
    {
        buffer.put( value );
        return this;
    }

    @Override
    public FlushableChannel putShort( short value ) throws IOException
    {
        buffer.putShort( value );
        return this;
    }

    @Override
    public FlushableChannel putInt( int value ) throws IOException
    {
        buffer.putInt( value );
        return this;
    }

    @Override
    public FlushableChannel putLong( long value ) throws IOException
    {
        buffer.putLong( value );
        return this;
    }

    @Override
    public FlushableChannel putFloat( float value ) throws IOException
    {
        buffer.putFloat( value );
        return this;
    }

    @Override
    public FlushableChannel putDouble( double value ) throws IOException
    {
        buffer.putDouble( value );
        return this;
    }

    @Override
    public FlushableChannel put( byte[] value, int length ) throws IOException
    {
        buffer.put( value, 0, length );
        return this;
    }

    @Override
    public void close() throws IOException
    {
    }

    @Override
    public void flush() throws IOException
    {
    }
}
