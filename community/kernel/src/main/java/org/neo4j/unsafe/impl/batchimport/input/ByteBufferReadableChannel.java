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
package org.neo4j.unsafe.impl.batchimport.input;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.kernel.impl.transaction.log.ReadableClosableChannel;

/**
 * Wraps a byte[] -> {@link ByteBuffer} -> {@link ReadableClosableChannel}
 */
public class ByteBufferReadableChannel implements ReadableClosableChannel
{
    private final ByteBuffer buffer;

    public ByteBufferReadableChannel( ByteBuffer buffer )
    {
        this.buffer = buffer;
    }

    @Override
    public byte get()
    {
        return buffer.get();
    }

    @Override
    public short getShort()
    {
        return buffer.getShort();
    }

    @Override
    public int getInt()
    {
        return buffer.getInt();
    }

    @Override
    public long getLong()
    {
        return buffer.getLong();
    }

    @Override
    public float getFloat()
    {
        return buffer.getFloat();
    }

    @Override
    public double getDouble()
    {
        return buffer.getDouble();
    }

    @Override
    public void get( byte[] bytes, int length )
    {
        buffer.get( bytes, 0, length );
    }

    @Override
    public void close()
    {
    }
}
