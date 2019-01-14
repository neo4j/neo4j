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
package org.neo4j.bolt.v1.packstream;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class PackedInputArray implements PackInput
{
    private final ByteArrayInputStream bytes;
    private final DataInputStream data;

    public PackedInputArray( byte[] bytes )
    {
        this.bytes = new ByteArrayInputStream( bytes );
        this.data = new DataInputStream( this.bytes );
    }

    @Override
    public byte readByte() throws IOException
    {
        return data.readByte();
    }

    @Override
    public short readShort() throws IOException
    {
        return data.readShort();
    }

    @Override
    public int readInt() throws IOException
    {
        return data.readInt();
    }

    @Override
    public long readLong() throws IOException
    {
        return data.readLong();
    }

    @Override
    public double readDouble() throws IOException
    {
        return data.readDouble();
    }

    @Override
    public PackInput readBytes( byte[] into, int offset, int toRead ) throws IOException
    {
        // TODO: fix the interface and all implementations - we should probably
        // TODO: return the no of bytes read instead of the instance
        data.read( into, offset, toRead );
        return this;
    }

    @Override
    public byte peekByte() throws IOException
    {
        data.mark(1);
        byte value = data.readByte();
        data.reset();
        return value;
    }
}
