/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.io.fs;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

public class InputStreamReadableChannel implements ReadableChannel {
    private final DataInputStream dataInputStream;

    public InputStreamReadableChannel(InputStream inputStream) {
        this.dataInputStream = new DataInputStream(inputStream);
    }

    @Override
    public byte get() throws IOException {
        return dataInputStream.readByte();
    }

    @Override
    public short getShort() throws IOException {
        return dataInputStream.readShort();
    }

    @Override
    public int getInt() throws IOException {
        return dataInputStream.readInt();
    }

    @Override
    public long getLong() throws IOException {
        return dataInputStream.readLong();
    }

    @Override
    public float getFloat() throws IOException {
        return dataInputStream.readFloat();
    }

    @Override
    public double getDouble() throws IOException {
        return dataInputStream.readDouble();
    }

    @Override
    public void get(byte[] bytes, int length) throws IOException {
        dataInputStream.read(bytes, 0, length);
    }

    @Override
    public void close() throws IOException {
        dataInputStream.close();
    }
}
