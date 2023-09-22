/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.io.fs;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class InputStreamReadableChannel implements ReadableChannel {
    private final DataInputStream dataInputStream;
    private boolean isClosed;

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
    public byte getVersion() throws IOException {
        return dataInputStream.readByte();
    }

    @Override
    public boolean isOpen() {
        return !isClosed;
    }

    @Override
    public void close() throws IOException {
        isClosed = true;
        dataInputStream.close();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        int remaining = dst.remaining();
        if (dst.hasArray()) {
            final var read = dataInputStream.read(dst.array(), dst.position(), remaining);
            dst.position(dst.position() + read);
            return read;
        }

        while (dst.hasRemaining()) {
            dst.put(dataInputStream.readByte());
        }
        return remaining;
    }

    @Override
    public void beginChecksum() {}

    @Override
    public int getChecksum() {
        return 0;
    }

    @Override
    public int endChecksumAndValidate() throws IOException {
        return 0;
    }

    @Override
    public long position() throws IOException {
        throw new UnsupportedOperationException("Steam does not have a position");
    }

    @Override
    public void position(long byteOffset) throws IOException {
        throw new UnsupportedOperationException("Steam does not have a position");
    }
}
