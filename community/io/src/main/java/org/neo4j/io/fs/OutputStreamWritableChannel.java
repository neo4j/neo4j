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

import java.io.DataOutputStream;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class OutputStreamWritableChannel implements FlushableChannel {
    private final DataOutputStream dataOutputStream;
    private boolean isClosed;

    public OutputStreamWritableChannel(OutputStream outputStream) {
        this.dataOutputStream = new DataOutputStream(outputStream);
    }

    @Override
    public Flushable prepareForFlush() throws IOException {
        return dataOutputStream;
    }

    @Override
    public OutputStreamWritableChannel put(byte value) throws IOException {
        dataOutputStream.writeByte(value);
        return this;
    }

    @Override
    public OutputStreamWritableChannel putShort(short value) throws IOException {
        dataOutputStream.writeShort(value);
        return this;
    }

    @Override
    public OutputStreamWritableChannel putInt(int value) throws IOException {
        dataOutputStream.writeInt(value);
        return this;
    }

    @Override
    public OutputStreamWritableChannel putLong(long value) throws IOException {
        dataOutputStream.writeLong(value);
        return this;
    }

    @Override
    public OutputStreamWritableChannel putFloat(float value) throws IOException {
        dataOutputStream.writeFloat(value);
        return this;
    }

    @Override
    public OutputStreamWritableChannel putDouble(double value) throws IOException {
        dataOutputStream.writeDouble(value);
        return this;
    }

    @Override
    public OutputStreamWritableChannel put(byte[] value, int length) throws IOException {
        dataOutputStream.write(value, 0, length);
        return this;
    }

    @Override
    public OutputStreamWritableChannel put(byte[] value, int offset, int length) throws IOException {
        dataOutputStream.write(value, offset, length);
        return this;
    }

    @Override
    public OutputStreamWritableChannel putAll(ByteBuffer src) throws IOException {
        if (src.hasArray()) {
            dataOutputStream.write(src.array(), src.position(), src.remaining());
        } else {
            while (src.hasRemaining()) {
                dataOutputStream.writeByte(src.get());
            }
        }
        return this;
    }

    @Override
    public OutputStreamWritableChannel putVersion(byte version) throws IOException {
        return put(version);
    }

    @Override
    public boolean isOpen() {
        return !isClosed;
    }

    @Override
    public void close() throws IOException {
        isClosed = true;
        dataOutputStream.close();
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        int remaining = src.remaining();
        putAll(src);
        return remaining;
    }

    @Override
    public void beginChecksumForWriting() {}

    @Override
    public int putChecksum() throws IOException {
        return 0;
    }
}
