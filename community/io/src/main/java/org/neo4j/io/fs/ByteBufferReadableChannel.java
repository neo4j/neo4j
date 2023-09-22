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

import static java.lang.Math.toIntExact;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ByteBufferReadableChannel implements ReadableChannel {

    private final ByteBuffer buffer;
    private boolean isClosed;

    public ByteBufferReadableChannel(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public byte get() throws IOException {
        return buffer.get();
    }

    @Override
    public short getShort() throws IOException {
        return buffer.getShort();
    }

    @Override
    public int getInt() throws IOException {
        return buffer.getInt();
    }

    @Override
    public long getLong() throws IOException {
        return buffer.getLong();
    }

    @Override
    public float getFloat() throws IOException {
        return buffer.getFloat();
    }

    @Override
    public double getDouble() throws IOException {
        return buffer.getDouble();
    }

    @Override
    public void get(byte[] bytes, int length) throws IOException {
        buffer.get(bytes, 0, length);
    }

    @Override
    public byte getVersion() throws IOException {
        return get();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        int remaining = buffer.remaining();
        dst.put(buffer);
        return remaining;
    }

    @Override
    public boolean isOpen() {
        return !isClosed;
    }

    @Override
    public void close() throws IOException {
        isClosed = true;
    }

    @Override
    public void beginChecksum() {
        // no-op
    }

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
        return buffer.position();
    }

    @Override
    public void position(long byteOffset) throws IOException {
        buffer.position(toIntExact(byteOffset));
    }
}
