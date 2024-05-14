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
package org.neo4j.cloud.storage.io;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;

public class SeekableReadableChannel implements SeekableByteChannel {

    private final ReadableChannel readChannel;

    private volatile boolean closed;

    public SeekableReadableChannel(ReadableChannel channel) {
        this.readChannel = channel;
    }

    public InputStream asStream() throws IOException {
        ensureOpen();
        return readChannel;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        ensureOpen();
        return readChannel.read(dst);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        throw new NonWritableChannelException();
    }

    @Override
    public long position() throws IOException {
        ensureOpen();
        return readChannel.position();
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        readChannel.position(newPosition);
        return this;
    }

    @Override
    public long size() throws IOException {
        ensureOpen();
        return readChannel.size();
    }

    @Override
    public SeekableByteChannel truncate(long size) {
        throw new UnsupportedOperationException("truncate(size)");
    }

    @Override
    public boolean isOpen() {
        return !closed;
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            readChannel.close();
        }
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new ClosedChannelException();
        }
    }
}
