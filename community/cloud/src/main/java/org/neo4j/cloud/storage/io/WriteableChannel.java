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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicReference;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.memory.MemoryTracker;

public abstract class WriteableChannel extends OutputStream implements WritableByteChannel {

    private final AtomicReference<IOException> ioError = new AtomicReference<>();

    protected final MemoryTracker memoryTracker;

    protected final ByteBuffer buffer;

    protected long writtenChunks = 0;

    private boolean closed;

    protected WriteableChannel(int bufferSize, MemoryTracker memoryTracker) {
        this.memoryTracker = memoryTracker;
        this.buffer = ByteBuffers.allocateDirect(bufferSize, ByteOrder.LITTLE_ENDIAN, memoryTracker);
    }

    protected abstract long internalGetSize();

    protected abstract void completeWriteProcess() throws IOException;

    protected abstract void doBufferWrite(ByteBuffer buffer) throws IOException;

    protected abstract void reportChunksWritten(long chunks);

    protected abstract boolean hasBeenReplicated();

    public long size() throws IOException {
        ensureOpen();
        return internalGetSize();
    }

    @Override
    public void write(int b) throws IOException {
        ensureOk();
        checkBufferIsFull();
        buffer.put((byte) b);
    }

    @Override
    public void write(byte[] bytes, int offset, int length) throws IOException {
        ensureOk();

        var pos = 0;
        while (pos < length) {
            checkBufferIsFull();
            final var toWrite = Math.min(length - pos, buffer.remaining());
            buffer.put(bytes, offset + pos, toWrite);
            pos += toWrite;
        }
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        ensureOk();

        var written = 0;
        while (src.hasRemaining()) {
            checkBufferIsFull();

            final var toWrite = Math.min(src.remaining(), buffer.remaining());
            final var thisPos = buffer.position();
            final var srcPos = src.position();
            buffer.put(thisPos, src, srcPos, toWrite).position(thisPos + toWrite);

            written += toWrite;
            src.position(srcPos + toWrite);
        }

        return written;
    }

    @Override
    public boolean isOpen() {
        return !closed;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        closed = true;
        try {
            completeWriteProcess();
        } finally {
            ByteBuffers.releaseBuffer(buffer, memoryTracker);
        }
    }

    protected void ensureOpen() throws IOException {
        if (closed) {
            throw new ClosedChannelException();
        }
    }

    protected void ensureOk() throws IOException {
        ensureOpen();
        if (hasBeenReplicated()) {
            throw new IOException("Cannot write to a channel that has been used in a copyFrom");
        }

        final var error = ioError.get();
        if (error != null) {
            throw error;
        }
    }

    private void checkBufferIsFull() throws IOException {
        if (!buffer.hasRemaining()) {
            doBufferWrite(buffer.flip());
            reportChunksWritten(++writtenChunks);
            buffer.clear();
        }
    }
}
