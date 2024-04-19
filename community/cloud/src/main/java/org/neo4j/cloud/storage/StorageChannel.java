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
package org.neo4j.cloud.storage;

import static org.neo4j.io.fs.FileSystemAbstraction.INVALID_FILE_DESCRIPTOR;

import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.nio.channels.SeekableByteChannel;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.util.Preconditions;

/**
 * Implementation of a {@link StoreChannel} that wraps access to some storage resource via access to it's
 * {@link SeekableByteChannel}.
 */
public class StorageChannel implements StoreChannel {

    private final SeekableByteChannel channel;

    public StorageChannel(SeekableByteChannel channel) {
        this.channel = channel;
    }

    @Override
    public StorageChannel position(long newPosition) throws IOException {
        channel.position(newPosition);
        return this;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        return channel.write(src);
    }

    @Override
    public long write(ByteBuffer... srcs) throws IOException {
        return write(srcs, 0, srcs.length);
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        Preconditions.checkArgument(
                offset >= 0 && offset < srcs.length, "Offset must be within the range of buffers provided");
        Preconditions.checkArgument(
                offset + length <= srcs.length, "Length must be within the range of buffers provided");
        var written = 0L;
        for (var i = offset; i < offset + length; i++) {
            written += doWrite(srcs[i]);
        }
        return written;
    }

    @Override
    public void writeAll(ByteBuffer src, long position) throws IOException {
        //noinspection resource
        position(position).writeAll(src);
    }

    @Override
    public void writeAll(ByteBuffer src) throws IOException {
        doWrite(src);
    }

    @Override
    public StorageChannel truncate(long size) throws IOException {
        channel.truncate(size);
        return this;
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        //noinspection resource
        return position(position).read(dst);
    }

    @Override
    public void readAll(ByteBuffer dst) throws IOException {
        while (dst.hasRemaining()) {
            if (channel.read(dst) < 0) {
                throw new IllegalStateException("Channel has reached end-of-stream.");
            }
        }
    }

    @Override
    public void readAll(ByteBuffer dst, long position) throws IOException {
        position(position).readAll(dst);
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return channel.read(dst);
    }

    @Override
    public long read(ByteBuffer[] dsts) throws IOException {
        return read(dsts, 0, dsts.length);
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        var read = 0L;
        for (var i = offset; i < offset + length; i++) {
            final var r = read(dsts[i]);
            if (r == -1) {
                return read == 0L ? -1L : read;
            }
            read += r;
        }

        return read;
    }

    @Override
    public long position() throws IOException {
        return channel.position();
    }

    @Override
    public FileLock tryLock() {
        throw new UnsupportedOperationException("tryLock");
    }

    @Override
    public boolean isOpen() {
        return channel.isOpen();
    }

    @Override
    public long size() throws IOException {
        return channel.size();
    }

    @Override
    public int getFileDescriptor() {
        return INVALID_FILE_DESCRIPTOR;
    }

    @Override
    public boolean hasPositionLock() {
        return false;
    }

    @Override
    public Object getPositionLock() {
        throw new UnsupportedOperationException("getPositionLock");
    }

    @Override
    public void tryMakeUninterruptible() {
        // no-op
    }

    @Override
    public void flush() throws IOException {
        force(false);
    }

    @Override
    public void force(boolean metaData) throws IOException {
        if (channel instanceof Flushable flushable) {
            flushable.flush();
        }
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    private int doWrite(ByteBuffer src) throws IOException {
        final var total = src.limit() - src.position();
        var bytesToWrite = total;
        int bytesWritten;
        while ((bytesToWrite -= bytesWritten = write(src)) > 0) {
            if (bytesWritten < 0) {
                throw new IOException("Unable to write to disk, reported bytes written was " + bytesWritten);
            }
        }
        return total;
    }
}
