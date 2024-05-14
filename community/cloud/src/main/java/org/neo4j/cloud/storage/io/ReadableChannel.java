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

import static org.neo4j.io.ByteUnit.bytesToString;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.StandardOpenOption;
import java.util.OptionalLong;
import org.neo4j.cloud.storage.queues.PullQueue;
import org.neo4j.cloud.storage.queues.PushQueue;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.Level;
import org.neo4j.logging.LoggerPrintStreamAdaptor;

public abstract class ReadableChannel extends InputStream implements ReadableByteChannel {

    protected final long channelSize;

    protected final int queueBufferSize;

    private final String progressText;

    private final InternalLog log;

    private final PullQueue queue;

    private ByteBuffer buffer;

    private Mark mark;

    private boolean queuePositioned = false;

    protected long position;

    private boolean closed;

    protected ReadableChannel(
            PullQueue queue, long channelSize, int queueBufferSize, String progressText, InternalLog log) {
        this.channelSize = channelSize;
        this.queueBufferSize = queueBufferSize;
        this.progressText = progressText;
        this.log = log;
        this.queue = queue;
    }

    protected abstract OptionalLong replicateWithinSameProvider(OutputStream out) throws IOException;

    protected abstract PushQueue newPushQueue(ByteBufferHandler handler, ProgressListener progress);

    public long position() throws IOException {
        ensureOpen();
        return position;
    }

    public ReadableChannel position(long newPosition) throws IOException {
        ensureOpen();

        final var newPos = Math.min(newPosition, channelSize);
        if (buffer == null) {
            queuePositioned = false;
        } else {
            if (newPos < (position - buffer.position()) || newPos >= (position + buffer.remaining())) {
                // new position is out of the range of the current content so reload from queue
                buffer = queue.positionAndGet(newPos);
            } else {
                // new position is within existing buffer so just move there
                final var diff = (int) (newPos - position);
                buffer.position(buffer.position() + diff);
            }
        }

        position = newPos;
        return this;
    }

    public long size() throws IOException {
        ensureOpen();
        return channelSize;
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public synchronized void mark(int readLimit) {
        if (!closed) {
            mark = new Mark(position, readLimit);
        }
    }

    @Override
    public synchronized void reset() throws IOException {
        if (mark == null) {
            throw new IOException("No mark has been set on this stream");
        }

        final var limit = mark.limit();
        if (position > limit) {
            throw new IOException("The stream has exceeded the read limit of %d from the mark at %d by %d byte(s)"
                    .formatted(mark.readLimit, mark.position, position - limit));
        }

        //noinspection resource
        position(mark.position);
        mark = null;
    }

    @Override
    public long skip(long n) throws IOException {
        if (n <= 0) {
            return 0;
        }
        final var prevPos = position;
        final var newPos = position + n;
        //noinspection resource
        position(newPos);
        return newPos > channelSize ? channelSize - prevPos : n;
    }

    @Override
    public void skipNBytes(long n) throws IOException {
        if (n > 0) {
            final var skipped = skip(n);
            if (skipped < n) {
                throw new EOFException("Skipping %d bytes took the stream passed the end of the file at %d bytes"
                        .formatted(n, channelSize));
            }
        }
    }

    @Override
    public int read() throws IOException {
        ensureOpen();
        var current = currentBuffer();
        while (current != null) {
            if (current.hasRemaining()) {
                final var b = current.get() & 0xFF;
                position++;
                return b;
            } else {
                current = nextBuffer();
            }
        }

        return -1;
    }

    @Override
    public int read(byte[] bytes, int offset, int length) throws IOException {
        ensureOpen();
        var current = currentBuffer();
        if (current == null) {
            return -1;
        }

        var read = 0;
        while (read < length) {
            if (current.hasRemaining()) {
                final var toRead = Math.min(length - read, current.remaining());
                current.get(bytes, offset + read, toRead);

                position += toRead;
                read += toRead;
            } else {
                current = nextBuffer();
                if (current == null) {
                    return read == 0 ? -1 : read;
                }
            }
        }

        return read;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        ensureOpen();

        var current = currentBuffer();
        if (current == null) {
            return -1;
        }

        var read = 0;
        while (dst.hasRemaining()) {
            if (current.hasRemaining()) {
                final var toRead = Math.min(current.remaining(), dst.remaining());
                final var dstPos = dst.position();
                final var srcPos = current.position();
                dst.put(dstPos, current, srcPos, toRead).position(dstPos + toRead);
                current.position(srcPos + toRead);

                position += toRead;
                read += toRead;
            } else {
                current = nextBuffer();
                if (current == null) {
                    return read == 0 ? -1 : read;
                }
            }
        }

        return read;
    }

    @Override
    public long transferTo(OutputStream out) throws IOException {
        ensureOpen();

        final var replicatedBytes = replicateWithinSameProvider(out);
        if (replicatedBytes.isPresent()) {
            return replicatedBytes.getAsLong();
        }

        long transferred;
        if (out instanceof PathBasedOutputStream pathOut) {
            transferred = download(pathOut);
        } else {
            transferred = download(out);
        }

        position = channelSize;
        return transferred;
    }

    @Override
    public boolean isOpen() {
        return !closed;
    }

    @Override
    public void close() {
        if (!closed) {
            queue.close();
            closed = true;
        }
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new ClosedChannelException();
        }
    }

    private ByteBuffer currentBuffer() throws IOException {
        if (buffer == null) {
            if (!queuePositioned) {
                queuePositioned = true;
                buffer = queue.positionAndGet(position);
            } else {
                buffer = queue.get();
            }
        }

        return buffer;
    }

    private ByteBuffer nextBuffer() throws IOException {
        assert queuePositioned;
        buffer = queue.get();
        return buffer;
    }

    private long download(PathBasedOutputStream output) throws IOException {
        // optimised to handle transfer direct to file paths by having many requests running in parallel
        final var actualSize = channelSize - position;
        output.replicate(path -> {
            try (var channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                    var progress = progressListener(actualSize);
                    var queue = newPushQueue(channel::write, progress)) {
                queue.run();
            }
        });
        return actualSize;
    }

    private long download(OutputStream out) throws IOException {
        // optimised to handle transfer to output by having many requests running in parallel
        final var actualSize = channelSize - position;
        log.warn(
                "Downloading %s of a file in chunks of %s - consider using a path based output stream",
                bytesToString(actualSize), bytesToString(queueBufferSize));
        try (var progress = progressListener(actualSize)) {
            final var bytes = new byte[queueBufferSize];
            try (var queue = newPushQueue(
                    data -> {
                        final var length = data.remaining();
                        if (data.hasArray()) {
                            out.write(data.array(), 0, length);
                        } else {
                            data.get(bytes, 0, length);
                            out.write(bytes, 0, length);
                        }

                        progress.add(length);
                        return length;
                    },
                    progress)) {
                queue.run();
            }
            return actualSize;
        } catch (UncheckedIOException ex) {
            throw ex.getCause();
        }
    }

    private ProgressListener progressListener(long totalCount) {
        return ProgressMonitorFactory.textual(new LoggerPrintStreamAdaptor(log, Level.INFO))
                .singlePart(progressText, totalCount);
    }

    @FunctionalInterface
    public interface ByteBufferHandler {
        long apply(ByteBuffer buffer) throws IOException;
    }

    private record Mark(long position, int readLimit) {
        private long limit() {
            return position + readLimit;
        }
    }
}
