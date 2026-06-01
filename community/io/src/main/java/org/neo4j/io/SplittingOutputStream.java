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
package org.neo4j.io;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Objects;
import org.neo4j.util.Preconditions;

/**
 * This class provides the ability to split a single output stream into multiple output streams, after having
 * written a specific size {@code blksize} bytes to the output stream. The mechanism of opening new output streams
 * is user defined, and can be implemented in a lazy fashion (see example below).
 *
 * <p>Usage:
 * <pre>{@code
 *   Iterator<OutputStream> lazyFiles = new Iterator() {
 *       final AtomicInteger numFiles = new AtomicInteger();
 *       @Override
 *       OutputStream next() {
 *           return Files.newOutputStream("file-" + numFiles.incrementAndGet(), CREATE_NEW);
 *       }
 *       @Override
 *       boolean hasNext() {
 *           return true;
 *       }
 *   };
 *   long fileSize = ByteUnit.gib(2);
 *
 *   long numBytes = ByteUnit.gib(10);
 *   int bufferSize = (int)ByteUnit.kib(8);
 *   byte[] chunk = new byte[bufferSize];
 *   try(OutputStream os = new SplittingOutputStream(lazyFiles, fileSize)) {
 *       for (long i = 0; i < numBytes/bufferSize; i++) {
 *           os.write(chunk);
 *       }
 *   }
 * }</pre>
 * <pre>
 *   &gt;&gt; file-1
 *   &gt;&gt; file-2
 *   &gt;&gt; file-3
 *   &gt;&gt; file-4
 *   &gt;&gt; file-5
 * </pre>
 *
 * The intended use-case for this class is to split output from compressors such as com.github.luben.zstd.ZstdOutputStream into multiple files.
 * To read back the input, it needs to be stitched together before being decompressed by something like SequenceInputStream.
 *
 * <p>Limitations:
 * <ul>
 *   <li>This implementation is not thread-safe.</li>
 *   <li>This implementation does not provide atomic writes, writes may be partially observable.</li>
 *   <li>This implementation assumes well-behaved OutputStreams, that flushes its output if necessary at {@code OutputStream.close()}.</li>
 *   <li>This implementation assumes well-behaved OutputStream source, that produces enough OutputStreams for the data written.</li>
 * </ul>
 */
public class SplittingOutputStream extends OutputStream {
    private final Iterator<? extends OutputStream> it;
    private final long blksize;
    private final Runnable onClose;

    private OutputStream out;
    private long remainingBytes;
    private boolean closed = false;

    public SplittingOutputStream(Iterable<? extends OutputStream> iterable, long blksize) {
        this(iterable.iterator(), blksize, null);
    }

    public SplittingOutputStream(Iterator<? extends OutputStream> iterator, long blksize, Runnable onClose) {
        Preconditions.checkArgument(blksize > 0, "blksize must be positive");
        this.out = null;
        this.it = iterator;
        this.blksize = blksize;
        this.remainingBytes = blksize;
        this.onClose = onClose;
    }

    @Override
    public void write(int b) throws IOException {
        advanceIfNecessary();
        countingWrite(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        Objects.requireNonNull(b);
        Objects.checkFromIndexSize(off, len, b.length);
        if (!closed && len == 0) {
            // Empty writes are no-op.
            return;
        }
        advanceIfNecessary();
        // Write as much as we can in one go, then advance the output stream
        do {
            int available = (int) Math.min(len, remainingBytes);
            countingWrite(b, off, available);
            off += available;
            len -= available;
            if (len <= 0) {
                return;
            }

            // More data remaining, advance the stream
            getNextStream();
        } while (true);
    }

    @Override
    public void flush() throws IOException {
        if (closed) {
            throw new IOException("stream closed");
        }
        if (out != null) {
            out.flush();
        }
    }

    /**
     * Closes the stream, and the underlying stream.
     * Streams are assumed to take necessary flushing actions on close.
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;

        if (out != null) {
            var closeable = out;
            out = null;
            closeable.close();
            if (onClose != null) {
                onClose.run();
            }
        }
    }

    private void countingWrite(int b) throws IOException {
        out.write(b);
        remainingBytes--;
        assert remainingBytes >= 0;
    }

    private void countingWrite(byte[] b, int off, int len) throws IOException {
        out.write(b, off, len);
        remainingBytes -= len;
        assert remainingBytes >= 0;
    }

    private void advanceIfNecessary() throws IOException {
        if (closed) {
            throw new IOException("stream closed");
        }

        if (out == null || remainingBytes == 0) {
            getNextStream();
        }

        assert !closed : "getNextStream either throws or collects new stream";
        assert out != null : "out is null";
        assert remainingBytes > 0 : "remaining bytes must be > 0";
        assert remainingBytes <= blksize : "remainingBytes must be <= blksize";
    }

    private void getNextStream() throws IOException {
        // Assume we are closed, unless we manage to acquire a new stream
        closed = true;

        // If we have a stream opened, try to close it.
        if (out != null) {
            var closeable = out;
            out = null;
            closeable.close();
        }

        if (!it.hasNext()) {
            throw new IOException("no more output streams available");
        }

        out = Objects.requireNonNull(it.next());
        closed = false;
        remainingBytes = blksize;
    }
}
