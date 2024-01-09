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

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Objects.requireNonNull;
import static org.neo4j.io.fs.FileUtils.toBufferedStream;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Set;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.io.fs.StoreFileChannel;

/**
 * An {@link OutputStream} that wraps a {@link Path}. Write operation to this path can then either proceed via
 * delegated operations on the {@link OutputStream} from this path <strong>OR</strong> via a bulk-write operation via
 * {@link #replicate(ThrowingConsumer)}, <strong>BUT NOT BOTH</strong>
 * <br>
 * This can be useful when a {@link StorageSystem} provides bulk operations for downloading resources to the local
 * file system.
 */
public class PathBasedOutputStream extends OutputStream {

    private final Path path;

    private State state = State.OPEN;

    private OutputStream output;

    public PathBasedOutputStream(Path path) {
        this.path = requireNonNull(path);
    }

    /**
     * Provides the means to perform a bulk-write operation on the underlying {@link Path} object of this stream.
     * @param writeOp the bulk-write operation to be performed
     * @throws IOException if unable to proceed with the replication
     */
    public void replicate(ThrowingConsumer<Path, IOException> writeOp) throws IOException {
        checkNotClosed();
        if (state == State.WRITING) {
            throw new IOException("Already written to the path: " + path);
        }

        state = State.REPLICATING;
        writeOp.accept(path);
    }

    @SuppressWarnings("resource")
    @Override
    public void write(int b) throws IOException {
        setupWriting().write(b);
    }

    @SuppressWarnings("resource")
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        setupWriting().write(b, off, len);
    }

    @Override
    public void close() throws IOException {
        if (state == State.CLOSED) {
            return;
        }

        state = State.CLOSED;
        if (output != null) {
            output.close();
        }
    }

    private void checkNotClosed() throws IOException {
        if (state == State.CLOSED) {
            throw new IOException("Stream is already closed");
        }
    }

    private OutputStream setupWriting() throws IOException {
        checkNotClosed();
        if (state == State.REPLICATING) {
            throw new IOException("Stream has already been replicated to the path: " + path);
        }

        if (output == null) {
            state = State.WRITING;
            output = toBufferedStream(path, StoreFileChannel::new, Set.of(CREATE_NEW, WRITE));
        }

        return output;
    }

    private enum State {
        OPEN,
        REPLICATING,
        WRITING,
        CLOSED
    }
}
