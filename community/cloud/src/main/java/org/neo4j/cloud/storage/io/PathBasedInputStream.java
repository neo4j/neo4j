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

import static java.util.Objects.requireNonNull;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.neo4j.cloud.storage.StorageSystem;

/**
 * An {@link InputStream} that wraps a {@link Path}. Read operation to this path can then either proceed via
 * delegated operations on the {@link InputStream} from this path <strong>OR</strong> via a bulk-read operation via
 * {@link #transferTo(OutputStream)}, <strong>BUT NOT BOTH</strong>
 * <br>
 * This can be useful when a {@link StorageSystem} provides bulk operations for uploading resources from the local
 * file system.
 */
@SuppressWarnings("resource")
public class PathBasedInputStream extends InputStream {

    private final Path path;

    private State state = State.OPEN;

    private InputStream input;

    public PathBasedInputStream(Path path) {
        this.path = requireNonNull(path);
    }

    @Override
    public long transferTo(OutputStream out) throws IOException {
        checkNotClosed();
        if (state == State.READING) {
            throw new IOException("Already read from the path: " + path);
        }

        if (out instanceof WriteableChannel channel) {
            state = State.TRANSFERRING;
            return channel.transferFrom(path);
        } else {
            return setupReading().transferTo(out);
        }
    }

    @Override
    public byte[] readAllBytes() throws IOException {
        return setupReading().readAllBytes();
    }

    @Override
    public int read() throws IOException {
        return setupReading().read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return setupReading().read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return setupReading().read(b, off, len);
    }

    @Override
    public byte[] readNBytes(int len) throws IOException {
        return setupReading().readNBytes(len);
    }

    @Override
    public int readNBytes(byte[] b, int off, int len) throws IOException {
        return setupReading().readNBytes(b, off, len);
    }

    @Override
    public void close() throws IOException {
        if (state == State.CLOSED) {
            return;
        }

        state = State.CLOSED;
        if (input != null) {
            input.close();
        }
    }

    private void checkNotClosed() throws IOException {
        if (state == State.CLOSED) {
            throw new IOException("Stream is already closed");
        }
    }

    private InputStream setupReading() throws IOException {
        checkNotClosed();
        if (state == State.TRANSFERRING) {
            throw new IOException("Stream is already being transferred from the path: " + path);
        }

        if (input == null) {
            state = State.READING;
            input = new BufferedInputStream(Files.newInputStream(path));
        }

        return input;
    }

    private enum State {
        OPEN,
        TRANSFERRING,
        READING,
        CLOSED
    }
}
