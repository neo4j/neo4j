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
package org.neo4j.csv.reader;

import static org.neo4j.io.IOUtils.closeAllSilently;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * An {@link InputStream} with details of its file type via the {@link Magic} setting. This stream is unbuffered.
 */
public class MagicInputStream extends InputStream {

    private final Path path;

    private final Magic magic;

    private final InputStream delegate;

    private MagicInputStream(Path path, Magic magic, InputStream delegate) {
        this.path = path;
        this.magic = magic;
        this.delegate = delegate;
    }

    /**
     * Extracts and matches the magic of the header in the given {@code file} with an {@link InputStream} for reading
     * the data. If no magic matches then {@link Magic#NONE} is returned with the stream.
     *
     * @param path {@link Path} to extract the magic from.
     * @return matching {@link Magic} (or {@link Magic#NONE} if no match) input stream. NB this stream is unbuffered.
     * @throws IOException for errors reading from the file.
     */
    public static MagicInputStream create(Path path) throws IOException {
        InputStream in = null;
        try {
            in = Files.newInputStream(path);

            final var bytes = new byte[Magic.longest()];
            if (in.markSupported()) {
                in.mark(bytes.length);
            }

            final var read = in.read(bytes);
            if (read > 0) {
                return wrap(path, Magic.of(Arrays.copyOf(bytes, read)), in);
            }
        } catch (EOFException e) {
            // This is OK
        }

        closeAllSilently(in);
        return wrap(path, Magic.NONE, null);
    }

    public Path path() {
        return path;
    }

    public Magic magic() {
        return magic;
    }

    public boolean isDefaultFileSystemBased() {
        return path.getFileSystem().equals(FileSystems.getDefault());
    }

    @Override
    public int read() throws IOException {
        return delegate.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return delegate.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return delegate.read(b, off, len);
    }

    @Override
    public byte[] readAllBytes() throws IOException {
        return delegate.readAllBytes();
    }

    @Override
    public byte[] readNBytes(int len) throws IOException {
        return delegate.readNBytes(len);
    }

    @Override
    public int readNBytes(byte[] b, int off, int len) throws IOException {
        return delegate.readNBytes(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        return delegate.skip(n);
    }

    @Override
    public void skipNBytes(long n) throws IOException {
        delegate.skipNBytes(n);
    }

    @Override
    public int available() throws IOException {
        return delegate.available();
    }

    @Override
    public boolean markSupported() {
        return delegate.markSupported();
    }

    @Override
    public void mark(int readLimit) {
        delegate.mark(readLimit);
    }

    @Override
    public void reset() throws IOException {
        delegate.reset();
    }

    @Override
    public long transferTo(OutputStream out) throws IOException {
        return delegate.transferTo(out);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    private static MagicInputStream wrap(Path path, Magic magic, InputStream in) throws IOException {
        if (in != null && in.markSupported()) {
            in.reset();
            return new MagicInputStream(path, magic, in);
        }

        try {
            return new MagicInputStream(path, magic, Files.newInputStream(path));
        } finally {
            closeAllSilently(in);
        }
    }
}
