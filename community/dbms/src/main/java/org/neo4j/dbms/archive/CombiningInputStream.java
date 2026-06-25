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
package org.neo4j.dbms.archive;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Objects;
import org.neo4j.dbms.archive.Dumper.SplitFileOutput;
import org.neo4j.util.Preconditions;

public class CombiningInputStream extends InputStream {
    private static final int MAGIC_LENGTH = ArchiveFormat.MAGIC_PREFIX_LENGTH;
    private static final int INDEX_BYTES = 4;
    private static final int ID_BYTES = 16;

    private final int numParts;
    private final byte[] archiveId;
    private final StreamSource src;

    // The data part currently being read, and the one-based index of the part that opened it. Parts are opened lazily,
    // so until the first read both are in their initial state.
    private InputStream current;
    private int openIndex;

    private CombiningInputStream(int numParts, byte[] archiveId, StreamSource src) {
        Preconditions.checkArgument(archiveId.length == ID_BYTES, "The archive id should be " + ID_BYTES + " bytes");
        Preconditions.checkArgument(numParts > 0, "The number of parts should be greater than zero" + numParts);
        this.archiveId = archiveId;
        this.src = src;
        this.numParts = numParts;
    }

    public static CombiningInputStream of(InputStream metadataStream, StreamSource src, String description)
            throws IOException {
        try (metadataStream) {
            // The metadata file only contains the part count and archive id, the magic header was already consumed.
            byte[] numPartsBytes = metadataStream.readNBytes(INDEX_BYTES);
            byte[] archiveId = metadataStream.readNBytes(ID_BYTES);

            if (numPartsBytes.length != INDEX_BYTES || archiveId.length != ID_BYTES) {
                throw new IOException(
                        "Unexpected end of stream while reading metadata for split archive part: " + description);
            }

            int numParts = intFromBytes(numPartsBytes);
            return new CombiningInputStream(numParts, archiveId, src);
        }
    }

    /**
     * Opens and validates the next data part, making it {@link #current}. Returns {@code false} when all parts have
     * been consumed.
     */
    private boolean openNextPart() throws IOException {
        if (openIndex >= numParts) {
            return false;
        }

        openIndex++;

        InputStream in = src.next();
        try {
            byte[] header = in.readNBytes(MAGIC_LENGTH);
            byte[] indexBytes = in.readNBytes(INDEX_BYTES);
            byte[] partId = in.readNBytes(ID_BYTES);
            if (header.length != MAGIC_LENGTH || indexBytes.length != INDEX_BYTES || partId.length != ID_BYTES) {
                throw new IOException(
                        "Unexpected end of stream while reading metadata for split archive part: " + openIndex);
            }
            if (!SplitFileOutput.MAGIC_DATA_HEADER.matches(header)) {
                throw new IllegalArgumentException("Unexpected format magic in split archive part: " + openIndex);
            }
            int partIndex = intFromBytes(indexBytes);
            if (partIndex != openIndex) {
                throw new IllegalArgumentException(
                        "Unexpected part index in split archive. Expected: " + openIndex + ", actual: " + partIndex);
            }
            if (!Arrays.equals(archiveId, partId)) {
                throw new IllegalArgumentException("Mismatching archive id in split archive part: " + openIndex);
            }
        } catch (Throwable t) {
            try {
                in.close();
            } catch (IOException e) {
                t.addSuppressed(e);
            }
            throw t;
        }
        current = in;
        return true;
    }

    private void closeCurrent() throws IOException {
        if (current != null) {
            InputStream closeable = current;
            current = null;
            closeable.close();
        }
    }

    @Override
    public int read() throws IOException {
        do {
            if (current == null && !openNextPart()) {
                return -1;
            }
            int b = current.read();
            if (b != -1) {
                return b;
            }
            closeCurrent();
        } while (true);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        Objects.checkFromIndexSize(off, len, b.length);
        if (len == 0) {
            return 0;
        }
        do {
            if (current == null && !openNextPart()) {
                return -1;
            }
            int read = current.read(b, off, len);
            if (read > 0) {
                return read;
            }
            closeCurrent();
        } while (true);
    }

    @Override
    public long transferTo(OutputStream out) throws IOException {
        Objects.requireNonNull(out, "Provided OutputStream can't be null");
        long transferred = 0;
        while (current != null || openNextPart()) {
            transferred += current.transferTo(out);
            closeCurrent();
        }
        return transferred;
    }

    @Override
    public int available() throws IOException {
        return current == null ? 0 : current.available();
    }

    @Override
    public void close() throws IOException {
        // Prevent any further parts from being opened, then release the one currently in use.
        openIndex = numParts;
        closeCurrent();
    }

    private static int intFromBytes(byte[] bytes) {
        return ((bytes[0] & 0xFF) << 24) | ((bytes[1] & 0xFF) << 16) | ((bytes[2] & 0xFF) << 8) | (bytes[3] & 0xFF);
    }
}
