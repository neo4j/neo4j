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

import static java.lang.String.format;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import org.neo4j.dbms.archive.Dumper.SplitFileOutput;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.filename.SequentialFileNameHelper;
import org.neo4j.util.Preconditions;

public class CombiningInputStream extends InputStream {
    private static final int MAGIC_LENGTH = ArchiveFormat.MAGIC_PREFIX_LENGTH;
    private static final int INDEX_BYTES = 4;
    private static final int ID_BYTES = 16;

    private final FileSystemAbstraction fileSystem;
    private final Path[] partFiles;
    private final byte[] archiveId;

    // The data part currently being read, and the one-based index of the part that opened it. Parts are opened lazily,
    // so until the first read both are in their initial state.
    private InputStream current;
    private int openIndex;

    private CombiningInputStream(FileSystemAbstraction fileSystem, Path[] partFiles, byte[] archiveId) {
        Preconditions.checkArgument(archiveId.length == ID_BYTES, "The archive id should be " + ID_BYTES + " bytes");
        this.fileSystem = fileSystem;
        this.partFiles = partFiles;
        this.archiveId = archiveId;
    }

    /**
     * Combines a split archive, written by {@link SplitFileOutput}, back into a single stream.
     *
     * <p>A split archive consists of a metadata file (version {@code 0}) holding only the magic header, the number of
     * data parts and the archive id, followed by the data files (versions {@code 1..numParts}). Each data file is
     * prefixed with the magic header, its own one-based index and the archive id before the actual archive data.
     *
     * <p>The metadata file is read eagerly to validate that all parts are present, but the data parts themselves are
     * only opened on demand as the stream is consumed.
     *
     * @param firstArtifact the metadata file (version {@code 0}) of the split archive.
     * @param inputStream    the stream of the metadata file, positioned right after the magic header.
     * @param fileSystem     the file system the parts live on.
     */
    static CombiningInputStream forArtifact(
            Path firstArtifact, InputStream inputStream, FileSystemAbstraction fileSystem) throws IOException {
        if (SequentialFileNameHelper.getVersion(firstArtifact) != 0) {
            throw new IllegalArgumentException(
                    "First artifact in split archive should have version 0, but was: " + firstArtifact.getFileName());
        }
        SequentialFileNameHelper fileHelper = new SequentialFileNameHelper(
                firstArtifact.getParent(), SequentialFileNameHelper.getBaseName(firstArtifact));
        Path[] partFiles = fileHelper.getFiles(fileSystem);

        // The metadata file only contains the part count and archive id, the magic header was already consumed.
        byte[] numPartsBytes = inputStream.readNBytes(INDEX_BYTES);
        byte[] archiveId = inputStream.readNBytes(ID_BYTES);

        if (numPartsBytes.length != INDEX_BYTES || archiveId.length != ID_BYTES) {
            throw new IOException("Unexpected end of stream while reading metadata for split archive part: "
                    + firstArtifact.getFileName());
        }

        int numParts = intFromBytes(numPartsBytes);
        // partFiles includes the metadata file (version 0), so the number of data files is one less.
        int dataParts = partFiles.length - 1;
        if (numParts != dataParts) {
            throw new IllegalArgumentException(format(
                    "All parts for artifact %s are not present in the same location. Expected: %d files, actual: %d",
                    firstArtifact.getFileName(), numParts, dataParts));
        }
        return new CombiningInputStream(fileSystem, partFiles, archiveId);
    }

    /**
     * Opens and validates the next data part, making it {@link #current}. Returns {@code false} when all parts have
     * been consumed.
     */
    private boolean openNextPart() throws IOException {
        if (openIndex >= partFiles.length - 1) {
            return false;
        }
        openIndex++;
        Path nextPart = partFiles[openIndex];
        if (SequentialFileNameHelper.getVersion(nextPart) != openIndex) {
            throw new IllegalArgumentException("Missing part of archive file with index: " + openIndex);
        }
        InputStream in = fileSystem.openAsInputStream(nextPart);
        try {
            byte[] header = in.readNBytes(MAGIC_LENGTH);
            byte[] indexBytes = in.readNBytes(INDEX_BYTES);
            byte[] partId = in.readNBytes(ID_BYTES);
            if (header.length != MAGIC_LENGTH || indexBytes.length != INDEX_BYTES || partId.length != ID_BYTES) {
                throw new IOException("Unexpected end of stream while reading metadata for split archive part: "
                        + nextPart.getFileName());
            }
            if (!SplitFileOutput.MAGIC_HEADER.matches(header)) {
                throw new IllegalArgumentException(
                        "Unexpected format of split archive part: " + nextPart.getFileName());
            }
            int partIndex = intFromBytes(indexBytes);
            if (partIndex != openIndex) {
                throw new IllegalArgumentException(
                        "Unexpected part index in split archive. Expected: " + openIndex + ", actual: " + partIndex);
            }
            if (!Arrays.equals(archiveId, partId)) {
                throw new IllegalArgumentException(
                        "Mismatching archive id in split archive part: " + nextPart.getFileName());
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
        openIndex = partFiles.length;
        closeCurrent();
    }

    private static int intFromBytes(byte[] bytes) {
        return ((bytes[0] & 0xFF) << 24) | ((bytes[1] & 0xFF) << 16) | ((bytes[2] & 0xFF) << 8) | (bytes[3] & 0xFF);
    }
}
