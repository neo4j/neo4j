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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Objects;
import org.apache.commons.io.input.CloseShieldInputStream;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.io.fs.FileSystemAbstraction;

/**
 * A source of archive (dump or backup) data: and the methods to open the primary stream,
 * including for archives split into multiple parts by {@link Dumper.SplitFileOutput}.
 */
public sealed interface ArchiveInput {
    String description();

    /**
     * Recombines a split archive. Called when the magic prefix identifies a split archive; {@code metadataStream} is
     * positioned right after the magic header. Sources that have no way of locating the remaining parts throw.
     */
    default CombiningInputStream combine(InputStream metadataStream) throws IOException {
        throw new IllegalArgumentException(
                "Found split file format, but " + description() + " cannot locate the archive parts");
    }

    /**
     * Opens the archive: reads the magic prefix, combining if necessary.
     */
    default OpenedArchive open() throws IOException {
        InputStream stream =
                switch (this) {
                    case StreamInput streamInput -> streamInput.stream();
                    case FileInput fileInput -> fileInput.source().next();
                };
        try {
            byte[] magic = stream.readNBytes(ArchiveFormat.MAGIC_PREFIX_LENGTH);
            if (Dumper.SplitFileOutput.MAGIC_MANIFEST_HEADER.matches(magic)) {
                stream = combine(stream);
                magic = stream.readNBytes(ArchiveFormat.MAGIC_PREFIX_LENGTH);
            }
            return new OpenedArchive(stream, magic);
        } catch (IOException | RuntimeException e) {
            try {
                stream.close();
            } catch (IOException closeException) {
                e.addSuppressed(closeException);
            }
            throw e;
        }
    }

    record OpenedArchive(InputStream stream, byte[] magic) implements Closeable {
        @Override
        public void close() throws IOException {
            stream.close();
        }
    }

    /**
     * An archive in a file; the parts of a split archive are located as sibling files.
     */
    record FileInput(StreamSource source, String description) implements ArchiveInput {
        public static FileInput of(FileSystemAbstraction fs, Path path) {
            Objects.requireNonNull(fs);
            Objects.requireNonNull(path);
            Path absolutePath = path.toAbsolutePath();
            return new FileInput(StreamSource.siblingsOf(fs, absolutePath), absolutePath.toString());
        }

        @Override
        public CombiningInputStream combine(InputStream metadataStream) throws IOException {
            return CombiningInputStream.of(metadataStream, source, description);
        }
    }

    /**
     * An archive in a single stream.
     */
    record StreamInput(InputStream stream, String description) implements ArchiveInput {
        public static StreamInput of(InputStream is, String description) {
            Objects.requireNonNull(is);
            Objects.requireNonNull(description);
            return new StreamInput(is, description);
        }

        public static StreamInput stdin(ExecutionContext ctx) {
            return of(CloseShieldInputStream.wrap(ctx.in()), "reading from stdin");
        }
    }
}
