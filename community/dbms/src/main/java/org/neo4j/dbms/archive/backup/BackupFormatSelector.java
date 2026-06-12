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
package org.neo4j.dbms.archive.backup;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;
import org.neo4j.dbms.archive.ArchiveFormat;
import org.neo4j.dbms.archive.CombiningInputStream;
import org.neo4j.dbms.archive.DumpFormatSelector;
import org.neo4j.dbms.archive.Dumper;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.io.fs.FileSystemAbstraction;

public class BackupFormatSelector {

    private BackupFormatSelector() {}

    public static List<BackupCompressionFormat> availableFormats() {
        return List.of(
                new BackupZstdFormatV1(),
                new BackupTarFormatV1(),
                new BackupZstdFormatV2(),
                new BackupTarFormatV2(),
                new BackupZstdFormatV3());
    }

    public static BackupCompressionFormat selectWriteFormat(boolean compress, boolean useNewFormat) {
        // Do remove this check and the added boolean once V3 format is finalized
        if (useNewFormat && compress) {
            return new BackupZstdFormatV3();
        }
        return compress ? new BackupZstdFormatV2() : new BackupTarFormatV2();
    }

    public static BackupDescription readDescription(
            Path path, FileSystemAbstraction fs, ThrowingSupplier<InputStream, IOException> streamSupplier)
            throws IOException {
        InputStream stream = streamSupplier.get();
        try {
            InputStream input = stream;
            FormatAndStream record = getFormat(path, input, () -> CombiningInputStream.forArtifact(path, input, fs));
            stream = record.inputStream;
            return record.format.readMetadata(stream);
        } finally {
            stream.close();
        }
    }

    public static BackupDescription readDescription(
            CombiningInputStream.PartSupplier parts, ThrowingSupplier<InputStream, Exception> streamSupplier)
            throws Exception {
        InputStream stream = streamSupplier.get();
        try {
            InputStream input = stream;
            FormatAndStream record = getFormat(parts, input, () -> CombiningInputStream.forParts(input, parts));
            stream = record.inputStream;
            return record.format.readMetadata(stream);
        } finally {
            stream.close();
        }
    }

    public static InputStream decompress(
            Path path, FileSystemAbstraction fs, ThrowingSupplier<InputStream, IOException> streamSupplier)
            throws IOException {
        InputStream stream = streamSupplier.get();
        try {
            InputStream input = stream;
            FormatAndStream record = getFormat(path, input, () -> CombiningInputStream.forArtifact(path, input, fs));
            stream = record.inputStream;
            return record.format.decompress(stream);
        } catch (Exception e) {
            stream.close();
            throw e;
        }
    }

    private static FormatAndStream getFormat(
            Object artifact, InputStream stream, ThrowingSupplier<CombiningInputStream, IOException> combiningSupplier)
            throws IOException {
        byte[] magicPrefix = stream.readNBytes(ArchiveFormat.MAGIC_PREFIX_LENGTH);
        if (Dumper.SplitFileOutput.MAGIC_HEADER.matches(magicPrefix)) {
            if (artifact == null) {
                throw new IllegalArgumentException("Found split file format, but not supplied in split files");
            }
            stream = combiningSupplier.get();
            magicPrefix = stream.readNBytes(ArchiveFormat.MAGIC_PREFIX_LENGTH);
        }
        BackupCompressionFormat format = selectReadFormat(magicPrefix);
        if (format == null) {
            DumpFormatSelector.throwUnsupported(magicPrefix);
        }
        return new FormatAndStream(format, stream);
    }

    public static BackupCompressionFormat selectReadFormat(byte[] bytes) {
        if (BackupZstdFormatV3.MAGIC_HEADER.matches(bytes)) {
            return new BackupZstdFormatV3();
        }
        if (BackupZstdFormatV2.MAGIC_HEADER.matches(bytes)) {
            return new BackupZstdFormatV2();
        }
        if (BackupZstdFormatV1.MAGIC_HEADER.matches(bytes)) {
            return new BackupZstdFormatV1();
        }
        if (BackupTarFormatV2.MAGIC_HEADER.matches(bytes)) {
            return new BackupTarFormatV2();
        }
        if (BackupTarFormatV1.MAGIC_HEADER.matches(bytes)) {
            return new BackupTarFormatV1();
        }

        return null;
    }

    private record FormatAndStream(BackupCompressionFormat format, InputStream inputStream) {}
}
