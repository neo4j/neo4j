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
import org.neo4j.dbms.archive.ArchiveInput;
import org.neo4j.dbms.archive.ArchiveInput.FileInput;
import org.neo4j.dbms.archive.DumpFormatSelector;
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

    public static BackupDescription readDescription(FileSystemAbstraction fs, Path path) throws IOException {
        return readDescription(FileInput.of(fs, path));
    }

    public static BackupDescription readDescription(ArchiveInput input) throws IOException {
        try (ArchiveInput.OpenedArchive opened = input.open()) {
            return requireFormat(opened.magic()).readMetadata(opened.stream());
        }
    }

    public static InputStream decompress(ArchiveInput input) throws IOException {
        ArchiveInput.OpenedArchive opened = input.open();
        try {
            return requireFormat(opened.magic()).decompress(opened.stream());
        } catch (IOException e) {
            opened.stream().close();
            throw e;
        }
    }

    private static BackupCompressionFormat requireFormat(byte[] magicPrefix) {
        BackupCompressionFormat format = selectReadFormat(magicPrefix);
        if (format == null) {
            DumpFormatSelector.throwUnsupported(magicPrefix);
        }
        return format;
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
}
