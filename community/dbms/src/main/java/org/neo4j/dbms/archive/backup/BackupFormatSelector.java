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
import java.util.List;
import org.neo4j.dbms.archive.ArchiveFormat;
import org.neo4j.dbms.archive.DumpFormatSelector;
import org.neo4j.function.ThrowingSupplier;

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

    public static BackupDescription readDescription(InputStream inputStream) throws IOException {
        return selectReadFormat(inputStream).readMetadata(inputStream);
    }

    public static InputStream decompress(ThrowingSupplier<InputStream, IOException> streamSupplier) throws IOException {
        InputStream inputStream = streamSupplier.get();
        return selectReadFormat(inputStream).decompress(inputStream);
    }

    private static BackupCompressionFormat selectReadFormat(InputStream inputStream) throws IOException {
        byte[] magicPrefix = inputStream.readNBytes(ArchiveFormat.MAGIC_PREFIX_LENGTH);
        var format = selectReadFormat(magicPrefix);
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
