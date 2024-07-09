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
import java.util.Optional;
import org.neo4j.dbms.archive.ArchiveFormat;
import org.neo4j.function.ThrowingSupplier;

public class BackupFormatSelector {

    private BackupFormatSelector() {}

    public static List<BackupCompressionFormat> availableFormats() {
        return List.of(new BackupZstdFormatV1(), new BackupTarFormatV1());
    }

    public static BackupCompressionFormat selectFormat(boolean compress) {
        return compress ? new BackupZstdFormatV1() : new BackupTarFormatV1();
    }

    public static BackupDescription readDescription(InputStream inputStream) throws IOException {
        return selectFormat(inputStream).readMetadata(inputStream);
    }

    public static InputStream decompress(ThrowingSupplier<InputStream, IOException> streamSupplier) throws IOException {
        InputStream inputStream = streamSupplier.get();
        return selectFormat(inputStream).decompress(inputStream);
    }

    private static BackupCompressionFormat selectFormat(InputStream inputStream) throws IOException {
        String magicPrefix = new String(inputStream.readNBytes(ArchiveFormat.MAGIC_PREFIX_LENGTH));
        return selectFormat(magicPrefix)
                .orElseThrow(() -> new IllegalArgumentException("Unsupported format backup format: " + magicPrefix));
    }

    public static Optional<BackupCompressionFormat> selectFormat(String magicPrefix) {
        return switch (magicPrefix) {
            case BackupZstdFormatV1.MAGIC_HEADER -> Optional.of(new BackupZstdFormatV1());
            case BackupTarFormatV1.MAGIC_HEADER -> Optional.of(new BackupTarFormatV1());
            default -> Optional.empty();
        };
    }
}
