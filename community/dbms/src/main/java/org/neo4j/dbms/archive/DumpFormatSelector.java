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

import static org.neo4j.dbms.archive.ArchiveFormat.MAGIC_PREFIX_LENGTH;
import static org.neo4j.dbms.archive.StandardCompressionFormat.ZSTD;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.PushbackInputStream;
import java.util.function.Consumer;
import org.neo4j.dbms.archive.backup.BackupDescription;
import org.neo4j.dbms.archive.backup.BackupFormatSelector;
import org.neo4j.function.ThrowingSupplier;

public class DumpFormatSelector {

    public static InputStream decompress(ThrowingSupplier<InputStream, IOException> streamSupplier) throws IOException {
        // use pushback stream to support reading of legacy zstd dumps from stdin
        // if dump magic isn't recognized, this could be legacy dump format (plain zstd or gzip stream)
        // if source is file - stream can be recreated and this way both zstd and gzip formats can be read
        // but if source is stdin - it can't be reopened, so unrecognized magic pushed back to the stream
        // and format selection attempted using it if it's not closed yet
        var input = new PushbackInputStream(streamSupplier.get(), MAGIC_PREFIX_LENGTH);
        var bytes = input.readNBytes(MAGIC_PREFIX_LENGTH);
        var format = selectDumpFormat(bytes);
        if (format != null) {
            return format.decompress(input);
        }
        input.unread(bytes);
        return legacyDecompress(streamSupplier, input);
    }

    /**
     * This method supports decompression of dumps and backups
     */
    public static InputStream decompressWithBackupSupport(
            ThrowingSupplier<InputStream, IOException> streamSupplier,
            Consumer<BackupDescription> backupDescriptionConsumer)
            throws IOException {
        var input = new PushbackInputStream(streamSupplier.get(), MAGIC_PREFIX_LENGTH);
        var bytes = input.readNBytes(MAGIC_PREFIX_LENGTH);
        var magic = new String(bytes);
        var backupFormat = BackupFormatSelector.selectFormat(magic);
        if (backupFormat.isPresent()) {
            var streamWithDescription = backupFormat.get().decompressAndDescribe(input);
            backupDescriptionConsumer.accept(streamWithDescription.backupDescription());
            return streamWithDescription.inputStream();
        }
        var format = selectDumpFormat(bytes);
        if (format != null) {
            return format.decompress(input);
        }
        input.unread(bytes);
        return legacyDecompress(streamSupplier, input);
    }

    static InputStream legacyDecompress(
            ThrowingSupplier<InputStream, IOException> streamSupplier, PushbackInputStream input) throws IOException {
        return StandardCompressionFormat.decompress(() -> {
            try {
                // StandardCompressionFormat.decompress() closes stream if it fails to parse it as ZSTD
                // this call here to ensure that stream still open before returning it, otherwise get new one from
                // supplier
                if (input.available() > 0) {
                    return input;
                }
            } catch (IOException e) {
                // ignore
            }
            return streamSupplier.get();
        });
    }

    private static CompressionFormat selectDumpFormat(byte[] bytes) {
        return switch (new String(bytes)) {
            case DumpZstdFormatV1.MAGIC_HEADER -> new DumpZstdFormatV1();
            case DumpGzipFormatV1.MAGIC_HEADER -> new DumpGzipFormatV1();
            default -> null;
        };
    }

    public static CompressionFormat selectFormat() {
        return selectFormat(null);
    }

    public static CompressionFormat selectFormat(PrintStream err) {
        if (StandardCompressionFormat.selectCompressionFormat(err) == ZSTD) {
            return new DumpZstdFormatV1();
        }
        return new DumpGzipFormatV1();
    }
}
