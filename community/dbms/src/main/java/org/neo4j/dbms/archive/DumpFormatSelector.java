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
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import org.neo4j.dbms.archive.Dumper.DumpFormat;
import org.neo4j.dbms.archive.backup.BackupDescription;
import org.neo4j.dbms.archive.backup.BackupFormatSelector;

public class DumpFormatSelector {

    public static List<DumpFormat> availableFormats() {
        return List.of(
                new DumpGzipFormatV1(),
                new DumpZstdFormatV1(),
                new DumpGzipFormatVLegacy(),
                new DumpZstdFormatVLegacy());
    }

    public static InputStream decompress(ArchiveInput input) throws IOException {
        var opened = input.open();
        try {
            return decompress0(opened.stream(), opened.magic());
        } catch (IOException | IllegalArgumentException exc) {
            opened.stream().close();
            throw exc;
        }
    }

    /**
     * This method supports decompression of dumps and backups
     */
    public static InputStream decompressWithBackupSupport(
            ArchiveInput input, Consumer<BackupDescription> backupDescriptionConsumer) throws IOException {
        var opened = input.open();
        try {
            // Note: num read bytes may be less than MAGIC_PREFIX_LENGTH
            var backupFormat = BackupFormatSelector.selectReadFormat(opened.magic());
            if (backupFormat != null) {
                var streamWithDescription = backupFormat.decompressAndDescribe(opened.stream());
                backupDescriptionConsumer.accept(streamWithDescription.backupDescription());
                return streamWithDescription.inputStream();
            }

            return decompress0(opened.stream(), opened.magic());
        } catch (IOException | IllegalArgumentException exc) {
            opened.stream().close();
            throw exc;
        }
    }

    public static void throwUnsupported(byte[] magic) {
        throw new IllegalArgumentException("Unsupported format backup format: " + Arrays.toString(magic));
    }

    private static InputStream decompress0(InputStream input, byte[] magic) throws IOException {
        var format = selectReadFormat(magic);
        if (format == null) {
            throwUnsupported(magic);
        }

        if (format instanceof DumpZstdFormatVLegacy || format instanceof DumpGzipFormatVLegacy) {
            // When we are exposed to a legacy dump, we need to return the magic bytes of the compression format
            // to the stream.
            var pushback = new PushbackInputStream(input, MAGIC_PREFIX_LENGTH);
            pushback.unread(magic);
            input = pushback;
        }

        return format.decompress(input);
    }

    private static DumpFormat selectReadFormat(byte[] bytes) {
        if (DumpZstdFormatV1.MAGIC_HEADER.matches(bytes)) {
            return new DumpZstdFormatV1();
        }
        if (DumpGzipFormatV1.MAGIC_HEADER.matches(bytes)) {
            return new DumpGzipFormatV1();
        }
        if (DumpZstdFormatVLegacy.MAGIC_HEADER.matches(bytes)) {
            return new DumpZstdFormatVLegacy();
        }
        if (DumpGzipFormatVLegacy.MAGIC_HEADER.matches(bytes)) {
            return new DumpGzipFormatVLegacy();
        }
        return null;
    }

    public static DumpFormat selectWriteFormat() {
        return selectWriteFormat(null);
    }

    public static DumpFormat selectWriteFormat(PrintStream err) {
        if (StandardCompressionFormat.selectCompressionFormat(err) == ZSTD) {
            return new DumpZstdFormatV1();
        }
        return new DumpGzipFormatV1();
    }
}
