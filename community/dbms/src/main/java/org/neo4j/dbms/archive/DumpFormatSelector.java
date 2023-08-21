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

import static org.neo4j.dbms.archive.StandardCompressionFormat.ZSTD;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.PushbackInputStream;
import org.neo4j.function.ThrowingSupplier;

public class DumpFormatSelector {
    private static final int MAGIC_PREFIX_LENGTH = 4;

    public static final char DUMP_PREFIX = 'D';

    public static InputStream decompress(ThrowingSupplier<InputStream, IOException> streamSupplier) throws IOException {
        // use pushback stream to support reading of legacy zstd dumps from stdin
        // if dump magic isn't recognized, this could be legacy dump format (plain zstd or gzip stream)
        // if source is file - stream can be recreated and this way both zstd and gzip formats can be read
        // but if source is stdin - it can't be reopened, so unrecognized magic pushed back to the stream
        // and format selection attempted using it if it's not closed yet
        var input = new PushbackInputStream(streamSupplier.get(), MAGIC_PREFIX_LENGTH);
        var format = selectInputFormat(input);
        if (format != null) {
            return format.decompress(input);
        }
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

    // try to detect input format and push back magic bytes back if not successful
    private static CompressionFormat selectInputFormat(PushbackInputStream stream) throws IOException {
        var bytes = stream.readNBytes(MAGIC_PREFIX_LENGTH);
        var magic = new String(bytes);
        return switch (magic) {
            case DumpZstdFormatV1.MAGIC_HEADER -> new DumpZstdFormatV1();
            case DumpGzipFormatV1.MAGIC_HEADER -> new DumpGzipFormatV1();
            default -> {
                stream.unread(bytes);
                yield null;
            }
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
