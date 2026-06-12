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

public class DumpZstdFormatVLegacy implements Dumper.DumpFormat {
    // Zstd frame magic: 0xFD2FB528 (little endian)
    public static final MagicSignature MAGIC_HEADER = MagicSignature.of(0x28, 0xB5, 0x2F, 0xFD);

    @Override
    public OutputStream compress(OutputStream stream) throws IOException {
        return StandardCompressionFormat.ZSTD.compress(stream);
    }

    @Override
    public InputStream decompress(InputStream stream) throws IOException {
        return StandardCompressionFormat.ZSTD.decompress(stream);
    }

    @Override
    public String toString() {
        return "DumpZstdFormatVLegacy";
    }
}
