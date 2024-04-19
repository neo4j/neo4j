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
import java.io.OutputStream;
import org.neo4j.dbms.archive.ArchiveFormat;
import org.neo4j.dbms.archive.StandardCompressionFormat;

public class BackupZstdFormatV1 implements BackupCompressionFormat {
    static final String MAGIC_HEADER = ArchiveFormat.BACKUP_PREFIX + "ZV1";

    private BackupMetadataV1 metadata;

    @Override
    public void setMetadata(BackupDescription description) {
        this.metadata = new BackupMetadataV1(description);
    }

    @Override
    public OutputStream compress(OutputStream stream) throws IOException {
        stream.write(MAGIC_HEADER.getBytes());
        OutputStream compressionStream = StandardCompressionFormat.ZSTD.compress(stream);
        metadata.writeToStream(compressionStream);
        return compressionStream;
    }

    @Override
    public InputStream decompress(InputStream stream) throws IOException {
        InputStream decompress = StandardCompressionFormat.ZSTD.decompress(stream);
        readMetadataFromZstdStream(decompress);
        return decompress;
    }

    @Override
    public StreamWithDescription decompressAndDescribe(InputStream stream) throws IOException {
        InputStream decompress = StandardCompressionFormat.ZSTD.decompress(stream);
        var description = readMetadataFromZstdStream(decompress).toBackupDescription();
        return new StreamWithDescription(decompress, description);
    }

    @Override
    public BackupDescription readMetadata(InputStream inputStream) throws IOException {
        return readMetadataFromZstdStream(StandardCompressionFormat.ZSTD.decompress(inputStream))
                .toBackupDescription();
    }

    private static BackupMetadataV1 readMetadataFromZstdStream(InputStream inputStream) throws IOException {
        return BackupMetadataV1.readFromStream(inputStream);
    }

    @Override
    public String toString() {
        return "BackupZstdFormatV1{metadata=" + metadata + '}';
    }
}
