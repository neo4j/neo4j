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

public class BackupTarFormatV2 implements BackupCompressionFormat {
    static final String MAGIC_HEADER = ArchiveFormat.BACKUP_PREFIX + "TV2";

    private BackupMetadataV2 metadata;

    @Override
    public void setMetadata(BackupDescription description) {
        this.metadata = BackupMetadataV2.from(description);
    }

    @Override
    public OutputStream compress(OutputStream stream) throws IOException {
        stream.write(MAGIC_HEADER.getBytes());
        writeDescriptionToStream(stream);
        return stream;
    }

    @Override
    public InputStream decompress(InputStream stream) throws IOException {
        readMetadataFromStream(stream);
        return stream;
    }

    @Override
    public StreamWithDescription decompressAndDescribe(InputStream stream) throws IOException {
        var description = readMetadataFromStream(stream).toBackupDescription();
        return new StreamWithDescription(stream, description);
    }

    @Override
    public BackupDescription readMetadata(InputStream inputStream) throws IOException {
        return readMetadataFromStream(inputStream).toBackupDescription();
    }

    private void writeDescriptionToStream(OutputStream outputStream) throws IOException {
        // Always write the newest version of BackupMetadata
        var metadataVersion = BackupMetadataV2.VERSION;
        outputStream.write(metadataVersion);
        metadata.writeToStreamV2(outputStream);
    }

    private static BackupMetadataV2 readMetadataFromStream(InputStream inputStream) throws IOException {
        var metadataVersion = inputStream.read();
        return switch (metadataVersion) {
            case BackupMetadataV2.VERSION -> BackupMetadataV2.readFromStream(inputStream);
            default -> throw new IOException(
                    String.format("Unsupported metadata version %d found in backup", metadataVersion));
        };
    }
}
