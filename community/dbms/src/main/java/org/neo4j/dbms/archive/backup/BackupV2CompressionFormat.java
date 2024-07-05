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

public abstract class BackupV2CompressionFormat implements BackupCompressionFormat {
    protected BackupDescription description;

    @Override
    public void setMetadata(BackupDescription description) {
        this.description = description;
    }

    protected static BackupDescription readDescriptionFromStream(InputStream inputStream) throws IOException {
        var metadataVersion = inputStream.read();
        var backupMetadata =
                switch (metadataVersion) {
                    case BackupMetadataV2.VERSION -> BackupMetadataV2.readFromStream(inputStream);
                    default -> throw new IOException(
                            String.format("Unsupported metadata version %d found in backup", metadataVersion));
                };
        return backupMetadata.toBackupDescription();
    }

    protected void writeDescriptionToStream(OutputStream outputStream) throws IOException {
        // Always write the newest version of BackupMetadata
        var metadataVersion = BackupMetadataV2.VERSION;
        outputStream.write(metadataVersion);
        var metadata = BackupMetadataV2.from(this.description);
        metadata.writeToStreamV2(outputStream);
    }
}
