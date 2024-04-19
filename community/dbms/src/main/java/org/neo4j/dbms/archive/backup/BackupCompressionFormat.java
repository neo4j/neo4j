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
import org.neo4j.dbms.archive.CompressionFormat;

/**
 * Compression format that is used for backup artifacts.
 * Backup compression format have custom metadata that is stored together with artifact.
 * Content of metadata is compression format and version specific.
 */
public interface BackupCompressionFormat extends CompressionFormat {
    void setMetadata(BackupDescription compressedMetadata);

    BackupDescription readMetadata(InputStream inputStream) throws IOException;

    record StreamWithDescription(InputStream inputStream, BackupDescription backupDescription) {}

    StreamWithDescription decompressAndDescribe(InputStream stream) throws IOException;
}
