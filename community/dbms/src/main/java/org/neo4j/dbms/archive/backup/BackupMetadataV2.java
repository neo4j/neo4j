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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class BackupMetadataV2 extends BackupMetadataV1 {

    public static final String METADATA_SCRIPT_FIELD = "metadataScript";
    public static final int METADATA_SCRIPT_MAX_LENGTH = 1048576; // 1Mb
    public static final int VERSION = 2;

    private final BackupMetadataV1 backupMetadataV1;
    private final Map<String, String> additionalFields;

    public static BackupMetadataV2 from(BackupDescription description) {
        var backupMetadataV1 = new BackupMetadataV1(description);
        var metadataScript = description.getMetadataScript();
        Map<String, String> additionalFields;
        if (metadataScript != null) {
            additionalFields = Map.of(METADATA_SCRIPT_FIELD, metadataScript);
        } else {
            additionalFields = Collections.EMPTY_MAP;
        }
        return new BackupMetadataV2(backupMetadataV1, additionalFields);
    }

    public static BackupMetadataV2 readFromStream(InputStream inputStream) throws IOException {
        var backupMetadataV1 = readMetadataV1(inputStream);
        var additionalFields = readMap(inputStream);
        return new BackupMetadataV2(backupMetadataV1, additionalFields);
    }

    private BackupMetadataV2(BackupMetadataV1 backupMetadataV1, Map<String, String> additionalFields) {
        super(
                backupMetadataV1.getDatabaseName(),
                backupMetadataV1.getStoreId(),
                backupMetadataV1.getDatabaseId(),
                backupMetadataV1.getBackupTime(),
                backupMetadataV1.getLowestAppendIndex(),
                backupMetadataV1.getHighestAppendIndex(),
                backupMetadataV1.isRecovered(),
                backupMetadataV1.isCompressed(),
                backupMetadataV1.isFull());
        this.backupMetadataV1 = backupMetadataV1;
        this.additionalFields = additionalFields;
    }

    public Map<String, String> getAdditionalFields() {
        return additionalFields;
    }

    void writeToStreamV2(OutputStream compressionStream) throws IOException {
        writeToStreamV1(compressionStream);
        writeMap(compressionStream, getAdditionalFields());
    }

    private void writeMap(OutputStream compressionStream, Map<String, String> additionalFields) throws IOException {
        var dataStream = new DataOutputStream(compressionStream);
        int mapSize = additionalFields.size();
        dataStream.writeInt(mapSize);
        for (var field : additionalFields.entrySet()) {
            writeString(dataStream, field.getKey());
            writeString(dataStream, field.getValue());
        }
        dataStream.flush();
    }

    private static Map<String, String> readMap(InputStream inputStream) throws IOException {
        Map<String, String> deserializedMap = new HashMap<>();
        var dataStream = new DataInputStream(inputStream);
        var mapSize = dataStream.readInt();
        for (int i = 0; i < mapSize; i++) {
            String key = readString(dataStream, Integer.MAX_VALUE);
            var maxValueLength = (key.equals(METADATA_SCRIPT_FIELD)) ? METADATA_SCRIPT_MAX_LENGTH : Integer.MAX_VALUE;
            String value = readString(dataStream, maxValueLength);
            if (value != null) {
                deserializedMap.put(key, value);
            }
        }
        return deserializedMap;
    }

    private static void writeString(DataOutputStream outputStream, String value) throws IOException {
        byte[] data = value.getBytes(UTF_8);
        outputStream.writeInt(data.length);
        outputStream.write(data);
    }

    private static String readString(DataInputStream inputStream, int maxLength) throws IOException {
        var length = inputStream.readInt();
        if (length > maxLength) {
            inputStream.skipBytes(length);
            return null;
        } else {
            return new String(inputStream.readNBytes(length), UTF_8);
        }
    }

    @Override
    public BackupDescription toBackupDescription() {
        var metadataScript = additionalFields.get(METADATA_SCRIPT_FIELD);
        return new BackupDescription(
                        backupMetadataV1.getDatabaseName(),
                        backupMetadataV1.getStoreId(),
                        backupMetadataV1.getDatabaseId(),
                        backupMetadataV1.getBackupTime(),
                        backupMetadataV1.isRecovered(),
                        backupMetadataV1.isCompressed(),
                        backupMetadataV1.isFull(),
                        backupMetadataV1.getLowestAppendIndex(),
                        backupMetadataV1.getHighestAppendIndex())
                .withMetadataScript(metadataScript);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BackupMetadataV2 that = (BackupMetadataV2) o;
        return Objects.equals(backupMetadataV1, that.backupMetadataV1)
                && Objects.equals(additionalFields, that.additionalFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(backupMetadataV1, additionalFields);
    }
}
