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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.storageengine.api.StoreId;

public class BackupMetadataV2Test {

    @Test
    void shouldSerializeDeserializeBackupMetadataV2() throws IOException {
        // given
        var backupDescription =
                createBackupDescriptionWithScript(bigString(BackupMetadataV2.METADATA_SCRIPT_MAX_LENGTH));
        var originalBackupMetadataV2 = BackupMetadataV2.from(backupDescription);

        // when
        var outputStream = new ByteArrayOutputStream();
        originalBackupMetadataV2.writeToStreamV2(outputStream);
        var inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        var recreatedBackupMetadataV2 = BackupMetadataV2.readFromStream(inputStream);

        // then
        assertThat(recreatedBackupMetadataV2).isEqualTo(originalBackupMetadataV2);
    }

    /* This test is used to detect an unexpected change in serialized bytes */
    @Test
    void serializedBytesShouldMatch() throws IOException, NoSuchAlgorithmException {
        // given
        var backupDescription = createBackupDescriptionWithScript("script");
        var originalBackupMetadataV2 = BackupMetadataV2.from(backupDescription);

        // when
        var outputStream = new ByteArrayOutputStream();
        originalBackupMetadataV2.writeToStreamV2(outputStream);

        // then
        var messageDigest = MessageDigest.getInstance("MD5");
        var md5 = messageDigest.digest(outputStream.toByteArray());
        var md5String = bytesToHex(md5);
        assertThat(md5String).isEqualTo("1C232DC8EBC18026DD47886393EC5068");
    }

    @Test
    void shouldNotDeserializeBigScript() throws IOException {
        var backupDescription =
                createBackupDescriptionWithScript(bigString(BackupMetadataV2.METADATA_SCRIPT_MAX_LENGTH + 1));
        var backupMetadata = BackupMetadataV2.from(backupDescription);
        var outputStream = new ByteArrayOutputStream();
        backupMetadata.writeToStreamV2(outputStream);

        var is = new ByteArrayInputStream(outputStream.toByteArray());
        var rehydrated = BackupMetadataV2.readMetadataV1(is);
        assertThat(rehydrated.toBackupDescription().getMetadataScript()).isNull();
    }

    private static BackupDescription createBackupDescriptionWithScript(String script) {
        var storeId = new StoreId(4, 5, "legacy", "legacy", 1, 1);
        var backupDescription = new BackupDescription(
                "foo",
                storeId,
                DatabaseId.SYSTEM_DATABASE_ID,
                LocalDateTime.of(2024, 5, 30, 15, 54),
                true,
                true,
                true,
                1,
                2);
        backupDescription = backupDescription.withMetadataScript(script);
        return backupDescription;
    }

    private static String bigString(int size) {
        return "a".repeat(size);
    }

    // from https://stackoverflow.com/questions/9655181/java-convert-a-byte-array-to-a-hex-string
    private static final byte[] HEX_ARRAY = "0123456789ABCDEF".getBytes(StandardCharsets.US_ASCII);

    private static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = (char) HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = (char) HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }
}
