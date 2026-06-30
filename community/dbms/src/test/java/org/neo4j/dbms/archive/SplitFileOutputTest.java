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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
@RandomSupportExtension
class SplitFileOutputTest {

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private RandomSupport random;

    private static final int FILE_SIZE = 100;

    @Test
    void shouldCreateSplitFiles() throws IOException {
        Path base = testDirectory.file("test.dump");

        try (OutputStream stream = new Dumper.SplitFileOutput(fileSystem, base, FILE_SIZE).stream()) {
            stream.write(new byte[250]); // 3 parts
        }

        assertThat(Files.exists(base)).isTrue(); // header file
        assertThat(Files.exists(base.resolveSibling("test.dump.0"))).isFalse();
        assertThat(Files.exists(base.resolveSibling("test.dump.1"))).isTrue();
        assertThat(Files.exists(base.resolveSibling("test.dump.2"))).isTrue();
        assertThat(Files.exists(base.resolveSibling("test.dump.3"))).isTrue();
    }

    @Test
    void shouldHaveCorrectDataInFirstFile() throws IOException {
        Path base = testDirectory.file("test.dump");

        try (OutputStream stream = new Dumper.SplitFileOutput(fileSystem, base, FILE_SIZE).stream()) {
            stream.write(new byte[200]); // 3 parts
        }

        byte[] part1 = Files.readAllBytes(base);
        // First part: 4 byte magic + 4 byte total part count + 16 bytes UUID + data
        assertThat(Arrays.copyOfRange(part1, 0, 4)).isEqualTo(Dumper.SplitFileOutput.MAGIC_MANIFEST_HEADER.getBytes());
        assertThat(intFromBytes(Arrays.copyOfRange(part1, 4, 8))).isEqualTo(3);
    }

    @Test
    void shouldHaveMagicHeaderFirstInAllFiles() throws IOException {
        Path base = testDirectory.file("test.dump");

        try (OutputStream stream = new Dumper.SplitFileOutput(fileSystem, base, FILE_SIZE).stream()) {
            stream.write(new byte[250]); // 3 parts
        }

        for (int i = 0; i <= 3; i++) {
            if (i == 0) {
                byte[] bytes = Files.readAllBytes(base);
                assertThat(Arrays.copyOfRange(bytes, 0, 4))
                        .isEqualTo(Dumper.SplitFileOutput.MAGIC_MANIFEST_HEADER.getBytes());
            } else {
                byte[] bytes = Files.readAllBytes(base.resolveSibling("test.dump." + i));
                assertThat(Arrays.copyOfRange(bytes, 0, 4))
                        .isEqualTo(Dumper.SplitFileOutput.MAGIC_DATA_HEADER.getBytes());
            }
        }
    }

    @Test
    void shouldCreateSplitFilesWithSameUUID() throws IOException {
        Path base = testDirectory.file("test.dump");

        try (OutputStream stream = new Dumper.SplitFileOutput(fileSystem, base, FILE_SIZE).stream()) {
            stream.write(new byte[250]); // 3 parts
        }

        byte[] part0 = Files.readAllBytes(base);
        byte[] part1 = Files.readAllBytes(base.resolveSibling("test.dump.1"));
        byte[] part2 = Files.readAllBytes(base.resolveSibling("test.dump.2"));
        byte[] part3 = Files.readAllBytes(base.resolveSibling("test.dump.3"));

        // First part: 4 byte magic + 4 byte total part count + 16 bytes UUID + data
        byte[] uuid0 = Arrays.copyOfRange(part0, 8, 24);
        // All other parts: 4 byte magic + 4 byte part index + 16 bytes UUID + data
        byte[] uuid1 = Arrays.copyOfRange(part1, 8, 24);
        byte[] uuid2 = Arrays.copyOfRange(part2, 8, 24);
        byte[] uuid3 = Arrays.copyOfRange(part3, 8, 24);

        assertThat(uuid1).isEqualTo(uuid0);
        assertThat(uuid2).isEqualTo(uuid0);
        assertThat(uuid3).isEqualTo(uuid0);
    }

    @Test
    void shouldCreateSplitFilesWithCorrectIndex() throws IOException {
        Path base = testDirectory.file("test.dump");

        try (OutputStream stream = new Dumper.SplitFileOutput(fileSystem, base, FILE_SIZE).stream()) {
            stream.write(new byte[250]); // 3 parts
        }

        byte[] part1 = Files.readAllBytes(base.resolveSibling("test.dump.1"));
        byte[] part2 = Files.readAllBytes(base.resolveSibling("test.dump.2"));
        byte[] part3 = Files.readAllBytes(base.resolveSibling("test.dump.3"));

        // All other parts: 4 byte magic + 4 byte part index + 16 bytes UUID + data
        assertThat(intFromBytes(Arrays.copyOfRange(part1, 4, 8))).isEqualTo(1);
        assertThat(intFromBytes(Arrays.copyOfRange(part2, 4, 8))).isEqualTo(2);
        assertThat(intFromBytes(Arrays.copyOfRange(part3, 4, 8))).isEqualTo(3);
    }

    @Test
    void shouldWriteExpectedData() throws IOException {
        Path base = testDirectory.file("test.dump");
        byte[] expected = new byte[200];
        for (int i = 0; i < expected.length; i++) {
            expected[i] = (byte) i;
        }

        try (OutputStream stream = new Dumper.SplitFileOutput(fileSystem, base, FILE_SIZE).stream()) {
            stream.write(expected);
        }

        var actual = new ByteArrayOutputStream();
        var files = new String[] {"test.dump.1", "test.dump.2", "test.dump.3"};
        for (String file : files) {
            byte[] partBytes = Files.readAllBytes(base.resolveSibling(file));
            // All part files has a 24-byte header
            actual.write(partBytes, 24, partBytes.length - 24);
        }

        assertThat(actual.toByteArray()).isEqualTo(expected);
    }

    @Test
    void shouldWriteMoreExpectedRandomData() throws IOException {
        Path base = testDirectory.file("test.dump");
        int parts = random.nextInt(100, 1000);
        byte[] expected = random.nextBytes(parts * (FILE_SIZE - Dumper.SplitFileOutput.HEADER_SIZE));

        try (OutputStream stream = new Dumper.SplitFileOutput(fileSystem, base, FILE_SIZE).stream()) {
            stream.write(expected);
        }

        var actual = new ByteArrayOutputStream();
        for (int i = 0; i <= parts; i++) {
            if (i == 0) {
                // First part has a 24-bytes (4 magic + 4 total count + 16 UUID)
                byte[] partBytes = Files.readAllBytes(base);
                assertThat(intFromBytes(Arrays.copyOfRange(partBytes, 4, 8))).isEqualTo(parts);
            } else {
                // All other part files has a 24-byte header (4 magic + 4 index + 16 UUID) + data
                byte[] partBytes = Files.readAllBytes(base.resolveSibling("test.dump." + i));
                assertThat(intFromBytes(Arrays.copyOfRange(partBytes, 4, 8))).isEqualTo(i);
                actual.write(partBytes, 24, partBytes.length - 24);
            }
        }

        assertThat(actual.toByteArray()).isEqualTo(expected);
    }

    @Test
    void shouldFailIfPartFileAlreadyExists() throws IOException {
        Path base = testDirectory.file("test.dump");
        Files.createFile(base.resolveSibling("test.dump"));

        assertThatThrownBy(() -> {
                    try (OutputStream stream = new Dumper.SplitFileOutput(fileSystem, base, FILE_SIZE).stream()) {
                        stream.write(new byte[50]);
                    }
                })
                .hasMessageContaining("test.dump")
                .isInstanceOf(UncheckedIOException.class)
                .hasCauseInstanceOf(FileAlreadyExistsException.class);
    }

    private int intFromBytes(byte[] bytes) {
        return ((bytes[0] & 0xFF) << 24) | ((bytes[1] & 0xFF) << 16) | ((bytes[2] & 0xFF) << 8) | (bytes[3] & 0xFF);
    }
}
