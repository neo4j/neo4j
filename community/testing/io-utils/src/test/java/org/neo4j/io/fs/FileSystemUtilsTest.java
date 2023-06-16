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
package org.neo4j.io.fs;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.io.fs.FileSystemUtils.readLines;
import static org.neo4j.io.fs.FileSystemUtils.readString;
import static org.neo4j.io.fs.FileSystemUtils.writeString;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;
import org.neo4j.io.ByteUnit;
import org.neo4j.memory.DefaultScopedMemoryTracker;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.ThreadSafePeakMemoryTracker;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.utils.TestDirectory;

abstract class FileSystemUtilsTest {
    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private TestDirectory testDirectory;

    @Test
    void shouldCheckNonExistingDirectory() throws IOException {
        Path nonExistingDir = Path.of("nonExistingDir");

        assertTrue(FileSystemUtils.isEmptyOrNonExistingDirectory(fs, nonExistingDir));
    }

    @Test
    void shouldCheckExistingEmptyDirectory() throws IOException {
        Path existingEmptyDir = testDirectory.directory("existingEmptyDir");

        assertTrue(FileSystemUtils.isEmptyOrNonExistingDirectory(fs, existingEmptyDir));
    }

    @Test
    void dropDirectoryWithFile() throws IOException {
        Path directory = testDirectory.directory("directory");
        fs.openAsOutputStream(directory.resolve("a"), false).close();

        assertEquals(1, fs.listFiles(directory).length);

        FileSystemUtils.deleteFile(fs, directory);

        assertThrows(NoSuchFileException.class, () -> fs.listFiles(directory));
    }

    @Test
    void shouldCheckExistingNonEmptyDirectory() throws Exception {
        Path existingEmptyDir = testDirectory.directory("existingEmptyDir");
        fs.write(existingEmptyDir.resolve("someFile")).close();

        assertFalse(FileSystemUtils.isEmptyOrNonExistingDirectory(fs, existingEmptyDir));
    }

    @Test
    void shouldCheckExistingFile() throws IOException {
        Path existingFile = testDirectory.createFile("existingFile");

        assertFalse(FileSystemUtils.isEmptyOrNonExistingDirectory(fs, existingFile));
    }

    @Test
    void shouldCheckSizeOfFile() throws Exception {
        Path file = testDirectory.createFile("a");

        try (StoreChannel storeChannel = fs.write(file)) {
            storeChannel.writeAll(ByteBuffer.wrap(new byte[] {1}));
        }
        assertThat(FileSystemUtils.size(fs, file)).isEqualTo(1L);
    }

    @Test
    void shouldCheckSizeOfDirectory() throws Exception {
        Path dir = testDirectory.directory("dir");
        Path file1 = dir.resolve("file1");
        Path file2 = dir.resolve("file2");

        try (StoreChannel storeChannel = fs.write(file1)) {
            storeChannel.writeAll(ByteBuffer.wrap(new byte[] {1, 2}));
        }

        try (StoreChannel storeChannel = fs.write(file2)) {
            storeChannel.writeAll(ByteBuffer.wrap(new byte[] {1}));
        }

        assertThat(FileSystemUtils.size(fs, dir)).isEqualTo(3L);
    }

    @Test
    void shouldReturnZeroIfFileDoesNotExist() {
        var dir = testDirectory.directory("dir");
        var file = dir.resolve("file");
        assertThat(FileSystemUtils.size(fs, file)).isZero();
    }

    @Test
    void writeStringIntoAFile() throws IOException {
        Path file = testDirectory.file("lila");
        String data = RandomStringUtils.random((int) ByteUnit.kibiBytes(117));
        writeString(fs, file, data, EmptyMemoryTracker.INSTANCE);

        assertEquals(data, readString(fs, file, EmptyMemoryTracker.INSTANCE));
    }

    @Test
    void writeMultiLineStringIntoAFile() throws IOException {
        Path file = testDirectory.file("fry");
        int numberOfLines = 100;
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < numberOfLines; i++) {
            builder.append(i).append(System.lineSeparator());
        }
        var data = builder.toString();
        writeString(fs, file, data, EmptyMemoryTracker.INSTANCE);

        List<String> lines = readLines(fs, file, EmptyMemoryTracker.INSTANCE);
        assertEquals(100, lines.size());
        for (int i = 0; i < numberOfLines; i++) {
            assertEquals(i + "", lines.get(i));
        }
    }

    @Test
    void trackMemoryDuringFileWritingAndReading() throws IOException {
        Path file = testDirectory.file("lila");
        String data = RandomStringUtils.random((int) ByteUnit.kibiBytes(117));

        var writePeakTracker = new ThreadSafePeakMemoryTracker();
        var writeMemoryTracker = new DefaultScopedMemoryTracker(writePeakTracker);
        writeString(fs, file, data, writeMemoryTracker);
        assertEquals(0, writeMemoryTracker.usedNativeMemory());
        assertEquals(data.getBytes(UTF_8).length, writePeakTracker.peakMemoryUsage());

        var readPeakTracker = new ThreadSafePeakMemoryTracker();
        var readMemoryTracker = new DefaultScopedMemoryTracker(readPeakTracker);
        assertEquals(data, readString(fs, file, readMemoryTracker));
        assertEquals(0, readMemoryTracker.usedNativeMemory());
        assertEquals(data.getBytes(UTF_8).length, readPeakTracker.peakMemoryUsage());
    }
}
