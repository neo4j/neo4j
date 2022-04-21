/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.io.fs;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
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

        try (var fileWriter = fs.openAsWriter(file, UTF_8, false)) {
            fileWriter.append('a');
        }

        assertThat(FileSystemUtils.size(fs, file)).isEqualTo(1L);
    }

    @Test
    void shouldCheckSizeOfDirectory() throws Exception {
        Path dir = testDirectory.directory("dir");
        Path file1 = dir.resolve("file1");
        Path file2 = dir.resolve("file2");

        try (var fileWriter = fs.openAsWriter(file1, UTF_8, false)) {
            fileWriter.append('a').append('b');
        }
        try (var fileWriter = fs.openAsWriter(file2, UTF_8, false)) {
            fileWriter.append('a');
        }

        assertThat(FileSystemUtils.size(fs, dir)).isEqualTo(3L);
    }

    @Test
    void shouldReturnZeroIfFileDoesNotExist() {
        var dir = testDirectory.directory("dir");
        var file = dir.resolve("file");
        assertThat(FileSystemUtils.size(fs, file)).isZero();
    }
}
