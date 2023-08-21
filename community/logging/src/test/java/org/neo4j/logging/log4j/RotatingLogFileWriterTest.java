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
package org.neo4j.logging.log4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.condition.OS.LINUX;
import static org.neo4j.test.assertion.Assert.assertEventually;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.string.UTF8;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class RotatingLogFileWriterTest {
    @Inject
    private DefaultFileSystemAbstraction fs;

    @Inject
    private TestDirectory dir;

    private RotatingLogFileWriter writer;

    @AfterEach
    void tearDown() throws IOException {
        writer.close();
    }

    @Test
    void shouldRotateOnThreshold() {
        Path targetFile = dir.homePath().resolve("test.log");
        Path targetFile1 = dir.homePath().resolve("test.log.01");
        Path targetFile2 = dir.homePath().resolve("test.log.02");

        writer = new RotatingLogFileWriter(fs, targetFile, 10, 2, "", "myHeader%n");

        assertThat(fs.fileExists(targetFile)).isEqualTo(true);

        writer.printf("more than 10B that will trigger rotation on next written message");

        assertThat(fs.fileExists(targetFile)).isEqualTo(true);
        assertThat(fs.fileExists(targetFile1)).isEqualTo(false);

        writer.printf("test string");
        assertThat(fs.fileExists(targetFile)).isEqualTo(true);
        assertThat(fs.fileExists(targetFile1)).isEqualTo(true);
        assertThat(fs.fileExists(targetFile2)).isEqualTo(false);
    }

    @Test
    void rotationShouldRespectMaxArchives() throws IOException {
        Path targetFile = dir.homePath().resolve("test.log");
        Path targetFile1 = dir.homePath().resolve("test.log.01");
        Path targetFile2 = dir.homePath().resolve("test.log.02");
        Path targetFile3 = dir.homePath().resolve("test.log.03");

        writer = new RotatingLogFileWriter(fs, targetFile, 10, 2, "", "");

        assertThat(fs.fileExists(targetFile)).isEqualTo(true);

        writer.printf("test string 1");
        writer.printf("test string 2");
        writer.printf("test string 3");
        writer.printf("test string 4");
        assertThat(fs.fileExists(targetFile)).isEqualTo(true);
        assertThat(fs.fileExists(targetFile1)).isEqualTo(true);
        assertThat(fs.fileExists(targetFile2)).isEqualTo(true);
        assertThat(fs.fileExists(targetFile3)).isEqualTo(false);

        assertThat(Files.readAllLines(targetFile)).containsExactly("test string 4");
        assertThat(Files.readAllLines(targetFile1)).containsExactly("test string 3");
        assertThat(Files.readAllLines(targetFile2)).containsExactly("test string 2");
    }

    @Test
    void rotationShouldCompressToZipIfRequested() throws IOException {
        Path targetFile = dir.homePath().resolve("test.log");
        Path targetFile1 = dir.homePath().resolve("test.log.01.zip");

        writer = new RotatingLogFileWriter(fs, targetFile, 10, 2, ".zip", "");

        assertThat(fs.fileExists(targetFile)).isEqualTo(true);

        writer.printf("test string 1");
        writer.printf("test string 2");
        assertThat(fs.fileExists(targetFile)).isEqualTo(true);
        // The files are compressed asynchronously after the log rotation so wait for the file to exist.
        assertEventually(() -> fs.fileExists(targetFile1), bool -> bool, 5, TimeUnit.SECONDS);
        writer.close();

        assertThat(Files.readAllLines(targetFile)).containsExactly("test string 2");

        try (FileSystem fileSystem =
                FileSystems.newFileSystem(targetFile1, this.getClass().getClassLoader())) {
            assertThat(Files.readAllLines(fileSystem.getPath("test.log.01"))).containsExactly("test string 1");
        }
    }

    @Test
    @EnabledOnOs({LINUX})
    void rotationShouldCompressToGzipIfRequested() throws IOException {
        Path targetFile = dir.homePath().resolve("test.log");
        Path targetFile1 = dir.homePath().resolve("test.log.01.gz");

        writer = new RotatingLogFileWriter(fs, targetFile, 10, 2, ".gz", "");

        assertThat(fs.fileExists(targetFile)).isEqualTo(true);

        writer.printf("test string 1");
        writer.printf("test string 2");
        assertThat(fs.fileExists(targetFile)).isEqualTo(true);
        // The files are compressed asynchronously after the log rotation so wait for the file to exist.
        assertEventually(() -> fs.fileExists(targetFile1), bool -> bool, 5, TimeUnit.SECONDS);
        writer.close();

        assertThat(Files.readAllLines(targetFile)).containsExactly("test string 2");

        try (GZIPInputStream stream = new GZIPInputStream(Files.newInputStream(targetFile1))) {
            assertThat(UTF8.decode(stream.readAllBytes())).isEqualTo("test string 1" + System.lineSeparator());
        }
    }

    @Test
    void rotationShouldUseFileSuffixWithoutCompressionIfRequested() throws IOException {
        Path targetFile = dir.homePath().resolve("test.log");
        Path targetFile1 = dir.homePath().resolve("test.log.01.weird-%e.filesuffix");

        writer = new RotatingLogFileWriter(fs, targetFile, 10, 2, ".weird-%e.filesuffix", "");

        assertThat(fs.fileExists(targetFile)).isEqualTo(true);

        writer.printf("test string 1");
        writer.printf("test string 2");
        assertThat(fs.fileExists(targetFile)).isEqualTo(true);
        assertThat(fs.fileExists(targetFile1)).isEqualTo(true);
        writer.close();

        assertThat(Files.readAllLines(targetFile)).containsExactly("test string 2");
        assertThat(Files.readAllLines(targetFile1)).containsExactly("test string 1");
    }

    @Test
    void headerShouldBeUsedInEachFile() throws IOException {
        Path targetFile = dir.homePath().resolve("test.log");
        Path targetFile1 = dir.homePath().resolve("test.log.01");

        writer = new RotatingLogFileWriter(fs, targetFile, 10, 2, "", "my header%n");

        assertThat(fs.fileExists(targetFile)).isEqualTo(true);

        writer.printf("Long line that will get next message to be written to next file");
        writer.printf("test2");

        assertThat(fs.fileExists(targetFile)).isEqualTo(true);
        assertThat(fs.fileExists(targetFile1)).isEqualTo(true);

        assertThat(Files.readAllLines(targetFile1))
                .containsExactly("my header", "Long line that will get next message to be written to next file");
        assertThat(Files.readAllLines(targetFile)).containsExactly("my header", "test2");
    }

    @Test
    void shouldHandleFormatStrings() throws IOException {
        Path targetFile = dir.homePath().resolve("test.log");

        writer = new RotatingLogFileWriter(fs, targetFile, 100, 2, "", "");

        assertThat(fs.fileExists(targetFile)).isEqualTo(true);

        String formatString = "%s,%d,%f";
        Object[] formatArguments = new Object[] {"string", 1, 1.234567f};

        writer.printf(formatString, formatArguments);
        writer.printf("test2");

        assertThat(Files.readAllLines(targetFile))
                .containsExactly(String.format(formatString, formatArguments), "test2");
    }
}
