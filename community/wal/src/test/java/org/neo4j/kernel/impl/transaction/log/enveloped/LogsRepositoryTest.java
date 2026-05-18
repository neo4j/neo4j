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
package org.neo4j.kernel.impl.transaction.log.enveloped;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.filename.SequentialFileNameHelper;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class LogsRepositoryTest {

    private static final String BASE_NAME = "logFile";

    @Inject
    TestDirectory testDirectory;

    @Inject
    FileSystemAbstraction fs;

    private LogsRepository logRepository;
    private Path directory;

    @BeforeEach
    void setUp() {
        directory = testDirectory.directory("repository");
        logRepository = new LogsRepository(fs, new SequentialFileNameHelper(directory, BASE_NAME));
    }

    @Test
    void shouldCreateDirOnInitilaiseIfDoesNotExist() throws IOException {
        var someDir = testDirectory.homePath().resolve("someDir");
        var logsRepository =
                new LogsRepository(testDirectory.getFileSystem(), new SequentialFileNameHelper(someDir, BASE_NAME));

        assertThat(fs.fileExists(someDir)).isFalse();

        logsRepository.initialise();
        assertThat(fs.fileExists(someDir)).isTrue();
        assertThat(fs.isDirectory(someDir)).isTrue();
    }

    @Test
    void shouldFailIfBaseDirIsAFile() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> new LogsRepository(
                        testDirectory.getFileSystem(),
                        new SequentialFileNameHelper(testDirectory.createFile("file"), BASE_NAME)));
    }

    @Test
    void shouldWriteAndReadToCorrectlySelectedFile() throws IOException {
        var files = 3;
        createLogFiles(files);

        for (int i = 1; i <= files; i++) {
            var channel = logRepository.openWriteChannel(i);
            assertThat(channel.path()).isEqualTo(directory.resolve(BASE_NAME + "." + i));
            channel.channel()
                    .write(ByteBuffer.wrap(
                            channel.path().getFileName().toString().getBytes(StandardCharsets.UTF_8)));
            channel.channel().close();
        }

        for (int i = 1; i <= files; i++) {
            var channel = logRepository.openReadChannel(i);
            assertThat(channel.path()).isEqualTo(directory.resolve(BASE_NAME + "." + i));
            var expectedName = channel.path().getFileName().toString();
            var array = new byte[expectedName.getBytes(StandardCharsets.UTF_8).length];
            channel.channel().read(ByteBuffer.wrap(array));
            channel.channel().close();
            var content = new String(array, StandardCharsets.UTF_8);
            assertThat(content).isEqualTo(expectedName);
        }
    }

    @Test
    void shouldFailWitNoSuchFileExceptionIfNotExistingVersion() {
        assertThatExceptionOfType(NoSuchFileException.class).isThrownBy(() -> logRepository.openReadChannel(2));
    }

    @Test
    void shouldCreateFileIfNotExistingVersionWhenWriting() throws IOException {
        assertThat(fs.listFiles(directory)).isEmpty();
        logRepository.createWriteChannel(2);
        assertThat(fs.listFiles(directory)).containsExactly(directory.resolve(BASE_NAME + ".2"));
    }

    @Test
    void shouldIgnoreFileWithOtherVersionSuffix() throws IOException {
        var similarFile = directory.resolve(BASE_NAME + "-2");
        fs.write(similarFile).close();
        assertThat(fs.fileExists(similarFile)).isTrue();
        assertThat(logRepository.isEmpty()).isTrue();
    }

    @Test
    void shouldListEmptyIfNoLogFiles() throws IOException {
        assertThat(fs.listFiles(directory)).isEmpty();
        assertThat(logRepository.isEmpty()).isTrue();

        createFile(directory.resolve("otherLog.1"));

        assertThat(fs.listFiles(directory)).isNotEmpty();
        assertThat(logRepository.isEmpty()).isTrue();

        createLogFile(1);

        assertThat(fs.listFiles(directory)).isNotEmpty();
        assertThat(logRepository.isEmpty()).isFalse();
    }

    @Test
    void shouldDeleteFilesFromWitCorrectNameAndVersion() throws IOException {
        createLogFiles(5);
        createLogFile(100);
        createFile(directory.resolve("otherLog.5"));
        createFile(directory.resolve("otherLog.4"));
        createFile(directory.resolve("otherLog.90"));

        assertThat(fs.listFiles(directory))
                .containsExactlyInAnyOrder(
                        directory.resolve(BASE_NAME + ".1"),
                        directory.resolve(BASE_NAME + ".2"),
                        directory.resolve(BASE_NAME + ".3"),
                        directory.resolve(BASE_NAME + ".4"),
                        directory.resolve(BASE_NAME + ".5"),
                        directory.resolve(BASE_NAME + ".100"),
                        directory.resolve("otherLog.5"),
                        directory.resolve("otherLog.4"),
                        directory.resolve("otherLog.90"));

        logRepository.deleteLogFilesFrom(3);

        assertThat(fs.listFiles(directory))
                .containsExactlyInAnyOrder(
                        directory.resolve(BASE_NAME + ".1"),
                        directory.resolve(BASE_NAME + ".2"),
                        directory.resolve("otherLog.5"),
                        directory.resolve("otherLog.4"),
                        directory.resolve("otherLog.90"));
    }

    @Test
    void shouldDeleteFilesToWitCorrectNameAndVersion() throws IOException {
        createLogFiles(5);
        createLogFile(100);
        createFile(directory.resolve("otherLog.5"));
        createFile(directory.resolve("otherLog.4"));
        createFile(directory.resolve("otherLog.90"));

        assertThat(fs.listFiles(directory))
                .containsExactlyInAnyOrder(
                        directory.resolve(BASE_NAME + ".1"),
                        directory.resolve(BASE_NAME + ".2"),
                        directory.resolve(BASE_NAME + ".3"),
                        directory.resolve(BASE_NAME + ".4"),
                        directory.resolve(BASE_NAME + ".5"),
                        directory.resolve(BASE_NAME + ".100"),
                        directory.resolve("otherLog.5"),
                        directory.resolve("otherLog.4"),
                        directory.resolve("otherLog.90"));

        logRepository.deleteLogFilesTo(3);

        assertThat(fs.listFiles(directory))
                .containsExactlyInAnyOrder(
                        directory.resolve(BASE_NAME + ".4"),
                        directory.resolve(BASE_NAME + ".5"),
                        directory.resolve(BASE_NAME + ".100"),
                        directory.resolve("otherLog.5"),
                        directory.resolve("otherLog.4"),
                        directory.resolve("otherLog.90"));
    }

    @Test
    void shouldListVersionsCorrectly() throws IOException {
        createLogFiles(5);
        createLogFile(100);
        createFile(directory.resolve("otherLog.6"));
        createFile(directory.resolve("otherLog.7"));
        createFile(directory.resolve("otherLog.90"));

        assertThat(fs.listFiles(directory))
                .containsExactlyInAnyOrder(
                        directory.resolve(BASE_NAME + ".1"),
                        directory.resolve(BASE_NAME + ".2"),
                        directory.resolve(BASE_NAME + ".3"),
                        directory.resolve(BASE_NAME + ".4"),
                        directory.resolve(BASE_NAME + ".5"),
                        directory.resolve(BASE_NAME + ".100"),
                        directory.resolve("otherLog.6"),
                        directory.resolve("otherLog.7"),
                        directory.resolve("otherLog.90"));

        assertThat(logRepository.logVersions(false)).containsExactly(1, 2, 3, 4, 5, 100);
    }

    @Test
    void shouldListVersionRangeCorrectlyAndIgnoreGaps() throws IOException {
        createLogFiles(5);
        createLogFile(100);
        createFile(directory.resolve("otherLog.6"));
        createFile(directory.resolve("otherLog.7"));
        createFile(directory.resolve("otherLog.90"));

        assertThat(fs.listFiles(directory))
                .containsExactlyInAnyOrder(
                        directory.resolve(BASE_NAME + ".1"),
                        directory.resolve(BASE_NAME + ".2"),
                        directory.resolve(BASE_NAME + ".3"),
                        directory.resolve(BASE_NAME + ".4"),
                        directory.resolve(BASE_NAME + ".5"),
                        directory.resolve(BASE_NAME + ".100"),
                        directory.resolve("otherLog.6"),
                        directory.resolve("otherLog.7"),
                        directory.resolve("otherLog.90"));

        assertThat(logRepository.logVersionsRange()).isEqualTo(LongRange.range(1, 100));
    }

    private void createLogFiles(int files) throws IOException {
        if (files < 1) {
            throw new IllegalArgumentException("At least 1 file. Got " + files);
        }
        var log1 = createLogFile(1);
        for (int i = 2; i <= files; i++) {
            fs.copyFile(log1, directory.resolve(BASE_NAME + "." + i));
        }
    }

    private Path createLogFile(int version) throws IOException {
        var log = directory.resolve(BASE_NAME + "." + version);
        return createFile(log);
    }

    private Path createFile(Path filePath) throws IOException {
        try (var channel = fs.open(filePath, Set.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE))) {}
        return filePath;
    }

    @Test
    void shouldIgnoreOtherFiles() {}

    @Test
    void shouldDeleteFiles() {}
}
