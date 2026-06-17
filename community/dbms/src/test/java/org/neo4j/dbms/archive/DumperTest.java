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

import static java.util.Collections.emptySet;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.neo4j.dbms.archive.Dumper.FileOutput;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.DisabledForRoot;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class DumperTest {
    @Inject
    private TestDirectory testDirectory;

    @Inject
    private FileSystemAbstraction filesystem;

    @Test
    void shouldGiveAClearErrorIfTheArchiveAlreadyExists() throws IOException {
        Path directory = testDirectory.directory("a-directory");
        Path archive = testDirectory.file("the-archive.dump");
        Files.write(archive, EMPTY_BYTE_ARRAY);
        assertThatThrownBy(() -> {
                    Dumper dumper = new Dumper(filesystem);
                    dumper.dump(
                            FileOutput.of(filesystem, archive),
                            new DumpGzipFormatV1(),
                            Dumper.collectManifest(directory));
                })
                .isInstanceOf(FileAlreadyExistsException.class)
                .hasMessage(archive.toString());
    }

    @Test
    void shouldGiveAClearErrorMessageIfTheDirectoryDoesntExist() {
        Path directory = testDirectory.file("a-directory");
        Path archive = testDirectory.file("the-archive.dump");
        assertThatThrownBy(() -> {
                    Dumper dumper = new Dumper(filesystem);
                    dumper.dump(
                            FileOutput.of(filesystem, archive),
                            new DumpGzipFormatV1(),
                            Dumper.collectManifest(directory));
                })
                .isInstanceOf(NoSuchFileException.class)
                .hasMessage(directory.toString());
    }

    @Test
    void shouldGiveAClearErrorMessageIfTheArchivesParentDirectoryDoesntExist() {
        Path directory = testDirectory.directory("a-directory");
        Path archive = testDirectory.file("subdir").resolve("the-archive.dump");
        assertThatThrownBy(() -> {
                    Dumper dumper = new Dumper(filesystem);
                    dumper.dump(
                            FileOutput.of(filesystem, archive),
                            new DumpGzipFormatV1(),
                            Dumper.collectManifest(directory));
                })
                .isInstanceOf(NoSuchFileException.class)
                .hasMessage(archive.getParent().toString());
    }

    @Test
    void shouldGiveAClearErrorMessageIfTheArchivesParentDirectoryIsAFile() throws IOException {
        Path directory = testDirectory.directory("a-directory");
        Path archive = testDirectory.file("subdir").resolve("the-archive.dump");
        Files.write(archive.getParent(), EMPTY_BYTE_ARRAY);
        assertThatThrownBy(() -> {
                    Dumper dumper = new Dumper(filesystem);
                    dumper.dump(
                            FileOutput.of(filesystem, archive),
                            new DumpGzipFormatV1(),
                            Dumper.collectManifest(directory));
                })
                .isInstanceOf(FileSystemException.class)
                .hasMessage(archive.getParent() + ": Not a directory");
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    @DisabledForRoot
    void shouldGiveAClearErrorMessageIfTheArchivesParentDirectoryIsNotWritable() throws IOException {
        Path directory = testDirectory.directory("a-directory");
        Path archive = testDirectory.file("subdir").resolve("the-archive.dump");
        Files.createDirectories(archive.getParent());
        try (Closeable ignored = TestUtils.withPermissions(archive.getParent(), emptySet())) {
            assertThatThrownBy(() -> {
                        Dumper dumper = new Dumper(filesystem);
                        dumper.dump(
                                FileOutput.of(filesystem, archive),
                                new DumpGzipFormatV1(),
                                Dumper.collectManifest(directory));
                    })
                    .isInstanceOf(AccessDeniedException.class)
                    .hasMessage(archive.getParent().toString());
        }
    }

    @Test
    void shouldKeepOriginalFilesWhenDump() throws IOException {
        Path db = testDirectory.directory("a-directory");
        Path sub = db.resolve("subdir");
        Files.createDirectories(sub);

        Path storeFile = sub.resolve("store-file");
        byte[] data = new byte[] {0, 1, 2, 3, 4};
        Files.write(storeFile, data);

        Path archive = testDirectory.file("the-archive.dump");
        Dumper dumper = new Dumper(filesystem);
        dumper.dump(FileOutput.of(filesystem, archive), new DumpGzipFormatV1(), Dumper.collectManifest(db));

        // Source unchanged (no diffs)
        assertThat(Files.readAllBytes(storeFile)).isEqualTo(data);
    }

    @Test
    void shouldDeleteOriginalFilesWhenDumpWithDeleteAfterCopy() throws IOException {
        Path db = testDirectory.directory("a-directory");
        Path sub = db.resolve("subdir");
        Files.createDirectories(sub);

        Path storeFile = sub.resolve("store-file");
        byte[] data = new byte[] {0, 1, 2, 3, 4};
        Files.write(storeFile, data);

        Path archive = testDirectory.file("the-archive.dump");
        Dumper dumper = new Dumper(filesystem, NullLogProvider.getInstance(), true);
        dumper.dump(FileOutput.of(filesystem, archive), new DumpGzipFormatV1(), Dumper.collectManifest(db));

        // Source unchanged (no diffs)
        assertThat(Files.exists(storeFile)).isFalse();
    }
}
