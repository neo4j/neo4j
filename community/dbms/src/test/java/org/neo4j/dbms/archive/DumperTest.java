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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.dbms.archive.StandardCompressionFormat.GZIP;

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
import org.neo4j.function.Predicates;
import org.neo4j.io.fs.FileSystemAbstraction;
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
        FileAlreadyExistsException exception = assertThrows(FileAlreadyExistsException.class, () -> {
            Dumper dumper = new Dumper(filesystem);
            dumper.dump(directory, directory, dumper.openForDump(archive), GZIP, Predicates.alwaysFalse());
        });
        assertEquals(archive.toString(), exception.getMessage());
    }

    @Test
    void shouldGiveAClearErrorMessageIfTheDirectoryDoesntExist() {
        Path directory = testDirectory.file("a-directory");
        Path archive = testDirectory.file("the-archive.dump");
        NoSuchFileException exception = assertThrows(NoSuchFileException.class, () -> {
            Dumper dumper = new Dumper(filesystem);
            dumper.dump(directory, directory, dumper.openForDump(archive), GZIP, Predicates.alwaysFalse());
        });
        assertEquals(directory.toString(), exception.getMessage());
    }

    @Test
    void shouldGiveAClearErrorMessageIfTheArchivesParentDirectoryDoesntExist() {
        Path directory = testDirectory.directory("a-directory");
        Path archive = testDirectory.file("subdir").resolve("the-archive.dump");
        NoSuchFileException exception = assertThrows(NoSuchFileException.class, () -> {
            Dumper dumper = new Dumper(filesystem);
            dumper.dump(directory, directory, dumper.openForDump(archive), GZIP, Predicates.alwaysFalse());
        });
        assertEquals(archive.getParent().toString(), exception.getMessage());
    }

    @Test
    void shouldGiveAClearErrorMessageIfTheArchivesParentDirectoryIsAFile() throws IOException {
        Path directory = testDirectory.directory("a-directory");
        Path archive = testDirectory.file("subdir").resolve("the-archive.dump");
        Files.write(archive.getParent(), EMPTY_BYTE_ARRAY);
        FileSystemException exception = assertThrows(FileSystemException.class, () -> {
            Dumper dumper = new Dumper(filesystem);
            dumper.dump(directory, directory, dumper.openForDump(archive), GZIP, Predicates.alwaysFalse());
        });
        assertEquals(archive.getParent() + ": Not a directory", exception.getMessage());
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    @DisabledForRoot
    void shouldGiveAClearErrorMessageIfTheArchivesParentDirectoryIsNotWritable() throws IOException {
        Path directory = testDirectory.directory("a-directory");
        Path archive = testDirectory.file("subdir").resolve("the-archive.dump");
        Files.createDirectories(archive.getParent());
        try (Closeable ignored = TestUtils.withPermissions(archive.getParent(), emptySet())) {
            AccessDeniedException exception = assertThrows(AccessDeniedException.class, () -> {
                Dumper dumper = new Dumper(filesystem);
                dumper.dump(directory, directory, dumper.openForDump(archive), GZIP, Predicates.alwaysFalse());
            });
            assertEquals(archive.getParent().toString(), exception.getMessage());
        }
    }
}
