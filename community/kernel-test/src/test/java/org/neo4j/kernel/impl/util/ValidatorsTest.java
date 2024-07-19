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
package org.neo4j.kernel.impl.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.cloud.storage.SchemeFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class ValidatorsTest {
    @Inject
    private TestDirectory directory;

    @Inject
    private FileSystemAbstraction filesystem;

    @Test
    void shouldFindLocalFilesByRegex() throws Exception {
        // GIVEN
        final var abc = existenceOfFile("abc");
        final var bcd = existenceOfFile("bcd");
        final var qwer0 = existenceOfFile("qwer.0");
        final var qwer1 = existenceOfFile("qwer.1");
        final var qwer2 = existenceOfFile("qwer.2");
        final var qwer10 = existenceOfFile("qwer.10");

        // WHEN/THEN
        assertValid("abc", abc);
        assertValid("bcd", bcd);
        assertValid("ab.", abc);
        assertValid(".*bc", abc);
        assertValid(".*bc.*", abc, bcd);
        assertValid("qwer\\.\\d", qwer0, qwer1, qwer2);
        assertValid("qwer\\.\\p{Digit}", qwer0, qwer1, qwer2);
        assertValid("qwer\\.\\d+", qwer0, qwer1, qwer2, qwer10);
        assertValid("qwer\\.\\d{1,2}", qwer0, qwer1, qwer2, qwer10);

        assertNotValid("abcd");
        assertNotValid(".*de.*");
        assertNotValid("qwer\\.\\d{3,}");
    }

    @Test
    void shouldFindStoragePathsByRegex() throws Exception {
        // GIVEN
        final var abc = existenceOfFile("abc");
        final var bcd = existenceOfFile("bcd");
        final var qwer0 = existenceOfFile("qwer.0");
        final var qwer1 = existenceOfFile("qwer.1");
        final var qwer2 = existenceOfFile("qwer.2");
        final var qwer10 = existenceOfFile("qwer.10");

        final var base = directory.homePath().toUri().toString();
        assertThat(base).endsWith("/");

        // the file scheme resolver is always present so will be able to handle the file URIs below
        final var schemeFilesystem = new SchemeFileSystemAbstraction(filesystem);

        // WHEN/THEN
        assertValid(schemeFilesystem, base + "abc", abc);
        assertValid(schemeFilesystem, base + "bcd", bcd);
        assertValid(schemeFilesystem, base + "ab.", abc);
        assertValid(schemeFilesystem, base + ".*bc", abc);
        assertValid(schemeFilesystem, base + ".*bc.*", abc, bcd);
        assertValid(schemeFilesystem, base + "qwer\\.\\d", qwer0, qwer1, qwer2);
        assertValid(schemeFilesystem, base + "qwer\\.\\p{Digit}", qwer0, qwer1, qwer2);
        assertValid(schemeFilesystem, base + "qwer\\.\\d+", qwer0, qwer1, qwer2, qwer10);
        assertValid(schemeFilesystem, base + "qwer\\.\\d{1,2}", qwer0, qwer1, qwer2, qwer10);

        assertNotValid(schemeFilesystem, base + "abcd");
        assertNotValid(schemeFilesystem, base + ".*de.*");
        assertNotValid(schemeFilesystem, base + "qwer\\.\\d{3,}");
    }

    private void assertValid(String fileByName, Path... expected) {
        final var path = directory.homePath().resolve(fileByName);
        assertValid(filesystem, path.toString(), expected);
    }

    private static void assertValid(FileSystemAbstraction fs, String fileByName, Path... expected) {
        final var matching = validate(fs, fileByName);
        assertThat(matching).containsExactlyInAnyOrder(expected);
    }

    private void assertNotValid(String string) {
        assertNotValid(filesystem, string);
    }

    private static void assertNotValid(FileSystemAbstraction fs, String string) {
        assertThatThrownBy(() -> validate(fs, string)).isInstanceOf(IllegalArgumentException.class);
    }

    private static List<Path> validate(FileSystemAbstraction fs, String fileByName) {
        return Validators.matchingFiles(fs, fileByName);
    }

    private Path existenceOfFile(String name) throws IOException {
        return Files.createFile(directory.file(name));
    }
}
