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

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.file.Files;
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

    @Test
    void shouldFindLocalFilesByRegex() throws Exception {
        // GIVEN
        existenceOfFile("abc");
        existenceOfFile("bcd");

        // WHEN/THEN
        assertValid("abc");
        assertValid("bcd");
        assertValid("ab.");
        assertValid(".*bc");
        assertNotValid("abcd");
        assertNotValid(".*de.*");
    }

    @Test
    void shouldFindStoragePathsByRegex() throws Exception {
        // GIVEN
        existenceOfFile("abc");
        existenceOfFile("bcd");

        final var base = directory.homePath().toUri().toString();
        assert base.endsWith("/");

        // the file scheme resolver is always present so will be able to handle the file URIs below
        final var fs = new SchemeFileSystemAbstraction(directory.getFileSystem());

        // WHEN/THEN
        assertValid(fs, base + "abc");
        assertValid(fs, base + "bcd");
        assertValid(fs, base + "ab.");
        assertValid(fs, base + ".*bc");
        assertNotValid(fs, base + "abcd");
        assertNotValid(fs, base + ".*de.*");
    }

    private void assertNotValid(String string) {
        assertThrows(IllegalArgumentException.class, () -> validate(string));
    }

    private void assertValid(String fileByName) {
        validate(fileByName);
    }

    private void assertNotValid(FileSystemAbstraction fs, String string) {
        assertThrows(IllegalArgumentException.class, () -> validate(fs, string));
    }

    private void assertValid(FileSystemAbstraction fs, String fileByName) {
        validate(fs, fileByName);
    }

    private void validate(String fileByName) {
        final var home = directory.homePath();
        validate(directory.getFileSystem(), home + home.getFileSystem().getSeparator() + fileByName);
    }

    private void validate(FileSystemAbstraction fs, String fileByName) {
        Validators.matchingFiles(fs, fileByName);
    }

    private void existenceOfFile(String name) throws IOException {
        Files.createFile(directory.file(name));
    }
}
