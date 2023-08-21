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
package org.neo4j.shell;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.shell.Historian.defaultHistoryFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.neo4j.shell.exception.CypherShellIOException;

class HistorianTest {
    @Test
    void getHistory() {
        assertTrue(Historian.empty.getHistory().isEmpty());
    }

    @Test
    void shouldCreateHistoryFileIfDoesntExists() throws IOException {
        // given
        var tempDir = Files.createTempDirectory("temp-dir");
        Path notThere = Path.of(tempDir.toString(), "notThere");

        // when
        assertFalse(Files.exists(notThere));
        Path created = defaultHistoryFile(notThere);

        // then
        assertEquals("notThere", created.getFileName().toString());
        assertTrue(Files.exists(created));
        assertFalse(Files.isDirectory(created));
    }

    @Test
    void shouldNotCreateHistoryFileIfExists() throws IOException {
        var existing = Files.createTempFile("temp-file", null);
        assertTrue(Files.exists(existing));
        Path actual = defaultHistoryFile(existing);

        // then
        assertTrue(Files.exists(actual));
        assertFalse(Files.isDirectory(actual));
    }

    @Test
    void shouldFailIfFileIsDirectory() throws IOException {
        // given
        var tempDir = Files.createTempDirectory("temp-dir");

        // when
        assertTrue(Files.isDirectory(tempDir));

        // then
        var error = assertThrows(CypherShellIOException.class, () -> defaultHistoryFile(tempDir));
        assertEquals("History file cannot be a directory, please delete " + tempDir, error.getMessage());
    }
}
