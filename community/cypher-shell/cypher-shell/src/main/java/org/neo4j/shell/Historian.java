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

import static java.lang.System.getProperty;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Collections;
import java.util.List;
import org.neo4j.shell.exception.CypherShellIOException;

/**
 * An object which keeps a record of past commands
 */
public interface Historian {
    Historian empty = new EmptyHistory();
    boolean isPosix = FileSystems.getDefault().supportedFileAttributeViews().contains("posix");

    /**
     * @return a list of all past commands in the history, in order of execution (first command sorted first).
     */
    List<String> getHistory();

    /**
     * Flush history to disk
     */
    void flushHistory() throws IOException;

    void clear() throws IOException;

    static Path defaultHistoryFile() {
        // Storing in same directory as driver uses
        var path = Path.of(getProperty("user.home"), ".neo4j", ".cypher_shell_history");
        return defaultHistoryFile(path);
    }

    static Path defaultHistoryFile(Path path) {
        try {
            var historyFile = safeExists(path) ? path : createFileAndDirectories(path);
            if (isPosix) {
                Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rw-------"));
            }
            return historyFile;
        } catch (IOException e) {
            throw new CypherShellIOException(e);
        }
    }

    private static boolean safeExists(Path path) throws IOException {
        if (Files.exists(path)) {
            if (Files.isDirectory(path)) {
                throw new CypherShellIOException("History file cannot be a directory, please delete " + path);
            } else {
                return true;
            }
        } else {
            return false;
        }
    }

    private static Path createFileAndDirectories(Path path) throws IOException {
        Files.createDirectories(path.getParent());
        try {
            return Files.createFile(path);
        } catch (FileAlreadyExistsException e) {
            return path;
        }
    }

    class EmptyHistory implements Historian {
        @Override
        public List<String> getHistory() {
            return Collections.emptyList();
        }

        @Override
        public void flushHistory() throws IOException {}

        @Override
        public void clear() throws IOException {}
    }
}
