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
package org.neo4j.server.security.auth;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileSystemUtils;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.server.security.FormatException;
import org.neo4j.string.UTF8;

public abstract class FileRepositorySerializer<S> {
    private final SecureRandom random = new SecureRandom();

    public static void writeToFile(FileSystemAbstraction fs, Path path, byte[] bytes) throws IOException {
        try (OutputStream o = fs.openAsOutputStream(path, false)) {
            o.write(bytes);
        }
    }

    private static List<String> readFromFile(FileSystemAbstraction fs, Path path, MemoryTracker memoryTracker)
            throws IOException {
        return FileSystemUtils.readLines(fs, path, memoryTracker);
    }

    public void saveRecordsToFile(FileSystemAbstraction fileSystem, Path recordsFile, Collection<S> records)
            throws IOException {
        Path tempFile = getTempFile(fileSystem, recordsFile);

        try {
            writeToFile(fileSystem, tempFile, serialize(records));
            fileSystem.renameFile(tempFile, recordsFile, ATOMIC_MOVE, REPLACE_EXISTING);
        } catch (Throwable e) {
            fileSystem.deleteFile(tempFile);
            throw e;
        }
    }

    private Path getTempFile(FileSystemAbstraction fileSystem, Path recordsFile) throws IOException {
        Path directory = recordsFile.getParent();
        if (!fileSystem.fileExists(directory)) {
            fileSystem.mkdirs(directory);
        }

        long n = random.nextLong();
        n = (n == Long.MIN_VALUE) ? 0 : Math.abs(n);
        return directory.resolve(n + "_" + recordsFile.getFileName() + ".tmp");
    }

    public List<S> loadRecordsFromFile(FileSystemAbstraction fileSystem, Path recordsFile, MemoryTracker memoryTracker)
            throws IOException, FormatException {
        return deserializeRecords(readFromFile(fileSystem, recordsFile, memoryTracker));
    }

    public byte[] serialize(Collection<S> records) {
        StringBuilder sb = new StringBuilder();
        for (S record : records) {
            sb.append(serialize(record)).append("\n");
        }
        return UTF8.encode(sb.toString());
    }

    public List<S> deserializeRecords(byte[] bytes) throws FormatException {
        return deserializeRecords(Arrays.asList(UTF8.decode(bytes).split("\n")));
    }

    private List<S> deserializeRecords(List<String> lines) throws FormatException {
        List<S> out = new ArrayList<>();
        int lineNumber = 1;
        for (String line : lines) {
            if (!line.isBlank()) {
                out.add(deserializeRecord(line, lineNumber));
            }
            lineNumber++;
        }
        return out;
    }

    protected abstract String serialize(S record);

    protected abstract S deserializeRecord(String line, int lineNumber) throws FormatException;
}
