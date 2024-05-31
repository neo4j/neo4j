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
package org.neo4j.kernel.impl.transaction.log.files;

import static java.util.Objects.requireNonNull;
import static java.util.regex.Pattern.compile;
import static java.util.regex.Pattern.quote;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import org.neo4j.io.fs.FileSystemAbstraction;

public class TransactionLogFilesHelper {
    public static final String DEFAULT_NAME = "neostore.transaction.db";
    public static final String CHECKPOINT_FILE_PREFIX = "checkpoint";
    public static final DirectoryStream.Filter<Path> DEFAULT_FILENAME_FILTER =
            new LogicalLogFilenameFilter(quote(DEFAULT_NAME), quote(CHECKPOINT_FILE_PREFIX));
    public static final Predicate<String> DEFAULT_FILENAME_PREDICATE =
            file -> file.startsWith(DEFAULT_NAME) || file.startsWith(CHECKPOINT_FILE_PREFIX);

    private static final String VERSION_SUFFIX = ".";
    private static final String REGEX_VERSION_SUFFIX = "\\.";
    private static final Path[] EMPTY_FILES_ARRAY = {};

    private final Path logBaseName;
    private final FileSystemAbstraction fileSystem;
    private final Path logDirectory;
    private final DirectoryStream.Filter<Path> filenameFilter;

    public static TransactionLogFilesHelper forTransactions(FileSystemAbstraction fs, Path dir) {
        return new TransactionLogFilesHelper(fs, dir, DEFAULT_NAME);
    }

    public static TransactionLogFilesHelper forCheckpoints(FileSystemAbstraction fs, Path dir) {
        return new TransactionLogFilesHelper(fs, dir, CHECKPOINT_FILE_PREFIX);
    }

    private TransactionLogFilesHelper(FileSystemAbstraction fileSystem, Path directory, String name) {
        this.fileSystem = fileSystem;
        this.logDirectory = directory;
        this.logBaseName = directory.resolve(name);
        this.filenameFilter = new LogicalLogFilenameFilter(quote(name));
    }

    public Path getLogFileForVersion(long version) {
        return Path.of(logBaseName.toAbsolutePath() + VERSION_SUFFIX + version);
    }

    public static long getLogVersion(Path historyLogFile) {
        String historyLogFilename = historyLogFile.getFileName().toString();
        int index = historyLogFilename.lastIndexOf(VERSION_SUFFIX);
        if (index == -1) {
            throw new RuntimeException("Invalid log file '" + historyLogFilename + "'");
        }
        return Long.parseLong(historyLogFilename.substring(index + VERSION_SUFFIX.length()));
    }

    DirectoryStream.Filter<Path> getLogFilenameFilter() {
        return filenameFilter;
    }

    public Path[] getMatchedFiles() throws IOException {
        Path[] files = fileSystem.listFiles(logDirectory, getLogFilenameFilter());
        if (files.length == 0) {
            return EMPTY_FILES_ARRAY;
        }
        Arrays.sort(files, Comparator.comparingLong(TransactionLogFilesHelper::getLogVersion));
        return files;
    }

    public boolean isLogFile(Path path) {
        try {
            return filenameFilter.accept(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void accept(LogVersionVisitor visitor) throws IOException {
        for (Path file : getMatchedFiles()) {
            visitor.visit(file, getLogVersion(file));
        }
    }

    private static final class LogicalLogFilenameFilter implements DirectoryStream.Filter<Path> {
        private final Pattern[] patterns;

        LogicalLogFilenameFilter(String... logFileNameBase) {
            requireNonNull(logFileNameBase);
            patterns = Arrays.stream(logFileNameBase)
                    .map(name -> compile(name + REGEX_VERSION_SUFFIX + ".*"))
                    .toArray(Pattern[]::new);
        }

        @Override
        public boolean accept(Path entry) {
            for (Pattern pattern : patterns) {
                if (pattern.matcher(entry.getFileName().toString()).matches()) {
                    return true;
                }
            }
            return false;
        }
    }
}
