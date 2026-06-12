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
package org.neo4j.io.fs.filename;

import static java.util.Objects.requireNonNull;
import static java.util.regex.Pattern.compile;
import static java.util.regex.Pattern.quote;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.regex.Pattern;
import org.neo4j.io.fs.FileSystemAbstraction;

/// Deals with file names for the sequentially incrementing file names, e.g.
///
///   - transactions.0
///   - transactions.1
///   - ...
///   - transactions.20
///   - ...
///
/// where the suffix represents the version, which is a strictly monotonic sequence.
public class SequentialFileNameHelper {
    private static final String VERSION_SUFFIX = ".";
    private static final String REGEX_VERSION_SUFFIX = "\\.";
    private static final Path[] EMPTY_FILES_ARRAY = {};

    private final Path baseName;
    private final Path directory;
    private final DirectoryStream.Filter<Path> filenameFilter;

    public SequentialFileNameHelper(Path directory, String baseName) {
        this.directory = directory;
        this.baseName = directory.resolve(baseName);
        this.filenameFilter = new SequentialFileNameFilter(quote(baseName));
    }

    public Path getFileForVersion(long version) {
        return Path.of(baseName.toAbsolutePath() + VERSION_SUFFIX + version);
    }

    public Path directory() {
        return this.directory;
    }

    public static long getVersion(Path path) {
        String filename = path.getFileName().toString();
        int index = filename.lastIndexOf(VERSION_SUFFIX);
        if (index == -1) {
            throw new IllegalArgumentException("Invalid sequential file '" + filename + "'");
        }
        try {
            long version = Long.parseLong(filename.substring(index + VERSION_SUFFIX.length()));
            if (version < 0) {
                throw new IllegalArgumentException("Negative version suffix for file '" + filename + "'");
            }
            return version;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("The file '" + filename + "' does not have a valid version suffix", e);
        }
    }

    public Path[] getFiles(FileSystemAbstraction fs) throws IOException {
        Path[] files = fs.listFiles(directory, filenameFilter);
        if (files.length == 0) {
            return EMPTY_FILES_ARRAY;
        }
        Arrays.sort(files, Comparator.comparingLong(SequentialFileNameHelper::getVersion));
        return files;
    }

    public boolean isSequentialFile(Path path) {
        try {
            return filenameFilter.accept(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static final class SequentialFileNameFilter implements DirectoryStream.Filter<Path> {
        private final Pattern[] patterns;

        public SequentialFileNameFilter(String... logFileNameBase) {
            requireNonNull(logFileNameBase);
            patterns = Arrays.stream(logFileNameBase)
                    .map(name -> compile("^" + name + REGEX_VERSION_SUFFIX + "(0|[1-9]\\d*)$"))
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
