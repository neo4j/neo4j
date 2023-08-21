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
package org.neo4j.io.fs;

import static org.neo4j.io.IOUtils.uncheckedFunction;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Stream;

public final class StreamFilesRecursive {
    private StreamFilesRecursive() {
        // This is a helper class, do not instantiate it.
    }

    /**
     * Static implementation of {@link FileSystemAbstraction#streamFilesRecursive(Path,boolean)} that does not require
     * any external state, other than what is presented through the given {@link FileSystemAbstraction}.
     * <p>
     * Return a stream of {@link FileHandle file handles} for every file within the given directory, and its
     * subdirectories.
     * <p>
     * Alternatively, if the {@link Path} given as an argument refers to a file instead of a directory, then a stream
     * will be returned with a file handle for just that file.
     * <p>
     * The stream is based on a snapshot of the file tree, so changes made to the tree using the returned file handles
     * will not be reflected in the stream.
     * <p>
     * If a file handle ends up leaving a directory empty through a rename or a delete, then the empty directory will
     * automatically be deleted as well. Likewise, if a file is moved to a path where not all the directories in the
     * path exists, then those missing directories will be created prior to the file rename.
     *
     * @param fs The {@link FileSystemAbstraction} to use for manipulating files.
     * @param path The base directory to start streaming files from, or the specific individual file to stream.
     * @param includeDirectories {@code true} to include directories in the {@link Stream}, {@code false} otherwise.
     * @return A {@link Stream} of {@link FileHandle}s
     */
    public static Stream<FileHandle> streamFilesRecursive(
            FileSystemAbstraction fs, Path path, boolean includeDirectories) throws IOException {
        final var canonicalizedDirectory = path.toAbsolutePath().normalize();
        // We grab a snapshot of the file tree to avoid seeing the same file twice or more due to renames.
        final var snapshot =
                streamFilesRecursiveInner(fs, canonicalizedDirectory, includeDirectories).toList().stream();
        return snapshot.map(f -> new WrappingFileHandle(f, canonicalizedDirectory, fs));
    }

    private static Stream<Path> streamFilesRecursiveInner(
            FileSystemAbstraction fs, Path path, boolean includeDirectories) throws IOException {
        if (!fs.fileExists(path)) {
            return Stream.empty();
        }
        if (!fs.isDirectory(path)) {
            return Stream.of(path);
        }

        final var recurse = Arrays.stream(fs.listFiles(path))
                .flatMap(uncheckedFunction(f -> streamFilesRecursiveInner(fs, f, includeDirectories)));
        return includeDirectories ? Stream.concat(Stream.of(path), recurse) : recurse;
    }
}
