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
package org.neo4j.kernel.impl.index.schema;

import static org.neo4j.internal.helpers.collection.Iterators.asResourceIterator;
import static org.neo4j.internal.helpers.collection.Iterators.iterator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.io.compress.ZipUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;

/**
 * Surface for a schema indexes to act on the files that it owns.
 * Wraps all {@link IOException IOExceptions} in {@link UncheckedIOException}.
 */
public class IndexFiles {
    public static final String INDEX_FILE_PREFIX = "index-";

    private final FileSystemAbstraction fs;
    private final Path directory;
    private final Path storeFile;

    public IndexFiles(FileSystemAbstraction fs, IndexDirectoryStructure directoryStructure, long indexId) {
        this.fs = fs;
        this.directory = directoryStructure.directoryForIndex(indexId);
        this.storeFile = directory.resolve(indexFileName(indexId));
    }

    public Path getStoreFile() {
        return storeFile;
    }

    public Path getBase() {
        return directory;
    }

    public void clear() {
        try {
            if (fs.fileExists(directory)) {
                fs.deleteRecursively(directory);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void archiveIndex() throws IOException {
        if (fs.isDirectory(directory) && fs.fileExists(directory) && fs.listFiles(directory).length > 0) {
            ZipUtils.zip(
                    fs,
                    directory,
                    directory
                            .getParent()
                            .resolve("archive-" + directory.getFileName() + "-" + System.currentTimeMillis() + ".zip"));
        }
    }

    public void ensureDirectoryExist() {
        try {
            fs.mkdirs(directory);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String toString() {
        return String.format("%s[base=%s,storeFile=%s]", getClass().getSimpleName(), getBase(), getStoreFile());
    }

    private static String indexFileName(long indexId) {
        return INDEX_FILE_PREFIX + indexId;
    }

    public ResourceIterator<Path> snapshot() {
        return asResourceIterator(iterator(getStoreFile()));
    }
}
