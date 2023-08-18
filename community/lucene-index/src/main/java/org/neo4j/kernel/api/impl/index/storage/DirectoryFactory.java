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
package org.neo4j.kernel.api.impl.index.storage;

import static java.lang.Math.toIntExact;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.NRTCachingDirectory;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.util.FeatureToggles;

public interface DirectoryFactory extends AutoCloseable {
    static DirectoryFactory directoryFactory(FileSystemAbstraction fs) {
        return fs.isPersistent() ? DirectoryFactory.PERSISTENT : new DirectoryFactory.InMemoryDirectoryFactory(fs);
    }

    Directory open(Path dir) throws IOException;

    DirectoryFactory PERSISTENT = new DirectoryFactory() {
        private final int MAX_MERGE_SIZE_MB = FeatureToggles.getInteger(DirectoryFactory.class, "max_merge_size_mb", 5);
        private final int MAX_CACHED_MB = FeatureToggles.getInteger(DirectoryFactory.class, "max_cached_mb", 50);
        private final boolean USE_DEFAULT_DIRECTORY_FACTORY =
                FeatureToggles.flag(DirectoryFactory.class, "default_directory_factory", true);

        @SuppressWarnings("ResultOfMethodCallIgnored")
        @Override
        public Directory open(Path dir) throws IOException {
            Files.createDirectories(dir);
            FSDirectory directory = USE_DEFAULT_DIRECTORY_FACTORY ? FSDirectory.open(dir) : new NIOFSDirectory(dir);
            return new NRTCachingDirectory(directory, MAX_MERGE_SIZE_MB, MAX_CACHED_MB);
        }

        @Override
        public void close() {
            // No resources to release. This method only exists as a hook for test implementations.
        }
    };

    final class InMemoryDirectoryFactory implements DirectoryFactory {
        private final Map<Path, Directory> directories = new HashMap<>();
        private final FileSystemAbstraction fs;

        public InMemoryDirectoryFactory() {
            this(null);
        }

        public InMemoryDirectoryFactory(FileSystemAbstraction fs) {
            this.fs = fs;
        }

        @Override
        public synchronized Directory open(Path dir) {
            if (!directories.containsKey(dir)) {
                directories.put(dir, openFromFs(dir));
            }
            return new UncloseableDirectory(directories.get(dir));
        }

        private ByteBuffersDirectory openFromFs(Path dir) {
            var directory = new ByteBuffersDirectory();
            if (fs != null) {
                try {
                    if (fs.fileExists(dir)) {
                        if (!fs.isDirectory(dir)) {
                            throw new RuntimeException("File " + dir + " existed, but was not a directory");
                        }
                        // Load the state of the directory from the time it was closed
                        for (var file : fs.listFiles(dir)) {
                            try (var in = fs.openAsInputStream(file);
                                    var out = directory.createOutput(
                                            file.getFileName().toString(), new IOContext())) {
                                var length = in.available();
                                var bytes = new byte[length];
                                var bytesRead = in.read(bytes, 0, length);
                                if (bytesRead < length) {
                                    throw new RuntimeException("Couldn't read it all " + bytesRead + " < " + length);
                                }
                                out.writeBytes(bytes, 0, length);
                            }
                        }
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            return directory;
        }

        @Override
        public synchronized void close() throws IOException {
            try {
                // Store the directories in the provided file system (supposedly ephemeral)
                if (fs != null) {
                    for (var entry : directories.entrySet()) {
                        var directoryPath = entry.getKey();
                        fs.deleteRecursively(directoryPath);
                        fs.mkdirs(directoryPath);
                        var directory = entry.getValue();
                        for (var name : directory.listAll()) {
                            var filePath = directoryPath.resolve(name);
                            try (var out = fs.openAsOutputStream(filePath, false);
                                    var in = directory.openInput(name, new IOContext())) {
                                var length = toIntExact(in.length());
                                var bytes = new byte[length];
                                in.readBytes(bytes, 0, length);
                                out.write(bytes, 0, length);
                            }
                        }
                    }
                }
            } finally {
                IOUtils.closeAll(directories.values());
                directories.clear();
            }
        }
    }

    final class Single implements DirectoryFactory {
        private final Directory directory;

        public Single(Directory directory) {
            this.directory = directory;
        }

        @Override
        public Directory open(Path dir) {
            return directory;
        }

        @Override
        public void close() {}
    }

    final class UncloseableDirectory extends FilterDirectory {

        public UncloseableDirectory(Directory delegate) {
            super(delegate);
        }

        @Override
        public void close() {
            // No-op
        }
    }
}
