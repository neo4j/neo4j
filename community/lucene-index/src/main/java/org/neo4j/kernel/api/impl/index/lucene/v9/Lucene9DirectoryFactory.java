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
package org.neo4j.kernel.api.impl.index.lucene.v9;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.lucene.LuceneContext;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDirectory;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDirectoryFactory;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.shaded.lucene9.store.ByteBuffersDirectory;
import org.neo4j.shaded.lucene9.store.FSDirectory;
import org.neo4j.shaded.lucene9.store.IOContext;
import org.neo4j.shaded.lucene9.store.IndexOutput;
import org.neo4j.shaded.lucene9.store.NIOFSDirectory;
import org.neo4j.shaded.lucene9.store.NRTCachingDirectory;

public class Lucene9DirectoryFactory implements LuceneDirectoryFactory {
    public static final LuceneDirectoryFactory INSTANCE = new Lucene9DirectoryFactory();

    @Override
    public LuceneDirectory openPersistent(Path dir) throws IOException {
        Files.createDirectories(dir);
        FSDirectory directory = USE_DEFAULT_DIRECTORY_FACTORY ? FSDirectory.open(dir) : new NIOFSDirectory(dir);
        NRTCachingDirectory nrtCachingDirectory = new NRTCachingDirectory(directory, MAX_MERGE_SIZE_MB, MAX_CACHED_MB);
        return new Lucene9Directory(nrtCachingDirectory);
    }

    @Override
    public LuceneDirectory inMemoryDirectory() {
        return new Lucene9Directory(new ByteBuffersDirectory());
    }

    @Override
    public DirectoryFactory newInMemoryDirectoryFactory() {
        return newInMemoryDirectoryFactory(null);
    }

    @Override
    public DirectoryFactory newInMemoryDirectoryFactory(FileSystemAbstraction fs) {
        return new Lucene9InMemoryDirectoryFactory(fs);
    }

    private static final class Lucene9InMemoryDirectoryFactory implements DirectoryFactory {
        private final Map<Path, Lucene9Directory> directories = new HashMap<>();
        private final FileSystemAbstraction fs;

        public Lucene9InMemoryDirectoryFactory(FileSystemAbstraction fs) {
            this.fs = fs;
        }

        @Override
        public synchronized LuceneDirectory open(Path dir) {
            Lucene9Directory directory = directories.get(dir);
            if (directory == null) {
                directory = openFromFs(dir);
                directories.put(dir, directory);
            }
            return new LuceneDirectory.DelegatingLuceneDirectory(directories.get(dir)) {
                @Override
                public void close() {
                    // Don't close to allow sharing, the factory will close it later.
                }
            };
        }

        @Override
        public LuceneContext getContext() {
            return LuceneContext.LUCENE_9;
        }

        private Lucene9Directory openFromFs(Path dir) {
            ByteBuffersDirectory directory = new ByteBuffersDirectory();
            if (fs != null) {
                try {
                    if (fs.fileExists(dir)) {
                        if (!fs.isDirectory(dir)) {
                            throw new RuntimeException("File " + dir + " existed, but was not a directory");
                        }
                        // Load the state of the directory from the time it was closed
                        for (Path file : fs.listFiles(dir)) {
                            try (InputStream in = fs.openAsInputStream(file);
                                    IndexOutput out = directory.createOutput(
                                            file.getFileName().toString(), IOContext.DEFAULT)) {
                                int length = in.available();
                                byte[] bytes = new byte[length];
                                int bytesRead = in.read(bytes, 0, length);
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
            return new Lucene9Directory(directory);
        }

        @Override
        public synchronized void close() throws IOException {
            try {
                // Store the directories in the provided file system (supposedly ephemeral)
                if (fs != null) {
                    for (Entry<Path, Lucene9Directory> entry : directories.entrySet()) {
                        Path directoryPath = entry.getKey();
                        fs.deleteRecursively(directoryPath);
                        fs.mkdirs(directoryPath);
                        Lucene9Directory directory = entry.getValue();
                        for (String name : directory.listAll()) {
                            Path filePath = directoryPath.resolve(name);
                            try (OutputStream out = fs.openAsOutputStream(filePath, false)) {
                                byte[] bytes = directory.readFile(name);
                                out.write(bytes, 0, bytes.length);
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
}
