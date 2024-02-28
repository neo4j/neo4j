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

import static java.lang.Integer.parseInt;
import static java.util.Arrays.asList;
import static org.apache.commons.lang3.ArrayUtils.isEmpty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigBuilder;
import org.neo4j.kernel.api.impl.index.TestIndexWriterModes;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory.InMemoryDirectoryFactory;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class PartitionedIndexStorageTest {
    private static final InMemoryDirectoryFactory directoryFactory = new InMemoryDirectoryFactory();

    @Inject
    private DefaultFileSystemAbstraction fs;

    @Inject
    private TestDirectory testDir;

    private PartitionedIndexStorage storage;

    @BeforeEach
    void createIndexStorage() {
        storage = new PartitionedIndexStorage(directoryFactory, fs, testDir.homePath());
    }

    @Test
    void prepareFolderCreatesFolder() throws IOException {
        Path folder = createRandomFolder(testDir.homePath());

        storage.prepareFolder(folder);

        assertTrue(fs.fileExists(folder));
    }

    @Test
    void prepareFolderRemovesFromFileSystem() throws IOException {
        Path folder = createRandomFolder(testDir.homePath());
        createRandomFilesAndFolders(folder);

        storage.prepareFolder(folder);

        assertTrue(fs.fileExists(folder));
        assertTrue(isEmpty(fs.listFiles(folder)));
    }

    @Test
    void prepareFolderRemovesFromLucene() throws IOException {
        Path folder = createRandomFolder(testDir.homePath());
        Directory dir = createRandomLuceneDir(folder);

        assertFalse(isEmpty(dir.listAll()));

        storage.prepareFolder(folder);

        assertTrue(fs.fileExists(folder));
        assertTrue(isEmpty(dir.listAll()));
    }

    @Test
    void openIndexDirectoriesForEmptyIndex() throws IOException {
        storage.getIndexFolder();

        Map<Path, Directory> directories = storage.openIndexDirectories();

        assertTrue(directories.isEmpty());
    }

    @Test
    void openIndexDirectories() throws IOException {
        Path indexFolder = storage.getIndexFolder();
        createRandomLuceneDir(indexFolder).close();
        createRandomLuceneDir(indexFolder).close();

        Map<Path, Directory> directories = storage.openIndexDirectories();
        try {
            assertEquals(2, directories.size());
            for (Directory dir : directories.values()) {
                assertFalse(isEmpty(dir.listAll()));
            }
        } finally {
            IOUtils.closeAll(directories.values());
        }
    }

    @Test
    void listFoldersForEmptyFolder() throws IOException {
        Path indexFolder = storage.getIndexFolder();
        fs.mkdirs(indexFolder);

        List<Path> folders = storage.listFolders();

        assertTrue(folders.isEmpty());
    }

    @Test
    void listFolders() throws IOException {
        Path indexFolder = storage.getIndexFolder();
        fs.mkdirs(indexFolder);

        createRandomFile(indexFolder);
        createRandomFile(indexFolder);
        Path folder1 = createRandomFolder(indexFolder);
        Path folder2 = createRandomFolder(indexFolder);

        List<Path> folders = storage.listFolders();

        assertEquals(asSet(folder1, folder2), new HashSet<>(folders));
    }

    @Test
    void shouldListIndexPartitionsSorted() throws Exception {
        // GIVEN
        try (FileSystemAbstraction scramblingFs = new DefaultFileSystemAbstraction() {
            @Override
            public Path[] listFiles(Path directory) throws IOException {
                List<Path> files = asList(super.listFiles(directory));
                Collections.shuffle(files);
                return files.toArray(new Path[0]);
            }
        }) {
            PartitionedIndexStorage myStorage =
                    new PartitionedIndexStorage(directoryFactory, scramblingFs, testDir.homePath());
            Path parent = myStorage.getIndexFolder();
            int directoryCount = 10;
            for (int i = 0; i < directoryCount; i++) {
                scramblingFs.mkdirs(parent.resolve(String.valueOf(i + 1)));
            }

            // WHEN
            Map<Path, Directory> directories = myStorage.openIndexDirectories();

            // THEN
            assertEquals(directoryCount, directories.size());
            int previous = 0;
            for (Map.Entry<Path, Directory> directory : directories.entrySet()) {
                int current = parseInt(directory.getKey().getFileName().toString());
                assertTrue(
                        current > previous,
                        "Wanted directory " + current + " to have higher id than previous " + previous);
                previous = current;
            }
        }
    }

    private void createRandomFilesAndFolders(Path rootFolder) throws IOException {
        int count = ThreadLocalRandom.current().nextInt(10) + 1;
        for (int i = 0; i < count; i++) {
            if (ThreadLocalRandom.current().nextBoolean()) {
                createRandomFile(rootFolder);
            } else {
                createRandomFolder(rootFolder);
            }
        }
    }

    private Directory createRandomLuceneDir(Path rootFolder) throws IOException {
        Path folder = createRandomFolder(rootFolder);
        Directory directory = directoryFactory.open(folder);
        Config config = Config.defaults();
        IndexWriterConfig writerConfig = new IndexWriterConfigBuilder(TestIndexWriterModes.STANDARD, config).build();
        try (IndexWriter writer = new IndexWriter(directory, writerConfig)) {
            writer.addDocument(randomDocument());
            writer.commit();
        }
        return directory;
    }

    private void createRandomFile(Path rootFolder) throws IOException {
        Path file;
        do {
            file = rootFolder.resolve(RandomStringUtils.randomNumeric(5));
        } while (fs.fileExists(file));

        try (StoreChannel channel = fs.write(file);
                var scopedBuffer = new HeapScopedBuffer(100, ByteOrder.LITTLE_ENDIAN, EmptyMemoryTracker.INSTANCE)) {
            channel.writeAll(scopedBuffer.getBuffer());
        }
    }

    private Path createRandomFolder(Path rootFolder) throws IOException {
        Path folder;
        do {
            folder = rootFolder.resolve(RandomStringUtils.randomNumeric(5));
        } while (fs.fileExists(folder));

        fs.mkdirs(folder);
        return folder;
    }

    private static Document randomDocument() {
        Document doc = new Document();
        doc.add(new StringField("field", RandomStringUtils.randomNumeric(5), Field.Store.YES));
        return doc;
    }
}
