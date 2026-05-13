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
import static org.apache.commons.lang3.RandomStringUtils.insecure;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.configuration.Config;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.io.memory.ScopedBuffer;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigBuilder;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigMode;
import org.neo4j.kernel.api.impl.index.lucene.LuceneContext;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDirectory;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDocument;
import org.neo4j.kernel.api.impl.index.lucene.LuceneIndexWriter;
import org.neo4j.kernel.api.impl.index.lucene.LuceneIndexWriterConfig;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class PartitionedIndexStorageTest {
    @Inject
    private DefaultFileSystemAbstraction fs;

    @Inject
    private TestDirectory testDir;

    private DirectoryFactory directoryFactory;

    @AfterEach
    void tearDown() throws Exception {
        directoryFactory.close();
    }

    @ParameterizedTest
    @EnumSource
    void prepareFolderCreatesFolder(LuceneContext luceneContext) throws IOException {
        Path folder = createRandomFolder(testDir.homePath());
        PartitionedIndexStorage storage = createIndexStorage(luceneContext);

        storage.prepareFolder(folder);

        assertTrue(fs.fileExists(folder));
    }

    @ParameterizedTest
    @EnumSource
    void prepareFolderRemovesFromFileSystem(LuceneContext luceneContext) throws IOException {
        Path folder = createRandomFolder(testDir.homePath());
        createRandomFilesAndFolders(folder);

        PartitionedIndexStorage storage = createIndexStorage(luceneContext);
        storage.prepareFolder(folder);

        assertTrue(fs.fileExists(folder));
        assertTrue(isEmpty(fs.listFiles(folder)));
    }

    @ParameterizedTest
    @EnumSource
    void prepareFolderRemovesFromLucene(LuceneContext luceneContext) throws IOException {
        PartitionedIndexStorage storage = createIndexStorage(luceneContext);
        Path folder = createRandomFolder(testDir.homePath());
        try (LuceneDirectory dir = createRandomLuceneDir(folder, luceneContext)) {

            assertFalse(isEmpty(dir.listAll()));

            storage.prepareFolder(folder);

            assertTrue(fs.fileExists(folder));
            assertTrue(isEmpty(dir.listAll()));
        }
    }

    @ParameterizedTest
    @EnumSource
    void openIndexDirectoriesForEmptyIndex(LuceneContext luceneContext) throws IOException {
        PartitionedIndexStorage storage = createIndexStorage(luceneContext);
        storage.getIndexFolder();

        Map<Path, LuceneDirectory> directories = storage.openIndexDirectories();

        assertTrue(directories.isEmpty());
    }

    @ParameterizedTest
    @EnumSource
    void openIndexDirectories(LuceneContext luceneContext) throws IOException {
        PartitionedIndexStorage storage = createIndexStorage(luceneContext);
        Path indexFolder = storage.getIndexFolder();
        createRandomLuceneDir(indexFolder, luceneContext).close();
        createRandomLuceneDir(indexFolder, luceneContext).close();

        Map<Path, LuceneDirectory> directories = storage.openIndexDirectories();
        try {
            assertEquals(2, directories.size());
            for (LuceneDirectory dir : directories.values()) {
                assertFalse(isEmpty(dir.listAll()));
            }
        } finally {
            IOUtils.closeAll(directories.values());
        }
    }

    @ParameterizedTest
    @EnumSource
    void listFoldersForEmptyFolder(LuceneContext luceneContext) throws IOException {
        PartitionedIndexStorage storage = createIndexStorage(luceneContext);
        Path indexFolder = storage.getIndexFolder();
        fs.mkdirs(indexFolder);

        List<Path> folders = storage.listFolders();

        assertTrue(folders.isEmpty());
    }

    @ParameterizedTest
    @EnumSource
    void listFolders(LuceneContext luceneContext) throws IOException {
        PartitionedIndexStorage storage = createIndexStorage(luceneContext);
        Path indexFolder = storage.getIndexFolder();
        fs.mkdirs(indexFolder);

        createRandomFile(indexFolder);
        createRandomFile(indexFolder);
        Path folder1 = createRandomFolder(indexFolder);
        Path folder2 = createRandomFolder(indexFolder);

        List<Path> folders = storage.listFolders();

        assertEquals(asSet(folder1, folder2), new HashSet<>(folders));
    }

    @ParameterizedTest
    @EnumSource
    void shouldListIndexPartitionsSorted(LuceneContext luceneContext) throws Exception {
        // GIVEN
        try (FileSystemAbstraction scramblingFs = new DefaultFileSystemAbstraction() {
            @Override
            public Path[] listFiles(Path directory) throws IOException {
                List<Path> files = asList(super.listFiles(directory));
                Collections.shuffle(files);
                return files.toArray(new Path[0]);
            }
        }) {
            directoryFactory = luceneContext.directoryFactory().newInMemoryDirectoryFactory(fs);
            PartitionedIndexStorage myStorage =
                    new PartitionedIndexStorage(luceneContext, directoryFactory, scramblingFs, testDir.homePath());
            Path parent = myStorage.getIndexFolder();
            int directoryCount = 10;
            for (int i = 0; i < directoryCount; i++) {
                scramblingFs.mkdirs(parent.resolve(String.valueOf(i + 1)));
            }

            // WHEN
            Map<Path, LuceneDirectory> directories = myStorage.openIndexDirectories();

            // THEN
            assertEquals(directoryCount, directories.size());
            int previous = 0;
            for (Map.Entry<Path, LuceneDirectory> directory : directories.entrySet()) {
                int current = parseInt(directory.getKey().getFileName().toString());
                assertTrue(
                        current > previous,
                        "Wanted directory " + current + " to have higher id than previous " + previous);
                previous = current;
            }
        }
    }

    private PartitionedIndexStorage createIndexStorage(LuceneContext luceneContext) {
        directoryFactory = luceneContext.directoryFactory().newInMemoryDirectoryFactory(fs);
        return new PartitionedIndexStorage(luceneContext, directoryFactory, fs, testDir.homePath());
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

    private LuceneDirectory createRandomLuceneDir(Path rootFolder, LuceneContext luceneContext) throws IOException {
        Path folder = createRandomFolder(rootFolder);
        LuceneDirectory directory = directoryFactory.open(folder);
        Config config = Config.defaults();
        LuceneIndexWriterConfig writerConfig = new IndexWriterConfigBuilder(IndexWriterConfigMode.TEXT, config).build();
        try (LuceneIndexWriter writer = directory.newWriter(writerConfig)) {
            writer.addDocument(randomDocument(luceneContext));
            writer.commit();
        }
        return directory;
    }

    private void createRandomFile(Path rootFolder) throws IOException {
        Path file;
        do {
            file = rootFolder.resolve(insecure().nextNumeric(5));
        } while (fs.fileExists(file));

        try (StoreChannel channel = fs.write(file);
                ScopedBuffer scopedBuffer =
                        new HeapScopedBuffer(100, ByteOrder.LITTLE_ENDIAN, EmptyMemoryTracker.INSTANCE)) {
            channel.writeAll(scopedBuffer.getBuffer());
        }
    }

    private Path createRandomFolder(Path rootFolder) throws IOException {
        Path folder;
        do {
            folder = rootFolder.resolve(insecure().nextNumeric(5));
        } while (fs.fileExists(folder));

        fs.mkdirs(folder);
        return folder;
    }

    private static LuceneDocument randomDocument(LuceneContext luceneContext) {
        LuceneDocument doc = luceneContext.documentsFactory().newDocument();
        doc.addStringField("field", insecure().nextNumeric(5), true);
        return doc;
    }
}
