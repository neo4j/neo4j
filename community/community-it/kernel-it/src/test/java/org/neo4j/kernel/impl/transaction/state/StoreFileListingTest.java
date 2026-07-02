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
package org.neo4j.kernel.impl.transaction.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.helpers.collection.Iterators.asResourceIterator;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.CommonDatabaseStores;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.impl.muninn.StoreFile;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.store.StoreFileListing;
import org.neo4j.kernel.impl.store.StoreFileProvider;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.utils.TestDirectory;

@DbmsExtension
class StoreFileListingTest {
    private final Predicate<Path> DEFAULT_FILENAME_FILTER =
            IOUtils.uncheckedPredicate(TransactionLogFiles.DEFAULT_FILENAME_FILTER::accept);

    @Inject
    private GraphDatabaseAPI db;

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private FileSystemAbstraction filesystem;

    @Inject
    private Database database;

    @BeforeEach
    void setUp() throws IOException {
        createIndexDbFile();
    }

    @Test
    void shouldCloseIndexSnapshots() throws Exception {
        // Given
        String indexDir = "indexes";
        IndexingService indexingService = mock(IndexingService.class);
        DatabaseLayout databaseLayout = mock(DatabaseLayout.class);
        StoreFile storeFile = mock(StoreFile.class, RETURNS_MOCKS);
        when(databaseLayout.pathForStore(eq(CommonDatabaseStores.METADATA))).thenReturn(storeFile);
        LogFiles logFiles = mock(LogFiles.class);
        StorageEngine storageEngine = mock(StorageEngine.class);
        StoreFileListing fileListing = new StoreFileListing(
                databaseLayout, testDirectory.getFileSystem(), logFiles, indexingService, storageEngine);

        ResourceIterator<Path> indexSnapshot = indexFilesAre(
                testDirectory.getFileSystem(), indexingService, new String[] {indexDir + "/mock/my.index"});

        ResourceIterator<Path> result = fileListing.builder().excludeLogFiles().build();

        // When
        result.close();

        // Then
        verify(indexSnapshot).close();
    }

    @Test
    void shouldListMetaDataStoreLast() throws Exception {
        Path fileMetadata = Iterators.last(database.listStoreFiles(false));
        assertEquals(
                fileMetadata,
                database.getDatabaseLayout()
                        .pathForStore(CommonDatabaseStores.METADATA)
                        .baseSegment());
    }

    @Test
    void shouldListMetaDataStoreLastWithTxLogs() throws Exception {
        Path fileMetadata = Iterators.last(database.listStoreFiles(true));
        assertEquals(
                fileMetadata,
                database.getDatabaseLayout()
                        .pathForStore(CommonDatabaseStores.METADATA)
                        .baseSegment());
    }

    @Test
    void shouldListTxLogFiles() throws Exception {
        try (var storeFiles = database.listStoreFiles(true)) {
            assertTrue(storeFiles.stream().map(Path::getFileName).anyMatch(DEFAULT_FILENAME_FILTER));
        }
    }

    @Test
    void shouldNotListTxLogFiles() throws Exception {
        try (var storeFiles = database.listStoreFiles(false)) {
            assertTrue(storeFiles.stream().map(Path::getFileName).noneMatch(DEFAULT_FILENAME_FILTER));
        }
    }

    @Test
    void shouldListStoreFiles() throws Exception {
        final var layout = database.getDatabaseLayout();
        // there was no rotation
        final var fileListingBuilder = database.getStoreFileListing().builder();
        fileListingBuilder.excludeIdFiles();
        fileListingBuilder.excludeLogFiles();
        fileListingBuilder.excludeSchemaIndexStoreFiles();

        try (var storeFiles = fileListingBuilder.build()) {
            Set<Path> files = new HashSet<>();
            while (storeFiles.hasNext()) {
                files.add(storeFiles.next());
            }
            assertThat(files).contains(layout.metadataStore().baseSegment());
            assertThat(files.size()).isGreaterThan(1);
        }
    }

    @Test
    void shouldListIdFiles() throws Exception {
        final var fileListingBuilder = database.getStoreFileListing().builder();
        fileListingBuilder.excludeAll();
        fileListingBuilder.includeIdFiles();
        try (var storeFiles = fileListingBuilder.build()) {
            assertThat(storeFiles).hasNext();
        }
    }

    @Test
    void doNotListFilesFromAdditionalProviderThatRegisterTwice() throws IOException {
        StoreFileListing storeFileListing = database.getStoreFileListing();
        MarkerFileProvider provider = new MarkerFileProvider();
        storeFileListing.registerStoreFileProvider(provider);
        storeFileListing.registerStoreFileProvider(provider);
        try (ResourceIterator<Path> storeFiles = storeFileListing.builder().build()) {
            assertEquals(
                    1,
                    storeFiles.stream()
                            .filter(metadata ->
                                    "marker".equals(metadata.getFileName().toString()))
                            .count());
        }
    }

    private static ResourceIterator<Path> indexFilesAre(
            FileSystemAbstraction fs, IndexingService indexingService, String[] fileNames) throws IOException {
        List<Path> files = new ArrayList<>();
        mockFiles(fileNames, files, false);
        ResourceIterator<Path> snapshot = spy(asResourceIterator(files.iterator()));
        when(indexingService.snapshotIndexFiles(fs)).thenReturn(snapshot);
        return snapshot;
    }

    private void createIndexDbFile() throws IOException {
        DatabaseLayout databaseLayout = db.databaseLayout();
        final Path indexFile = databaseLayout.file("index.db").baseSegment();
        filesystem.write(indexFile).close();
    }

    private static void mockFiles(String[] filenames, List<Path> files, boolean isDirectories) {
        for (String filename : filenames) {
            File file = mock(File.class);
            Path path = mock(Path.class);

            String[] fileNameParts = filename.split("/");
            when(file.getName()).thenReturn(fileNameParts[fileNameParts.length - 1]);

            when(file.isFile()).thenReturn(!isDirectories);
            when(file.isDirectory()).thenReturn(isDirectories);
            when(file.exists()).thenReturn(true);
            when(file.getPath()).thenReturn(filename);
            when(path.toFile()).thenReturn(file);
            files.add(path);
        }
    }

    private static class MarkerFileProvider implements StoreFileProvider {
        @Override
        public Resource addFilesTo(Collection<Path> fileMetadataCollection) {
            fileMetadataCollection.add(Path.of("marker"));
            return Resource.EMPTY;
        }
    }
}
