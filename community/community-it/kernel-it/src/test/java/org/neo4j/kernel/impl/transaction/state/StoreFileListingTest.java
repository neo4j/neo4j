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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.Iterators.asResourceIterator;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.CommonDatabaseStores;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.store.StoreFileListing;
import org.neo4j.kernel.impl.store.StoreFileProvider;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StoreFileMetadata;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
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
        when(databaseLayout.pathForStore(eq(CommonDatabaseStores.METADATA))).thenReturn(mock(Path.class));
        LogFiles logFiles = mock(LogFiles.class);
        filesInStoreDirAre(databaseLayout, indexDir);
        StorageEngine storageEngine = mock(StorageEngine.class);
        StoreFileListing fileListing = new StoreFileListing(databaseLayout, logFiles, indexingService, storageEngine);

        ResourceIterator<Path> indexSnapshot =
                indexFilesAre(indexingService, new String[] {indexDir + "/mock/my.index"});

        ResourceIterator<StoreFileMetadata> result =
                fileListing.builder().excludeLogFiles().build();

        // When
        result.close();

        // Then
        verify(indexSnapshot).close();
    }

    @Test
    void shouldListMetaDataStoreLast() throws Exception {
        StoreFileMetadata fileMetadata = Iterators.last(database.listStoreFiles(false));
        assertEquals(fileMetadata.path(), database.getDatabaseLayout().pathForStore(CommonDatabaseStores.METADATA));
    }

    @Test
    void shouldListMetaDataStoreLastWithTxLogs() throws Exception {
        StoreFileMetadata fileMetadata = Iterators.last(database.listStoreFiles(true));
        assertEquals(fileMetadata.path(), database.getDatabaseLayout().pathForStore(CommonDatabaseStores.METADATA));
    }

    @Test
    void shouldListTransactionLogsFromCustomAbsoluteLocationWhenConfigured() throws IOException {
        Path customLogLocation = testDirectory.directory("customLogLocation");
        verifyLogFilesWithCustomPathListing(customLogLocation.toAbsolutePath());
    }

    @Test
    void shouldListTxLogFiles() throws Exception {
        try (var storeFiles = database.listStoreFiles(true)) {
            assertTrue(storeFiles.stream()
                    .map(metaData -> metaData.path().getFileName())
                    .anyMatch(DEFAULT_FILENAME_FILTER));
        }
    }

    @Test
    void shouldNotListTxLogFiles() throws Exception {
        try (var storeFiles = database.listStoreFiles(false)) {
            assertTrue(storeFiles.stream()
                    .map(metaData -> metaData.path().getFileName())
                    .noneMatch(DEFAULT_FILENAME_FILTER));
        }
    }

    @Test
    void shouldListStoreFiles() throws Exception {
        final var layout = database.getDatabaseLayout();
        final var statsPath = layout.pathForStore(CommonDatabaseStores.INDEX_STATISTICS);
        final var expectedFiles = new HashSet<>(layout.storeFiles());
        expectedFiles.remove(statsPath);
        // there was no rotation
        final var fileListingBuilder = database.getStoreFileListing().builder();
        fileListingBuilder.excludeIdFiles();
        fileListingBuilder.excludeLogFiles();
        fileListingBuilder.excludeSchemaIndexStoreFiles();
        try (var storeFiles = fileListingBuilder.build()) {
            final var listedStoreFiles =
                    storeFiles.stream().map(StoreFileMetadata::path).collect(Collectors.toSet());
            assertThat(listedStoreFiles).containsExactlyInAnyOrderElementsOf(expectedFiles);
        }
    }

    @Test
    void shouldListIdFiles() throws Exception {
        final var layout = database.getDatabaseLayout();
        final var expectedFiles = new HashSet<>(layout.idFiles());
        final var fileListingBuilder = database.getStoreFileListing().builder();
        fileListingBuilder.excludeAll();
        fileListingBuilder.includeIdFiles();
        try (var storeFiles = fileListingBuilder.build()) {
            final var listedIdFiles =
                    storeFiles.stream().map(StoreFileMetadata::path).collect(Collectors.toSet());
            assertThat(listedIdFiles).containsExactlyInAnyOrderElementsOf(expectedFiles);
        }
    }

    @Test
    void doNotListFilesFromAdditionalProviderThatRegisterTwice() throws IOException {
        StoreFileListing storeFileListing = database.getStoreFileListing();
        MarkerFileProvider provider = new MarkerFileProvider();
        storeFileListing.registerStoreFileProvider(provider);
        storeFileListing.registerStoreFileProvider(provider);
        try (ResourceIterator<StoreFileMetadata> storeFiles =
                storeFileListing.builder().build()) {
            assertEquals(
                    1,
                    storeFiles.stream()
                            .filter(metadata -> "marker"
                                    .equals(metadata.path().getFileName().toString()))
                            .count());
        }
    }

    private void verifyLogFilesWithCustomPathListing(Path path) throws IOException {
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder(
                        testDirectory.homePath("customDb"))
                .setConfig(GraphDatabaseSettings.transaction_logs_root_path, path)
                .build();
        GraphDatabaseAPI graphDatabase = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        Database database = graphDatabase.getDependencyResolver().resolveDependency(Database.class);
        LogFiles logFiles = graphDatabase.getDependencyResolver().resolveDependency(LogFiles.class);
        try (var storeFiles = database.listStoreFiles(true)) {
            assertTrue(storeFiles.stream()
                    .anyMatch(metadata -> metadata.isLogFile() && logFiles.isLogFile(metadata.path())));
        }
        assertEquals(
                path.getFileName().toString(),
                logFiles.logFilesDirectory().getParent().getFileName().toString());
        managementService.shutdown();
    }

    private static void filesInStoreDirAre(DatabaseLayout databaseLayout, String indexDir) {
        final String[] mockDbFiles = new String[] {
            "biff",
            "baff",
            "boff.log",
            "mockstore",
            "mockstore.id",
            "mockstore.counts.db",
            "mockstore.labeltokenstore.db",
            "mockstore.labeltokenstore.db.id",
            "mockstore.labeltokenstore.db.names",
            "mockstore.labeltokenstore.db.names.id",
            "mockstore.nodestore.db",
            "mockstore.nodestore.db.id",
            "mockstore.nodestore.db.labels",
            "mockstore.nodestore.db.labels.id",
            "mockstore.propertystore.db",
            "mockstore.propertystore.db.arrays",
            "mockstore.propertystore.db.arrays.id",
            "mockstore.propertystore.db.id",
            "mockstore.propertystore.db.index",
            "mockstore.propertystore.db.index.id",
            "mockstore.propertystore.db.index.keys",
            "mockstore.propertystore.db.index.keys.id",
            "mockstore.propertystore.db.strings",
            "mockstore.propertystore.db.strings.id",
            "mockstore.relationshipgroupstore.db",
            "mockstore.relationshipgroupstore.db.id",
            "mockstore.relationshipstore.db",
            "mockstore.relationshipstore.db.id",
            "mockstore.relationshiptypestore.db",
            "mockstore.relationshiptypestore.db.id",
            "mockstore.relationshiptypestore.db.names",
            "mockstore.relationshiptypestore.db.names.id",
            "mockstore.schemastore.db",
            "mockstore.schemastore.db.id",
            "mockstore.transaction.db.0",
            "mockstore.transaction.db.1",
            "mockstore.transaction.db.2",
            "mockstore_lock"
        };

        final String[] mockDirectories = new String[] {"foo", "bar", "baz", indexDir};

        List<Path> files = new ArrayList<>();
        mockFiles(mockDbFiles, files, false);
        mockFiles(mockDirectories, files, true);
        when(databaseLayout.listDatabaseFiles(any(), any())).thenReturn(files.toArray(new Path[0]));
    }

    private static ResourceIterator<Path> indexFilesAre(IndexingService indexingService, String[] fileNames)
            throws IOException {
        List<Path> files = new ArrayList<>();
        mockFiles(fileNames, files, false);
        ResourceIterator<Path> snapshot = spy(asResourceIterator(files.iterator()));
        when(indexingService.snapshotIndexFiles()).thenReturn(snapshot);
        return snapshot;
    }

    private void createIndexDbFile() throws IOException {
        DatabaseLayout databaseLayout = db.databaseLayout();
        final Path indexFile = databaseLayout.file("index.db");
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
        public Resource addFilesTo(Collection<StoreFileMetadata> fileMetadataCollection) {
            fileMetadataCollection.add(new StoreFileMetadata(Path.of("marker")));
            return Resource.EMPTY;
        }
    }
}
