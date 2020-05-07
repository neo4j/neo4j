/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.state;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.index.label.LabelScanStore;
import org.neo4j.internal.index.label.RelationshipTypeScanStore;
import org.neo4j.internal.index.label.RelationshipTypeScanStoreSettings;
import org.neo4j.internal.index.label.TokenScanStore;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StoreFileMetadata;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.Iterators.asResourceIterator;

@DbmsExtension
class DatabaseFileListingTest
{
    @Inject
    private GraphDatabaseAPI db;
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private Database database;
    private static final String[] STANDARD_STORE_DIR_FILES = new String[]{
            "index",
            "lock",
            "debug.log",
            "neostore",
            "neostore.id",
            "neostore.counts.db",
            "neostore.labelscanstore.db",
            "neostore.labeltokenstore.db",
            "neostore.labeltokenstore.db.id",
            "neostore.labeltokenstore.db.names",
            "neostore.labeltokenstore.db.names.id",
            "neostore.nodestore.db",
            "neostore.nodestore.db.id",
            "neostore.nodestore.db.labels",
            "neostore.nodestore.db.labels.id",
            "neostore.propertystore.db",
            "neostore.propertystore.db.arrays",
            "neostore.propertystore.db.arrays.id",
            "neostore.propertystore.db.id",
            "neostore.propertystore.db.index",
            "neostore.propertystore.db.index.id",
            "neostore.propertystore.db.index.keys",
            "neostore.propertystore.db.index.keys.id",
            "neostore.propertystore.db.strings",
            "neostore.propertystore.db.strings.id",
            "neostore.relationshipgroupstore.db",
            "neostore.relationshipgroupstore.db.id",
            "neostore.relationshipstore.db",
            "neostore.relationshipstore.db.id",
            "neostore.relationshiptypestore.db",
            "neostore.relationshiptypestore.db.id",
            "neostore.relationshiptypestore.db.names",
            "neostore.relationshiptypestore.db.names.id",
            "neostore.schemastore.db",
            "neostore.schemastore.db.id",
            "neostore.transaction.db.0",
            "neostore.transaction.db.1",
            "neostore.transaction.db.2",
            "store_lock"};

    private static final String[] STANDARD_STORE_DIR_DIRECTORIES = new String[]{"schema", "index", "branched"};

    @BeforeEach
    void setUp() throws IOException
    {
        createIndexDbFile();
    }

    @Test
    void shouldCloseIndexAndScanStoreSnapshots() throws Exception
    {
        // Given
        LabelScanStore labelScanStore = mock( LabelScanStore.class );
        RelationshipTypeScanStore relationshipTypeScanStore = mock( RelationshipTypeScanStore.class );
        IndexingService indexingService = mock( IndexingService.class );
        DatabaseLayout databaseLayout = mock( DatabaseLayout.class );
        when( databaseLayout.metadataStore() ).thenReturn( mock( File.class ) );
        LogFiles logFiles = mock( LogFiles.class );
        filesInStoreDirAre( databaseLayout, STANDARD_STORE_DIR_FILES, STANDARD_STORE_DIR_DIRECTORIES );
        StorageEngine storageEngine = mock( StorageEngine.class );
        IdGeneratorFactory idGeneratorFactory = mock( IdGeneratorFactory.class );
        DatabaseFileListing fileListing = new DatabaseFileListing( databaseLayout, logFiles, labelScanStore, relationshipTypeScanStore,
                indexingService, storageEngine, idGeneratorFactory );

        ResourceIterator<File> labelScanSnapshot = scanStoreFilesAre( labelScanStore,
                new String[]{"blah/scan.store", "scan.more"} );
        ResourceIterator<File> relationshipTypeScanSnapshot = scanStoreFilesAre( relationshipTypeScanStore,
                new String[]{"blah/scan.store", "scan.more"} );
        ResourceIterator<File> indexSnapshot = indexFilesAre( indexingService, new String[]{"schema/index/my.index"} );

        ResourceIterator<StoreFileMetadata> result = fileListing.builder().excludeLogFiles().build();

        // When
        result.close();

        // Then
        verify( labelScanSnapshot ).close();
        verify( relationshipTypeScanSnapshot ).close();
        verify( indexSnapshot ).close();
    }

    @Test
    void shouldListMetaDataStoreLast() throws Exception
    {
        StoreFileMetadata fileMetadata = Iterators.last( database.listStoreFiles( false ) );
        assertEquals( fileMetadata.file(), database.getDatabaseLayout().metadataStore() );
    }

    @Test
    void shouldListMetaDataStoreLastWithTxLogs() throws Exception
    {
        StoreFileMetadata fileMetadata = Iterators.last( database.listStoreFiles( true ) );
        assertEquals( fileMetadata.file(), database.getDatabaseLayout().metadataStore() );
    }

    @Test
    void shouldListTransactionLogsFromCustomAbsoluteLocationWhenConfigured() throws IOException
    {
        File customLogLocation = testDirectory.directory( "customLogLocation" );
        verifyLogFilesWithCustomPathListing( customLogLocation.toPath().toAbsolutePath() );
    }

    @Test
    void shouldListTxLogFiles() throws Exception
    {
        assertTrue( database.listStoreFiles( true ).stream()
                .map( metaData -> metaData.file().getName() )
                .anyMatch( fileName -> TransactionLogFiles.DEFAULT_FILENAME_FILTER.accept( null, fileName ) ) );
    }

    @Test
    void shouldNotListTxLogFiles() throws Exception
    {
        assertTrue( database.listStoreFiles( false ).stream()
                .map( metaData -> metaData.file().getName() )
                .noneMatch( fileName -> TransactionLogFiles.DEFAULT_FILENAME_FILTER.accept( null, fileName ) ) );
    }

    @Test
    void shouldListNeostoreFiles() throws Exception
    {
        DatabaseLayout layout = database.getDatabaseLayout();
        Set<File> expectedFiles = layout.storeFiles();
        if ( !Config.defaults().get( RelationshipTypeScanStoreSettings.enable_relationship_type_scan_store ) )
        {
            expectedFiles.removeIf( f -> DatabaseFile.RELATIONSHIP_TYPE_SCAN_STORE.getName().equals( f.getName() ) );
        }
        // there was no rotation
        ResourceIterator<StoreFileMetadata> storeFiles = database.listStoreFiles( false );
        Set<File> listedStoreFiles = storeFiles.stream()
                .map( StoreFileMetadata::file )
                .collect( Collectors.toSet() );
        assertEquals( expectedFiles, listedStoreFiles );
    }

    @Test
    void doNotListFilesFromAdditionalProviderThatRegisterTwice() throws IOException
    {
        DatabaseFileListing databaseFileListing = database.getDatabaseFileListing();
        MarkerFileProvider provider = new MarkerFileProvider();
        databaseFileListing.registerStoreFileProvider( provider );
        databaseFileListing.registerStoreFileProvider( provider );
        ResourceIterator<StoreFileMetadata> metadataResourceIterator = databaseFileListing.builder().build();
        assertEquals( 1, metadataResourceIterator.stream().filter( metadata -> "marker".equals( metadata.file().getName() ) ).count() );
    }

    private void verifyLogFilesWithCustomPathListing( Path path ) throws IOException
    {
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( testDirectory.homeDir( "customDb" ) )
                .setConfig( GraphDatabaseSettings.transaction_logs_root_path, path )
                .build();
        GraphDatabaseAPI graphDatabase = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        Database database = graphDatabase.getDependencyResolver().resolveDependency( Database.class );
        LogFiles logFiles = graphDatabase.getDependencyResolver().resolveDependency( LogFiles.class );
        assertTrue( database.listStoreFiles( true ).stream()
                .anyMatch( metadata -> metadata.isLogFile() && logFiles.isLogFile( metadata.file() ) ) );
        assertEquals( path.getFileName().toString(), logFiles.logFilesDirectory().getParentFile().getName() );
        managementService.shutdown();
    }

    private static void filesInStoreDirAre( DatabaseLayout databaseLayout, String[] filenames, String[] dirs )
    {
        List<File> files = new ArrayList<>();
        mockFiles( filenames, files, false );
        mockFiles( dirs, files, true );
        when( databaseLayout.listDatabaseFiles(any()) ).thenReturn( files.toArray( new File[0] ) );
    }

    private static ResourceIterator<File> scanStoreFilesAre( TokenScanStore labelScanStore, String[] fileNames )
    {
        List<File> files = new ArrayList<>();
        mockFiles( fileNames, files, false );
        ResourceIterator<File> snapshot = spy( asResourceIterator( files.iterator() ) );
        when( labelScanStore.snapshotStoreFiles() ).thenReturn( snapshot );
        return snapshot;
    }

    private static ResourceIterator<File> indexFilesAre( IndexingService indexingService, String[] fileNames )
            throws IOException
    {
        List<File> files = new ArrayList<>();
        mockFiles( fileNames, files, false );
        ResourceIterator<File> snapshot = spy( asResourceIterator( files.iterator() ) );
        when( indexingService.snapshotIndexFiles() ).thenReturn( snapshot );
        return snapshot;
    }

    private void createIndexDbFile() throws IOException
    {
        DatabaseLayout databaseLayout = db.databaseLayout();
        final File indexFile = databaseLayout.file( "index.db" );
        if ( !indexFile.exists() )
        {
            assertTrue( indexFile.createNewFile() );
        }
    }

    private static void mockFiles( String[] filenames, List<File> files, boolean isDirectories )
    {
        for ( String filename : filenames )
        {
            File file = mock( File.class );

            String[] fileNameParts = filename.split( "/" );
            when( file.getName() ).thenReturn( fileNameParts[fileNameParts.length - 1] );

            when( file.isFile() ).thenReturn( !isDirectories );
            when( file.isDirectory() ).thenReturn( isDirectories );
            when( file.exists() ).thenReturn( true );
            when( file.getPath() ).thenReturn( filename );
            files.add( file );
        }
    }

    private static class MarkerFileProvider implements DatabaseFileListing.StoreFileProvider
    {
        @Override
        public Resource addFilesTo( Collection<StoreFileMetadata> fileMetadataCollection )
        {
            fileMetadataCollection.add( new StoreFileMetadata( new File( "marker" ), 0 ) );
            return Resource.EMPTY;
        }
    }
}
