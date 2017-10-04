/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.api.ExplicitIndexProviderLookup;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.storemigration.LogFiles;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StoreFileMetadata;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterators.asResourceIterator;

public class NeoStoreFileListingTest
{
    @Rule
    public EmbeddedDatabaseRule db = new EmbeddedDatabaseRule();
    private NeoStoreDataSource neoStoreDataSource;
    private static final String[] STANDARD_STORE_DIR_FILES = new String[]{
            "index",
            "lock",
            "debug.log",
            "neostore",
            "neostore.id",
            "neostore.counts.db.a",
            "neostore.counts.db.b",
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
            PhysicalLogFile.DEFAULT_NAME + ".active",
            PhysicalLogFile.DEFAULT_NAME + PhysicalLogFile.DEFAULT_VERSION_SUFFIX + "0",
            PhysicalLogFile.DEFAULT_NAME + PhysicalLogFile.DEFAULT_VERSION_SUFFIX + "1",
            PhysicalLogFile.DEFAULT_NAME + PhysicalLogFile.DEFAULT_VERSION_SUFFIX + "2",
            "store_lock"};

    private static final String[] STANDARD_STORE_DIR_DIRECTORIES = new String[]{"schema", "index", "branched"};
    private static final Function<StoreFileMetadata,Optional<StoreType>> toStoreType =
            fileMetaData -> StoreType.typeOf( fileMetaData.file().getName() );

    @Before
    public void setUp() throws IOException
    {
        createIndexDbFile();
        neoStoreDataSource = db.getDependencyResolver().resolveDependency( NeoStoreDataSource.class );
    }

    private void createIndexDbFile() throws IOException
    {
        File storeDir = db.getStoreDir();
        final File indexFile = new File( storeDir, "index.db" );
        if ( !indexFile.exists() )
        {
            assertTrue( indexFile.createNewFile() );
        }
    }

    @Test
    public void shouldCloseIndexAndLabelScanSnapshots() throws Exception
    {
        // Given
        LabelScanStore labelScanStore = mock( LabelScanStore.class );
        IndexingService indexingService = mock( IndexingService.class );
        ExplicitIndexProviderLookup explicitIndexes = mock( ExplicitIndexProviderLookup.class );
        when( explicitIndexes.all() ).thenReturn( Collections.emptyList() );
        File storeDir = mock( File.class );
        filesInStoreDirAre( storeDir, STANDARD_STORE_DIR_FILES, STANDARD_STORE_DIR_DIRECTORIES );
        StorageEngine storageEngine = mock( StorageEngine.class );
        NeoStoreFileListing fileListing = new NeoStoreFileListing(
                storeDir, labelScanStore, indexingService, explicitIndexes, storageEngine );

        ResourceIterator<File> scanSnapshot = scanStoreFilesAre( labelScanStore,
                new String[]{"blah/scan.store", "scan.more"} );
        ResourceIterator<File> indexSnapshot = indexFilesAre( indexingService, new String[]{"schema/index/my.index"} );

        ResourceIterator<StoreFileMetadata> result = fileListing.listStoreFiles( false );

        // When
        result.close();

        // Then
        verify( scanSnapshot ).close();
        verify( indexSnapshot ).close();
    }

    @Test
    public void shouldListMetaDataStoreLast() throws Exception
    {
        assertTrue( neoStoreDataSource.listStoreFiles( false ).stream()
                .reduce( ( a, b ) -> b )
                .map( toStoreType )
                .filter( Optional::isPresent )
                .map( Optional::get )
                .filter( StoreType.META_DATA::equals )
                .isPresent() );
    }

    @Test
    public void shouldListMetaDataStoreLastWithTxLogs() throws Exception
    {
        assertTrue( neoStoreDataSource.listStoreFiles( true ).stream()
                .reduce( ( a, b ) -> b )
                .map( toStoreType )
                .filter( Optional::isPresent )
                .map( Optional::get )
                .filter( StoreType.META_DATA::equals )
                .isPresent() );
    }

    @Test
    public void shouldListTxLogFiles() throws Exception
    {
        assertTrue( neoStoreDataSource.listStoreFiles( true ).stream()
                .map( metaData -> metaData.file().getName() )
                .anyMatch( fileName -> LogFiles.FILENAME_FILTER.accept( null, fileName ) ) );
    }

    @Test
    public void shouldNotListTxLogFiles() throws Exception
    {
        assertTrue( neoStoreDataSource.listStoreFiles( false ).stream()
                .map( metaData -> metaData.file().getName() )
                .noneMatch( fileName -> LogFiles.FILENAME_FILTER.accept( null, fileName ) ) );
    }

    @Test
    public void shouldListNeostoreFiles() throws Exception
    {
        StoreType[] values = StoreType.values();
        ResourceIterator<StoreFileMetadata> storeFiles =
                neoStoreDataSource.listStoreFiles( false );
        List<StoreType> listedStoreFiles = storeFiles.stream()
                .map( toStoreType )
                .filter( Optional::isPresent )
                .map( Optional::get )
                .collect( Collectors.toList() );
        assertEquals( Arrays.asList(values), listedStoreFiles );
    }

    private void filesInStoreDirAre( File storeDir, String[] filenames, String[] dirs )
    {
        ArrayList<File> files = new ArrayList<>();
        mockFiles( filenames, files, false );
        mockFiles( dirs, files, true );
        when( storeDir.listFiles() ).thenReturn( files.toArray( new File[files.size()] ) );
    }

    private ResourceIterator<File> scanStoreFilesAre( LabelScanStore labelScanStore, String[] fileNames )
            throws IOException
    {
        ArrayList<File> files = new ArrayList<>();
        mockFiles( fileNames, files, false );
        ResourceIterator<File> snapshot = spy( asResourceIterator( files.iterator() ) );
        when( labelScanStore.snapshotStoreFiles() ).thenReturn( snapshot );
        return snapshot;
    }

    private ResourceIterator<File> indexFilesAre( IndexingService indexingService, String[] fileNames )
            throws IOException
    {
        ArrayList<File> files = new ArrayList<>();
        mockFiles( fileNames, files, false );
        ResourceIterator<File> snapshot = spy( asResourceIterator( files.iterator() ) );
        when( indexingService.snapshotStoreFiles() ).thenReturn( snapshot );
        return snapshot;
    }

    private void mockFiles( String[] filenames, ArrayList<File> files, boolean isDirectories )
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
}
