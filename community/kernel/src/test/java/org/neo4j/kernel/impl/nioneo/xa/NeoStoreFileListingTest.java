/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.xa;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.transaction.xaframework.XaContainer;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.neo4j.helpers.collection.IteratorUtil.asResourceIterator;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.asUniqueSet;
import static org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog.getHistoryFileNamePattern;

public class NeoStoreFileListingTest
{
    private XaContainer xaContainer;
    private LabelScanStore labelScanStore;
    private IndexingService indexingService;
    private File storeDir;

    private final static String[] STANDARD_STORE_DIR_FILES = new String[]{
           "active_tx_log",
           "lock",
           "messages.log",
           "neostore",
           "neostore.id",
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
           "neostore.relationshipstore.db",
           "neostore.relationshipstore.db.id",
           "neostore.relationshiptypestore.db",
           "neostore.relationshiptypestore.db.id",
           "neostore.relationshiptypestore.db.names",
           "neostore.relationshiptypestore.db.names.id",
           "neostore.schemastore.db",
           "neostore.schemastore.db.id",
           "nioneo_logical.log.1",
           "nioneo_logical.log.active",
           "nioneo_logical.log.v0",
           "nioneo_logical.log.v1",
           "nioneo_logical.log.v2",
           "store_lock",
           "tm_tx_log.1"};

    private final static String[] STANDARD_STORE_DIR_DIRECTORIES = new String[]{ "schema", "index", "branched"};

    @Before
    public void setUp() throws IOException
    {
        xaContainer = mock( XaContainer.class );
        labelScanStore = mock( LabelScanStore.class );
        indexingService = mock( IndexingService.class );
        storeDir = mock( File.class );

        XaLogicalLog xaLogicalLog = mock( XaLogicalLog.class );
        when( xaLogicalLog.getHistoryFileNamePattern()).thenReturn( getHistoryFileNamePattern( "nioneo_logical.log" ) );
        when( xaContainer.getLogicalLog() ).thenReturn( xaLogicalLog );

        // Defaults, overridden in individual tests
        filesInStoreDirAre( new String[]{}, new String[]{} );
        scanStoreFilesAre( new String[]{} );
        indexFilesAre( new String[]{} );
    }

    @Test
    public void shouldOnlyListLogicalLogs() throws Exception
    {
        // Given
        filesInStoreDirAre( STANDARD_STORE_DIR_FILES, STANDARD_STORE_DIR_DIRECTORIES );
        NeoStoreFileListing fileListing = newFileListing();

        // When
        ResourceIterator<File> result = fileListing.listLogicalLogs();

        // Then
        assertThat( asSetOfPaths( result ), equalTo( asSet(
                "nioneo_logical.log.v0",
                "nioneo_logical.log.v1",
                "nioneo_logical.log.v2") ) );
    }

    @Test
    public void shouldOnlyListNeoStoreFiles() throws Exception
    {
        // Given
        filesInStoreDirAre( STANDARD_STORE_DIR_FILES, STANDARD_STORE_DIR_DIRECTORIES );
        NeoStoreFileListing fileListing = newFileListing();

        // When
        ResourceIterator<File> result = fileListing.listStoreFiles(  );

        // Then
        assertThat( asSetOfPaths( result ), equalTo( asSet(
                "neostore.labeltokenstore.db",
                "neostore.labeltokenstore.db.names",
                "neostore.nodestore.db",
                "neostore.nodestore.db.labels",
                "neostore.propertystore.db",
                "neostore.propertystore.db.arrays",
                "neostore.propertystore.db.index",
                "neostore.propertystore.db.index.keys",
                "neostore.propertystore.db.strings",
                "neostore.relationshipstore.db",
                "neostore.relationshiptypestore.db",
                "neostore.relationshiptypestore.db.names",
                "neostore.schemastore.db",
                "neostore" ) ) );
    }

    @Test
    public void shouldListNeoStoreFiles() throws Exception
    {
        // Given
        filesInStoreDirAre( STANDARD_STORE_DIR_FILES, STANDARD_STORE_DIR_DIRECTORIES );
        NeoStoreFileListing fileListing = newFileListing();

        // When
        ResourceIterator<File> result = fileListing.listStoreFiles( false );

        // Then
        assertThat( asSetOfPaths( result ), equalTo( asSet(
                "neostore.labeltokenstore.db",
                "neostore.labeltokenstore.db.names",
                "neostore.nodestore.db",
                "neostore.nodestore.db.labels",
                "neostore.propertystore.db",
                "neostore.propertystore.db.arrays",
                "neostore.propertystore.db.index",
                "neostore.propertystore.db.index.keys",
                "neostore.propertystore.db.strings",
                "neostore.relationshipstore.db",
                "neostore.relationshiptypestore.db",
                "neostore.relationshiptypestore.db.names",
                "neostore.schemastore.db",
                "neostore" ) ) );
    }

    @Test
    public void shouldListNeoStoreFilesAndLogicalLogs() throws Exception
    {
        // Given
        filesInStoreDirAre( STANDARD_STORE_DIR_FILES, STANDARD_STORE_DIR_DIRECTORIES );
        NeoStoreFileListing fileListing = newFileListing();

        // When
        ResourceIterator<File> result = fileListing.listStoreFiles( true );

        // Then
        assertThat( asSetOfPaths( result ), equalTo(asSet(
                "neostore.labeltokenstore.db",
                "neostore.labeltokenstore.db.names",
                "neostore.nodestore.db",
                "neostore.nodestore.db.labels",
                "neostore.propertystore.db",
                "neostore.propertystore.db.arrays",
                "neostore.propertystore.db.index",
                "neostore.propertystore.db.index.keys",
                "neostore.propertystore.db.strings",
                "neostore.relationshipstore.db",
                "neostore.relationshiptypestore.db",
                "neostore.relationshiptypestore.db.names",
                "neostore.schemastore.db",
                "neostore",
                "nioneo_logical.log.v0",
                "nioneo_logical.log.v1",
                "nioneo_logical.log.v2" )));
    }

    @Test
    public void shouldListLabelScanStoreAndSchemaIndexes() throws Exception
    {
        // Given
        filesInStoreDirAre( STANDARD_STORE_DIR_FILES, STANDARD_STORE_DIR_DIRECTORIES );
        scanStoreFilesAre( new String[]{"blah/scan.store", "scan.more"} );
        indexFilesAre( new String[]{"schema/index/my.index", "schema/index/their.index"} );
        NeoStoreFileListing fileListing = newFileListing();

        // When
        ResourceIterator<File> result = fileListing.listStoreFiles( false );

        // Then
        assertThat( asSetOfPaths( result ), equalTo(asSet(
                "blah/scan.store",
                "scan.more",
                "schema/index/my.index",
                "schema/index/their.index",
                "neostore.labeltokenstore.db",
                "neostore.labeltokenstore.db.names",
                "neostore.nodestore.db",
                "neostore.nodestore.db.labels",
                "neostore.propertystore.db",
                "neostore.propertystore.db.arrays",
                "neostore.propertystore.db.index",
                "neostore.propertystore.db.index.keys",
                "neostore.propertystore.db.strings",
                "neostore.relationshipstore.db",
                "neostore.relationshiptypestore.db",
                "neostore.relationshiptypestore.db.names",
                "neostore.schemastore.db",
                "neostore")));
    }

    @Test
    public void shouldCloseIndexAndLabelScanSnapshots() throws Exception
    {
        // Given
        filesInStoreDirAre( STANDARD_STORE_DIR_FILES, STANDARD_STORE_DIR_DIRECTORIES );
        ResourceIterator<File> scanSnapshot = scanStoreFilesAre( new String[]{"blah/scan.store", "scan.more"} );
        ResourceIterator<File> indexSnapshot = indexFilesAre( new String[]{"schema/index/my.index" } );
        NeoStoreFileListing fileListing = newFileListing();

        ResourceIterator<File> result = fileListing.listStoreFiles( false );

        // When
        result.close();

        // Then
        verify( scanSnapshot ).close();
        verify( indexSnapshot ).close();
    }

    private NeoStoreFileListing newFileListing()
    {
        return new NeoStoreFileListing( xaContainer, storeDir, labelScanStore, indexingService );
    }

    private Set<String> asSetOfPaths( ResourceIterator<File> result )
    {
        List<String> fnames = new ArrayList<>();
        while(result.hasNext())
        {
            fnames.add( result.next().getPath() );
        }
        return asUniqueSet( fnames );
    }

    private void filesInStoreDirAre( String[] filenames, String[] dirs )
    {
        ArrayList<File> files = new ArrayList<>();
        mockFiles( filenames, files, false );
        mockFiles( dirs, files, true );
        when(storeDir.listFiles()).thenReturn( files.toArray( new File[files.size()] ) );
    }

    private ResourceIterator<File> scanStoreFilesAre( String[] fileNames ) throws IOException
    {
        ArrayList<File> files = new ArrayList<>();
        mockFiles( fileNames, files, false );
        ResourceIterator<File> snapshot = spy( asResourceIterator( files.iterator() ) );
        when(labelScanStore.snapshotStoreFiles()).thenReturn( snapshot );
        return snapshot;
    }

    private ResourceIterator<File> indexFilesAre( String[] fileNames ) throws IOException
    {
        ArrayList<File> files = new ArrayList<>();
        mockFiles( fileNames, files, false );
        ResourceIterator<File> snapshot = spy(asResourceIterator( files.iterator() ));
        when(indexingService.snapshotStoreFiles()).thenReturn( snapshot );
        return snapshot;
    }

    private void mockFiles( String[] filenames, ArrayList<File> files, boolean isDirectories )
    {
        for ( String filename : filenames )
        {
            File file = mock( File.class );

            String[] fileNameParts = filename.split( "/" );
            when(file.getName()).thenReturn( fileNameParts[fileNameParts.length-1] );

            when(file.isFile()).thenReturn( !isDirectories );
            when(file.isDirectory()).thenReturn( isDirectories );
            when(file.exists()).thenReturn( true );
            when(file.getPath()).thenReturn( filename );
            files.add( file );
        }
    }

}
