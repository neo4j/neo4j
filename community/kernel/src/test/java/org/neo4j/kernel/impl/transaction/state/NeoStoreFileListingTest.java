/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.api.LegacyIndexProviderLookup;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.storemigration.StoreFile;
import org.neo4j.kernel.impl.storemigration.StoreFileType;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StoreFileMetadata;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterators.asResourceIterator;

public class NeoStoreFileListingTest
{
    @Rule
    public EmbeddedDatabaseRule db = new EmbeddedDatabaseRule( getClass() );
    private NeoStoreDataSource neoStoreDataSource;

    @Before
    public void setUp() throws IOException
    {
        neoStoreDataSource = db.getDependencyResolver().resolveDependency( NeoStoreDataSource.class );
    }

    private Set<String> expectedStoreFiles( boolean includeLogFiles )
    {
        Set<String> storeFileNames = new HashSet<>();
        for ( StoreType type : StoreType.values() )
        {
            if ( type.equals( StoreType.COUNTS ) )
            {
                // Skip
            }
            else
            {
                storeFileNames.add( type.getStoreFile().fileName( StoreFileType.STORE ) );
            }
        }
        if ( includeLogFiles )
        {
            storeFileNames.add( PhysicalLogFile.DEFAULT_NAME + ".0" );
        }
        return storeFileNames;
    }

    private List<String> countStoreFiles()
    {
        List<String> countStoreFiles = new ArrayList<>();
        countStoreFiles.add( StoreFile.COUNTS_STORE_LEFT.fileName( StoreFileType.STORE ) );
        countStoreFiles.add( StoreFile.COUNTS_STORE_RIGHT.fileName( StoreFileType.STORE ) );
        return countStoreFiles;
    }

    @Test
    public void shouldNotIncludeTransactionLogFile() throws Exception
    {
        final ResourceIterator<StoreFileMetadata> storeFiles = neoStoreDataSource.listStoreFiles( false );
        final Set<String> actual = asSetOfPaths( storeFiles );
        final Set<String> expectedStoreFiles = expectedStoreFiles( false );
        final List<String> countStoreFiles = countStoreFiles();
        assertTrue( actual.containsAll( expectedStoreFiles ) );
        assertFalse( Collections.disjoint( actual, countStoreFiles ) );
    }

    @Test
    public void shouldIncludeTransactionLogFile() throws Exception
    {
        final ResourceIterator<StoreFileMetadata> storeFiles = neoStoreDataSource.listStoreFiles( true );
        final Set<String> actual = asSetOfPaths( storeFiles );
        final Set<String> expectedStoreFiles = expectedStoreFiles( true );
        final List<String> countStoreFiles = countStoreFiles();
        assertTrue( actual.containsAll( expectedStoreFiles ) );
        assertFalse( Collections.disjoint( actual, countStoreFiles ) );
    }

    @Test
    public void shouldCloseIndexAndLabelScanSnapshots() throws Exception
    {
        // Given
        LabelScanStore labelScanStore = mock( LabelScanStore.class );
        IndexingService indexingService = mock( IndexingService.class );
        LegacyIndexProviderLookup legacyIndexes = mock( LegacyIndexProviderLookup.class );
        when( legacyIndexes.all() ).thenReturn( Collections.emptyList() );
        File storeDir = mock( File.class );
        StorageEngine storageEngine = mock( StorageEngine.class );
        NeoStoreFileListing fileListing = new NeoStoreFileListing(
                storeDir, labelScanStore, indexingService, legacyIndexes, storageEngine );

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

    private String diffSet( Set<String> actual, Set<String> expected )
    {
        Set<String> extra = new HashSet<>( actual );
        Set<String> missing = new HashSet<>( expected );
        extra.removeAll( expected );
        missing.removeAll( actual );
        return "Extra entries: " + extra + "\nMissing entries: " + missing;
    }

    private Set<String> asSetOfPaths( ResourceIterator<StoreFileMetadata> result )
    {
        List<String> fnames = new ArrayList<>();
        while ( result.hasNext() )
        {
            fnames.add( result.next().file().getName() );
        }
        return Iterables.asUniqueSet( fnames );
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
