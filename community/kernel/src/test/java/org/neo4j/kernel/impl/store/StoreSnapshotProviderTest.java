/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.store;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.Database;
import org.neo4j.storageengine.api.StoreFileMetadata;
import org.neo4j.storageengine.api.StoreResource;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@TestDirectoryExtension
class StoreSnapshotProviderTest
{
    @Inject
    public TestDirectory testDirectory;

    private StoreSnapshotProvider storeSnapshotProvider;
    private DatabaseLayout databaseLayout;
    private StoreFileListing.Builder fileListingBuilder;

    @BeforeEach
    void setUp()
    {
        var dataSource = mock( Database.class );
        fileListingBuilder = mock( StoreFileListing.Builder.class, CALLS_REAL_METHODS );
        databaseLayout = DatabaseLayout.ofFlat( testDirectory.directory( "neo4j", "data", "databases" ) );
        when( dataSource.getDatabaseLayout() ).thenReturn( databaseLayout );
        var storeFileListing = mock( StoreFileListing.class );
        when( storeFileListing.builder() ).thenReturn( fileListingBuilder );
        when( dataSource.getStoreFileListing() ).thenReturn( storeFileListing );
        storeSnapshotProvider = new StoreSnapshotProvider( dataSource, testDirectory.getFileSystem() );
    }

    @Test
    void shouldHanldeEmptyListOfFilesForeEachType() throws Exception
    {
        setExpectedFiles( new StoreFileMetadata[0] );
        var prepareStoreCopyFiles = storeSnapshotProvider.prepareStoreCopySnapshot().get();
        var files = prepareStoreCopyFiles.recoverableFiles();
        var atomicFilesSnapshotLength = prepareStoreCopyFiles.unrecoverableFiles().count();
        assertEquals( 0, files.length );
        assertEquals( 0, atomicFilesSnapshotLength );
    }

    private void setExpectedFiles( StoreFileMetadata[] expectedFiles ) throws IOException
    {
        doAnswer( invocation -> Iterators.asResourceIterator( Iterators.iterator( expectedFiles ) ) ).when( fileListingBuilder ).build();
    }

    @Test
    void shouldReturnExpectedListOfFileNamesForEachType() throws Exception
    {
        // given
        var expectedFiles = new StoreFileMetadata[]{new StoreFileMetadata( databaseLayout.file( "a" ), 1 ),
                new StoreFileMetadata( databaseLayout.file( "b" ), 2 )};
        setExpectedFiles( expectedFiles );

        // when
        var prepareStoreCopyFiles = storeSnapshotProvider.prepareStoreCopySnapshot().get();
        var files = prepareStoreCopyFiles.recoverableFiles();
        var atomicFilesSnapshot = prepareStoreCopyFiles.unrecoverableFiles().toArray( StoreResource[]::new );

        // then
        var expectedFilesConverted = Arrays.stream( expectedFiles ).map( StoreFileMetadata::path ).toArray( Path[]::new );
        var expectedAtomicFilesConverted = Arrays.stream( expectedFiles ).map(
                f -> new StoreResource( f.path(), getRelativePath( f ), f.recordSize(), testDirectory.getFileSystem() ) ).toArray( StoreResource[]::new );
        assertArrayEquals( expectedFilesConverted, files );
        assertEquals( expectedAtomicFilesConverted.length, atomicFilesSnapshot.length );
        for ( var i = 0; i < expectedAtomicFilesConverted.length; i++ )
        {
            StoreResource expected = expectedAtomicFilesConverted[i];
            StoreResource actual = atomicFilesSnapshot[i];
            var relativePath = databaseLayout.databaseDirectory().relativize( actual.path() );
            assertEquals( expected.relativePath(), relativePath.toString() );
            assertEquals( expected.recordSize(), actual.recordSize() );
        }
    }

    private String getRelativePath( StoreFileMetadata f )
    {
        return databaseLayout.databaseDirectory().relativize( f.path() ).toString();
    }
}
