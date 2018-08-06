/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.catchup.storecopy;

import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.transaction.state.NeoStoreFileIndexListing;
import org.neo4j.kernel.impl.transaction.state.NeoStoreFileListing;
import org.neo4j.storageengine.api.StoreFileMetadata;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.io.fs.FileUtils.relativePath;

public class PrepareStoreCopyFilesTest
{
    private final FileSystemAbstraction fileSystemAbstraction = new DefaultFileSystemAbstraction();

    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory( fileSystemAbstraction );
    private PrepareStoreCopyFiles prepareStoreCopyFiles;
    private NeoStoreFileIndexListing indexListingMock;
    private DatabaseLayout databaseLayout;
    private NeoStoreFileListing.StoreFileListingBuilder fileListingBuilder;

    @Before
    public void setUp()
    {
        NeoStoreDataSource dataSource = mock( NeoStoreDataSource.class );
        fileListingBuilder = mock( NeoStoreFileListing.StoreFileListingBuilder.class, CALLS_REAL_METHODS );
        databaseLayout = testDirectory.databaseLayout();
        when( dataSource.getDatabaseLayout() ).thenReturn( databaseLayout );
        indexListingMock = mock( NeoStoreFileIndexListing.class );
        when( indexListingMock.getIndexIds() ).thenReturn( new LongHashSet() );
        NeoStoreFileListing storeFileListing = mock( NeoStoreFileListing.class );
        when( storeFileListing.getNeoStoreFileIndexListing() ).thenReturn( indexListingMock );
        when( storeFileListing.builder() ).thenReturn( fileListingBuilder );
        when( dataSource.getNeoStoreFileListing() ).thenReturn( storeFileListing );
        prepareStoreCopyFiles = new PrepareStoreCopyFiles( dataSource, fileSystemAbstraction );
    }

    @Test
    public void shouldHanldeEmptyListOfFilesForeEachType() throws Exception
    {
        setExpectedFiles( new StoreFileMetadata[0] );
        File[] files = prepareStoreCopyFiles.listReplayableFiles();
        StoreResource[] atomicFilesSnapshot = prepareStoreCopyFiles.getAtomicFilesSnapshot();
        assertEquals( 0, files.length );
        assertEquals( 0, atomicFilesSnapshot.length );
    }

    private void setExpectedFiles( StoreFileMetadata[] expectedFiles ) throws IOException
    {
        doAnswer( invocation -> Iterators.asResourceIterator( Iterators.iterator( expectedFiles ) ) ).when( fileListingBuilder ).build();
    }

    @Test
    public void shouldReturnExpectedListOfFileNamesForEachType() throws Exception
    {
        // given
        StoreFileMetadata[] expectedFiles = new StoreFileMetadata[]{new StoreFileMetadata( databaseLayout.file( "a" ), 1 ),
                new StoreFileMetadata( databaseLayout.file( "b" ), 2 )};
        setExpectedFiles( expectedFiles );

        //when
        File[] files = prepareStoreCopyFiles.listReplayableFiles();
        StoreResource[] atomicFilesSnapshot = prepareStoreCopyFiles.getAtomicFilesSnapshot();

        //then
        File[] expectedFilesConverted = Arrays.stream( expectedFiles ).map( StoreFileMetadata::file ).toArray( File[]::new );
        StoreResource[] exeptedAtomicFilesConverted = Arrays.stream( expectedFiles ).map(
                f -> new StoreResource( f.file(), getRelativePath( f ), f.recordSize(), fileSystemAbstraction ) ).toArray( StoreResource[]::new );
        assertArrayEquals( expectedFilesConverted, files );
        assertEquals( exeptedAtomicFilesConverted.length, atomicFilesSnapshot.length );
        for ( int i = 0; i < exeptedAtomicFilesConverted.length; i++ )
        {
            StoreResource expected = exeptedAtomicFilesConverted[i];
            StoreResource storeResource = atomicFilesSnapshot[i];
            assertEquals( expected.path(), storeResource.path() );
            assertEquals( expected.recordSize(), storeResource.recordSize() );
        }
    }

    @Test
    public void shouldHandleEmptyDescriptors()
    {
        LongSet indexIds = prepareStoreCopyFiles.getNonAtomicIndexIds();

        assertEquals( 0, indexIds.size() );
    }

    @Test
    public void shouldReturnEmptySetOfIdsAndIgnoreIndexListing()
    {
        LongSet expectedIndexIds = LongSets.immutable.of( 42 );
        when( indexListingMock.getIndexIds() ).thenReturn( expectedIndexIds );

        LongSet actualIndexIndexIds = prepareStoreCopyFiles.getNonAtomicIndexIds();

        assertTrue( actualIndexIndexIds.isEmpty() );
    }

    private String getRelativePath( StoreFileMetadata f )
    {
        try
        {
            return relativePath( databaseLayout.databaseDirectory(), f.file() );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Failed to create relative path" );
        }
    }
}
