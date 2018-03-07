/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.transaction.state.NeoStoreFileIndexListing;
import org.neo4j.kernel.impl.transaction.state.NeoStoreFileListing;
import org.neo4j.storageengine.api.StoreFileMetadata;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
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
    private PageCache pageCache;
    private NeoStoreFileIndexListing indexListingMock;
    private File storeDir;
    private NeoStoreFileListing.StoreFileListingBuilder fileListingBuilder;

    @Before
    public void setUp()
    {
        pageCache = StandalonePageCacheFactory.createPageCache( fileSystemAbstraction );
        NeoStoreDataSource dataSource = mock( NeoStoreDataSource.class );
        fileListingBuilder = mock( NeoStoreFileListing.StoreFileListingBuilder.class, CALLS_REAL_METHODS );
        storeDir = testDirectory.graphDbDir();
        when( dataSource.getStoreDir() ).thenReturn( storeDir );
        indexListingMock = mock( NeoStoreFileIndexListing.class );
        when( indexListingMock.getIndexIds() ).thenReturn( Primitive.longSet() );
        NeoStoreFileListing storeFileListing = mock( NeoStoreFileListing.class );
        when( storeFileListing.getNeoStoreFileIndexListing() ).thenReturn( indexListingMock );
        when( storeFileListing.builder() ).thenReturn( fileListingBuilder );
        when( dataSource.getNeoStoreFileListing() ).thenReturn( storeFileListing );
        prepareStoreCopyFiles = new PrepareStoreCopyFiles( dataSource, pageCache, fileSystemAbstraction );
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
        StoreFileMetadata[] expectedFiles =
                new StoreFileMetadata[]{new StoreFileMetadata( new File( storeDir, "a" ), 1 ), new StoreFileMetadata( new File( storeDir, "b" ), 2 )};
        setExpectedFiles( expectedFiles );

        //when
        File[] files = prepareStoreCopyFiles.listReplayableFiles();
        StoreResource[] atomicFilesSnapshot = prepareStoreCopyFiles.getAtomicFilesSnapshot();

        //then
        File[] expectedFilesConverted = Arrays.stream( expectedFiles ).map( StoreFileMetadata::file ).toArray( File[]::new );
        StoreResource[] exeptedAtomicFilesConverted = Arrays.stream( expectedFiles ).map(
                f -> new StoreResource( f.file(), getRelativePath( f ), f.recordSize(), pageCache, fileSystemAbstraction ) ).toArray( StoreResource[]::new );
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
        PrimitiveLongSet indexIds = prepareStoreCopyFiles.getIndexIds();

        assertEquals( 0, indexIds.size() );
    }

    @Test
    public void shouldReturnExpectedDescriptors()
    {
        PrimitiveLongSet expectedIndexIds = Primitive.longSet();
        expectedIndexIds.add( 42 );
        when( indexListingMock.getIndexIds() ).thenReturn( expectedIndexIds );

        PrimitiveLongSet actualIndexIndexIds = prepareStoreCopyFiles.getIndexIds();

        assertEquals( expectedIndexIds, actualIndexIndexIds );
    }

    private String getRelativePath( StoreFileMetadata f )
    {
        try
        {
            return relativePath( storeDir, f.file() );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Failed to create relative path" );
        }
    }
}
