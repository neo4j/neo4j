/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.com.storecopy;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FileMoveProviderTest
{
    private DefaultFileSystemAbstraction defaultFileSystemAbstraction = new DefaultFileSystemAbstraction();
    private EphemeralFileSystemAbstraction ephemeralFileSystemAbstraction = new EphemeralFileSystemAbstraction();

    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory( defaultFileSystemAbstraction );

    private FileMoveProvider subject;

    public PageCacheRule pageCacheRule = new PageCacheRule();
    private PageCache pageCache;
    private FileMoveActionInformer fileMoveActionInformer;

    @Before
    public void setup()
    {
        pageCache = pageCacheRule.getPageCache( ephemeralFileSystemAbstraction );
        fileMoveActionInformer = mock( FileMoveActionInformer.class );
        subject = new FileMoveProvider( pageCache, fileMoveActionInformer, defaultFileSystemAbstraction );
    }

    @Test
    public void moveSingleFiles() throws IOException
    {
        // given
        File sharedParent = testDirectory.cleanDirectory( "shared_parent" );
        File sourceParent = new File( sharedParent, "source" );
        assertTrue( sourceParent.mkdirs() );
        File sourceFile = new File( sourceParent, "file.txt" );
        assertTrue( sourceFile.createNewFile() );
        writeToFile( sourceFile, "Garbage data" );
        File targetParent = new File( sharedParent, "target" );
        assertTrue( targetParent.mkdirs() );
        File targetFile = new File( targetParent, "file.txt" );

        // when
        subject.traverseForMoving( sourceFile ).forEach( moveToDirectory( targetParent ) );

        // then
        assertEquals( "Garbage data", readFromFile( targetFile ) );
    }

    @Test
    public void singleDirectoriesAreNotMoved() throws IOException
    {
        // given
        File sharedParent = testDirectory.cleanDirectory( "shared_parent" );
        File sourceParent = new File( sharedParent, "source" );
        assertTrue( sourceParent.mkdirs() );
        File sourceDirectory = new File( sourceParent, "directory" );
        assertTrue( sourceDirectory.mkdirs() );

        // and
        File targetParent = new File( sharedParent, "target" );
        assertTrue( targetParent.mkdirs() );
        File targetDirectory = new File( targetParent, "directory" );
        assertFalse( targetDirectory.exists() );

        // when
        subject.traverseForMoving( sourceParent ).forEach( moveToDirectory( targetDirectory ) );

        // then
        assertTrue( sourceDirectory.exists() );
        assertFalse( targetDirectory.exists() );
    }

    @Test
    public void moveNestedFiles() throws IOException
    {
        // given
        File sharedParent = testDirectory.cleanDirectory( "shared_parent" );
        File sourceParent = new File( sharedParent, "source" );
        assertTrue( sourceParent.mkdirs() );
        File targetParent = new File( sharedParent, "target" );
        assertTrue( targetParent.mkdirs() );

        // and
        File dirA = new File( sourceParent, "A" );
        assertTrue( dirA.mkdirs() );
        File nestedFileOne = new File( dirA, "file.txt" );
        assertTrue( nestedFileOne.createNewFile() );
        File dirB = new File( sourceParent, "B" );
        assertTrue( dirB.mkdirs() );
        File nestedFileTwo = new File( dirB, "file.txt" );
        assertTrue( nestedFileTwo.createNewFile() );
        writeToFile( nestedFileOne, "This is the file contained in directory A" );
        writeToFile( nestedFileTwo, "This is the file contained in directory B" );

        // and
        File targetFileOne = new File( targetParent, "A/file.txt" );
        File targetFileTwo = new File( targetParent, "B/file.txt" );

        // when
        subject.traverseForMoving( sourceParent ).forEach( moveToDirectory( targetParent ) );

        // then
        assertEquals( "This is the file contained in directory A", readFromFile( targetFileOne ) );
        assertEquals( "This is the file contained in directory B", readFromFile( targetFileTwo ) );
    }

    @Test
    public void filesAreMovedViaPageCacheWhenNecessary() throws IOException
    {
        // given there is a file on the default file system
        File parentDirectory = testDirectory.cleanDirectory( "parent" );
        File aNormalFile = new File( parentDirectory, "aNormalFile.A" );
        assertTrue( aNormalFile.createNewFile() );

        // and we have an expected target directory
        File targetDirectory = testDirectory.cleanDirectory( "targetDirectory" );
        pageCache.getCachedFileSystem().mkdirs( targetDirectory );

        // and there is also a file on the block device
        File aPageCacheFile = new File( parentDirectory, "aBlockCopyFile.B" );
        pageCache.getCachedFileSystem().mkdirs( parentDirectory );
        StoreChannel storeChannel = pageCache.getCachedFileSystem().create( aPageCacheFile );
        storeChannel.write( ByteBuffer.allocate( 20 ).putChar( 'a' ).putChar( 'b' ) );

        // and some of these files are handled by the page cache
        when( fileMoveActionInformer.shouldBeManagedByPageCache( any() ) ).thenReturn( false );
        when( fileMoveActionInformer.shouldBeManagedByPageCache( eq( aPageCacheFile.getName() ) ) ).thenReturn( true );

        // when the files are copied to target location
        List<FileMoveAction> moveActions =
                subject.traverseForMoving( parentDirectory ).collect( Collectors.toList() );//.forEach( moveToDirectory( targetDirectory ) );
        moveActions.forEach( moveToDirectory( targetDirectory ) );

        // then some files are copied over the default file system
        File expectedNormalCopy = new File( targetDirectory, aNormalFile.getName() );
        assertTrue( expectedNormalCopy.exists() );

        // and correct files are copied over the page cache
        File expectedPageCacheCopy = new File( targetDirectory, aPageCacheFile.getName() );
        assertTrue( expectedPageCacheCopy.toString(), pageCache.getCachedFileSystem().fileExists( expectedPageCacheCopy ) );
    }

    @Test
    public void filesAreMovedBeforeDirectories() throws IOException
    {
        // given there is a file contained in a directory
        File parentDirectory = testDirectory.cleanDirectory( "parent" );
        File sourceDirectory = new File( parentDirectory, "source" );
        assertTrue( sourceDirectory.mkdirs() );
        File childFile = new File( sourceDirectory, "child" );
        assertTrue( childFile.createNewFile() );
        writeToFile( childFile, "Content" );

        // and we have an expected target directory
        File targetDirectory = new File( parentDirectory, "target" );
        assertTrue( targetDirectory.mkdirs() );

        // when
        subject.traverseForMoving( sourceDirectory ).forEach( moveToDirectory( targetDirectory ) );

        // then no exception due to files happening before empty target directory
    }

    private Consumer<FileMoveAction> moveToDirectory( File toDirectory )
    {
        return fileMoveAction ->
        {
            try
            {
                fileMoveAction.move( toDirectory );
            }
            catch ( Throwable throwable )
            {
                throw new AssertionError( throwable );
            }
        };
    }

    private String readFromFile( File input ) throws IOException
    {
        BufferedReader fileReader = new BufferedReader( new FileReader( input ) );
        StringBuilder stringBuilder = new StringBuilder();
        char[] data = new char[32];
        int read;
        while ( (read = fileReader.read( data )) != -1 )
        {
            stringBuilder.append( data, 0, read );
        }
        return stringBuilder.toString();
    }

    private void writeToFile( File output, String input ) throws IOException
    {
        try ( BufferedWriter bw = new BufferedWriter( new FileWriter( output ) ) )
        {
            bw.write( input );
            bw.close();
        }
    }
}
