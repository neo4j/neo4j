/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;
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
        subject = new FileMoveProvider( pageCache, fileMoveActionInformer );
    }

    @Test
    public void moveSingleFiles()
    {
        // given
        File sharedParent = testDirectory.directory( "shared_parent" );
        File sourceParent = createDirectory( new File( sharedParent, "source" ) );
        File sourceFile = createFile( new File( sourceParent, "file.txt" ) );
        writeToFile( sourceFile, "Garbage data" );
        File targetParent = createDirectory( new File( sharedParent, "target" ) );
        File targetFile = new File( targetParent, "file.txt" );

        // when
        subject.traverseGenerateMoveActions( sourceFile ).forEach( moveToDirectory( targetParent ) );

        // then
        assertEquals( "Garbage data", readFromFile( targetFile ) );
    }

    private interface RunnableThrowable
    {
        void run() throws Throwable;
    }

    private static Runnable runnableFromThrowable( RunnableThrowable runnableThrowable )
    {
        return () ->
        {
            try
            {
                runnableThrowable.run();
            }
            catch ( Throwable throwable )
            {
                throw new RuntimeException( throwable );
            }
        };
    }

    @Test
    public void singleDirectoriesAreNotMoved()
    {
        // given
        File sharedParent = testDirectory.directory( "shared_parent" );
        File sourceParent = createDirectory( new File( sharedParent, "source" ) );
        File sourceDirectory = createDirectory( new File( sourceParent, "directory" ) );

        // and
        File targetParent = createDirectory( new File( sharedParent, "target" ) );
        File targetDirectory = new File( targetParent, "directory" );
        assertFalse( targetDirectory.exists() );

        // when
        subject.traverseGenerateMoveActions( sourceParent ).forEach( moveToDirectory( targetDirectory ) );

        // then
        assertTrue( sourceDirectory.exists() );
        assertFalse( targetDirectory.exists() );
    }

    @Test
    public void moveNestedFiles()
    {
        // given
        File sharedParent = testDirectory.directory( "shared_parent" );
        File sourceParent = createDirectory( new File( sharedParent, "source" ) );
        File targetParent = createDirectory( new File( sharedParent, "target" ) );

        // and
        File nestedFileOne = createFile( new File( createDirectory( new File( sourceParent, "A" ) ), "file.txt" ) );
        File nestedFileTwo = createFile( new File( createDirectory( new File( sourceParent, "B" ) ), "file.txt" ) );
        writeToFile( nestedFileOne, "This is the file contained in directory A" );
        writeToFile( nestedFileTwo, "This is the file contained in directory B" );

        // and
        File targetFileOne = new File( targetParent, "A/file.txt" );
        File targetFileTwo = new File( targetParent, "B/file.txt" );

        // when
        subject.traverseGenerateMoveActions( sourceParent ).forEach( moveToDirectory( targetParent ) );

        // then
        assertEquals( "This is the file contained in directory A", readFromFile( targetFileOne ) );
        assertEquals( "This is the file contained in directory B", readFromFile( targetFileTwo ) );
    }

    @Test
    public void filesAreMovedViaPageCacheWhenNecessary() throws IOException
    {
        // given there is a file on the default file system
        File parentDirectory = createDirectory( testDirectory.directory( "parent" ) );
        File aNormalFile = createFile( new File( parentDirectory, "aNormalFile.A" ) );

        // and we have an expected target directory
        File targetDirectory = createDirectory( testDirectory.directory( "targetDirectory" ) );
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
                subject.traverseGenerateMoveActions( parentDirectory ).collect( Collectors.toList() );//.forEach( moveToDirectory( targetDirectory ) );
        moveActions.forEach( moveToDirectory( targetDirectory ) );

        // then some files are copied over the default file system
        File expectedNormalCopy = new File( targetDirectory, aNormalFile.getName() );
        assertTrue( expectedNormalCopy.exists() );

        // and correct files are copied over the page cache
        File expectedPageCacheCopy = new File( targetDirectory, aPageCacheFile.getName() );
        assertTrue( expectedPageCacheCopy.toString(), pageCache.getCachedFileSystem().fileExists( expectedPageCacheCopy ) );
    }

    @Test
    public void filesAreMovedBeforeDirectories() // TODO doesnt test anything maybe
    {
        // given there is a file contained in a directory
        File parentDirectory = createDirectory( testDirectory.directory( "parent" ) );
        File sourceDirectory = createDirectory( new File( parentDirectory, "source" ) );
        File childFile = createFile( new File( sourceDirectory, "child" ) );
        writeToFile( childFile, "Content" );

        // and we have an expected target directory
        File targetDirectory = createDirectory( new File( parentDirectory, "target" ) );

        // when
        subject.traverseGenerateMoveActions( sourceDirectory ).forEach( moveToDirectory( targetDirectory ) );

        // then no exception due to files happening before empty target directory
    }

    private Supplier<RuntimeException> failure()
    {
        return () -> new RuntimeException( "Fail" );
    }

    private List<File> safeList( File dir )
    {
        return Arrays.asList( Optional.ofNullable( dir ).map( File::listFiles ).orElse( new File[]{} ) );
    }

    private Consumer<FileMoveAction> moveToDirectory( File fileToMove )
    {
        return fileMoveAction -> runnableFromThrowable( () -> fileMoveAction.move( fileToMove ) ).run();
    }

    private String readFromFile( File input )
    {
        try
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
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private File createDirectory( File file )
    {
        runnableFromThrowable( file::mkdirs ).run();
        return file;
    }

    private File createFile( File file )
    {
        runnableFromThrowable( file::createNewFile ).run();
        return file;
    }

    private void writeToFile( File output, String input )
    {
        try
        {
            BufferedWriter bw = new BufferedWriter( new FileWriter( output ) );
            bw.write( input );
            bw.close();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
