/*
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
package org.neo4j.io.pagecache.impl;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.graphdb.mockfs.DelegatingFileSystemAbstraction;
import org.neo4j.graphdb.mockfs.DelegatingStoreChannel;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.fs.StoreFileChannel;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.PageSwapperTest;
import org.neo4j.test.TargetDirectory;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.neo4j.test.ByteArrayMatcher.byteArray;

public class SingleFilePageSwapperTest extends PageSwapperTest
{
    @Rule
    public final TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );

    private final File file = new File( "file" );
    private EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction();

    @After
    public void tearDown()
    {
        fs.shutdown();
    }

    protected PageSwapperFactory swapperFactory()
    {
        SingleFilePageSwapperFactory factory = new SingleFilePageSwapperFactory();
        factory.setFileSystemAbstraction( fs );
        return factory;
    }

    @Test
    public void swappingInMustFillPageWithData() throws IOException
    {
        byte[] bytes = new byte[] { 1, 2, 3, 4 };
        StoreChannel channel = fs.create( file );
        channel.writeAll( wrap( bytes ) );
        channel.close();

        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapper = factory.createPageSwapper( file, 4, null, false );
        ByteBuffer target = ByteBuffer.allocateDirect( 4 );
        ByteBufferPage page = new ByteBufferPage( target );
        swapper.read( 0, page );

        assertThat( array( target ), byteArray( bytes ) );
    }

    @Test
    public void mustZeroFillPageBeyondEndOfFile() throws IOException
    {
        byte[] bytes = new byte[] {
                // --- page 0:
                1, 2, 3, 4,
                // --- page 1:
                5, 6
        };
        StoreChannel channel = fs.create( file );
        channel.writeAll( wrap( bytes ) );
        channel.close();

        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapper = factory.createPageSwapper( file, 4, null, false );
        ByteBuffer target = ByteBuffer.allocateDirect( 4 );
        ByteBufferPage page = new ByteBufferPage( target );
        swapper.read( 1, page );

        assertThat( array( target ), byteArray( new byte[]{5, 6, 0, 0} ) );
    }

    @Test
    public void swappingOutMustWritePageToFile() throws IOException
    {
        fs.create( file ).close();

        byte[] expected = new byte[] { 1, 2, 3, 4 };
        ByteBufferPage page = new ByteBufferPage( wrap( expected ) );

        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapper = factory.createPageSwapper( file, 4, null, false );
        swapper.write( 0, page );

        InputStream stream = fs.openAsInputStream( file );
        byte[] actual = new byte[expected.length];

        assertThat( stream.read( actual ), is( actual.length ) );
        assertThat( actual, byteArray( expected ) );
    }

    @Test
    public void swappingOutMustNotOverwriteDataBeyondPage() throws IOException
    {
        byte[] initialData = new byte[] {
                // --- page 0:
                1, 2, 3, 4,
                // --- page 1:
                5, 6, 7, 8,
                // --- page 2:
                9, 10
        };
        byte[] finalData = new byte[] {
                // --- page 0:
                1, 2, 3, 4,
                // --- page 1:
                8, 7, 6, 5,
                // --- page 2:
                9, 10
        };
        StoreChannel channel = fs.create( file );
        channel.writeAll( wrap( initialData ) );
        channel.close();

        byte[] change = new byte[] { 8, 7, 6, 5 };
        ByteBufferPage page = new ByteBufferPage( wrap( change ) );

        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapper = factory.createPageSwapper( file, 4, null, false );
        swapper.write( 1, page );

        InputStream stream = fs.openAsInputStream( file );
        byte[] actual = new byte[(int) fs.getFileSize( file )];

        assertThat( stream.read( actual ), is( actual.length ) );
        assertThat( actual, byteArray( finalData ) );
    }

    /**
     * The OverlappingFileLockException is thrown when tryLock is called on the same file *in the same JVM*.
     * @throws Exception
     */
    @Test( expected = OverlappingFileLockException.class )
    public void creatingSwapperForFileMustTakeLockOnFile() throws Exception
    {
        assumeFalse( FileUtils.OS_IS_WINDOWS ); // no file locking on Windows.

        PageSwapperFactory factory = swapperFactory();
        DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        factory.setFileSystemAbstraction( fs );
        File file = testDir.file( "file" );
        fs.create( file ).close();

        PageSwapper pageSwapper = factory.createPageSwapper( file, 4, NO_CALLBACK, false );

        try
        {
            StoreChannel channel = fs.open( file, "rw" );
            assertThat( channel.tryLock(), is( nullValue() ) );
        }
        finally
        {
            pageSwapper.close();
        }
    }

    @Test( expected = FileLockException.class )
    public void creatingSwapperForInternallyLockedFileMustThrow() throws Exception
    {
        assumeFalse( FileUtils.OS_IS_WINDOWS ); // no file locking on Windows.

        PageSwapperFactory factory = swapperFactory();
        DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        factory.setFileSystemAbstraction( fs );
        File file = testDir.file( "file" );

        StoreFileChannel channel = fs.create( file );

        try ( FileLock fileLock = channel.tryLock() )
        {
            assertThat( fileLock, is( not( nullValue() ) ) );
            factory.createPageSwapper( file, 4, NO_CALLBACK, true );
        }
    }

    @Test( expected = FileLockException.class )
    public void creatingSwapperForExternallyLockedFileMustThrow() throws Exception
    {
        assumeFalse( FileUtils.OS_IS_WINDOWS ); // no file locking on Windows.

        PageSwapperFactory factory = swapperFactory();
        DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        factory.setFileSystemAbstraction( fs );
        File file = testDir.file( "file" );

        fs.create( file ).close();

        ProcessBuilder pb = new ProcessBuilder(
                "java", "-cp", ".",
                LockThisFileProgram.class.getCanonicalName(), file.getAbsolutePath() );
        File wd = new File( "target/test-classes" ).getAbsoluteFile();
        pb.directory( wd );
        Process process = pb.start();
        BufferedReader reader = new BufferedReader( new InputStreamReader( process.getInputStream() ) );
        assertThat( reader.readLine(), is( LockThisFileProgram.LOCKED_OUTPUT ) );

        try
        {
            factory.createPageSwapper( file, 4, NO_CALLBACK, true );
        }
        finally
        {
            process.getOutputStream().write( 0 );
            process.getOutputStream().flush();
            process.waitFor();
        }
    }

    @Test
    public void mustUnlockFileWhenThePageSwapperIsClosed() throws Exception
    {
        assumeFalse( FileUtils.OS_IS_WINDOWS ); // no file locking on Windows.

        PageSwapperFactory factory = swapperFactory();
        DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        factory.setFileSystemAbstraction( fs );
        File file = testDir.file( "file" );
        fs.create( file ).close();

        factory.createPageSwapper( file, 4, NO_CALLBACK, false ).close();

        try ( StoreFileChannel channel = fs.open( file, "rw" );
              FileLock fileLock = channel.tryLock() )
        {
            assertThat( fileLock, is( not( nullValue() ) ) );
        }
    }

    @Test( expected = OverlappingFileLockException.class )
    public void fileMustRemainLockedEvenIfChannelIsClosedByStrayInterrupt() throws Exception
    {
        assumeFalse( FileUtils.OS_IS_WINDOWS ); // no file locking on Windows.

        PageSwapperFactory factory = swapperFactory();
        DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        factory.setFileSystemAbstraction( fs );
        File file = testDir.file( "file" );
        fs.create( file ).close();

        PageSwapper pageSwapper = factory.createPageSwapper( file, 4, NO_CALLBACK, false );

        try
        {
            StoreChannel channel = fs.open( file, "rw" );

            Thread.currentThread().interrupt();
            pageSwapper.force();

            assertThat( channel.tryLock(), is( nullValue() ) );
        }
        finally
        {
            pageSwapper.close();
        }
    }

    @Test
    public void mustCloseFilesIfTakingFileLockThrows() throws Exception
    {
        assumeFalse( FileUtils.OS_IS_WINDOWS ); // no file locking on Windows.

        final AtomicInteger openFilesCounter = new AtomicInteger();
        PageSwapperFactory factory = swapperFactory();
        DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        factory.setFileSystemAbstraction( new DelegatingFileSystemAbstraction( fs )
        {
            @Override
            public StoreChannel open( File fileName, String mode ) throws IOException
            {
                openFilesCounter.getAndIncrement();
                return new DelegatingStoreChannel( super.open( fileName, mode ) )
                {
                    @Override
                    public void close() throws IOException
                    {
                        openFilesCounter.getAndDecrement();
                        super.close();
                    }
                };
            }
        } );
        File file = testDir.file( "file" );
        try ( StoreChannel ch = fs.create( file );
              FileLock lock = ch.tryLock() )
        {
            factory.createPageSwapper( file, 4, NO_CALLBACK, false ).close();
            fail( "Creating a page swapper for a locked channel should have thrown" );
        }
        catch ( FileLockException e )
        {
            // As expected.
        }
        assertThat( openFilesCounter.get(), is( 0 ) );
    }

    private byte[] array( ByteBuffer target )
    {
        target.clear();
        byte[] array = new byte[target.capacity()];
        while ( target.position() < target.capacity() )
        {
            array[target.position()] = target.get();
        }
        return array;
    }

    private ByteBuffer wrap( byte[] bytes )
    {
        ByteBuffer buffer = ByteBuffer.allocateDirect( bytes.length );
        for ( byte b : bytes )
        {
            buffer.put( b );
        }
        buffer.clear();
        return buffer;
    }
}
