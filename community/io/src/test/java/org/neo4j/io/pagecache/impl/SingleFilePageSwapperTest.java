/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.apache.commons.lang3.SystemUtils;
import org.junit.After;
import org.junit.Test;
import org.junit.internal.AssumptionViolatedException;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.adversaries.RandomAdversary;
import org.neo4j.adversaries.fs.AdversarialFileSystemAbstraction;
import org.neo4j.graphdb.mockfs.DelegatingFileSystemAbstraction;
import org.neo4j.graphdb.mockfs.DelegatingStoreChannel;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.fs.StoreFileChannel;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.PageSwapperTest;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.test.ByteArrayMatcher.byteArray;

public class SingleFilePageSwapperTest extends PageSwapperTest
{
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
        factory.setFileSystemAbstraction( getFs() );
        return factory;
    }

    protected void mkdirs( File dir ) throws IOException
    {
        getFs().mkdirs( dir );
    }

    protected File getFile()
    {
        return file;
    }

    protected FileSystemAbstraction getFs()
    {
        return fs;
    }

    protected void assumeFalse( String message, boolean test )
    {
        if ( test )
        {
            throw new AssumptionViolatedException( message );
        }
    }

    @Test
    public void swappingInMustFillPageWithData() throws IOException
    {
        byte[] bytes = new byte[] { 1, 2, 3, 4 };
        StoreChannel channel = getFs().create( getFile() );
        channel.writeAll( wrap( bytes ) );
        channel.close();

        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapper = createSwapper( factory, getFile(), 4, null, false );
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
        StoreChannel channel = getFs().create( getFile() );
        channel.writeAll( wrap( bytes ) );
        channel.close();

        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapper = createSwapper( factory, getFile(), 4, null, false );
        ByteBuffer target = ByteBuffer.allocateDirect( 4 );
        ByteBufferPage page = new ByteBufferPage( target );
        swapper.read( 1, page );

        assertThat( array( target ), byteArray( new byte[]{5, 6, 0, 0} ) );
    }

    @Test
    public void swappingOutMustWritePageToFile() throws IOException
    {
        getFs().create( getFile() ).close();

        byte[] expected = new byte[] { 1, 2, 3, 4 };
        ByteBufferPage page = new ByteBufferPage( wrap( expected ) );

        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapper = createSwapper( factory, getFile(), 4, null, false );
        swapper.write( 0, page );

        InputStream stream = getFs().openAsInputStream( getFile() );
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
        StoreChannel channel = getFs().create( getFile() );
        channel.writeAll( wrap( initialData ) );
        channel.close();

        byte[] change = new byte[] { 8, 7, 6, 5 };
        ByteBufferPage page = new ByteBufferPage( wrap( change ) );

        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapper = createSwapper( factory, getFile(), 4, null, false );
        swapper.write( 1, page );

        InputStream stream = getFs().openAsInputStream( getFile() );
        byte[] actual = new byte[(int) getFs().getFileSize( getFile() )];

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
        assumeFalse( "No file locking on Windows", SystemUtils.IS_OS_WINDOWS );

        PageSwapperFactory factory = swapperFactory();
        DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        factory.setFileSystemAbstraction( fs );
        File file = testDir.file( "file" );
        fs.create( file ).close();

        PageSwapper pageSwapper = createSwapper( factory, file, 4, NO_CALLBACK, false );

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
        assumeFalse( "No file locking on Windows", SystemUtils.IS_OS_WINDOWS ); // no file locking on Windows.

        PageSwapperFactory factory = swapperFactory();
        DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        factory.setFileSystemAbstraction( fs );
        File file = testDir.file( "file" );

        StoreFileChannel channel = fs.create( file );

        try ( FileLock fileLock = channel.tryLock() )
        {
            assertThat( fileLock, is( not( nullValue() ) ) );
            createSwapper( factory, file, 4, NO_CALLBACK, true );
        }
    }

    @Test( expected = FileLockException.class )
    public void creatingSwapperForExternallyLockedFileMustThrow() throws Exception
    {
        assumeFalse( "No file locking on Windows", SystemUtils.IS_OS_WINDOWS ); // no file locking on Windows.

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
            createSwapper( factory, file, 4, NO_CALLBACK, true );
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
        assumeFalse( "No file locking on Windows", SystemUtils.IS_OS_WINDOWS ); // no file locking on Windows.

        PageSwapperFactory factory = swapperFactory();
        DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        factory.setFileSystemAbstraction( fs );
        File file = testDir.file( "file" );
        fs.create( file ).close();

        createSwapper( factory, file, 4, NO_CALLBACK, false ).close();

        try ( StoreFileChannel channel = fs.open( file, "rw" );
              FileLock fileLock = channel.tryLock() )
        {
            assertThat( fileLock, is( not( nullValue() ) ) );
        }
    }

    @Test( expected = OverlappingFileLockException.class )
    public void fileMustRemainLockedEvenIfChannelIsClosedByStrayInterrupt() throws Exception
    {
        assumeFalse( "No file locking on Windows", SystemUtils.IS_OS_WINDOWS ); // no file locking on Windows.

        PageSwapperFactory factory = swapperFactory();
        DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        factory.setFileSystemAbstraction( fs );
        File file = testDir.file( "file" );
        fs.create( file ).close();

        PageSwapper pageSwapper = createSwapper( factory, file, 4, NO_CALLBACK, false );

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
        assumeFalse( "No file locking on Windows", SystemUtils.IS_OS_WINDOWS ); // no file locking on Windows.

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
            createSwapper( factory, file, 4, NO_CALLBACK, false ).close();
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

    @Test
    public void mustHandleMischiefInPositionedRead() throws Exception
    {
        int bytesTotal = 512;
        byte[] data = new byte[bytesTotal];
        ThreadLocalRandom.current().nextBytes( data );

        PageSwapperFactory factory = swapperFactory();
        factory.setFileSystemAbstraction( getFs() );
        File file = getFile();
        PageSwapper swapper = createSwapper( factory, file, bytesTotal, NO_CALLBACK, true );
        try
        {
            swapper.write( 0, new ByteBufferPage( wrap( data ) ) );
        }
        finally
        {
            swapper.close();
        }

        RandomAdversary adversary = new RandomAdversary( 0.5, 0.0, 0.0 );
        factory.setFileSystemAbstraction( new AdversarialFileSystemAbstraction( adversary, getFs() ) );
        swapper = createSwapper( factory, file, bytesTotal, NO_CALLBACK, false );

        ByteBufferPage page = createPage( bytesTotal );

        try
        {
            for ( int i = 0; i < 10_000; i++ )
            {
                clear( page );
                assertThat( swapper.read( 0, page ), is( (long) bytesTotal ) );
                assertThat( array( page.buffer ), is( data ) );
            }
        }
        finally
        {
            swapper.close();
        }
    }

    @Test
    public void mustHandleMischiefInPositionedWrite() throws Exception
    {
        int bytesTotal = 512;
        byte[] data = new byte[bytesTotal];
        ThreadLocalRandom.current().nextBytes( data );
        ByteBufferPage zeroPage = createPage( bytesTotal );
        clear( zeroPage );

        File file = getFile();
        PageSwapperFactory factory = swapperFactory();
        RandomAdversary adversary = new RandomAdversary( 0.5, 0.0, 0.0 );
        factory.setFileSystemAbstraction( new AdversarialFileSystemAbstraction( adversary, getFs() ) );
        PageSwapper swapper = createSwapper( factory, file, bytesTotal, NO_CALLBACK, true );

        ByteBufferPage page = createPage( bytesTotal );

        try
        {
            for ( int i = 0; i < 10_000; i++ )
            {
                adversary.setProbabilityFactor( 0 );
                swapper.write( 0, zeroPage );
                page.putBytes( data, 0, 0, data.length );
                adversary.setProbabilityFactor( 1 );
                assertThat( swapper.write( 0, page ), is( (long) bytesTotal ) );
                clear( page );
                adversary.setProbabilityFactor( 0 );
                swapper.read( 0, page );
                assertThat( array( page.buffer ), is( data ) );
            }
        }
        finally
        {
            swapper.close();
        }
    }

    @Test
    public void mustHandleMischiefInPositionedVectoredRead() throws Exception
    {
        int bytesTotal = 512;
        int bytesPerPage = 32;
        int pageCount = bytesTotal / bytesPerPage;
        byte[] data = new byte[bytesTotal];
        ThreadLocalRandom.current().nextBytes( data );

        PageSwapperFactory factory = swapperFactory();
        factory.setFileSystemAbstraction( getFs() );
        File file = getFile();
        PageSwapper swapper = createSwapper( factory, file, bytesTotal, NO_CALLBACK, true );
        try
        {
            swapper.write( 0, new ByteBufferPage( wrap( data ) ) );
        }
        finally
        {
            swapper.close();
        }

        RandomAdversary adversary = new RandomAdversary( 0.5, 0.0, 0.0 );
        factory.setFileSystemAbstraction( new AdversarialFileSystemAbstraction( adversary, getFs() ) );
        swapper = createSwapper( factory, file, bytesPerPage, NO_CALLBACK, false );

        ByteBufferPage[] pages = new ByteBufferPage[pageCount];
        for ( int i = 0; i < pageCount; i++ )
        {
            pages[i] = createPage( bytesPerPage );
        }

        byte[] temp = new byte[bytesPerPage];
        try
        {
            for ( int i = 0; i < 10_000; i++ )
            {
                for ( ByteBufferPage page : pages )
                {
                    clear( page );
                }
                assertThat( swapper.read( 0, pages, 0, pages.length ), is( (long) bytesTotal ) );
                for ( int j = 0; j < pageCount; j++ )
                {
                    System.arraycopy( data, j * bytesPerPage, temp, 0, bytesPerPage );
                    assertThat( array( pages[j].buffer ), is( temp ) );
                }
            }
        }
        finally
        {
            swapper.close();
        }
    }

    @Test
    public void mustHandleMischiefInPositionedVectoredWrite() throws Exception
    {
        int bytesTotal = 512;
        int bytesPerPage = 32;
        int pageCount = bytesTotal / bytesPerPage;
        byte[] data = new byte[bytesTotal];
        ThreadLocalRandom.current().nextBytes( data );
        ByteBufferPage zeroPage = createPage( bytesPerPage );
        clear( zeroPage );

        File file = getFile();
        PageSwapperFactory factory = swapperFactory();
        RandomAdversary adversary = new RandomAdversary( 0.5, 0.0, 0.0 );
        factory.setFileSystemAbstraction( new AdversarialFileSystemAbstraction( adversary, getFs() ) );
        PageSwapper swapper = createSwapper( factory, file, bytesPerPage, NO_CALLBACK, true );

        ByteBufferPage[] writePages = new ByteBufferPage[pageCount];
        ByteBufferPage[] readPages = new ByteBufferPage[pageCount];
        ByteBufferPage[] zeroPages = new ByteBufferPage[pageCount];
        for ( int i = 0; i < pageCount; i++ )
        {
            writePages[i] = createPage( bytesPerPage );
            writePages[i].putBytes( data, 0, i * bytesPerPage, bytesPerPage );
            readPages[i] = createPage( bytesPerPage );
            zeroPages[i] = zeroPage;
        }

        try
        {
            for ( int i = 0; i < 10_000; i++ )
            {
                adversary.setProbabilityFactor( 0 );
                swapper.write( 0, zeroPages, 0, pageCount );
                adversary.setProbabilityFactor( 1 );
                swapper.write( 0, writePages, 0, pageCount );
                for ( ByteBufferPage readPage : readPages )
                {
                    clear( readPage );
                }
                adversary.setProbabilityFactor( 0 );
                assertThat( swapper.read( 0, readPages, 0, pageCount ), is( (long) bytesTotal ) );
                for ( int j = 0; j < pageCount; j++ )
                {
                    assertThat( array( readPages[j].buffer ), is( array( writePages[j].buffer ) ) );
                }
            }
        }
        finally
        {
            swapper.close();
        }
    }
}
