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
package org.neo4j.io.pagecache;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.neo4j.io.pagecache.impl.ByteBufferPage;
import org.neo4j.test.rule.TestDirectory;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings( "OptionalGetWithoutIsPresent" )
public abstract class PageSwapperTest
{
    public static final PageEvictionCallback NO_CALLBACK = ( pageId, page ) -> {};
    public static final long X = 0xcafebabedeadbeefL;
    public static final long Y = X ^ (X << 1);
    public static final int Z = 0xfefefefe;

    protected static final int cachePageSize = 32;

    public final TestDirectory testDir = TestDirectory.testDirectory();
    public final ExpectedException expectedException = ExpectedException.none();
    @Rule
    public final RuleChain rules = RuleChain.outerRule( testDir ).around( expectedException );

    private final ConcurrentLinkedQueue<PageSwapperFactory> openedFactories = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<PageSwapper> openedSwappers = new ConcurrentLinkedQueue<>();

    protected abstract PageSwapperFactory swapperFactory() throws Exception;

    protected abstract void mkdirs( File dir ) throws IOException;

    protected abstract File baseDirectory() throws IOException;

    protected final PageSwapperFactory createSwapperFactory() throws Exception
    {
        PageSwapperFactory factory = swapperFactory();
        openedFactories.add( factory );
        return factory;
    }

    protected int cachePageSize()
    {
        return cachePageSize;
    }

    protected ByteBufferPage createPage( int cachePageSize )
    {
        return new ByteBufferPage( ByteBuffer.allocateDirect( cachePageSize ) );
    }

    protected ByteBufferPage createPage()
    {
        return createPage( cachePageSize() );
    }

    protected void clear( ByteBufferPage page )
    {
        byte b = (byte) 0;
        for ( int i = 0; i < cachePageSize(); i++ )
        {
            page.putByte( b, i );
        }
    }

    protected PageSwapper createSwapper(
            PageSwapperFactory factory,
            File file,
            int filePageSize,
            PageEvictionCallback callback,
            boolean createIfNotExist ) throws IOException
    {
        PageSwapper swapper = factory.createPageSwapper( file, filePageSize, callback, createIfNotExist );
        openedSwappers.add( swapper );
        return swapper;
    }

    protected final PageSwapper createSwapperAndFile( PageSwapperFactory factory, File file ) throws IOException
    {
        return createSwapperAndFile( factory, file, cachePageSize() );
    }

    protected final PageSwapper createSwapperAndFile( PageSwapperFactory factory, File file, int filePageSize )
            throws IOException
    {
        return createSwapper( factory, file, filePageSize, NO_CALLBACK, true );
    }

    private File file( String filename ) throws IOException
    {
        File file = testDir.file( filename );
        mkdirs( file.getParentFile() );
        return file;
    }

    private long sizeOf( ByteBufferPage page )
    {
        return page.size();
    }

    @Before
    @After
    public void clearStrayInterrupts()
    {
        Thread.interrupted();
    }

    @After
    public void closeOpenedPageSwappers() throws Exception
    {
        Exception exception = null;
        PageSwapperFactory factory;
        PageSwapper swapper;

        while ( (swapper = openedSwappers.poll()) != null )
        {
            try
            {
                swapper.close();
            }
            catch ( IOException e )
            {
                if ( exception == null )
                {
                    exception = e;
                }
                else
                {
                    exception.addSuppressed( e );
                }
            }
        }

        while ( (factory = openedFactories.poll()) != null )
        {
            try
            {
                factory.close();
            }
            catch ( Exception e )
            {
                if ( exception == null )
                {
                    exception = e;
                }
                else
                {
                    exception.addSuppressed( e );
                }
            }
        }

        if ( exception != null )
        {
            throw exception;
        }
    }

    @Test
    public void readMustNotSwallowInterrupts() throws Exception
    {
        File file = file( "a" );

        ByteBufferPage page = createPage();
        page.putInt( 1, 0 );
        PageSwapperFactory swapperFactory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( swapperFactory, file );

        assertThat( swapper.write( 0, page ), is( sizeOf( page ) ) );
                page.putInt( 0, 0 );
        Thread.currentThread().interrupt();

        assertThat( swapper.read( 0, page ), is( sizeOf( page ) ) );
        assertTrue( Thread.currentThread().isInterrupted() );
        assertThat( page.getInt( 0 ), is( 1 ) );

        assertThat( swapper.read( 0, page ), is( sizeOf( page ) ) );
        assertTrue( Thread.currentThread().isInterrupted() );
        assertThat( page.getInt( 0 ), is( 1 ) );
    }

    @Test
    public void vectoredReadMustNotSwallowInterrupts() throws Exception
    {
        File file = file( "a" );

        ByteBufferPage page = createPage();
        page.putInt( 1, 0 );
        PageSwapperFactory swapperFactory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( swapperFactory, file );

        assertThat( swapper.write( 0, page ), is( sizeOf( page ) ) );
                page.putInt( 0, 0 );
        Thread.currentThread().interrupt();

        assertThat( swapper.read( 0, new Page[]{page}, 0, 1 ), is( sizeOf( page ) ) );
        assertTrue( Thread.currentThread().isInterrupted() );
        assertThat( page.getInt( 0 ), is( 1 ) );

        assertThat( swapper.read( 0, new Page[] {page}, 0, 1 ), is( sizeOf( page ) ) );
        assertTrue( Thread.currentThread().isInterrupted() );
        assertThat( page.getInt( 0 ), is( 1 ) );
    }

    @Test
    public void writeMustNotSwallowInterrupts() throws Exception
    {
        File file = file( "a" );

        ByteBufferPage page = createPage();
        page.putInt( 1, 0 );
        PageSwapperFactory swapperFactory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( swapperFactory, file );

        Thread.currentThread().interrupt();

        assertThat( swapper.write( 0, page ), is( sizeOf( page ) ) );
        assertTrue( Thread.currentThread().isInterrupted() );

        page.putInt( 0, 0 );
        assertThat( swapper.read( 0, page ), is( sizeOf( page ) ) );
        assertThat( page.getInt( 0 ), is( 1 ) );

        assertThat( swapper.write( 0, page ), is( sizeOf( page ) ) );
        assertTrue( Thread.currentThread().isInterrupted() );

        page.putInt( 0, 0 );
        assertThat( swapper.read( 0, page ), is( sizeOf( page ) ) );
        assertThat( page.getInt( 0 ), is( 1 ) );
    }

    @Test
    public void vectoredWriteMustNotSwallowInterrupts() throws Exception
    {
        File file = file( "a" );

        ByteBufferPage page = createPage();
        page.putInt( 1, 0 );
        PageSwapperFactory swapperFactory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( swapperFactory, file );

        Thread.currentThread().interrupt();

        assertThat( swapper.write( 0, new Page[] {page}, 0, 1 ), is( sizeOf( page ) ) );
        assertTrue( Thread.currentThread().isInterrupted() );

        page.putInt( 0, 0 );
        assertThat( swapper.read( 0, page ), is( sizeOf( page ) ) );
        assertThat( page.getInt( 0 ), is( 1 ) );

        assertThat( swapper.write( 0, new Page[] {page}, 0, 1 ), is( sizeOf( page ) ) );
        assertTrue( Thread.currentThread().isInterrupted() );

        page.putInt( 0, 0 );
        assertThat( swapper.read( 0, page ), is( sizeOf( page ) ) );
        assertThat( page.getInt( 0 ), is( 1 ) );
    }

    @Test
    public void forcingMustNotSwallowInterrupts() throws Exception
    {
        File file = file( "a" );

        PageSwapperFactory swapperFactory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( swapperFactory, file );

        Thread.currentThread().interrupt();
        swapper.force();
        assertTrue( Thread.currentThread().isInterrupted() );
    }

    @Test
    public void mustReopenChannelWhenReadFailsWithAsynchronousCloseException() throws Exception
    {
        File file = file( "a" );
        PageSwapperFactory swapperFactory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( swapperFactory, file );

        ByteBufferPage page = createPage();
        page.putLong( X, 0 );
        page.putLong( Y, 8 );
        page.putInt( Z, 16 );
        swapper.write( 0, page );

        Thread.currentThread().interrupt();

        swapper.read( 0, page );

        // Clear the interrupted flag and assert that it was still raised
        assertTrue( Thread.interrupted() );

        assertThat( page.getLong( 0 ), is( X ) );
        assertThat( page.getLong( 8 ), is( Y ) );
        assertThat( page.getInt( 16 ), is( Z ) );

        // This must not throw because we should still have a usable channel
        swapper.force();
    }

    @Test
    public void mustReopenChannelWhenVectoredReadFailsWithAsynchronousCloseException() throws Exception
    {
        File file = file( "a" );
        PageSwapperFactory swapperFactory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( swapperFactory, file );

        ByteBufferPage page = createPage();
        page.putLong( X, 0 );
        page.putLong( Y, 8 );
        page.putInt( Z, 16 );
        swapper.write( 0, page );

        Thread.currentThread().interrupt();

        swapper.read( 0, new Page[] {page}, 0, 1 );

        // Clear the interrupted flag and assert that it was still raised
        assertTrue( Thread.interrupted() );

        assertThat( page.getLong( 0 ), is( X ) );
        assertThat( page.getLong( 8 ), is( Y ) );
        assertThat( page.getInt( 16 ), is( Z ) );

        // This must not throw because we should still have a usable channel
        swapper.force();
    }

    @Test
    public void mustReopenChannelWhenWriteFailsWithAsynchronousCloseException() throws Exception
    {
        ByteBufferPage page = createPage();
        page.putLong( X, 0 );
        page.putLong( Y, 8 );
        page.putInt( Z, 16 );
        File file = file( "a" );

        PageSwapperFactory swapperFactory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( swapperFactory, file );

        Thread.currentThread().interrupt();

        swapper.write( 0, page );

        // Clear the interrupted flag and assert that it was still raised
        assertTrue( Thread.interrupted() );

        // This must not throw because we should still have a usable channel
        swapper.force();

        clear( page );
        swapper.read( 0, page );
        assertThat( page.getLong( 0 ), is( X ) );
        assertThat( page.getLong( 8 ), is( Y ) );
        assertThat( page.getInt( 16 ), is( Z ) );
    }

    @Test
    public void mustReopenChannelWhenVectoredWriteFailsWithAsynchronousCloseException() throws Exception
    {
        ByteBufferPage page = createPage();
        page.putLong( X, 0 );
        page.putLong( Y, 8 );
        page.putInt( Z, 16 );
        File file = file( "a" );

        PageSwapperFactory swapperFactory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( swapperFactory, file );

        Thread.currentThread().interrupt();

        swapper.write( 0, new Page[] {page}, 0, 1 );

        // Clear the interrupted flag and assert that it was still raised
        assertTrue( Thread.interrupted() );

        // This must not throw because we should still have a usable channel
        swapper.force();

        clear( page );
        swapper.read( 0, page );
        assertThat( page.getLong( 0 ), is( X ) );
        assertThat( page.getLong( 8 ), is( Y ) );
        assertThat( page.getInt( 16 ), is( Z ) );
    }

    @Test
    public void mustReopenChannelWhenForceFailsWithAsynchronousCloseException() throws Exception
    {
        File file = file( "a" );

        PageSwapperFactory swapperFactory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( swapperFactory, file );

        for ( int i = 0; i < 10; i++ )
        {
            Thread.currentThread().interrupt();

            // This must not throw
            swapper.force();

            // Clear the interrupted flag and assert that it was still raised
            assertTrue( Thread.interrupted() );
        }
    }

    @Test
    public void readMustNotReopenExplicitlyClosedChannel() throws Exception
    {
        String filename = "a";
        File file = file( filename );

        ByteBufferPage page = createPage();
        PageSwapperFactory swapperFactory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( swapperFactory, file );
        swapper.write( 0, page );
        swapper.close();

        try
        {
            swapper.read( 0, page );
            fail( "Should have thrown because the channel should be closed" );
        }
        catch ( ClosedChannelException ignore )
        {
            // This is fine.
        }
    }

    @Test
    public void vectoredReadMustNotReopenExplicitlyClosedChannel() throws Exception
    {
        String filename = "a";
        File file = file( filename );

        ByteBufferPage page = createPage();
        PageSwapperFactory swapperFactory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( swapperFactory, file );
        swapper.write( 0, page );
        swapper.close();

        try
        {
            swapper.read( 0, new Page[] {page}, 0, 1 );
            fail( "Should have thrown because the channel should be closed" );
        }
        catch ( ClosedChannelException ignore )
        {
            // This is fine.
        }
    }

    @Test
    public void writeMustNotReopenExplicitlyClosedChannel() throws Exception
    {
        File file = file( "a" );

        ByteBufferPage page = createPage();
        PageSwapperFactory swapperFactory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( swapperFactory, file );
        swapper.close();

        try
        {
            swapper.write( 0, page );
            fail( "Should have thrown because the channel should be closed" );
        }
        catch ( ClosedChannelException ignore )
        {
            // This is fine.
        }
    }

    @Test
    public void vectoredWriteMustNotReopenExplicitlyClosedChannel() throws Exception
    {
        File file = file( "a" );

        ByteBufferPage page = createPage();
        PageSwapperFactory swapperFactory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( swapperFactory, file );
        swapper.close();

        try
        {
            swapper.write( 0, new Page[] {page}, 0, 1 );
            fail( "Should have thrown because the channel should be closed" );
        }
        catch ( ClosedChannelException ignore )
        {
            // This is fine.
        }
    }

    @Test
    public void forceMustNotReopenExplicitlyClosedChannel() throws Exception
    {
        File file = file( "a" );

        PageSwapperFactory swapperFactory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( swapperFactory, file );
        swapper.close();

        try
        {
            swapper.force();
            fail( "Should have thrown because the channel should be closed" );
        }
        catch ( ClosedChannelException ignore )
        {
            // This is fine.
        }
    }

    @Test
    public void mustNotOverwriteDataInOtherFiles() throws Exception
    {
        File fileA = file( "a" );
        File fileB = file( "b" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapperA =
                createSwapperAndFile( factory, fileA );
        PageSwapper swapperB =
                createSwapperAndFile( factory, fileB );

        ByteBufferPage page = createPage();
        page.putLong( X, 0 );
        swapperA.write( 0, page );
        page.putLong( Y, 8 );
        swapperB.write( 0, page );

        clear( page );
        swapperA.read( 0, page );

        assertThat( page.getLong( 0 ), is( X ) );
        assertThat( page.getLong( 8 ), is( 0L ) );
    }

    @Test
    public void mustRunEvictionCallbackOnEviction() throws Exception
    {
        final AtomicLong callbackFilePageId = new AtomicLong();
        final AtomicReference<Page> callbackPage = new AtomicReference<>();
        PageEvictionCallback callback = ( filePageId, page ) -> {
            callbackFilePageId.set( filePageId );
            callbackPage.set( page );
        };
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapper( factory, file, cachePageSize(), callback, true );
        Page page = createPage();
        swapper.evicted( 42, page );
        assertThat( callbackFilePageId.get(), is( 42L ) );
        assertThat( callbackPage.get(), sameInstance( page ) );
    }

    @Test
    public void mustNotIssueEvictionCallbacksAfterSwapperHasBeenClosed() throws Exception
    {
        final AtomicBoolean gotCallback = new AtomicBoolean();
        PageEvictionCallback callback = ( filePageId, page ) -> gotCallback.set( true );
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapper( factory, file, cachePageSize(), callback, true );
        Page page = createPage();
        swapper.close();
        swapper.evicted( 42, page );
        assertFalse( gotCallback.get() );
    }

    @Test
    public void mustThrowExceptionIfFileDoesNotExist() throws Exception
    {
        PageSwapperFactory factory = createSwapperFactory();
        expectedException.expect( NoSuchFileException.class );
        createSwapper( factory, file( "does not exist" ), cachePageSize(), NO_CALLBACK, false );
    }

    @Test
    public void mustCreateNonExistingFileWithCreateFlag() throws Exception
    {
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper pageSwapper = createSwapperAndFile( factory, file( "does not exist" ) );

        // After creating the file, we must also be able to read and write
        ByteBufferPage page = createPage();
        page.putLong( X, 0 );
        pageSwapper.write( 0, page );

        clear( page );
        pageSwapper.read( 0, page );

        assertThat( page.getLong( 0 ), is( X ) );
    }

    @Test
    public void truncatedFilesMustBeEmpty() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file );

        assertThat( swapper.getLastPageId(), is( -1L ) );

        ByteBufferPage page = createPage();
        page.putInt( 0xcafebabe, 0 );
        swapper.write( 10, page );
        clear( page );
        swapper.read( 10, page );
        assertThat( page.getInt( 0 ), is( 0xcafebabe ) );
        assertThat( swapper.getLastPageId(), is( 10L ) );

        swapper.close();
        swapper = createSwapper( factory, file, cachePageSize(), NO_CALLBACK, false );
        clear( page );
        swapper.read( 10, page );
        assertThat( page.getInt( 0 ), is( 0xcafebabe ) );
        assertThat( swapper.getLastPageId(), is( 10L ) );

        swapper.truncate();
        clear( page );
        swapper.read( 10, page );
        assertThat( page.getInt( 0 ), is( 0 ) );
        assertThat( swapper.getLastPageId(), is( -1L ) );

        swapper.close();
        swapper = createSwapper( factory, file, cachePageSize(), NO_CALLBACK, false );
        clear( page );
        swapper.read( 10, page );
        assertThat( page.getInt( 0 ), is( 0 ) );
        assertThat( swapper.getLastPageId(), is( -1L ) );

        swapper.close();
    }

    @Test
    public void positionedVectoredWriteMustFlushAllBuffersInOrder() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        ByteBufferPage pageA = createPage( 4 );
        ByteBufferPage pageB = createPage( 4 );
        ByteBufferPage pageC = createPage( 4 );
        ByteBufferPage pageD = createPage( 4 );

        pageA.putInt( 2, 0 );
        pageB.putInt( 3, 0 );
        pageC.putInt( 4, 0 );
        pageD.putInt( 5, 0 );

        swapper.write( 1, new Page[]{pageA, pageB, pageC, pageD}, 0, 4 );

        ByteBufferPage result = createPage( 4 );

        swapper.read( 0, result );
        assertThat( result.getInt( 0 ), is( 0 ) );
        result.putInt( 0, 0 );
        assertThat( swapper.read( 1, result ), is( 4L ) );
        assertThat( result.getInt( 0 ), is( 2 ) );
        result.putInt( 0, 0 );
        assertThat( swapper.read( 2, result ), is( 4L ) );
        assertThat( result.getInt( 0 ), is( 3 ) );
        result.putInt( 0, 0 );
        assertThat( swapper.read( 3, result ), is( 4L ) );
        assertThat( result.getInt( 0 ), is( 4 ) );
        result.putInt( 0, 0 );
        assertThat( swapper.read( 4, result ), is( 4L ) );
        assertThat( result.getInt( 0 ), is( 5 ) );
        result.putInt( 0, 0 );
        assertThat( swapper.read( 5, result ), is( 0L ) );
        assertThat( result.getInt( 0 ), is( 0 ) );
    }

    @Test
    public void positionedVectoredReadMustFillAllBuffersInOrder() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        ByteBufferPage output = createPage();

        output.putInt( 2, 0 );
        swapper.write( 1, output );
        output.putInt( 3, 0 );
        swapper.write( 2, output );
        output.putInt( 4, 0 );
        swapper.write( 3, output );
        output.putInt( 5, 0 );
        swapper.write( 4, output );

        ByteBufferPage pageA = createPage( 4 );
        ByteBufferPage pageB = createPage( 4 );
        ByteBufferPage pageC = createPage( 4 );
        ByteBufferPage pageD = createPage( 4 );

        // Read 4 pages of 4 bytes each
        assertThat( swapper.read( 1, new Page[]{pageA, pageB, pageC, pageD}, 0, 4 ), is( 4 * 4L ) );

        assertThat( pageA.getInt( 0 ), is( 2 ) );
        assertThat( pageB.getInt( 0 ), is( 3 ) );
        assertThat( pageC.getInt( 0 ), is( 4 ) );
        assertThat( pageD.getInt( 0 ), is( 5 ) );
    }

    @Test
    public void positionedVectoredReadFromEmptyFileMustFillPagesWithZeros() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        ByteBufferPage page = createPage( 4 );
        page.putInt( 1, 0 );
        assertThat( swapper.read( 0, new Page[]{page}, 0, 1 ), is( 0L ) );
        assertThat( page.getInt( 0 ), is( 0 ) );
    }

    @Test
    public void positionedVectoredReadBeyondEndOfFileMustFillPagesWithZeros() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        ByteBufferPage output = createPage( 4 );
        output.putInt( 0xFFFF_FFFF, 0 );
        swapper.write( 0, new Page[]{output, output, output}, 0, 3 );

        ByteBufferPage pageA = createPage( 4 );
        ByteBufferPage pageB = createPage( 4 );
        pageA.putInt( -1, 0 );
        pageB.putInt( -1, 0 );
        assertThat( swapper.read( 3, new Page[]{pageA, pageB}, 0, 2 ), is( 0L ) );
        assertThat( pageA.getInt( 0 ), is( 0 ) );
        assertThat( pageB.getInt( 0 ), is( 0 ) );
    }

    @Test
    public void positionedVectoredReadWhereLastPageExtendBeyondEndOfFileMustHaveRemainderZeroFilled() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        ByteBufferPage output = createPage( 4 );
        output.putInt( 0xFFFF_FFFF, 0 );
        swapper.write( 0, new Page[]{output, output, output, output, output}, 0, 5 );
        swapper.close();

        swapper = createSwapper( factory, file, 8, NO_CALLBACK, false );
        ByteBufferPage pageA = createPage( 8 );
        ByteBufferPage pageB = createPage( 8 );
        pageA.putLong( X, 0 );
        pageB.putLong( Y, 0 );
        assertThat( swapper.read( 1, new Page[]{pageA, pageB}, 0, 2 ), isOneOf( 12L, 16L ) );
        assertThat( pageA.getLong( 0 ), is( 0xFFFF_FFFF_FFFF_FFFFL ) );
        assertThat( pageB.getLong( 0 ), is( 0xFFFF_FFFF_0000_0000L ) );
    }

    @Test
    public void positionedVectoredReadWhereSecondLastPageExtendBeyondEndOfFileMustHaveRestZeroFilled() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        ByteBufferPage output = createPage( 4 );
        output.putInt( 1, 0 );
        swapper.write( 0, output );
        output.putInt( 2, 0 );
        swapper.write( 1, output );
        output.putInt( 3, 0 );
        swapper.write( 2, output );
        swapper.close();

        swapper = createSwapper( factory, file, 8, NO_CALLBACK, false );
        ByteBufferPage pageA = createPage( 8 );
        ByteBufferPage pageB = createPage( 8 );
        ByteBufferPage pageC = createPage( 8 );
        pageA.putInt( -1, 0 );
        pageB.putInt( -1, 0 );
        pageC.putInt( -1, 0 );
        assertThat( swapper.read( 0, new Page[]{pageA, pageB, pageC}, 0, 3 ), isOneOf( 12L, 16L ) );
        assertThat( pageA.getInt( 0 ), is( 1 ) );
        assertThat( pageA.getInt( 4 ), is( 2 ) );
        assertThat( pageB.getInt( 0 ), is( 3 ) );
        assertThat( pageB.getInt( 4 ), is( 0 ) );
        assertThat( pageC.getLong( 0 ), is( 0L ) );
    }

    @Test
    public void concurrentPositionedVectoredReadsAndWritesMustNotInterfere() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        final PageSwapper swapper = createSwapperAndFile( factory, file, 4 );
        final int pageCount = 100;
        final int iterations = 20000;
        final CountDownLatch startLatch = new CountDownLatch( 1 );
        ByteBufferPage output = createPage( 4 );
        for ( int i = 0; i < pageCount; i++ )
        {
            output.putInt( i+1, 0 );
            swapper.write( i, output );
        }

        Callable<Void> work = () -> {
            ThreadLocalRandom rng = ThreadLocalRandom.current();
            ByteBufferPage[] pages = new ByteBufferPage[10];
            for ( int i = 0; i < pages.length; i++ )
            {
                pages[i] = createPage( 4 );
            }

            startLatch.await();
            for ( int i = 0; i < iterations; i++ )
            {
                long startFilePageId = rng.nextLong( 0, pageCount - pages.length );
                if ( rng.nextBoolean() )
                {
                    // Do read
                    long bytesRead = swapper.read( startFilePageId, pages, 0, pages.length );
                    assertThat( bytesRead, is( pages.length * 4L ) );
                    for ( int j = 0; j < pages.length; j++ )
                    {
                        int expectedValue = (int) (1 + j + startFilePageId);
                        int actualValue = pages[j].getInt( 0 );
                        assertThat( actualValue, is( expectedValue ) );
                    }
                }
                else
                {
                    // Do write
                    for ( int j = 0; j < pages.length; j++ )
                    {
                        int value = (int) (1 + j + startFilePageId);
                        pages[j].putInt( value, 0 );
                    }
                    assertThat( swapper.write( startFilePageId, pages, 0, pages.length ), is( pages.length * 4L ) );
                }
            }
            return null;
        };

        int threads = 8;
        ExecutorService executor = Executors.newFixedThreadPool( threads, r -> {
            Thread thread = Executors.defaultThreadFactory().newThread( r );
            thread.setDaemon( true );
            return thread;
        } );
        List<Future<Void>> futures = new ArrayList<>( threads );
        for ( int i = 0; i < threads; i++ )
        {
            futures.add( executor.submit( work ) );
        }

        startLatch.countDown();
        for ( Future<Void> future : futures )
        {
            future.get();
        }
    }

    @Test
    public void positionedVectoredReadMustWorkOnSubsequenceOfGivenArray() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        ByteBufferPage pageA = createPage( 4 );
        ByteBufferPage pageB = createPage( 4 );
        ByteBufferPage pageC = createPage( 4 );
        ByteBufferPage pageD = createPage( 4 );

        pageA.putInt( 1, 0 );
        pageB.putInt( 2, 0 );
        pageC.putInt( 3, 0 );
        pageD.putInt( 4, 0 );

        Page[] pages = {pageA, pageB, pageC, pageD};
        long bytesWritten = swapper.write( 0, pages, 0, 4 );
        assertThat( bytesWritten, is( 16L ) );

        pageA.putInt( 5, 0 );
        pageB.putInt( 6, 0 );
        pageC.putInt( 7, 0 );
        pageD.putInt( 8, 0 );

        long bytesRead = swapper.read( 1, pages, 1, 2 );
        assertThat( bytesRead, is( 8L ) );

        int[] actualValues = {pageA.getInt( 0 ), pageB.getInt( 0 ), pageC.getInt( 0 ), pageD.getInt( 0 )};
        int[] expectedValues = {5, 2, 3, 8};
        assertThat( actualValues, is( expectedValues ) );
    }

    @Test
    public void positionedVectoredWriteMustWorkOnSubsequenceOfGivenArray() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        ByteBufferPage pageA = createPage( 4 );
        ByteBufferPage pageB = createPage( 4 );
        ByteBufferPage pageC = createPage( 4 );
        ByteBufferPage pageD = createPage( 4 );

        pageA.putInt( 1, 0 );
        pageB.putInt( 2, 0 );
        pageC.putInt( 3, 0 );
        pageD.putInt( 4, 0 );

        Page[] pages = {pageA, pageB, pageC, pageD};
        long bytesWritten = swapper.write( 0, pages, 0, 4 );
        assertThat( bytesWritten, is( 16L ) );

        pageB.putInt( 6, 0 );
        pageC.putInt( 7, 0 );

        bytesWritten = swapper.write( 1, pages, 1, 2 );
        assertThat( bytesWritten, is( 8L ) );

        pageA.putInt( 0, 0 );
        pageB.putInt( 0, 0 );
        pageC.putInt( 0, 0 );
        pageD.putInt( 0, 0 );

        long bytesRead = swapper.read( 0, pages, 0, 4 );
        assertThat( bytesRead, is( 16L ) );

        int[] actualValues = {pageA.getInt( 0 ), pageB.getInt( 0 ), pageC.getInt( 0 ), pageD.getInt( 0 )};
        int[] expectedValues = {1, 6, 7, 4};
        assertThat( actualValues, is( expectedValues ) );
    }

    @Test
    public void mustThrowNullPointerExceptionFromReadWhenPageArrayElementsAreNull() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        ByteBufferPage page = createPage( 4 );

        swapper.write( 0, new Page[]{page, page, page, page}, 0, 4 );

        try
        {
            swapper.read( 0, new Page[]{page, page, null, page}, 0, 4 );
            fail( "vectored read with nulls in array should have thrown" );
        }
        catch ( NullPointerException npe )
        {
            // This is fine
        }
    }

    @Test
    public void mustThrowNullPointerExceptionFromWriteWhenPageArrayElementsAreNull() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        ByteBufferPage page = createPage( 4 );

        try
        {
            swapper.write( 0, new Page[]{page, page, null, page}, 0, 4 );
            fail( "vectored read with nulls in array should have thrown" );
        }
        catch ( NullPointerException npe )
        {
            // This is fine
        }
    }

    @Test
    public void mustThrowNullPointerExceptionFromReadWhenPageArrayIsNull() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        ByteBufferPage page = createPage( 4 );

        swapper.write( 0, new Page[]{page, page, page, page}, 0, 4 );

        try
        {
            swapper.read( 0, null, 0, 4 );
            fail( "vectored read with null array should have thrown" );
        }
        catch ( NullPointerException npe )
        {
            // This is fine
        }
    }

    @Test
    public void mustThrowNullPointerExceptionFromWriteWhenPageArrayIsNull() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        try
        {
            swapper.write( 0, null, 0, 4 );
            fail( "vectored write with null array should have thrown" );
        }
        catch ( NullPointerException npe )
        {
            // This is fine
        }
    }

    @Test
    public void readMustThrowForNegativeFilePageIds() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        expectedException.expect( IOException.class );
        swapper.read( -1, createPage( 4 ) );
    }

    @Test
    public void writeMustThrowForNegativeFilePageIds() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        expectedException.expect( IOException.class );
        swapper.write( -1, createPage( 4 ) );
    }

    @Test
    public void vectoredReadMustThrowForNegativeFilePageIds() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        expectedException.expect( IOException.class );
        swapper.read( -1, new Page[]{createPage( 4 ), createPage( 4 )}, 0, 2 );
    }

    @Test
    public void vectoredWriteMustThrowForNegativeFilePageIds() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        expectedException.expect( IOException.class );
        swapper.write( -1, new Page[] {createPage( 4 ), createPage( 4 )}, 0, 2 );
    }

    @Test
    public void vectoredReadMustThrowForNegativeArrayOffsets() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        Page[] pages = {createPage( 4 ), createPage( 4 )};
        swapper.write( 0, pages, 0, 2 );
        expectedException.expect( ArrayIndexOutOfBoundsException.class );
        swapper.read( 0, pages, -1, 2 );
    }

    @Test
    public void vectoredWriteMustThrowForNegativeArrayOffsets() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        Page[] pages = {createPage( 4 ), createPage( 4 )};
        expectedException.expect( ArrayIndexOutOfBoundsException.class );
        swapper.write( 0, pages, -1, 2 );
    }

    @Test
    public void vectoredReadMustThrowWhenLengthGoesBeyondArraySize() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        Page[] pages = {createPage( 4 ), createPage( 4 )};
        swapper.write( 0, pages, 0, 2 );
        expectedException.expect( ArrayIndexOutOfBoundsException.class );
        swapper.read( 0, pages, 1, 2 );
    }

    @Test
    public void vectoredWriteMustThrowWhenLengthGoesBeyondArraySize() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        Page[] pages = {createPage( 4 ), createPage( 4 )};
        expectedException.expect( ArrayIndexOutOfBoundsException.class );
        swapper.write( 0, pages, 1, 2 );
    }

    @Test
    public void vectoredReadMustThrowWhenArrayOffsetIsEqualToArrayLength() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        Page[] pages = {createPage( 4 ), createPage( 4 )};
        swapper.write( 0, pages, 0, 2 );
        expectedException.expect( ArrayIndexOutOfBoundsException.class );
        swapper.read( 0, pages, 2, 1 );
    }

    @Test
    public void vectoredWriteMustThrowWhenArrayOffsetIsEqualToArrayLength() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        Page[] pages = {createPage( 4 ), createPage( 4 )};
        expectedException.expect( ArrayIndexOutOfBoundsException.class );
        swapper.write( 0, pages, 2, 1 );
    }

    @Test
    public void vectoredReadMustThrowWhenArrayOffsetIsGreaterThanArrayLength() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        Page[] pages = {createPage( 4 ), createPage( 4 )};
        swapper.write( 0, pages, 0, 2 );
        expectedException.expect( ArrayIndexOutOfBoundsException.class );
        swapper.read( 0, pages, 3, 1 );
    }

    @Test
    public void vectoredWriteMustThrowWhenArrayOffsetIsGreaterThanArrayLength() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        Page[] pages = {createPage( 4 ), createPage( 4 )};
        expectedException.expect( ArrayIndexOutOfBoundsException.class );
        swapper.write( 0, pages, 3, 1 );
    }

    @Test
    public void vectoredReadMustReadNothingWhenLengthIsZero() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        ByteBufferPage pageA = createPage( 4 );
        ByteBufferPage pageB = createPage( 4 );
        pageA.putInt( 1, 0 );
        pageB.putInt( 2, 0 );
        Page[] pages = {pageA, pageB};
        swapper.write( 0, pages, 0, 2 );
        pageA.putInt( 3, 0 );
        pageB.putInt( 4, 0 );
        swapper.read( 0, pages, 0, 0 );

        int[] expectedValues = {3, 4};
        int[] actualValues = {pageA.getInt( 0 ), pageB.getInt( 0 )};
        assertThat( actualValues, is( expectedValues ) );
    }

    @Test
    public void vectoredWriteMustReadNothingWhenLengthIsZero() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        ByteBufferPage pageA = createPage( 4 );
        ByteBufferPage pageB = createPage( 4 );
        pageA.putInt( 1, 0 );
        pageB.putInt( 2, 0 );
        Page[] pages = {pageA, pageB};
        swapper.write( 0, pages, 0, 2 );
        pageA.putInt( 3, 0 );
        pageB.putInt( 4, 0 );
        swapper.write( 0, pages, 0, 0 );
        swapper.read( 0, pages, 0, 2 );

        int[] expectedValues = {1, 2};
        int[] actualValues = {pageA.getInt( 0 ), pageB.getInt( 0 )};
        assertThat( actualValues, is( expectedValues ) );
    }

    @Test
    public void mustDeleteFileIfClosedWithCloseAndDelete() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );
        swapper.closeAndDelete();

        try
        {
            createSwapper( factory, file, 4, NO_CALLBACK, false );
            fail( "should not have been able to create a page swapper for non-existing file" );
        }
        catch ( IOException ignore )
        {
            // Just as planned!
        }
    }

    @Test
    public void streamFilesRecursiveMustBeEmptyForEmptyBaseDirectory() throws Exception
    {
        PageSwapperFactory factory = createSwapperFactory();
        assertThat( factory.streamFilesRecursive( baseDirectory() ).count(), is( 0L ) );
    }

    @Test
    public void streamFilesRecursiveMustListAllFilesInBaseDirectory() throws Exception
    {
        PageSwapperFactory factory = createSwapperFactory();
        File base = baseDirectory();
        File a = new File( base, "a" );
        File b = new File( base, "b" );
        createSwapperAndFile( factory, a );
        createSwapperAndFile( factory, b );
        Set<File> files = factory.streamFilesRecursive( base ).map( FileHandle::getFile ).collect( toSet() );
        assertThat( files, containsInAnyOrder( a, b ) );
    }

    @Test
    public void streamFilesRecursiveMustListAllFilesInSubDirectories() throws Exception
    {
        PageSwapperFactory factory = createSwapperFactory();
        File base = baseDirectory();
        File sub1 = new File( base, "sub1" );
        File sub1sub1 = new File( sub1, "sub1" );
        File sub2 = new File( base, "sub2" );
        File sub3 = new File( base, "sub3" );
        mkdirs( sub1 );
        mkdirs( sub1sub1 );
        mkdirs( sub2 );
        mkdirs( sub3 ); // empty, not listed
        File a = new File( base, "a" );
        File b = new File( sub1, "b" );
        File c = new File( sub1sub1, "c" );
        File d = new File( sub1sub1, "d" );
        File e = new File( sub2, "e" );
        File[] files = new File[] {a, b, c, d, e};
        for ( File f : files )
        {
            createSwapperAndFile( factory, f );
        }
        Set<File> set = factory.streamFilesRecursive( base ).map( FileHandle::getFile ).collect( toSet() );
        assertThat( set, containsInAnyOrder( files ) );
    }

    @Test
    public void streamFilesRecursiveFilePathsMustBeCanonical() throws Exception
    {
        PageSwapperFactory factory = createSwapperFactory();
        File base = baseDirectory();
        File sub = new File( base, "sub" );
        mkdirs( sub );
        File a = new File( new File( new File( sub, ".." ), "sub" ), "a" );
        File canonicalFile = a.getCanonicalFile();
        createSwapperAndFile( factory, canonicalFile );
        String actualPath = factory.streamFilesRecursive( a )
                                   .map( fh -> fh.getFile().getAbsolutePath() ).findAny().get();
        assertThat( actualPath, is( canonicalFile.getAbsolutePath() ) );
    }

    @Test
    public void streamFilesRecursiveMustListSingleFileGivenAsBase() throws Exception
    {
        PageSwapperFactory factory = createSwapperFactory();
        File base = baseDirectory();
        File a = new File( base, "a" );
        File b = new File( base, "b" );
        createSwapperAndFile( factory, a );
        createSwapperAndFile( factory, b );
        Set<File> files = factory.streamFilesRecursive( a ).map( FileHandle::getFile ).collect( toSet() );
        assertThat( files, containsInAnyOrder( a ) );
    }

    @Test
    public void streamFilesRecursiveMustThrowOnNonExistingBasePath() throws Exception
    {
        PageSwapperFactory factory = createSwapperFactory();
        File base = baseDirectory();
        File nonExisting = new File( base, "nonExisting" );
        expectedException.expect( NoSuchFileException.class );
        factory.streamFilesRecursive( nonExisting );
    }

    @Test
    public void streamFilesRecursiveMustRenameFiles() throws Exception
    {
        PageSwapperFactory factory = createSwapperFactory();
        File base = baseDirectory();
        File a = new File( base, "a" );
        File b = new File( base, "b" );
        createSwapperAndFile( factory, a ).close();
        FileHandle handle = factory.streamFilesRecursive( a ).findAny().get();
        handle.rename( b );
        createSwapper( factory, b, cachePageSize(), NO_CALLBACK, false ); // throws if 'b' does not exist
    }

    @Test
    public void streamFilesRecursiveMustRenameDelete() throws Exception
    {
        PageSwapperFactory factory = createSwapperFactory();
        File base = baseDirectory();
        File a = new File( base, "a" );
        File b = new File( base, "b" );
        createSwapperAndFile( factory, a ).close();
        createSwapperAndFile( factory, b ).close();
        FileHandle handle = factory.streamFilesRecursive( a ).findAny().get();
        handle.delete();
        Set<File> files = factory.streamFilesRecursive( base ).map( FileHandle::getFile ).collect( toSet() );
        assertThat( files, containsInAnyOrder( b ) );
    }

    @Test
    public void streamFilesRecursiveMustThrowWhenDeletingNonExistingFile() throws Exception
    {
        PageSwapperFactory factory = createSwapperFactory();
        File base = baseDirectory();
        File a = new File( base, "a" );
        PageSwapper swapperA = createSwapperAndFile( factory, a );
        FileHandle handle = factory.streamFilesRecursive( a ).findAny().get();
        swapperA.closeAndDelete();
        expectedException.expect( NoSuchFileException.class );
        handle.delete();
    }

    @Test
    public void streamFilesRecursiveMustThrowWhenTargetFileOfRenameAlreadyExists() throws Exception
    {
        PageSwapperFactory factory = createSwapperFactory();
        File base = baseDirectory();
        File a = new File( base, "a" );
        File b = new File( base, "b" );
        createSwapperAndFile( factory, a ).close();
        createSwapperAndFile( factory, b ).close();
        FileHandle handle = factory.streamFilesRecursive( a ).findAny().get();
        expectedException.expect( FileAlreadyExistsException.class );
        handle.rename( b );
    }

    @Test
    public void streamFilesRecursiveMustNotThrowWhenTargetFileOfRenameAlreadyExistsAndUsingReplaceExisting() throws Exception
    {
        PageSwapperFactory factory = createSwapperFactory();
        File base = baseDirectory();
        File a = new File( base, "a" );
        File b = new File( base, "b" );
        createSwapperAndFile( factory, a ).close();
        createSwapperAndFile( factory, b ).close();
        FileHandle handle = factory.streamFilesRecursive( a ).findAny().get();
        handle.rename( b, REPLACE_EXISTING );
    }

    @Test
    public void streamFilesRecursiveMustCreateMissingPathDirectoriesImpliedByFileRename() throws Exception
    {
        PageSwapperFactory factory = createSwapperFactory();
        File base = baseDirectory();
        File a = new File( base, "a" );
        File target = new File( new File( new File( base, "sub" ), "sub" ), "target" );
        createSwapperAndFile( factory, a ).close();
        FileHandle handle = factory.streamFilesRecursive( a ).findAny().get();
        handle.rename( target );
        createSwapper( factory, target, cachePageSize(), NO_CALLBACK, false ); // must not throw
    }

    @Test
    public void streamFilesRecursiveMustNotSeeFilesLaterCreatedBaseDirectory() throws Exception
    {
        PageSwapperFactory factory = createSwapperFactory();
        File base = baseDirectory();
        File a = new File( base, "a" );
        File b = new File( base, "b" );
        createSwapperAndFile( factory, a ).close(); // note that we don't create 'b' at this point
        Stream<FileHandle> stream = factory.streamFilesRecursive( base ); // stream takes a snapshot of file tree
        createSwapperAndFile( factory, b ).close(); // 'b' now exists, but it's too late to be included in snapshot
        assertThat( stream.map( FileHandle::getFile ).collect( toSet() ), containsInAnyOrder( a ) );
    }

    @Test
    public void streamFilesRecursiveMustNotSeeFilesRenamedIntoBaseDirectory() throws Exception
    {
        PageSwapperFactory factory = createSwapperFactory();
        File base = baseDirectory();
        File a = new File( base, "a" );
        File sub = new File( base, "sub" );
        mkdirs( sub );
        File x = new File( sub, "x" );
        createSwapperAndFile( factory, a ).close();
        createSwapperAndFile( factory, x ).close();
        File target = new File( base, "target" );
        Iterable<FileHandle> handles = factory.streamFilesRecursive( base )::iterator;
        Set<File> observedFiles = new HashSet<>();
        for ( FileHandle handle : handles )
        {
            File file = handle.getFile();
            observedFiles.add( file );
            if ( file.equals( x ) )
            {
                handle.rename( target );
            }
        }
        assertThat( observedFiles, containsInAnyOrder( a, x ) );
    }

    @Test
    public void streamFilesRecursiveMustNotSeeFilesRenamedIntoSubDirectory() throws Exception
    {
        PageSwapperFactory factory = createSwapperFactory();
        File base = baseDirectory();
        File a = new File( base, "a" );
        File sub = new File( base, "sub" );
        mkdirs( sub );
        File target = new File( sub, "target" );
        createSwapperAndFile( factory, a ).close();
        Iterable<FileHandle> handles = factory.streamFilesRecursive( base )::iterator;
        Set<File> observedFiles = new HashSet<>();
        for ( FileHandle handle : handles )
        {
            File file = handle.getFile();
            observedFiles.add( file );
            if ( file.equals( a ) )
            {
                handle.rename( target );
            }
        }
        assertThat( observedFiles, containsInAnyOrder( a ) );
    }

    @Test
    public void streamFilesRecursiveSourceFileMustNotExistAfterRename() throws Exception
    {
        PageSwapperFactory factory = createSwapperFactory();
        File base = baseDirectory();
        File a = new File( base, "a" );
        File b = new File( base, "b" );
        createSwapperAndFile( factory, a ).close();
        FileHandle handle = factory.streamFilesRecursive( a ).findAny().get();
        handle.rename( b );
        expectedException.expect( NoSuchFileException.class );
        createSwapper( factory, a, cachePageSize(), NO_CALLBACK, false ); // throws because 'a' no longer exists
    }

    @Test
    public void streamFilesRecursiveRenameMustNotChangeSourceFileContents() throws Exception
    {
        PageSwapperFactory factory = createSwapperFactory();
        File base = baseDirectory();
        File a = new File( base, "a" );
        File b = new File( base, "b" );
        ByteBufferPage page = createPage();
        PageSwapper swapper = createSwapperAndFile( factory, a );
        long expectedValue = 0xdeadbeeffefefeL;
        page.putLong( expectedValue, 0 );
        swapper.write( 0, page );
        clear( page );
        swapper.close();
        FileHandle handle = factory.streamFilesRecursive( a ).findAny().get();
        handle.rename( b );
        swapper = createSwapper( factory, b, cachePageSize(), NO_CALLBACK, false );
        swapper.read( 0, page );
        long actualValue = page.getLong( 0 );
        assertThat( actualValue, is( expectedValue ) );
    }

    @Test
    public void streamFilesRecursiveRenameMustNotChangeSourceFileContentsWithReplaceExisting()
            throws Exception
    {
        PageSwapperFactory factory = createSwapperFactory();
        File base = baseDirectory();
        File a = new File( base, "a" );
        File b = new File( base, "b" );
        ByteBufferPage page = createPage();
        PageSwapper swapper = createSwapperAndFile( factory, a );
        long expectedValue = 0xdeadbeeffefefeL;
        page.putLong( expectedValue, 0 );
        swapper.write( 0, page );
        clear( page );
        swapper.close();
        swapper = createSwapperAndFile( factory, b );
        page.putLong( ThreadLocalRandom.current().nextLong(), 0 );
        swapper.write( 0, page );
        swapper.close();
        clear( page );
        FileHandle handle = factory.streamFilesRecursive( a ).findAny().get();
        handle.rename( b, REPLACE_EXISTING );
        swapper = createSwapper( factory, b, cachePageSize(), NO_CALLBACK, false );
        swapper.read( 0, page );
        long actualValue = page.getLong( 0 );
        assertThat( actualValue, is( expectedValue ) );
    }
}
