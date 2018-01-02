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
package org.neo4j.io.pagecache;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.io.pagecache.impl.ByteBufferPage;
import org.neo4j.test.TargetDirectory;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class PageSwapperTest
{
    public static final PageEvictionCallback NO_CALLBACK = new PageEvictionCallback()
    {
        @Override
        public void onEvict( long pageId, Page page )
        {
        }
    };
    public static final long X = 0xcafebabedeadbeefl;
    public static final long Y = X ^ (X << 1);
    public static final int Z = 0xfefefefe;

    protected static final int cachePageSize = 32;

    protected abstract PageSwapperFactory swapperFactory() throws Exception;

    protected abstract void mkdirs( File dir ) throws IOException;

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
            PageSwapperFactory swapperFactory,
            File file,
            int filePageSize,
            PageEvictionCallback callback,
            boolean createIfNotExist ) throws IOException
    {
        PageSwapper swapper = swapperFactory.createPageSwapper( file, filePageSize, callback, createIfNotExist );
        openedSwappers.add( swapper );
        return swapper;
    }

    private File file( String filename ) throws IOException
    {
        File file = testDir.file( filename );
        mkdirs( file.getParentFile() );
        return file;
    }

    @Rule
    public final TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );

    private final ConcurrentLinkedQueue<PageSwapper> openedSwappers = new ConcurrentLinkedQueue<>();

    @Before
    @After
    public void clearStrayInterrupts()
    {
        Thread.interrupted();
    }

    @After
    public void closeOpenedPageSwappers() throws IOException
    {
        IOException exception = null;
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
        PageSwapperFactory swapperFactory = swapperFactory();
        PageSwapper swapper = createSwapper( swapperFactory, file, cachePageSize(), NO_CALLBACK, true );

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

    private long sizeOf( ByteBufferPage page )
    {
        return page.size();
    }

    @Test
    public void vectoredReadMustNotSwallowInterrupts() throws Exception
    {
        File file = file( "a" );

        ByteBufferPage page = createPage();
        page.putInt( 1, 0 );
        PageSwapperFactory swapperFactory = swapperFactory();
        PageSwapper swapper = createSwapper( swapperFactory, file, cachePageSize(), NO_CALLBACK, true );

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
        PageSwapperFactory swapperFactory = swapperFactory();
        PageSwapper swapper = createSwapper( swapperFactory, file, cachePageSize(), NO_CALLBACK, true );

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
        PageSwapperFactory swapperFactory = swapperFactory();
        PageSwapper swapper = createSwapper( swapperFactory, file, cachePageSize(), NO_CALLBACK, true );

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

        PageSwapperFactory swapperFactory = swapperFactory();
        PageSwapper swapper = createSwapper( swapperFactory, file, cachePageSize(), NO_CALLBACK, true );

        Thread.currentThread().interrupt();
        swapper.force();
        assertTrue( Thread.currentThread().isInterrupted() );
    }

    @Test
    public void mustReopenChannelWhenReadFailsWithAsynchronousCloseException() throws Exception
    {
        File file = file( "a" );
        PageSwapperFactory swapperFactory = swapperFactory();
        PageSwapper swapper = createSwapper( swapperFactory, file, cachePageSize(), NO_CALLBACK, true );

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
        PageSwapperFactory swapperFactory = swapperFactory();
        PageSwapper swapper = createSwapper( swapperFactory, file, cachePageSize(), NO_CALLBACK, true );

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

        PageSwapperFactory swapperFactory = swapperFactory();
        PageSwapper swapper = createSwapper( swapperFactory, file, cachePageSize(), NO_CALLBACK, true );

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

        PageSwapperFactory swapperFactory = swapperFactory();
        PageSwapper swapper = createSwapper( swapperFactory, file, cachePageSize(), NO_CALLBACK, true );

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

        PageSwapperFactory swapperFactory = swapperFactory();
        PageSwapper swapper = createSwapper( swapperFactory, file, cachePageSize(), NO_CALLBACK, true );

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
        PageSwapperFactory swapperFactory = swapperFactory();
        PageSwapper swapper = createSwapper( swapperFactory, file, cachePageSize(), NO_CALLBACK, true );
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
        PageSwapperFactory swapperFactory = swapperFactory();
        PageSwapper swapper = createSwapper( swapperFactory, file, cachePageSize(), NO_CALLBACK, true );
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
        PageSwapperFactory swapperFactory = swapperFactory();
        PageSwapper swapper = createSwapper( swapperFactory, file, cachePageSize(), NO_CALLBACK, true );
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
        PageSwapperFactory swapperFactory = swapperFactory();
        PageSwapper swapper = createSwapper( swapperFactory, file, cachePageSize(), NO_CALLBACK, true );
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

        PageSwapperFactory swapperFactory = swapperFactory();
        PageSwapper swapper = createSwapper( swapperFactory, file, cachePageSize(), NO_CALLBACK, true );
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
        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapperA =
                createSwapper( factory, fileA, cachePageSize(), NO_CALLBACK, true );
        PageSwapper swapperB =
                createSwapper( factory, fileB, cachePageSize(), NO_CALLBACK, true );

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
        PageEvictionCallback callback = new PageEvictionCallback()
        {
            @Override
            public void onEvict( long filePageId, Page page )
            {
                callbackFilePageId.set( filePageId );
                callbackPage.set( page );
            }
        };
        File file = file( "file" );
        PageSwapperFactory factory = swapperFactory();
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
        PageEvictionCallback callback = new PageEvictionCallback()
        {
            @Override
            public void onEvict( long filePageId, Page page )
            {
                gotCallback.set( true );
            }
        };
        File file = file( "file" );
        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapper = createSwapper( factory, file, cachePageSize(), callback, true );
        Page page = createPage();
        swapper.close();
        swapper.evicted( 42, page );
        assertFalse( gotCallback.get() );
    }

    @Test( expected = NoSuchFileException.class )
    public void mustThrowExceptionIfFileDoesNotExist() throws Exception
    {
        PageSwapperFactory factory = swapperFactory();
        createSwapper( factory, file( "does not exist" ), cachePageSize(), NO_CALLBACK, false );
    }

    @Test
    public void mustCreateNonExistingFileWithCreateFlag() throws Exception
    {
        PageSwapperFactory factory = swapperFactory();
        PageSwapper pageSwapper =
                createSwapper( factory, file( "does not exist" ), cachePageSize(), NO_CALLBACK, true );

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
        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapper = createSwapper( factory, file, cachePageSize(), NO_CALLBACK, true );

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
        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapper = createSwapper( factory, file, 4, NO_CALLBACK, true );

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
        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapper = createSwapper( factory, file, 4, NO_CALLBACK, true );

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
        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapper = createSwapper( factory, file, 4, NO_CALLBACK, true );

        ByteBufferPage page = createPage( 4 );
        page.putInt( 1, 0 );
        assertThat( swapper.read( 0, new Page[]{page}, 0, 1 ), is( 0L ) );
        assertThat( page.getInt( 0 ), is( 0 ) );
    }

    @Test
    public void positionedVectoredReadBeyondEndOfFileMustFillPagesWithZeros() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapper = createSwapper( factory, file, 4, NO_CALLBACK, true );

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
        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapper = createSwapper( factory, file, 4, NO_CALLBACK, true );

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
        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapper = createSwapper( factory, file, 4, NO_CALLBACK, true );

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
        PageSwapperFactory factory = swapperFactory();
        final PageSwapper swapper = createSwapper( factory, file, 4, NO_CALLBACK, true );
        final int pageCount = 100;
        final int iterations = 20000;
        final CountDownLatch startLatch = new CountDownLatch( 1 );
        ByteBufferPage output = createPage( 4 );
        for ( int i = 0; i < pageCount; i++ )
        {
            output.putInt( i+1, 0 );
            swapper.write( i, output );
        }

        Callable<Void> work = new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
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
            }
        };

        int threads = 8;
        ExecutorService executor = Executors.newFixedThreadPool( threads, new ThreadFactory()
        {
            @Override
            public Thread newThread( Runnable r )
            {
                Thread thread = Executors.defaultThreadFactory().newThread( r );
                thread.setDaemon( true );
                return thread;
            }
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
        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapper = createSwapper( factory, file, 4, NO_CALLBACK, true );

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
        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapper = createSwapper( factory, file, 4, NO_CALLBACK, true );

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
        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapper = createSwapper( factory, file, 4, NO_CALLBACK, true );

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
        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapper = createSwapper( factory, file, 4, NO_CALLBACK, true );

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
        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapper = createSwapper( factory, file, 4, NO_CALLBACK, true );

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
        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapper = createSwapper( factory, file, 4, NO_CALLBACK, true );

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

    @Test( expected = IOException.class )
    public void readMustThrowForNegativeFilePageIds() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapper = createSwapper( factory, file, 4, NO_CALLBACK, true );

        swapper.read( -1, createPage( 4 ) );
    }

    @Test( expected = IOException.class )
    public void writeMustThrowForNegativeFilePageIds() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapper = createSwapper( factory, file, 4, NO_CALLBACK, true );

        swapper.write( -1, createPage( 4 ) );
    }

    @Test( expected = IOException.class )
    public void vectoredReadMustThrowForNegativeFilePageIds() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapper = createSwapper( factory, file, 4, NO_CALLBACK, true );

        swapper.read( -1, new Page[]{createPage( 4 ), createPage( 4 )}, 0, 2 );
    }

    @Test( expected = IOException.class )
    public void vectoredWriteMustThrowForNegativeFilePageIds() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapper = createSwapper( factory, file, 4, NO_CALLBACK, true );

        swapper.write( -1, new Page[] {createPage( 4 ), createPage( 4 )}, 0, 2 );
    }

    @Test( expected = ArrayIndexOutOfBoundsException.class )
    public void vectoredReadMustThrowForNegativeArrayOffsets() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapper = createSwapper( factory, file, 4, NO_CALLBACK, true );

        Page[] pages = {createPage( 4 ), createPage( 4 )};
        swapper.write( 0, pages, 0, 2 );
        swapper.read( 0, pages, -1, 2 );
    }

    @Test( expected = ArrayIndexOutOfBoundsException.class )
    public void vectoredWriteMustThrowForNegativeArrayOffsets() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapper = createSwapper( factory, file, 4, NO_CALLBACK, true );

        Page[] pages = {createPage( 4 ), createPage( 4 )};
        swapper.write( 0, pages, -1, 2 );
    }

    @Test( expected = ArrayIndexOutOfBoundsException.class )
    public void vectoredReadMustThrowWhenLengthGoesBeyondArraySize() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapper = createSwapper( factory, file, 4, NO_CALLBACK, true );

        Page[] pages = {createPage( 4 ), createPage( 4 )};
        swapper.write( 0, pages, 0, 2 );
        swapper.read( 0, pages, 1, 2 );
    }

    @Test( expected = ArrayIndexOutOfBoundsException.class )
    public void vectoredWriteMustThrowWhenLengthGoesBeyondArraySize() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapper = createSwapper( factory, file, 4, NO_CALLBACK, true );

        Page[] pages = {createPage( 4 ), createPage( 4 )};
        swapper.write( 0, pages, 1, 2 );
    }

    @Test( expected = ArrayIndexOutOfBoundsException.class )
    public void vectoredReadMustThrowWhenArrayOffsetIsEqualToArrayLength() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapper = createSwapper( factory, file, 4, NO_CALLBACK, true );

        Page[] pages = {createPage( 4 ), createPage( 4 )};
        swapper.write( 0, pages, 0, 2 );
        swapper.read( 0, pages, 2, 1 );
    }

    @Test( expected = ArrayIndexOutOfBoundsException.class )
    public void vectoredWriteMustThrowWhenArrayOffsetIsEqualToArrayLength() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapper = createSwapper( factory, file, 4, NO_CALLBACK, true );

        Page[] pages = {createPage( 4 ), createPage( 4 )};
        swapper.write( 0, pages, 2, 1 );
    }

    @Test( expected = ArrayIndexOutOfBoundsException.class )
    public void vectoredReadMustThrowWhenArrayOffsetIsGreaterThanArrayLength() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapper = createSwapper( factory, file, 4, NO_CALLBACK, true );

        Page[] pages = {createPage( 4 ), createPage( 4 )};
        swapper.write( 0, pages, 0, 2 );
        swapper.read( 0, pages, 3, 1 );
    }

    @Test( expected = ArrayIndexOutOfBoundsException.class )
    public void vectoredWriteMustThrowWhenArrayOffsetIsGreaterThanArrayLength() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapper = createSwapper( factory, file, 4, NO_CALLBACK, true );

        Page[] pages = {createPage( 4 ), createPage( 4 )};
        swapper.write( 0, pages, 3, 1 );
    }

    @Test
    public void vectoredReadMustReadNothingWhenLengthIsZero() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapper = createSwapper( factory, file, 4, NO_CALLBACK, true );

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
        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapper = createSwapper( factory, file, 4, NO_CALLBACK, true );

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
}
