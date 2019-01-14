/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.io.pagecache;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.io.mem.MemoryAllocator;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings( "OptionalGetWithoutIsPresent" )
public abstract class PageSwapperTest
{
    public static final PageEvictionCallback NO_CALLBACK = filePageId -> {};
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
    private final MemoryAllocator mman = MemoryAllocator.createAllocator( "32 KiB", new LocalMemoryTracker() );

    protected abstract PageSwapperFactory swapperFactory();

    protected abstract void mkdirs( File dir ) throws IOException;

    protected abstract File baseDirectory() throws IOException;

    protected abstract boolean isRootAccessible();

    protected final PageSwapperFactory createSwapperFactory()
    {
        PageSwapperFactory factory = swapperFactory();
        openedFactories.add( factory );
        return factory;
    }

    protected int cachePageSize()
    {
        return cachePageSize;
    }

    protected long createPage( int cachePageSize )
    {
        long address = mman.allocateAligned( cachePageSize + Integer.BYTES, 1 );
        UnsafeUtil.putInt( address, cachePageSize );
        return address + Integer.BYTES;
    }

    protected long createPage()
    {
        return createPage( cachePageSize() );
    }

    protected void clear( long address )
    {
        byte b = (byte) 0;
        for ( int i = 0; i < cachePageSize(); i++ )
        {
            UnsafeUtil.putByte( address + i, b );
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

    protected long sizeOfAsLong( long page )
    {
        return sizeOfAsInt( page );
    }

    protected int sizeOfAsInt( long page )
    {
        return UnsafeUtil.getInt( page - Integer.BYTES );
    }

    protected void putInt( long address, int offset, int value )
    {
        UnsafeUtil.putInt( address + offset, value );
    }

    protected int getInt( long address, int offset )
    {
        return UnsafeUtil.getInt( address + offset );
    }

    protected void putLong( long address, int offset, long value )
    {
        UnsafeUtil.putLong( address + offset, value );
    }

    protected long getLong( long address, int offset )
    {
        return UnsafeUtil.getLong( address + offset );
    }

    protected byte getByte( long address, int offset )
    {
        return UnsafeUtil.getByte( address + offset );
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

    private long write( PageSwapper swapper, int filePageId, long address ) throws IOException
    {
        return swapper.write( filePageId, address );
    }

    private long read( PageSwapper swapper, int filePageId, int bufferSize, long address ) throws IOException
    {
        return swapper.read( filePageId, address, bufferSize );
    }

    private long read( PageSwapper swapper, long startFilePageId, long[] pages, int arrayOffset, int length )
            throws IOException
    {
        if ( pages.length == 0 )
        {
            return 0;
        }
        int bufferSize = sizeOfAsInt( pages[0] );
        return swapper.read( startFilePageId, pages, bufferSize, arrayOffset, length );
    }

    private long write( PageSwapper swapper, long startFilePageId, long[] pages, int arrayOffset, int length )
            throws IOException
    {
        if ( pages.length == 0 )
        {
            return 0;
        }
        return swapper.write( startFilePageId, pages, arrayOffset, length );
    }

    @Test
    public void readMustNotSwallowInterrupts() throws Exception
    {
        File file = file( "a" );

        long page = createPage();
        putInt( page, 0, 1 );
        PageSwapperFactory swapperFactory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( swapperFactory, file );

        assertThat( write( swapper, 0, page ), is( sizeOfAsLong( page ) ) );
                putInt( page, 0, 0 );
        Thread.currentThread().interrupt();

        assertThat( read( swapper, 0, sizeOfAsInt( page ), page ), is( sizeOfAsLong( page ) ) );
        assertTrue( Thread.currentThread().isInterrupted() );
        assertThat( getInt( page, 0 ), is( 1 ) );

        assertThat( read( swapper, 0, sizeOfAsInt( page ), page ), is( sizeOfAsLong( page ) ) );
        assertTrue( Thread.currentThread().isInterrupted() );
        assertThat( getInt( page, 0 ), is( 1 ) );
    }

    @Test
    public void vectoredReadMustNotSwallowInterrupts() throws Exception
    {
        File file = file( "a" );

        long page = createPage();
        putInt( page, 0, 1 );
        PageSwapperFactory swapperFactory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( swapperFactory, file );

        assertThat( write( swapper, 0, page ), is( sizeOfAsLong( page ) ) );
                putInt( page, 0, 0 );
        Thread.currentThread().interrupt();

        assertThat( read( swapper, 0, new long[]{page}, 0, 1 ), is( sizeOfAsLong( page ) ) );
        assertTrue( Thread.currentThread().isInterrupted() );
        assertThat( getInt( page, 0 ), is( 1 ) );

        assertThat( read( swapper, 0, new long[] {page}, 0, 1 ), is( sizeOfAsLong( page ) ) );
        assertTrue( Thread.currentThread().isInterrupted() );
        assertThat( getInt( page, 0 ), is( 1 ) );
    }

    @Test
    public void writeMustNotSwallowInterrupts() throws Exception
    {
        File file = file( "a" );

        long page = createPage();
        putInt( page, 0, 1 );
        PageSwapperFactory swapperFactory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( swapperFactory, file );

        Thread.currentThread().interrupt();

        assertThat( write( swapper, 0, page ), is( sizeOfAsLong( page ) ) );
        assertTrue( Thread.currentThread().isInterrupted() );

        putInt( page, 0, 0 );
        assertThat( read( swapper, 0, sizeOfAsInt( page ), page ), is( sizeOfAsLong( page ) ) );
        assertThat( getInt( page, 0 ), is( 1 ) );

        assertThat( write( swapper, 0, page ), is( sizeOfAsLong( page ) ) );
        assertTrue( Thread.currentThread().isInterrupted() );

        putInt( page, 0, 0 );
        assertThat( read( swapper, 0, sizeOfAsInt( page ), page ), is( sizeOfAsLong( page ) ) );
        assertThat( getInt( page, 0 ), is( 1 ) );
    }

    @Test
    public void vectoredWriteMustNotSwallowInterrupts() throws Exception
    {
        File file = file( "a" );

        long page = createPage();
        putInt( page, 0, 1 );
        PageSwapperFactory swapperFactory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( swapperFactory, file );

        Thread.currentThread().interrupt();

        assertThat( write( swapper, 0, new long[] {page}, 0, 1 ), is( sizeOfAsLong( page ) ) );
        assertTrue( Thread.currentThread().isInterrupted() );

        putInt( page, 0, 0 );
        assertThat( read( swapper, 0, sizeOfAsInt( page ), page ), is( sizeOfAsLong( page ) ) );
        assertThat( getInt( page, 0 ), is( 1 ) );

        assertThat( write( swapper, 0, new long[]{page}, 0, 1 ), is( sizeOfAsLong( page ) ) );
        assertTrue( Thread.currentThread().isInterrupted() );

        putInt( page, 0, 0 );
        assertThat( read( swapper, 0, sizeOfAsInt( page ), page ), is( sizeOfAsLong( page ) ) );
        assertThat( getInt( page, 0 ), is( 1 ) );
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

        long page = createPage();
        putLong( page, 0, X );
        putLong( page, 8, Y );
        putInt( page, 16, Z );
        write( swapper, 0, page );

        Thread.currentThread().interrupt();

        read( swapper, 0, sizeOfAsInt( page ), page );

        // Clear the interrupted flag and assert that it was still raised
        assertTrue( Thread.interrupted() );

        assertThat( getLong( page, 0 ), is( X ) );
        assertThat( getLong( page, 8 ), is( Y ) );
        assertThat( getInt( page, 16 ), is( Z ) );

        // This must not throw because we should still have a usable channel
        swapper.force();
    }

    @Test
    public void mustReopenChannelWhenVectoredReadFailsWithAsynchronousCloseException() throws Exception
    {
        File file = file( "a" );
        PageSwapperFactory swapperFactory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( swapperFactory, file );

        long page = createPage();
        putLong( page, 0, X );
        putLong( page, 8, Y );
        putInt( page, 16, Z );
        write( swapper, 0, page );

        Thread.currentThread().interrupt();

        read( swapper, 0, new long[]{page}, 0, 1 );

        // Clear the interrupted flag and assert that it was still raised
        assertTrue( Thread.interrupted() );

        assertThat( getLong( page, 0 ), is( X ) );
        assertThat( getLong( page, 8 ), is( Y ) );
        assertThat( getInt( page, 16 ), is( Z ) );

        // This must not throw because we should still have a usable channel
        swapper.force();
    }

    @Test
    public void mustReopenChannelWhenWriteFailsWithAsynchronousCloseException() throws Exception
    {
        long page = createPage();
        putLong( page, 0, X );
        putLong( page, 8, Y );
        putInt( page, 16, Z );
        File file = file( "a" );

        PageSwapperFactory swapperFactory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( swapperFactory, file );

        Thread.currentThread().interrupt();

        write( swapper, 0, page );

        // Clear the interrupted flag and assert that it was still raised
        assertTrue( Thread.interrupted() );

        // This must not throw because we should still have a usable channel
        swapper.force();

        clear( page );
        read( swapper, 0, sizeOfAsInt( page ), page );
        assertThat( getLong( page, 0 ), is( X ) );
        assertThat( getLong( page, 8 ), is( Y ) );
        assertThat( getInt( page, 16 ), is( Z ) );
    }

    @Test
    public void mustReopenChannelWhenVectoredWriteFailsWithAsynchronousCloseException() throws Exception
    {
        long page = createPage();
        putLong( page, 0, X );
        putLong( page, 8, Y );
        putInt( page, 16, Z );
        File file = file( "a" );

        PageSwapperFactory swapperFactory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( swapperFactory, file );

        Thread.currentThread().interrupt();

        write( swapper, 0, new long[] {page}, 0, 1 );

        // Clear the interrupted flag and assert that it was still raised
        assertTrue( Thread.interrupted() );

        // This must not throw because we should still have a usable channel
        swapper.force();

        clear( page );
        read( swapper, 0, sizeOfAsInt( page ), page );
        assertThat( getLong( page, 0 ), is( X ) );
        assertThat( getLong( page, 8 ), is( Y ) );
        assertThat( getInt( page, 16 ), is( Z ) );
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

        long page = createPage();
        PageSwapperFactory swapperFactory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( swapperFactory, file );
        write( swapper, 0, page );
        swapper.close();

        try
        {
            read( swapper, 0, sizeOfAsInt( page ), page );
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

        long page = createPage();
        PageSwapperFactory swapperFactory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( swapperFactory, file );
        write( swapper, 0, page );
        swapper.close();

        try
        {
            read( swapper, 0, new long[] {page}, 0, 1 );
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

        long page = createPage();
        PageSwapperFactory swapperFactory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( swapperFactory, file );
        swapper.close();

        try
        {
            write( swapper, 0, page );
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

        long page = createPage();
        PageSwapperFactory swapperFactory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( swapperFactory, file );
        swapper.close();

        try
        {
            write( swapper, 0, new long[] {page}, 0, 1 );
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

        long page = createPage();
        clear( page );
        putLong( page, 0, X );
        write( swapperA, 0, page );
        putLong( page, 8, Y );
        write( swapperB, 0, page );

        clear( page );
        assertThat( getLong( page, 0 ), is( 0L ) );
        assertThat( getLong( page, 8 ), is( 0L ) );

        read( swapperA, 0, sizeOfAsInt( page ), page );

        assertThat( getLong( page, 0 ), is( X ) );
        assertThat( getLong( page, 8 ), is( 0L ) );
    }

    @Test
    public void mustRunEvictionCallbackOnEviction() throws Exception
    {
        final AtomicLong callbackFilePageId = new AtomicLong();
        PageEvictionCallback callback = callbackFilePageId::set;
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapper( factory, file, cachePageSize(), callback, true );
        swapper.evicted( 42 );
        assertThat( callbackFilePageId.get(), is( 42L ) );
    }

    @Test
    public void mustNotIssueEvictionCallbacksAfterSwapperHasBeenClosed() throws Exception
    {
        final AtomicBoolean gotCallback = new AtomicBoolean();
        PageEvictionCallback callback = filePageId -> gotCallback.set( true );
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapper( factory, file, cachePageSize(), callback, true );
        swapper.close();
        swapper.evicted( 42 );
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
        long page = createPage();
        putLong( page, 0, X );
        write( pageSwapper, 0, page );

        clear( page );
        read( pageSwapper, 0, sizeOfAsInt( page ), page );

        assertThat( getLong( page, 0 ), is( X ) );
    }

    @Test
    public void truncatedFilesMustBeEmpty() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file );

        assertThat( swapper.getLastPageId(), is( -1L ) );

        long page = createPage();
        putInt( page, 0, 0xcafebabe );
        write( swapper, 10, page );
        clear( page );
        read( swapper, 10, sizeOfAsInt( page ), page );
        assertThat( getInt( page, 0 ), is( 0xcafebabe ) );
        assertThat( swapper.getLastPageId(), is( 10L ) );

        swapper.close();
        swapper = createSwapper( factory, file, cachePageSize(), NO_CALLBACK, false );
        clear( page );
        read( swapper, 10, sizeOfAsInt( page ), page );
        assertThat( getInt( page, 0 ), is( 0xcafebabe ) );
        assertThat( swapper.getLastPageId(), is( 10L ) );

        swapper.truncate();
        clear( page );
        read( swapper, 10, sizeOfAsInt( page ), page );
        assertThat( getInt( page, 0 ), is( 0 ) );
        assertThat( swapper.getLastPageId(), is( -1L ) );

        swapper.close();
        swapper = createSwapper( factory, file, cachePageSize(), NO_CALLBACK, false );
        clear( page );
        read( swapper, 10, sizeOfAsInt( page ), page );
        assertThat( getInt( page, 0 ), is( 0 ) );
        assertThat( swapper.getLastPageId(), is( -1L ) );

        swapper.close();
    }

    @Test
    public void positionedVectoredWriteMustFlushAllBuffersInOrder() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        long pageA = createPage( 4 );
        long pageB = createPage( 4 );
        long pageC = createPage( 4 );
        long pageD = createPage( 4 );

        putInt( pageA, 0, 2 );
        putInt( pageB, 0, 3 );
        putInt( pageC, 0, 4 );
        putInt( pageD, 0, 5 );

        write( swapper, 1, new long[]{pageA, pageB, pageC, pageD}, 0, 4 );

        long result = createPage( 4 );

        read( swapper, 0, sizeOfAsInt( result ), result );
        assertThat( getInt( result, 0 ), is( 0 ) );
        putInt( result, 0, 0 );
        assertThat( read( swapper, 1, sizeOfAsInt( result ), result ), is( 4L ) );
        assertThat( getInt( result, 0 ), is( 2 ) );
        putInt( result, 0, 0 );
        assertThat( read( swapper, 2, sizeOfAsInt( result ), result ), is( 4L ) );
        assertThat( getInt( result, 0 ), is( 3 ) );
        putInt( result, 0, 0 );
        assertThat( read( swapper, 3, sizeOfAsInt( result ), result ), is( 4L ) );
        assertThat( getInt( result, 0 ), is( 4 ) );
        putInt( result, 0, 0 );
        assertThat( read( swapper, 4, sizeOfAsInt( result ), result ), is( 4L ) );
        assertThat( getInt( result, 0 ), is( 5 ) );
        putInt( result, 0, 0 );
        assertThat( read( swapper, 5, sizeOfAsInt( result ), result ), is( 0L ) );
        assertThat( getInt( result, 0 ), is( 0 ) );
    }

    @Test
    public void positionedVectoredReadMustFillAllBuffersInOrder() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        long output = createPage();

        putInt( output, 0, 2 );
        write( swapper, 1, output );
        putInt( output, 0, 3 );
        write( swapper, 2, output );
        putInt( output, 0, 4 );
        write( swapper, 3, output );
        putInt( output, 0, 5 );
        write( swapper, 4, output );

        long pageA = createPage( 4 );
        long pageB = createPage( 4 );
        long pageC = createPage( 4 );
        long pageD = createPage( 4 );

        // Read 4 pages of 4 bytes each
        assertThat( read( swapper, 1, new long[]{pageA, pageB, pageC, pageD}, 0, 4 ), is( 4 * 4L ) );

        assertThat( getInt( pageA, 0 ), is( 2 ) );
        assertThat( getInt( pageB, 0 ), is( 3 ) );
        assertThat( getInt( pageC, 0 ), is( 4 ) );
        assertThat( getInt( pageD, 0 ), is( 5 ) );
    }

    @Test
    public void positionedVectoredReadFromEmptyFileMustFillPagesWithZeros() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        long page = createPage( 4 );
        putInt( page, 0, 1 );
        assertThat( read( swapper, 0, new long[]{page}, 0, 1 ), is( 0L ) );
        assertThat( getInt( page, 0 ), is( 0 ) );
    }

    @Test
    public void positionedVectoredReadBeyondEndOfFileMustFillPagesWithZeros() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        long output = createPage( 4 );
        putInt( output, 0, 0xFFFF_FFFF );
        write( swapper, 0, new long[]{output, output, output}, 0, 3 );

        long pageA = createPage( 4 );
        long pageB = createPage( 4 );
        putInt( pageA, 0, -1 );
        putInt( pageB, 0, -1 );
        assertThat( read( swapper, 3, new long[]{pageA, pageB}, 0, 2 ), is( 0L ) );
        assertThat( getInt( pageA, 0 ), is( 0 ) );
        assertThat( getInt( pageB, 0 ), is( 0 ) );
    }

    @Test
    public void positionedVectoredReadWhereLastPageExtendBeyondEndOfFileMustHaveRemainderZeroFilled() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        long output = createPage( 4 );
        putInt( output, 0, 0xFFFF_FFFF );
        write( swapper, 0, new long[]{output, output, output, output, output}, 0, 5 );
        swapper.close();

        swapper = createSwapper( factory, file, 8, NO_CALLBACK, false );
        long pageA = createPage( 8 );
        long pageB = createPage( 8 );
        putLong( pageA, 0, X );
        putLong( pageB, 0, Y );
        assertThat( read( swapper, 1, new long[]{pageA, pageB}, 0, 2 ), isOneOf( 12L, 16L ) );
        assertThat( getLong( pageA, 0 ), is( 0xFFFF_FFFF_FFFF_FFFFL ) );

//        assertThat( getLong( 0, pageB ), is( 0xFFFF_FFFF_0000_0000L ) );
        assertThat( getByte( pageB, 0 ), is( (byte) 0xFF ) );
        assertThat( getByte( pageB, 1 ), is( (byte) 0xFF ) );
        assertThat( getByte( pageB, 2 ), is( (byte) 0xFF ) );
        assertThat( getByte( pageB, 3 ), is( (byte) 0xFF ) );
        assertThat( getByte( pageB, 4 ), is( (byte) 0x00 ) );
        assertThat( getByte( pageB, 5 ), is( (byte) 0x00 ) );
        assertThat( getByte( pageB, 6 ), is( (byte) 0x00 ) );
        assertThat( getByte( pageB, 7 ), is( (byte) 0x00 ) );
    }

    @Test
    public void positionedVectoredReadWhereSecondLastPageExtendBeyondEndOfFileMustHaveRestZeroFilled() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        long output = createPage( 4 );
        putInt( output, 0, 1 );
        write( swapper, 0, output );
        putInt( output, 0, 2 );
        write( swapper, 1, output );
        putInt( output, 0, 3 );
        write( swapper, 2, output );
        swapper.close();

        swapper = createSwapper( factory, file, 8, NO_CALLBACK, false );
        long pageA = createPage( 8 );
        long pageB = createPage( 8 );
        long pageC = createPage( 8 );
        putInt( pageA, 0, -1 );
        putInt( pageB, 0, -1 );
        putInt( pageC, 0, -1 );
        assertThat( read( swapper, 0, new long[]{pageA, pageB, pageC}, 0, 3 ), isOneOf( 12L, 16L ) );
        assertThat( getInt( pageA, 0 ), is( 1 ) );
        assertThat( getInt( pageA, 4 ), is( 2 ) );
        assertThat( getInt( pageB, 0 ), is( 3 ) );
        assertThat( getInt( pageB, 4 ), is( 0 ) );
        assertThat( getLong( pageC, 0 ), is( 0L ) );
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
        long output = createPage( 4 );
        for ( int i = 0; i < pageCount; i++ )
        {
            putInt( output, 0, i + 1 );
            write( swapper, i, output );
        }

        Callable<Void> work = () ->
        {
            ThreadLocalRandom rng = ThreadLocalRandom.current();
            long[] pages = new long[10];
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
                    long bytesRead = read( swapper, startFilePageId, pages, 0, pages.length );
                    assertThat( bytesRead, is( pages.length * 4L ) );
                    for ( int j = 0; j < pages.length; j++ )
                    {
                        int expectedValue = (int) (1 + j + startFilePageId);
                        int actualValue = getInt( pages[j], 0 );
                        assertThat( actualValue, is( expectedValue ) );
                    }
                }
                else
                {
                    // Do write
                    for ( int j = 0; j < pages.length; j++ )
                    {
                        int value = (int) (1 + j + startFilePageId);
                        putInt( pages[j], 0, value );
                    }
                    assertThat( write( swapper, startFilePageId, pages, 0, pages.length ), is( pages.length * 4L ) );
                }
            }
            return null;
        };

        int threads = 8;
        ExecutorService executor = Executors.newFixedThreadPool( threads, r ->
        {
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

        long pageA = createPage( 4 );
        long pageB = createPage( 4 );
        long pageC = createPage( 4 );
        long pageD = createPage( 4 );

        putInt( pageA, 0, 1 );
        putInt( pageB, 0, 2 );
        putInt( pageC, 0, 3 );
        putInt( pageD, 0, 4 );

        long[] pages = {pageA, pageB, pageC, pageD};
        long bytesWritten = write( swapper, 0, pages, 0, 4 );
        assertThat( bytesWritten, is( 16L ) );

        putInt( pageA, 0, 5 );
        putInt( pageB, 0, 6 );
        putInt( pageC, 0, 7 );
        putInt( pageD, 0, 8 );

        long bytesRead = read( swapper, 1, pages, 1, 2 );
        assertThat( bytesRead, is( 8L ) );

        int[] actualValues = {getInt( pageA, 0 ), getInt( pageB, 0 ), getInt( pageC, 0 ), getInt(

                pageD, 0 )};
        int[] expectedValues = {5, 2, 3, 8};
        assertThat( actualValues, is( expectedValues ) );
    }

    @Test
    public void positionedVectoredWriteMustWorkOnSubsequenceOfGivenArray() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        long pageA = createPage( 4 );
        long pageB = createPage( 4 );
        long pageC = createPage( 4 );
        long pageD = createPage( 4 );

        putInt( pageA, 0, 1 );
        putInt( pageB, 0, 2 );
        putInt( pageC, 0, 3 );
        putInt( pageD, 0, 4 );

        long[] pages = {pageA, pageB, pageC, pageD};
        long bytesWritten = write( swapper, 0, pages, 0, 4 );
        assertThat( bytesWritten, is( 16L ) );

        putInt( pageB, 0, 6 );
        putInt( pageC, 0, 7 );

        bytesWritten = write( swapper, 1, pages, 1, 2 );
        assertThat( bytesWritten, is( 8L ) );

        putInt( pageA, 0, 0 );
        putInt( pageB, 0, 0 );
        putInt( pageC, 0, 0 );
        putInt( pageD, 0, 0 );

        long bytesRead = read( swapper, 0, pages, 0, 4 );
        assertThat( bytesRead, is( 16L ) );

        int[] actualValues = {getInt( pageA, 0 ), getInt( pageB, 0 ), getInt( pageC, 0 ), getInt(
                pageD, 0 )};
        int[] expectedValues = {1, 6, 7, 4};
        assertThat( actualValues, is( expectedValues ) );
    }

    @Test
    public void mustThrowNullPointerExceptionFromReadWhenPageArrayIsNull() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        long page = createPage( 4 );

        write( swapper, 0, new long[]{page, page, page, page}, 0, 4 );

        try
        {
            read( swapper, 0, null, 0, 4 );
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
            write( swapper, 0, null, 0, 4 );
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
        read( swapper, -1, sizeOfAsInt( createPage( 4 ) ), createPage( 4 ) );
    }

    @Test
    public void writeMustThrowForNegativeFilePageIds() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        expectedException.expect( IOException.class );
        write( swapper, -1, createPage( 4 ) );
    }

    @Test
    public void vectoredReadMustThrowForNegativeFilePageIds() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        expectedException.expect( IOException.class );
        read( swapper, -1, new long[]{createPage( 4 ), createPage( 4 )}, 0, 2 );
    }

    @Test
    public void vectoredWriteMustThrowForNegativeFilePageIds() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        expectedException.expect( IOException.class );
        write( swapper, -1, new long[] {createPage( 4 ), createPage( 4 )}, 0, 2 );
    }

    @Test
    public void vectoredReadMustThrowForNegativeArrayOffsets() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        long[] pages = {createPage( 4 ), createPage( 4 )};
        write( swapper, 0, pages, 0, 2 );
        expectedException.expect( ArrayIndexOutOfBoundsException.class );
        read( swapper, 0, pages, -1, 2 );
    }

    @Test
    public void vectoredWriteMustThrowForNegativeArrayOffsets() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        long[] pages = {createPage( 4 ), createPage( 4 )};
        expectedException.expect( ArrayIndexOutOfBoundsException.class );
        write( swapper, 0, pages, -1, 2 );
    }

    @Test
    public void vectoredReadMustThrowWhenLengthGoesBeyondArraySize() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        long[] pages = {createPage( 4 ), createPage( 4 )};
        write( swapper, 0, pages, 0, 2 );
        expectedException.expect( ArrayIndexOutOfBoundsException.class );
        read( swapper, 0, pages, 1, 2 );
    }

    @Test
    public void vectoredWriteMustThrowWhenLengthGoesBeyondArraySize() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        long[] pages = {createPage( 4 ), createPage( 4 )};
        expectedException.expect( ArrayIndexOutOfBoundsException.class );
        write( swapper, 0, pages, 1, 2 );
    }

    @Test
    public void vectoredReadMustThrowWhenArrayOffsetIsEqualToArrayLength() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        long[] pages = {createPage( 4 ), createPage( 4 )};
        write( swapper, 0, pages, 0, 2 );
        expectedException.expect( ArrayIndexOutOfBoundsException.class );
        read( swapper, 0, pages, 2, 1 );
    }

    @Test
    public void vectoredWriteMustThrowWhenArrayOffsetIsEqualToArrayLength() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        long[] pages = {createPage( 4 ), createPage( 4 )};
        expectedException.expect( ArrayIndexOutOfBoundsException.class );
        write( swapper, 0, pages, 2, 1 );
    }

    @Test
    public void vectoredReadMustThrowWhenArrayOffsetIsGreaterThanArrayLength() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        long[] pages = {createPage( 4 ), createPage( 4 )};
        write( swapper, 0, pages, 0, 2 );
        expectedException.expect( ArrayIndexOutOfBoundsException.class );
        read( swapper, 0, pages, 3, 1 );
    }

    @Test
    public void vectoredWriteMustThrowWhenArrayOffsetIsGreaterThanArrayLength() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        long[] pages = {createPage( 4 ), createPage( 4 )};
        expectedException.expect( ArrayIndexOutOfBoundsException.class );
        write( swapper, 0, pages, 3, 1 );
    }

    @Test
    public void vectoredReadMustReadNothingWhenLengthIsZero() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        long pageA = createPage( 4 );
        long pageB = createPage( 4 );
        putInt( pageA, 0, 1 );
        putInt( pageB, 0, 2 );
        long[] pages = {pageA, pageB};
        write( swapper, 0, pages, 0, 2 );
        putInt( pageA, 0, 3 );
        putInt( pageB, 0, 4 );
        read( swapper, 0, pages, 0, 0 );

        int[] expectedValues = {3, 4};
        int[] actualValues = {getInt( pageA, 0 ), getInt( pageB, 0 )};
        assertThat( actualValues, is( expectedValues ) );
    }

    @Test
    public void vectoredWriteMustReadNothingWhenLengthIsZero() throws Exception
    {
        File file = file( "file" );
        PageSwapperFactory factory = createSwapperFactory();
        PageSwapper swapper = createSwapperAndFile( factory, file, 4 );

        long pageA = createPage( 4 );
        long pageB = createPage( 4 );
        putInt( pageA, 0, 1 );
        putInt( pageB, 0, 2 );
        long[] pages = {pageA, pageB};
        write( swapper, 0, pages, 0, 2 );
        putInt( pageA, 0, 3 );
        putInt( pageB, 0, 4 );
        write( swapper, 0, pages, 0, 0 );
        read( swapper, 0, pages, 0, 2 );

        int[] expectedValues = {1, 2};
        int[] actualValues = {getInt( pageA, 0 ), getInt( pageB, 0 )};
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
}
