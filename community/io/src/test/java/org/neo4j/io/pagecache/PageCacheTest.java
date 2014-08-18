/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.neo4j.graphdb.mockfs.DelegatingFileSystemAbstraction;
import org.neo4j.graphdb.mockfs.DelegatingStoreChannel;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.neo4j.io.pagecache.PagedFile.PF_EXCLUSIVE_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_NO_FAULT;
import static org.neo4j.io.pagecache.PagedFile.PF_NO_GROW;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_LOCK;
import static org.neo4j.test.ByteArrayMatcher.byteArray;

public abstract class PageCacheTest<T extends RunnablePageCache>
{
    protected static ExecutorService executor;

    protected final File file = new File( "a" );

    protected int recordSize = 9;
    protected int recordCount = 1060;
    protected int maxPages = 20;
    protected int pageCachePageSize = 20;
    protected int filePageSize = 18;
    protected int recordsPerFilePage = filePageSize / recordSize;
    protected ByteBuffer bufA = ByteBuffer.allocate( recordSize );

    protected EphemeralFileSystemAbstraction fs;

    @BeforeClass
    public static void startExecutor()
    {
        executor = Executors.newCachedThreadPool();
    }

    @AfterClass
    public static void stopExecutor()
    {
        executor.shutdown();
    }

    protected abstract T createPageCache(
            FileSystemAbstraction fs,
            int maxPages,
            int pageSize,
            PageCacheMonitor monitor );

    protected abstract void tearDownPageCache( T pageCache ) throws IOException;

    private T pageCache;
    private Future<?> pageCacheFuture;

    protected final T getPageCache(
            FileSystemAbstraction fs,
            int maxPages,
            int pageSize,
            PageCacheMonitor monitor ) throws IOException
    {
        if ( pageCache != null )
        {
            tearDownPageCache( pageCache );
            pageCacheFuture.cancel( true );
        }
        pageCache = createPageCache( fs, maxPages, pageSize, monitor );
        pageCacheFuture = executor.submit( pageCache );
        return pageCache;
    }

    @Before
    public void setUp()
    {
        Thread.interrupted(); // Clear stray interrupts
        fs = new EphemeralFileSystemAbstraction();
    }

    @After
    public void tearDown() throws IOException
    {
        Thread.interrupted(); // Clear stray interrupts
        if ( pageCache != null )
        {
            tearDownPageCache( pageCache );
        }
        fs.shutdown();
    }

    /**
     * Verifies the records on the current page of the cursor.
     * <p>
     * This does the do-while-retry loop internally.
     */
    private void verifyRecordsMatchExpected( PageCursor cursor ) throws IOException
    {
        ByteBuffer expectedPageContents = ByteBuffer.allocate( filePageSize );
        ByteBuffer actualPageContents = ByteBuffer.allocate( filePageSize );
        byte[] record = new byte[recordSize];
        long pageId = cursor.getCurrentPageId();
        for ( int i = 0; i < recordsPerFilePage; i++ )
        {
            long recordId = (pageId * recordsPerFilePage) + i;
            expectedPageContents.position( recordSize * i );
            generateRecordForId( recordId, expectedPageContents.slice() );
            do
            {
                cursor.getBytes( record );
            } while ( cursor.shouldRetry() );
            actualPageContents.position( recordSize * i );
            actualPageContents.put( record );
        }
        assertRecord( pageId, actualPageContents, expectedPageContents );
    }

    private void assertRecord( long pageId, ByteBuffer actualPageContents, ByteBuffer expectedPageContents )
    {
        byte[] actualBytes = actualPageContents.array();
        byte[] expectedBytes = expectedPageContents.array();
        int estimatedPageId = estimateId( actualBytes );
        assertThat(
                "Page id: " + pageId + " " +
                        "(based on record data, it should have been " +
                        estimatedPageId + ", a difference of " +
                        Math.abs( pageId - estimatedPageId ) + ")",
                actualBytes,
                byteArray( expectedBytes ) );
    }

    private int estimateId( byte[] record )
    {
        return ByteBuffer.wrap( record ).getInt() - 1;
    }

    private void writeRecords( PageCursor cursor )
    {
        cursor.setOffset( 0 );
        for ( int i = 0; i < recordsPerFilePage; i++ )
        {
            long recordId = (cursor.getCurrentPageId() * recordsPerFilePage) + i;
            generateRecordForId( recordId, bufA );
            cursor.putBytes( bufA.array() );
        }
    }

    private void generateFileWithRecords(
            File file,
            int recordCount,
            int recordSize ) throws IOException
    {
        StoreChannel channel = fs.open( file, "w" );
        ByteBuffer buf = ByteBuffer.allocate( recordSize );
        for ( int i = 0; i < recordCount; i++ )
        {
            generateRecordForId( i, buf );
            channel.writeAll( buf );
        }
        channel.close();
    }

    private static void generateRecordForId( long id, ByteBuffer buf )
    {
        buf.position( 0 );
        int x = (int) (id + 1);
        buf.putInt( x );
        while ( buf.position() < buf.limit() )
        {
            x++;
            buf.put( (byte) (x & 0xFF) );
        }
        buf.position( 0 );
    }

    private Runnable $unmap( final PageCache pageCache, final File file )
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    pageCache.unmap( file );
                }
                catch ( IOException e )
                {
                    throw new AssertionError( e );
                }
            }
        };
    }

    private Thread fork( Runnable runnable )
    {
        String name = "Forked-from-" + Thread.currentThread().getName();
        Thread thread = new Thread( runnable, name );
        thread.start();
        return thread;
    }

    private void awaitTheadState( Thread thread, long maxWaitMillis, Thread.State first, Thread.State... rest )
    {
        EnumSet<Thread.State> set = EnumSet.of( first, rest );
        long deadline = maxWaitMillis + System.currentTimeMillis();
        Thread.State currentState;
        do
        {
            currentState = thread.getState();
            if ( System.currentTimeMillis() > deadline )
            {
                throw new AssertionError(
                        "Timed out waiting for thread state of <" +
                                set + ">: " + thread + " (state = " +
                                thread.getState() + ")" );
            }
        }
        while ( !set.contains( currentState ) );
    }

    @Test
    public void mustReportConfiguredMaxPages() throws IOException
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        assertThat( pageCache.maxCachedPages(), is( maxPages ) );
    }

    @Test
    public void mustReportConfiguredCachePageSize() throws IOException
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        assertThat( pageCache.pageSize(), is( pageCachePageSize ) );
    }

    @Test( timeout = 1000 )
    public void mustReadExistingData() throws IOException
    {
        generateFileWithRecords( file, recordCount, recordSize );

        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );

        int recordId = 0;
        try ( PageCursor cursor = pagedFile.io( 0L, PF_SHARED_LOCK ) )
        {
            while ( cursor.next() )
            {
                verifyRecordsMatchExpected( cursor );
                recordId += recordsPerFilePage;
            }
        }
        finally
        {
            cache.unmap( file );
        }

        assertThat( recordId, is( recordCount ) );
    }

    @Test( timeout = 1000 )
    public void mustScanInTheMiddleOfTheFile() throws IOException
    {
        long startPage = 10;
        long endPage = (recordCount / recordsPerFilePage) - 10;
        generateFileWithRecords( file, recordCount, recordSize );

        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );

        int recordId = (int) (startPage * recordsPerFilePage);
        try ( PageCursor cursor = pagedFile.io( startPage, PF_SHARED_LOCK ) )
        {
            while ( cursor.next() && cursor.getCurrentPageId() < endPage )
            {
                verifyRecordsMatchExpected( cursor );
                recordId += recordsPerFilePage;
            }
        }
        finally
        {
            cache.unmap( file );
        }

        assertThat( recordId, is( recordCount - (10 * recordsPerFilePage) ) );
    }

    @Test( timeout = 10000 )
    public void writesFlushedFromPageFileMustBeExternallyObservable() throws IOException
    {
        ByteBuffer buf = ByteBuffer.allocate( recordSize );
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );

        long startPageId = 0;
        long endPageId = recordCount / recordsPerFilePage;
        try ( PageCursor cursor = pagedFile.io( startPageId, PF_EXCLUSIVE_LOCK ) )
        {
            while ( cursor.getCurrentPageId() < endPageId && cursor.next() )
            {
                do
                {
                    writeRecords( cursor );
                } while ( cursor.shouldRetry() );
            }
        }

        pagedFile.flush();

        StoreChannel channel = fs.open( file, "r" );
        ByteBuffer observation = ByteBuffer.allocate( recordSize );
        for ( int i = 0; i < recordCount; i++ )
        {
            generateRecordForId( i, buf );
            observation.position( 0 );
            channel.read( observation );
            assertRecord( i, observation, buf );
        }
        channel.close();
        pageCache.unmap( file );
    }

    @Test( timeout = 60000 )
    public void repeatablyWritesFlushedFromPageFileMustBeExternallyObservable() throws IOException
    {
        // This test exposed a race in the EphemeralFileSystemAbstraction, that made the previous
        // writesFlushedFromPageFileMustBeExternallyObservable test flaky.
        for ( int i = 0; i < 100; i++ )
        {
            tearDown();
            setUp();
            try
            {
                writesFlushedFromPageFileMustBeExternallyObservable();
            }
            catch ( Throwable e )
            {
                System.err.println( "iteration " + i );
                System.err.flush();
                throw e;
            }
        }
    }

    @Test( timeout = 30000 )
    public void writesFlushedFromPageFileMustBeObservableEvenWhenRacingWithEviction() throws IOException
    {
        PageCache cache = getPageCache( fs, 20, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, pageCachePageSize );

        long startPageId = 0;
        long endPageId = 21;
        int iterations = 10000;
        int shortsPerPage = pageCachePageSize / 2;

        try
        {
            for ( int i = 1; i <= iterations; i++ )
            {
                try ( PageCursor cursor = pagedFile.io( startPageId, PF_EXCLUSIVE_LOCK ) )
                {
                    while ( cursor.getCurrentPageId() < endPageId && cursor.next() )
                    {
                        for ( int j = 0; j < shortsPerPage; j++ )
                        {
                            cursor.putShort( (short) i );
                        }
                    }
                }

                // There are 20 pages in the cache and we've overwritten 20 pages.
                // This means eviction has probably fallen behind and is likely
                // running concurrently right now.
                // Therefor, a flush right now would have a high chance of racing
                // with eviction.
                pagedFile.flush();

                // Race or not, a flush should still put all changes in storage,
                // so we should be able to verify the contents of the file.
                try ( DataInputStream stream = new DataInputStream( fs.openAsInputStream( file ) ) )
                {
                    for ( int j = 0; j < shortsPerPage; j++ )
                    {
                        int value = stream.readShort();
                        assertThat( "short pos = " + j + ", iteration = " + i, value, is( i ) );
                    }
                }
            }
        }
        finally
        {
            pageCache.unmap( file );
        }
    }

    @Test( timeout = 1000 )
    public void writesFlushedFromPageCacheMustBeExternallyObservable() throws IOException
    {
        ByteBuffer buf = ByteBuffer.allocate( recordSize );
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );

        long startPageId = 0;
        long endPageId = recordCount / recordsPerFilePage;
        try ( PageCursor cursor = pagedFile.io( startPageId, PF_EXCLUSIVE_LOCK ) )
        {
            while ( cursor.getCurrentPageId() < endPageId && cursor.next() )
            {
                do
                {
                    writeRecords( cursor );
                } while ( cursor.shouldRetry() );
            }
        }

        cache.unmap( file ); // unmapping implies flushing

        StoreChannel channel = fs.open( file, "r" );
        ByteBuffer observation = ByteBuffer.allocate( recordSize );
        for ( int i = 0; i < recordCount; i++ )
        {
            generateRecordForId( i, buf );
            observation.position( 0 );
            channel.read( observation );
            assertRecord( i, observation, buf );
        }
        channel.close();
    }

    @Test( timeout = 1000 )
    public void writesToPagesMustNotBleedIntoAdjacentPages() throws IOException
    {
        fs.create( file ).close();

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = pageCache.map( file, filePageSize );

        // Write the pageId+1 to every byte in the file
        try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            for ( int i = 1; i <= 100; i++ )
            {
                assertTrue( cursor.next() );
                for ( int j = 0; j < filePageSize; j++ )
                {
                    cursor.putByte( (byte) i );
                }
            }
        }
        pageCache.unmap( file );

        // Then check that none of those writes ended up in adjecent pages
        InputStream inputStream = fs.openAsInputStream( file );
        for ( int i = 1; i <= 100; i++ )
        {
            for ( int j = 0; j < filePageSize; j++ )
            {
                assertThat( inputStream.read(), is( i ) );
            }
        }
        inputStream.close();
    }

    @Test( timeout = 1000 )
    public void firstNextCallMustReturnFalseWhenTheFileIsEmptyAndNoGrowIsSpecified() throws IOException
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = pageCache.map( file, filePageSize );

        try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
        {
            assertFalse( cursor.next() );
        }
        finally
        {
            pageCache.unmap( file );
        }
    }

    @Test( timeout = 1000 )
    public void nextMustReturnTrueThenFalseWhenThereIsOnlyOnePageInTheFileAndNoGrowIsSpecified() throws IOException
    {
        int numberOfRecordsToGenerate = recordsPerFilePage; // one page worth
        generateFileWithRecords( file, numberOfRecordsToGenerate, recordSize );

        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );

        try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
        {
            assertTrue( cursor.next() );
            verifyRecordsMatchExpected( cursor );
            assertFalse( cursor.next() );
        }
        cache.unmap( file );
    }

    @Test( timeout = 1000 )
    public void closingWithoutCallingNextMustLeavePageUnpinnedAndUntouched() throws IOException
    {
        int numberOfRecordsToGenerate = recordsPerFilePage; // one page worth
        generateFileWithRecords( file, numberOfRecordsToGenerate, recordSize );

        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );

        try
        {
            //noinspection EmptyTryBlock
            try ( PageCursor ignore = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
            {
                // No call to next, so the page should never get pinned in the first place, nor
                // should the page corruption take place.
            }

            try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK ) )
            {
                // We didn't call next before, so the page and its records should still be fine
                cursor.next();
                verifyRecordsMatchExpected( cursor );
            }
        }
        finally
        {
            pageCache.unmap( file );
        }
    }

    @Test( timeout = 1000 )
    public void rewindMustStartScanningOverFromTheBeginning() throws IOException
    {
        int numberOfRewindsToTest = 10;
        generateFileWithRecords( file, recordCount, recordSize );
        int actualPageCounter = 0;
        int filePageCount = recordCount / recordsPerFilePage;
        int expectedPageCounterResult = numberOfRewindsToTest * filePageCount;

        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );

        try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK ) )
        {
            for ( int i = 0; i < numberOfRewindsToTest; i++ )
            {
                while ( cursor.next() )
                {

                    verifyRecordsMatchExpected( cursor );
                    actualPageCounter++;
                }
                cursor.rewind();
            }
        }
        finally
        {
            pageCache.unmap( file );
        }

        assertThat( actualPageCounter, is( expectedPageCounterResult ) );
    }

    @Test( timeout = 1000 )
    public void mustCloseFileChannelWhenTheLastHandleIsUnmapped() throws Exception
    {
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        cache.map( file, filePageSize );
        cache.map( file, filePageSize );
        cache.unmap( file );
        cache.unmap( file );
        fs.assertNoOpenFiles();
    }

    @Test( timeout = 1000 )
    public void dirtyPagesMustBeFlushedWhenTheCacheIsClosed() throws IOException
    {
        ByteBuffer buf = ByteBuffer.allocate( recordSize );
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = pageCache.map( file, filePageSize );

        long startPageId = 0;
        long endPageId = recordCount / recordsPerFilePage;
        try ( PageCursor cursor = pagedFile.io( startPageId, PF_EXCLUSIVE_LOCK ) )
        {
            while ( cursor.getCurrentPageId() < endPageId && cursor.next() )
            {
                do
                {
                    writeRecords( cursor );
                } while ( cursor.shouldRetry() );
            }
        }
        finally
        {
            pageCache.unmap( file );
            pageCache.close();
        }

        StoreChannel channel = fs.open( file, "r" );
        ByteBuffer observation = ByteBuffer.allocate( recordSize );
        for ( int i = 0; i < recordCount; i++ )
        {
            generateRecordForId( i, buf );
            observation.position( 0 );
            channel.read( observation );
            assertRecord( i, observation, buf );
        }
        channel.close();
    }

    @Test( timeout = 1000, expected = IllegalStateException.class )
    public void mappingFilesInClosedCacheMustThrow() throws IOException
    {
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        cache.close();
        cache.map( file, filePageSize );
    }

    @Test( timeout = 1000, expected = IllegalStateException.class )
    public void flushingClosedCacheMustThrow() throws IOException
    {
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        cache.close();
        cache.flush();
    }

    @Test( timeout = 1000, expected = IllegalArgumentException.class )
    public void mappingFileWithPageSizeGreaterThanCachePageSizeMustThrow() throws IOException
    {
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        cache.map( file, pageCachePageSize + 1 ); // this must throw
    }

    @Test( timeout = 1000 )
    public void mappingFileWithPageSizeEqualToCachePageSizeMustNotThrow() throws IOException
    {
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        cache.map( file, pageCachePageSize );// this must NOT throw
        cache.unmap( file );
    }

    @Test( timeout = 1000, expected = IllegalArgumentException.class )
    public void notSpecifyingAnyPfFlagsMustThrow() throws IOException
    {
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );
        try
        {
            pagedFile.io( 0, 0 ); // this must throw
        }
        finally
        {
            cache.unmap( file );
        }
    }

    @Test( timeout = 1000, expected = IllegalArgumentException.class )
    public void notSpecifyingAnyPfLockFlagsMustThrow() throws IOException
    {
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );
        try
        {
            pagedFile.io( 0, PF_NO_FAULT ); // this must throw
        }
        finally
        {
            cache.unmap( file );
        }
    }

    @Test( timeout = 1000, expected = IllegalArgumentException.class )
    public void specifyingBothSharedAndExclusiveLocksMustThrow() throws IOException
    {
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );
        try
        {
            pagedFile.io( 0, PF_EXCLUSIVE_LOCK | PF_SHARED_LOCK ); // this must throw
        }
        finally
        {
            cache.unmap( file );
        }
    }

    @Test( timeout = 1000 )
    public void mustNotPinPagesAfterNextReturnsFalse() throws Exception
    {
        final CountDownLatch startLatch = new CountDownLatch( 1 );
        final CountDownLatch unpinLatch = new CountDownLatch( 1 );
        final AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        generateFileWithRecords( file, recordsPerFilePage, recordSize );
        final PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        final PagedFile pagedFile = cache.map( file, filePageSize );

        Runnable runnable = new Runnable()
        {
            @Override
            public void run()
            {
                try ( PageCursor cursorA = pagedFile.io( 0, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
                {
                    assertTrue( cursorA.next() );
                    assertFalse( cursorA.next() );
                    startLatch.countDown();
                    unpinLatch.await();
                    cursorA.close();
                }
                catch ( Exception e )
                {
                    exceptionRef.set( e );
                }
            }
        };
        executor.submit( runnable );

        startLatch.await();
        try ( PageCursor cursorB = pagedFile.io( 1, PF_EXCLUSIVE_LOCK ) )
        {
            assertTrue( cursorB.next() );
            unpinLatch.countDown();
        }
        finally
        {
            pageCache.unmap( file );
        }
        Exception e = exceptionRef.get();
        if ( e != null )
        {
            throw new Exception( "Child thread got exception", e );
        }
    }

    @Test( timeout = 1000 )
    public void nextMustResetTheCursorOffset() throws IOException
    {
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );

        try ( PageCursor cursor = pagedFile.io( 0L, PF_EXCLUSIVE_LOCK ) )
        {
            assertTrue( cursor.next() );
            do
            {
                cursor.setOffset( 0 );
                cursor.putByte( (byte) 1 );
                cursor.putByte( (byte) 2 );
                cursor.putByte( (byte) 3 );
                cursor.putByte( (byte) 4 );
            } while ( cursor.shouldRetry() );
            assertTrue( cursor.next() );
            do
            {
                cursor.setOffset( 0 );
                cursor.putByte( (byte) 5 );
                cursor.putByte( (byte) 6 );
                cursor.putByte( (byte) 7 );
                cursor.putByte( (byte) 8 );
            } while ( cursor.shouldRetry() );
        }

        try ( PageCursor cursor = pagedFile.io( 0L, PF_EXCLUSIVE_LOCK ) )
        {
            byte[] bytes = new byte[4];
            assertTrue( cursor.next() );
            do
            {
                cursor.getBytes( bytes );
            } while ( cursor.shouldRetry() );
            assertThat( bytes, byteArray( new byte[]{1, 2, 3, 4} ) );
            assertTrue( cursor.next() );
            do
            {
                cursor.getBytes( bytes );
            } while ( cursor.shouldRetry() );
            assertThat( bytes, byteArray( new byte[]{5, 6, 7, 8} ) );
        }
        cache.unmap( file );
    }

    @Test( timeout = 1000 )
    public void nextMustAdvanceCurrentPageId() throws IOException
    {
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );

        try ( PageCursor cursor = pagedFile.io( 0L, PF_EXCLUSIVE_LOCK ) )
        {
            assertTrue( cursor.next() );
            assertThat( cursor.getCurrentPageId(), is( 0L ) );
            assertTrue( cursor.next() );
            assertThat( cursor.getCurrentPageId(), is( 1L ) );
        }
        finally
        {
            cache.unmap( file );
        }
    }

    @Test( timeout = 1000 )
    public void nextToSpecificPageIdMustAdvanceFromThatPointOn() throws IOException
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = pageCache.map( file, filePageSize );

        try ( PageCursor cursor = pagedFile.io( 1L, PF_EXCLUSIVE_LOCK ) )
        {
            assertTrue( cursor.next() );
            assertThat( cursor.getCurrentPageId(), is( 1L ) );
            assertTrue( cursor.next( 4L ) );
            assertThat( cursor.getCurrentPageId(), is( 4L ) );
            assertTrue( cursor.next() );
            assertThat( cursor.getCurrentPageId(), is( 5L ) );
        }
        finally
        {
            pageCache.unmap( file );
        }
    }

    @Test( timeout = 1000 )
    public void currentPageIdIsUnboundBeforeFirstNextAndAfterRewind() throws IOException
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = pageCache.map( file, filePageSize );

        try ( PageCursor cursor = pagedFile.io( 0L, PF_EXCLUSIVE_LOCK ) )
        {
            assertThat( cursor.getCurrentPageId(), is( PageCursor.UNBOUND_PAGE_ID ) );
            assertTrue( cursor.next() );
            assertThat( cursor.getCurrentPageId(), is( 0L ) );
            cursor.rewind();
            assertThat( cursor.getCurrentPageId(), is( PageCursor.UNBOUND_PAGE_ID ) );
        }
        finally
        {
            pageCache.unmap( file );
        }
    }

    @Test( expected = NullPointerException.class )
    public void readingFromUnboundCursorMustThrow() throws IOException
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = pageCache.map( file, filePageSize );

        try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            cursor.getByte();
        }
        finally
        {
            pageCache.unmap( file );
        }
    }

    @Test( expected = NullPointerException.class )
    public void writingFromUnboundCursorMustThrow() throws IOException
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = pageCache.map( file, filePageSize );

        try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            cursor.putInt( 1 );
        }
        finally
        {
            pageCache.unmap( file );
        }
    }

    @Test( timeout = 1000 )
    public void lastPageMustBeAccessibleWithNoGrowSpecified() throws IOException
    {
        generateFileWithRecords( file, recordsPerFilePage * 2, recordSize );

        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );

        try
        {
            try ( PageCursor cursor = pagedFile.io( 0L, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
            {
                assertTrue( cursor.next() );
                assertTrue( cursor.next() );
                assertFalse( cursor.next() );
            }

            try ( PageCursor cursor = pagedFile.io( 0L, PF_SHARED_LOCK ) )
            {
                assertTrue( cursor.next() );
                assertTrue( cursor.next() );
                assertFalse( cursor.next() );
            }

            try ( PageCursor cursor = pagedFile.io( 1L, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
            {
                assertTrue( cursor.next() );
                assertFalse( cursor.next() );
            }

            try ( PageCursor cursor = pagedFile.io( 1L, PF_SHARED_LOCK ) )
            {
                assertTrue( cursor.next() );
                assertFalse( cursor.next() );
            }

            try ( PageCursor cursor = pagedFile.io( 2L, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
            {
                assertFalse( cursor.next() );
            }

            try ( PageCursor cursor = pagedFile.io( 2L, PF_SHARED_LOCK ) )
            {
                assertFalse( cursor.next() );
            }

            try ( PageCursor cursor = pagedFile.io( 3L, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
            {
                assertFalse( cursor.next() );
            }

            try ( PageCursor cursor = pagedFile.io( 3L, PF_SHARED_LOCK ) )
            {
                assertFalse( cursor.next() );
            }
        }
        finally
        {
            cache.unmap( file );
        }
    }

    @Test( timeout = 1000 )
    public void lastPageMustBeAccessibleWithNoGrowSpecifiedEvenIfLessThanFilePageSize() throws IOException
    {
        generateFileWithRecords( file, (recordsPerFilePage * 2) - 1, recordSize );

        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );

        try ( PageCursor cursor = pagedFile.io( 0L, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
        {
            assertTrue( cursor.next() );
            assertTrue( cursor.next() );
            assertFalse( cursor.next() );
        }

        try ( PageCursor cursor = pagedFile.io( 0L, PF_SHARED_LOCK ) )
        {
            assertTrue( cursor.next() );
            assertTrue( cursor.next() );
            assertFalse( cursor.next() );
        }

        try ( PageCursor cursor = pagedFile.io( 1L, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
        {
            assertTrue( cursor.next() );
            assertFalse( cursor.next() );
        }

        try ( PageCursor cursor = pagedFile.io( 1L, PF_SHARED_LOCK ) )
        {
            assertTrue( cursor.next() );
            assertFalse( cursor.next() );
        }

        try ( PageCursor cursor = pagedFile.io( 2L, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
        {
            assertFalse( cursor.next() );
        }

        try ( PageCursor cursor = pagedFile.io( 2L, PF_SHARED_LOCK ) )
        {
            assertFalse( cursor.next() );
        }

        try ( PageCursor cursor = pagedFile.io( 3L, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
        {
            assertFalse( cursor.next() );
        }

        try ( PageCursor cursor = pagedFile.io( 3L, PF_SHARED_LOCK ) )
        {
            assertFalse( cursor.next() );
        }

        cache.unmap( file );
    }

    @Test( timeout = 1000 )
    public void firstPageMustBeAccessibleWithNoGrowSpecifiedIfItIsTheOnlyPage() throws IOException
    {
        generateFileWithRecords( file, recordsPerFilePage, recordSize );

        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );

        try
        {
            try ( PageCursor cursor = pagedFile.io( 0L, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
            {
                assertTrue( cursor.next() );
                assertFalse( cursor.next() );
            }

            try ( PageCursor cursor = pagedFile.io( 0L, PF_SHARED_LOCK ) )
            {
                assertTrue( cursor.next() );
                assertFalse( cursor.next() );
            }

            try ( PageCursor cursor = pagedFile.io( 1L, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
            {
                assertFalse( cursor.next() );
            }

            try ( PageCursor cursor = pagedFile.io( 1L, PF_SHARED_LOCK ) )
            {
                assertFalse( cursor.next() );
            }
        }
        finally
        {
            pageCache.unmap( file );
        }
    }

    @Test( timeout = 1000 )
    public void firstPageMustBeAccessibleEvenIfTheFileIsNonEmptyButSmallerThanFilePageSize() throws IOException
    {
        generateFileWithRecords( file, 1, recordSize );

        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );

        try
        {
            try ( PageCursor cursor = pagedFile.io( 0L, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
            {
                assertTrue( cursor.next() );
                assertFalse( cursor.next() );
            }

            try ( PageCursor cursor = pagedFile.io( 0L, PF_SHARED_LOCK ) )
            {
                assertTrue( cursor.next() );
                assertFalse( cursor.next() );
            }

            try ( PageCursor cursor = pagedFile.io( 1L, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
            {
                assertFalse( cursor.next() );
            }

            try ( PageCursor cursor = pagedFile.io( 1L, PF_SHARED_LOCK ) )
            {
                assertFalse( cursor.next() );
            }
        }
        finally
        {
            pageCache.unmap( file );
        }
    }

    @Test( timeout = 1000 )
    public void firstPageMustNotBeAccessibleIfFileIsEmptyAndNoGrowSpecified() throws IOException
    {
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );

        try
        {
            try ( PageCursor cursor = pagedFile.io( 0L, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
            {
                assertFalse( cursor.next() );
                assertFalse( cursor.next() );
            }

            try ( PageCursor cursor = pagedFile.io( 0L, PF_SHARED_LOCK ) )
            {
                assertFalse( cursor.next() );
                assertFalse( cursor.next() );
            }

            try ( PageCursor cursor = pagedFile.io( 1L, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
            {
                assertFalse( cursor.next() );
            }

            try ( PageCursor cursor = pagedFile.io( 1L, PF_SHARED_LOCK ) )
            {
                assertFalse( cursor.next() );
            }
        }
        finally
        {
            pageCache.unmap( file );
        }
    }

    @Test( timeout = 1000 )
    public void newlyWrittenPagesMustBeAccessibleWithNoGrow() throws IOException
    {
        int initialPages = 1;
        int pagesToAdd = 3;
        generateFileWithRecords( file, recordsPerFilePage * initialPages, recordSize );

        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );

        try ( PageCursor cursor = pagedFile.io( 1L, PF_EXCLUSIVE_LOCK ) )
        {
            for ( int i = 0; i < pagesToAdd; i++ )
            {
                assertTrue( cursor.next() );
                do
                {
                    writeRecords( cursor );
                } while ( cursor.shouldRetry() );
            }
        }

        int pagesChecked = 0;
        try ( PageCursor cursor = pagedFile.io( 0L, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
        {
            while ( cursor.next() )
            {
                verifyRecordsMatchExpected( cursor );
                pagesChecked++;
            }
        }
        assertThat( pagesChecked, is( initialPages + pagesToAdd ) );

        pagesChecked = 0;
        try ( PageCursor cursor = pagedFile.io( 0L, PF_SHARED_LOCK ) )
        {
            while ( cursor.next() )
            {
                verifyRecordsMatchExpected( cursor );
                pagesChecked++;
            }
        }
        assertThat( pagesChecked, is( initialPages + pagesToAdd ) );
        cache.unmap( file );
    }

    @Test( timeout = 1000 )
    public void sharedLockImpliesNoGrow() throws IOException
    {
        int initialPages = 3;
        generateFileWithRecords( file, recordsPerFilePage * initialPages, recordSize );

        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );

        int pagesChecked = 0;
        try ( PageCursor cursor = pagedFile.io( 0L, PF_SHARED_LOCK ) )
        {
            while ( cursor.next() )
            {
                pagesChecked++;
            }
        }
        finally
        {
            cache.unmap( file );
        }
        assertThat( pagesChecked, is( initialPages ) );
    }

    @Test( timeout = 1000 )
    public void retryMustResetCursorOffset() throws Exception
    {
        // The general idea here, is that we have a page with a particular value in its 0th position.
        // We also have a thread that constantly writes to the middle of the page, so it modifies
        // the page, but does not change the value in the 0th position. This thread will in principle
        // mean that it is possible for a reader to get an inconsistent view and must retry.
        // We then check that every retry iteration will read the special value in the 0th position.
        // We repeat the experiment a couple of times to make sure we didn't succeed by chance.

        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        final PagedFile pagedFile = cache.map( file, filePageSize );
        final AtomicReference<Exception> caughtWriterException = new AtomicReference<>();
        final CountDownLatch startLatch = new CountDownLatch( 1 );
        final byte expectedByte = (byte) 13;

        try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            if ( cursor.next() )
            {
                do
                {
                    cursor.putByte( expectedByte );
                } while ( cursor.shouldRetry() );
            }
        }

        Runnable writer = new Runnable()
        {
            @Override
            public void run()
            {
                while ( !Thread.currentThread().isInterrupted() )
                {
                    try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
                    {
                        if ( cursor.next() )
                        {
                            do
                            {
                                cursor.setOffset( recordSize );
                                cursor.putByte( (byte) 14 );
                            } while ( cursor.shouldRetry() );
                        }
                        startLatch.countDown();
                    }
                    catch ( IOException e )
                    {
                        caughtWriterException.set( e );
                        throw new RuntimeException( e );
                    }
                }
            }
        };
        Future<?> writerFuture = executor.submit( writer );

        startLatch.await();

        for ( int i = 0; i < 1000; i++ )
        {
            try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK ) )
            {
                assertTrue( cursor.next() );
                do
                {
                    assertThat( cursor.getByte(), is( expectedByte ) );
                } while ( cursor.shouldRetry() );
            }
        }

        writerFuture.cancel( true );
        pageCache.unmap( file );
    }

    @Test( timeout = 1000 )
    public void nextWithPageIdMustAllowTraversingInReverse() throws IOException
    {
        generateFileWithRecords( file, recordCount, recordSize );
        long lastFilePageId = (recordCount / recordsPerFilePage) - 1;

        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );

        try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK ) )
        {
            for ( long currentPageId = lastFilePageId; currentPageId >= 0; currentPageId-- )
            {
                assertTrue( "next( currentPageId = " + currentPageId + " )",
                        cursor.next( currentPageId ) );
                assertThat( cursor.getCurrentPageId(), is( currentPageId ) );
                verifyRecordsMatchExpected( cursor );
            }
        }
        finally
        {
            pageCache.unmap( file );
        }
    }

    @Test( timeout = 1000 )
    public void nextWithPageIdMustReturnFalseIfPageIdIsBeyondFilePageRangeAndNoGrowSpecified() throws IOException
    {
        generateFileWithRecords( file, recordsPerFilePage * 2, recordSize );

        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );

        try
        {
            try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
            {
                assertFalse( cursor.next( 2 ) );
                assertTrue( cursor.next( 1 ) );
            }

            try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK ) )
            {
                assertFalse( cursor.next( 2 ) );
                assertTrue( cursor.next( 1 ) );
            }
        }
        finally
        {
            cache.unmap( file );
        }
    }

    @Test( timeout = 1000 )
    public void pagesAddedWithNextWithPageIdMustBeAccessibleWithNoGrowSpecified() throws IOException
    {
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );

        try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            assertTrue( cursor.next( 2 ) );
            do
            {
                writeRecords( cursor );
            } while ( cursor.shouldRetry() );
            assertTrue( cursor.next( 0 ) );
            do
            {
                writeRecords( cursor );
            } while ( cursor.shouldRetry() );
            assertTrue( cursor.next( 1 ) );
            do
            {
                writeRecords( cursor );
            } while ( cursor.shouldRetry() );
        }

        try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
        {
            while ( cursor.next() )
            {
                verifyRecordsMatchExpected( cursor );
            }
        }

        try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK ) )
        {
            while ( cursor.next() )
            {
                verifyRecordsMatchExpected( cursor );
            }
        }
        cache.unmap( file );
    }

    @Test( timeout = 60000 )
    public void readsAndWritesMustBeMutuallyConsistent() throws Exception
    {
        // The idea is this: have a range of pages and we set off a bunch of threads to
        // do writes within a small region of the page set. The writes they'll perform
        // is to fill a random page within the region, with the same random byte value.
        // We then have our main thread scan through all the pages over and over, and
        // check that all pages can be read consistently, such that all the bytes in a
        // given page have the same value. We do this check many times, because the
        // test is inherently about catching data races in the act.

        final int pageCount = 100;
        int writerThreads = 8;
        final CountDownLatch startLatch = new CountDownLatch( writerThreads );
        final CountDownLatch writersDoneLatch = new CountDownLatch( writerThreads );
        List<Future<?>> writers = new ArrayList<>();

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        final PagedFile pagedFile = pageCache.map( file, filePageSize );

        // zero-fill the file
        try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            for ( int i = 0; i < pageCount; i++ )
            {
                assertTrue( cursor.next() );
            }
        }

        Runnable writer = new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    int pageRangeMin = pageCount / 2;
                    int pageRangeMax = pageRangeMin + 5;
                    ThreadLocalRandom rng = ThreadLocalRandom.current();
                    int[] offsets = new int[filePageSize];
                    for ( int i = 0; i < offsets.length; i++ )
                    {
                        offsets[i] = i;
                    }

                    startLatch.countDown();

                    while ( !Thread.interrupted() )
                    {
                        byte value = (byte) rng.nextInt();
                        int pageId = rng.nextInt( pageRangeMin, pageRangeMax );
                        // shuffle offsets
                        for ( int i = 0; i < offsets.length; i++ )
                        {
                            int j = rng.nextInt( i, offsets.length );
                            int s = offsets[i];
                            offsets[i] = offsets[j];
                            offsets[j] = s;
                        }
                        // fill page
                        try ( PageCursor cursor = pagedFile.io( pageId, PF_EXCLUSIVE_LOCK ) )
                        {
                            if ( cursor.next() )
                            {
                                do
                                {
                                    for ( int i = 0; i < offsets.length; i++ )
                                    {
                                        cursor.setOffset( offsets[i] );
                                        cursor.putByte( value );
                                    }
                                } while ( cursor.shouldRetry() );
                            }
                        }
                    }
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
                finally
                {
                    writersDoneLatch.countDown();
                }
            }
        };

        for ( int i = 0; i < writerThreads; i++ )
        {
            writers.add( executor.submit( writer ) );
        }

        startLatch.await();

        try
        {
            for ( int i = 0; i < 2000; i++ )
            {
                int countedConsistentPageReads = 0;
                try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK ) )
                {
                    while ( cursor.next() )
                    {
                        boolean consistent;
                        do
                        {
                            consistent = true;
                            byte first = cursor.getByte();
                            for ( int j = 1; j < filePageSize; j++ )
                            {
                                byte b = cursor.getByte();
                                consistent = consistent && b == first;
                            }
                        } while ( cursor.shouldRetry() );
                        assertTrue( "checked consistency at itr " + i, consistent );
                        countedConsistentPageReads++;
                    }
                }
                assertThat( countedConsistentPageReads, is( pageCount ) );
            }

            for ( Future<?> future : writers )
            {
                if ( future.isDone() )
                {
                    future.get();
                }
                else
                {
                    future.cancel( true );
                }
            }
            writersDoneLatch.await();
        }
        finally
        {
            pageCache.unmap( file );
        }
    }

    @Test(timeout = 1000)
    public void writesOfDifferentUnitsMustHaveCorrectEndianess() throws Exception
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = pageCache.map( file, 20 );

        try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            assertTrue( cursor.next() );
            byte[] data = { 42, 43, 44, 45, 46 };

            cursor.putLong( 41 );          //  0+8 = 8
            cursor.putInt( 41 );           //  8+4 = 12
            cursor.putShort( (short) 41 ); // 12+2 = 14
            cursor.putByte( (byte) 41 );   // 14+1 = 15
            cursor.putBytes( data );       // 15+5 = 20
        }

        try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            assertTrue( cursor.next() );

            long a = cursor.getLong();  //  8
            int b = cursor.getInt();    // 12
            short c = cursor.getShort();// 14
            byte[] data = new byte[] {
                    cursor.getByte(),   // 15
                    cursor.getByte(),   // 16
                    cursor.getByte(),   // 17
                    cursor.getByte(),   // 18
                    cursor.getByte(),   // 19
                    cursor.getByte()    // 20
            };
            cursor.setOffset( 0 );
            cursor.putLong( 1 + a );
            cursor.putInt( 1 + b );
            cursor.putShort( (short) (1 + c) );
            for ( int i = 0; i < data.length; i++ )
            {
                byte d = data[i];
                d++;
                cursor.putByte( d );
            }
        }

        pageCache.unmap( file );

        StoreChannel channel = fs.open( file, "r" );
        ByteBuffer buf = ByteBuffer.allocate( 20 );
        channel.read( buf );
        buf.flip();

        assertThat( buf.getLong(), is( 42L ) );
        assertThat( buf.getInt(), is( 42 ) );
        assertThat( buf.getShort(), is( (short) 42 ) );
        assertThat( buf.get(), is( (byte) 42 ) );
        assertThat( buf.get(), is( (byte) 43 ) );
        assertThat( buf.get(), is( (byte) 44 ) );
        assertThat( buf.get(), is( (byte) 45 ) );
        assertThat( buf.get(), is( (byte) 46 ) );
        assertThat( buf.get(), is( (byte) 47 ) );
    }

    @Test( timeout = 1000, expected = IllegalArgumentException.class )
    public void mappingFileSecondTimeWithLesserPageSizeMustThrow() throws Exception
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        pageCache.map( file, filePageSize );
        try
        {
            pageCache.map( file, filePageSize - 1 );
        }
        finally
        {
            pageCache.unmap( file );
        }
    }

    @Test( timeout = 1000, expected = IllegalArgumentException.class )
    public void mappingFileSecondTimeWithGreaterPageSizeMutThrow() throws Exception
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        pageCache.map( file, filePageSize );
        try
        {
            pageCache.map( file, filePageSize + 1 );
        }
        finally
        {
            pageCache.unmap( file );
        }
    }

    @Test( timeout = 1000)
    public void mustNotFlushCleanPagesWhenEvicting() throws Exception
    {
        generateFileWithRecords( file, recordCount, recordSize );

        final AtomicBoolean observedWrite = new AtomicBoolean();
        FileSystemAbstraction fs = new DelegatingFileSystemAbstraction( this.fs ) {
            @Override
            public StoreChannel open( File fileName, String mode ) throws IOException
            {
                StoreChannel channel = super.open( fileName, mode );
                return new DelegatingStoreChannel( channel ) {
                    @Override
                    public int write( ByteBuffer src, long position ) throws IOException
                    {
                        observedWrite.set( true );
                        throw new IOException( "not allowed" );
                    }

                    @Override
                    public long write( ByteBuffer[] srcs, int offset, int length ) throws IOException
                    {
                        observedWrite.set( true );
                        throw new IOException( "not allowed" );
                    }

                    @Override
                    public void writeAll( ByteBuffer src, long position ) throws IOException
                    {
                        observedWrite.set( true );
                        throw new IOException( "not allowed" );
                    }

                    @Override
                    public void writeAll( ByteBuffer src ) throws IOException
                    {
                        observedWrite.set( true );
                        throw new IOException( "not allowed" );
                    }

                    @Override
                    public int write( ByteBuffer src ) throws IOException
                    {
                        observedWrite.set( true );
                        throw new IOException( "not allowed" );
                    }

                    @Override
                    public long write( ByteBuffer[] srcs ) throws IOException
                    {
                        observedWrite.set( true );
                        throw new IOException( "not allowed" );
                    }
                };
            }
        };
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = pageCache.map( file, filePageSize );

        try( PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK ) )
        {
            while ( cursor.next() )
            {
                verifyRecordsMatchExpected( cursor );
            }
        }
        pageCache.unmap( file );
        assertFalse( observedWrite.get() );
    }

    @Test( timeout = 1000 )
    public void evictionMustFlushPagesToTheRightFiles() throws IOException
    {
        generateFileWithRecords( file, recordCount, recordSize );

        int filePageSize2 = filePageSize - 3; // diff. page size just to be difficult
        long maxPageIdCursor1 = recordCount / recordsPerFilePage;
        File file2 = new File( "b" );
        OutputStream outputStream = fs.openAsOutputStream( file2, false );
        long file2sizeBytes = (maxPageIdCursor1 + 17) * filePageSize2;
        for ( int i = 0; i < file2sizeBytes; i++ )
        {
            // We will ues the page cache to change these 'a's into 'b's.
            outputStream.write( 'a' );
        }
        outputStream.flush();
        outputStream.close();

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );

        PagedFile pagedFile1 = pageCache.map( file, filePageSize );
        PagedFile pagedFile2 = pageCache.map( file2, filePageSize2 );

        try ( PageCursor cursor1 = pagedFile1.io( 0, PF_EXCLUSIVE_LOCK );
              PageCursor cursor2 = pagedFile2.io( 0, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
        {
            assertThat( cursor1, not( sameInstance( cursor2 ) ) );
            boolean moreWorkToDo;
            do {
                boolean cursorReady1 = cursor1.getCurrentPageId() < maxPageIdCursor1 && cursor1.next();
                boolean cursorReady2 = cursor2.next();
                moreWorkToDo = cursorReady1 || cursorReady2;

                if ( cursorReady1 )
                {
                    writeRecords( cursor1 );
                }

                if ( cursorReady2 )
                {
                    do {
                        for ( int i = 0; i < filePageSize2; i++ )
                        {
                            cursor2.putByte( (byte) 'b' );
                        }
                    }
                    while ( cursor2.shouldRetry() );
                }
            }
            while ( moreWorkToDo );
        }

        pageCache.unmap( file );
        pageCache.unmap( file2 );

        // Verify the file contents
        assertThat( fs.getFileSize( file2 ), is( file2sizeBytes ) );
        InputStream inputStream = fs.openAsInputStream( file2 );
        for ( int i = 0; i < file2sizeBytes; i++ )
        {
            int b = inputStream.read();
            assertThat( b, is( (int) 'b' ) );
        }
        assertThat( inputStream.read(), is( -1 ) );
        inputStream.close();

        StoreChannel channel = fs.open( file, "r" );
        ByteBuffer bufB = ByteBuffer.allocate( recordSize );
        for ( int i = 0; i < recordCount; i++ )
        {
            bufA.clear();
            channel.read( bufA );
            bufA.flip();
            bufB.clear();
            generateRecordForId( i, bufB );
            assertThat( bufB.array(), byteArray( bufA.array() ) );
        }
    }

    @Test( timeout = 1000 )
    public void monitorMustBeNotifiedAboutPinUnpinFaultAndEvictEventsWhenReading() throws IOException
    {
        CountingPageCacheMonitor monitor = new CountingPageCacheMonitor();
        generateFileWithRecords( file, recordCount, recordSize );

        getPageCache( fs, maxPages, pageCachePageSize, monitor );

        PagedFile pagedFile = pageCache.map( file, filePageSize );

        int countedPages = 0;
        try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK ) )
        {
            while ( cursor.next() )
            {
                assertTrue( cursor.next( cursor.getCurrentPageId() ) );
                countedPages++;
            }
        }

        assertThat( monitor.countPins(), is( countedPages * 2 ) );
        assertThat( monitor.countUnpins(), is( countedPages * 2 ) );
        assertThat( monitor.countTakenExclusiveLocks(), is( 0 ) );
        assertThat( monitor.countTakenSharedLocks(), is( countedPages * 2 ) );
        assertThat( monitor.countReleasedExclusiveLocks(), is( 0 ) );
        assertThat( monitor.countReleasedSharedLocks(), is( countedPages * 2 ) );
        
        // We might be unlucky and fault in the second next call, on the page
        // we brought up in the first next call. That's why we assert that we
        // have observed *at least* the countedPages number of faults.
        assertThat( monitor.countFaults(), greaterThanOrEqualTo( countedPages ) );
        assertThat( monitor.countEvictions(),
                both( greaterThanOrEqualTo( countedPages - maxPages ) )
                        .and( lessThan( countedPages ) ) );
        pageCache.unmap( file );
    }

    @Test( timeout = 1000 )
    public void monitorMustBeNotifiedAboutPinUnpinFaultFlushAndEvictionEventsWhenWriting() throws IOException
    {
        int pagesToGenerate = 142;
        CountingPageCacheMonitor monitor = new CountingPageCacheMonitor();
        fs.create( file ).close();

        getPageCache( fs, maxPages, pageCachePageSize, monitor );

        PagedFile pagedFile = pageCache.map( file, filePageSize );

        try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            for ( long i = 0; i < pagesToGenerate; i++ )
            {
                assertTrue( cursor.next() );
                assertThat( cursor.getCurrentPageId(), is( i ) );
                assertTrue( cursor.next( i ) );
                assertThat( cursor.getCurrentPageId(), is( i ) );

                writeRecords( cursor );
            }
        }
        finally
        {
            pageCache.unmap( file );
        }

        assertThat( monitor.countPins(), is( pagesToGenerate * 2 ) );
        assertThat( monitor.countTakenExclusiveLocks(), is( pagesToGenerate * 2 ) );
        assertThat( monitor.countTakenSharedLocks(), is( 0 ) );
        assertThat( monitor.countReleasedExclusiveLocks(), is( pagesToGenerate * 2 ) );
        assertThat( monitor.countReleasedSharedLocks(), is( 0 ) );
        assertThat( monitor.countUnpins(), is( pagesToGenerate * 2 ) );

        // We might be unlucky and fault in the second next call, on the page
        // we brought up in the first next call. That's why we assert that we
        // have observed *at least* the countedPages number of faults.
        assertThat( monitor.countFaults(), greaterThanOrEqualTo( pagesToGenerate ) );
        assertThat( monitor.countEvictions(),
                both( greaterThanOrEqualTo( pagesToGenerate - maxPages ) )
                        .and( lessThan( pagesToGenerate ) ) );

        // We use greaterThanOrEqualTo because we visit each page twice, and
        // that leaves a small window wherein we can race with eviction, have
        // the evictor flush the page, and then fault it back and mark it as
        // dirty again.
        assertThat( monitor.countFlushes(), greaterThanOrEqualTo( pagesToGenerate ) );
    }

    @Test
    public void lastPageIdOfEmptyFileIsMinusOne() throws IOException
    {
        fs.create( file ).close();

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );

        PagedFile pagedFile = pageCache.map( file, filePageSize );
        try
        {
            assertThat( pagedFile.getLastPageId(), is( -1L ) );
        }
        finally
        {
            pageCache.unmap( file );
        }
    }

    @Test
    public void lastPageIdOfFileWithOneByteIsZero() throws IOException
    {
        StoreChannel channel = fs.create( file );
        channel.write( ByteBuffer.wrap( new byte[]{1} ) );
        channel.close();

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );

        PagedFile pagedFile = pageCache.map( file, filePageSize );
        assertThat( pagedFile.getLastPageId(), is( 0L ) );
        pageCache.unmap( file );
    }

    @Test
    public void lastPageIdOfFileWithExactlyTwoPagesWorthOfDataIsOne() throws IOException
    {
        int twoPagesWorthOfRecords = recordsPerFilePage * 2;
        generateFileWithRecords( file, twoPagesWorthOfRecords, recordSize );

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );

        PagedFile pagedFile = pageCache.map( file, filePageSize );
        try
        {
            assertThat( pagedFile.getLastPageId(), is( 1L ) );
        }
        finally
        {
            pageCache.unmap( file );
        }
    }

    @Test
    public void lastPageIdOfFileWithExactlyTwoPagesAndOneByteWorthOfDataIsTwo() throws IOException
    {
        int twoPagesWorthOfRecords = recordsPerFilePage * 2;
        generateFileWithRecords( file, twoPagesWorthOfRecords, recordSize );
        OutputStream outputStream = fs.openAsOutputStream( file, true );
        outputStream.write( 'a' );
        outputStream.close();

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );

        PagedFile pagedFile = pageCache.map( file, filePageSize );
        try
        {
            assertThat( pagedFile.getLastPageId(), is( 2L ) );
        }
        finally
        {
            pageCache.unmap( file );
        }
    }

    @Test
    public void lastPageIdMustNotIncreaseWhenReadingToEndWithSharedLock() throws IOException
    {
        generateFileWithRecords( file, recordCount, recordSize );
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = pageCache.map( file, filePageSize );

        long initialLastPageId = pagedFile.getLastPageId();
        try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK ) )
        {
            while ( cursor.next() )
            {
                // scan through the lot
            }
        }
        long resultingLastPageId = pagedFile.getLastPageId();
        pageCache.unmap( file );
        assertThat( resultingLastPageId, is( initialLastPageId ) );
    }

    @Test
    public void lastPageIdMustNotIncreaseWhenReadingToEndWithNoGrowAndExclusiveLock()
            throws IOException
    {
        generateFileWithRecords( file, recordCount, recordSize );
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = pageCache.map( file, filePageSize );

        long initialLastPageId = pagedFile.getLastPageId();
        try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
        {
            while ( cursor.next() )
            {
                // scan through the lot
            }
        }
        long resultingLastPageId = pagedFile.getLastPageId();

        try
        {
            assertThat( resultingLastPageId, is( initialLastPageId ) );
        }
        finally
        {
            pageCache.unmap( file );
        }
    }

    @Test
    public void lastPageIdMustIncreaseWhenScanningPastEndWithExclusiveLock()
            throws IOException
    {
        generateFileWithRecords( file, recordsPerFilePage * 10, recordSize );
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = pageCache.map( file, filePageSize );

        assertThat( pagedFile.getLastPageId(), is( 9L ) );
        try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            for ( int i = 0; i < 15; i++ )
            {
                assertTrue( cursor.next() );
            }
        }
        try
        {
            assertThat( pagedFile.getLastPageId(), is( 14L ) );
        }
        finally
        {
            pageCache.unmap( file );
        }
    }

    @Test
    public void lastPageIdMustIncreaseWhenJumpingPastEndWithExclusiveLock()
            throws IOException
    {
        generateFileWithRecords( file, recordsPerFilePage * 10, recordSize );
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = pageCache.map( file, filePageSize );

        assertThat( pagedFile.getLastPageId(), is( 9L ) );
        try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            assertTrue( cursor.next( 15 ) );
        }
        try
        {
            assertThat( pagedFile.getLastPageId(), is( 15L ) );
        }
        finally
        {
            pageCache.unmap( file );
        }
    }

    @Test
    public void cursorOffsetMustBeUpdatedReadAndWrite() throws IOException
    {
        fs.create( file ).close();

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = pageCache.map( file, filePageSize );

        try
        {
            try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
            {
                assertTrue( cursor.next() );
                verifyWriteOffsets( cursor );

                cursor.setOffset( 0 );
                verifyReadOffsets( cursor );
            }

            try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK ) )
            {
                assertTrue( cursor.next() );
                verifyReadOffsets( cursor );
            }
        }
        finally
        {
            pageCache.unmap( file );
        }
    }

    private void verifyWriteOffsets( PageCursor cursor )
    {
        assertThat( cursor.getOffset(), is( 0 ) );
        cursor.putLong( 1 );
        assertThat( cursor.getOffset(), is( 8 ) );
        cursor.putInt( 1 );
        assertThat( cursor.getOffset(), is( 12 ) );
        cursor.putShort( (short) 1 );
        assertThat( cursor.getOffset(), is( 14 ) );
        cursor.putByte( (byte) 1 );
        assertThat( cursor.getOffset(), is( 15 ) );
        cursor.putBytes( new byte[]{ 1, 2, 3 } );
        assertThat( cursor.getOffset(), is( 18 ) );
    }

    private void verifyReadOffsets( PageCursor cursor )
    {
        assertThat( cursor.getOffset(), is( 0 ) );
        cursor.getLong();
        assertThat( cursor.getOffset(), is( 8 ) );
        cursor.getInt();
        assertThat( cursor.getOffset(), is( 12 ) );
        cursor.getShort();
        assertThat( cursor.getOffset(), is( 14 ) );
        cursor.getByte();
        assertThat( cursor.getOffset(), is( 15 ) );
        cursor.getBytes( new byte[3] );
        assertThat( cursor.getOffset(), is( 18 ) );

        byte[] expectedBytes = new byte[] {
                0, 0, 0, 0, 0, 0, 0, 1, // first; long
                0, 0, 0, 1, // second; int
                0, 1, // third; short
                1, // fourth; byte
                1, 2, 3 // lastly; more bytes
        };
        byte[] actualBytes = new byte[18];
        cursor.setOffset( 0 );
        cursor.getBytes( actualBytes );
        assertThat( actualBytes, byteArray( expectedBytes ) );
    }

    @Test
    public void cursorCanReadUnsignedIntGreaterThanMaxInt() throws IOException
    {
        fs.create( file ).close();

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = pageCache.map( file, filePageSize );

        try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            assertTrue( cursor.next() );
            long greaterThanMaxInt = (1L << 40) - 1;
            cursor.putLong( greaterThanMaxInt );
            cursor.setOffset( 0 );
            assertThat( cursor.getInt(), is( (1 << 8) - 1 ) );
            assertThat( cursor.getUnsignedInt(), is( (1L << 32) - 1 ) );
        }
        finally
        {
            pageCache.unmap( file );
        }
    }

    @Test( timeout = 1000, expected = IllegalStateException.class )
    public void pageCacheCloseMustThrowIfFilesAreStillMapped() throws IOException
    {
        fs.create( file ).close();

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        pageCache.map( file, filePageSize );

        try
        {
            pageCache.close();
        }
        finally
        {
            pageCache.unmap( file );
        }
    }

    @Test( timeout = 1000, expected = IllegalStateException.class )
    public void pagedFileIoMustThrowIfFileIsUnmapped() throws IOException
    {
        fs.create( file ).close();

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );

        PagedFile pagedFile = pageCache.map( file, filePageSize );
        pageCache.unmap( file );

        pagedFile.io( 0, PF_EXCLUSIVE_LOCK );
    }

    @Test( timeout = 1000, expected = IllegalStateException.class )
    public void writeLockedPageCursorNextMustThrowIfFileIsUnmapped() throws IOException
    {
        fs.create( file ).close();

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );

        PagedFile pagedFile = pageCache.map( file, filePageSize );
        PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK );
        pageCache.unmap( file );

        cursor.next();
    }

    @Test( timeout = 1000, expected = IllegalStateException.class )
    public void writeLockedPageCursorNextWithIdMustThrowIfFileIsUnmapped() throws IOException
    {
        fs.create( file ).close();

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );

        PagedFile pagedFile = pageCache.map( file, filePageSize );
        PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK );
        pageCache.unmap( file );

        cursor.next( 1 );
    }

    @Test( timeout = 1000, expected = IllegalStateException.class )
    public void readLockedPageCursorNextMustThrowIfFileIsUnmapped() throws IOException
    {
        generateFileWithRecords( file, 1, recordSize );

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );

        PagedFile pagedFile = pageCache.map( file, filePageSize );
        PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK );
        pageCache.unmap( file );

        cursor.next();
    }

    @Test( timeout = 1000, expected = IllegalStateException.class )
    public void readLockedPageCursorNextWithIdMustThrowIfFileIsUnmapped() throws IOException
    {
        generateFileWithRecords( file, recordsPerFilePage * 2, recordSize );

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );

        PagedFile pagedFile = pageCache.map( file, filePageSize );
        PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK );
        pageCache.unmap( file );

        cursor.next( 1 );
    }

    @Test( timeout = 1000 )
    public void writeLockedPageMustBlockFileUnmapping() throws Exception
    {
        fs.create( file ).close();

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );

        PagedFile pagedFile = pageCache.map( file, filePageSize );
        PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK );
        assertTrue( cursor.next() );

        Thread unmapper = fork( $unmap( pageCache, file ) );
        awaitTheadState( unmapper, 1000,
                Thread.State.BLOCKED, Thread.State.WAITING, Thread.State.TIMED_WAITING );

        assertFalse( cursor.shouldRetry() );
        cursor.close();

        unmapper.join();
    }

    @Test( timeout = 1000 )
    public void pessimisticReadLockedPageMustNotBlockFileUnmapping() throws Exception
    {
        generateFileWithRecords( file, 1, recordSize );

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );

        PagedFile pagedFile = pageCache.map( file, filePageSize );
        PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK );
        assertTrue( cursor.next() ); // Got a pessimistic read lock

        fork( $unmap( pageCache, file ) ).join();

        assertFalse( cursor.shouldRetry() );
        cursor.close();
    }

    @Test( timeout = 1000, expected = IllegalStateException.class )
    public void advancingPessimisticReadLockingCursorAfterUnmappingMustThrow() throws Exception
    {
        generateFileWithRecords( file, recordsPerFilePage * 2, recordSize );

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );

        PagedFile pagedFile = pageCache.map( file, filePageSize );
        PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK );
        assertTrue( cursor.next() ); // Got a pessimistic read lock

        fork( $unmap( pageCache, file ) ).join();

        cursor.next();
    }

    @Test( timeout = 1000 )
    public void advancingOptimisticReadLockingCursorAfterUnmappingMustThrow() throws Exception
    {
        generateFileWithRecords( file, recordsPerFilePage * 2, recordSize );

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );

        PagedFile pagedFile = pageCache.map( file, filePageSize );
        PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK );
        assertTrue( cursor.next() );    // fault
        assertTrue( cursor.next() );    // fault + unpin page 0
        assertTrue( cursor.next( 0 ) ); // potentially optimistic read lock page 0

        fork( $unmap( pageCache, file ) ).join();

        assertFalse( cursor.shouldRetry() );
        try {
            cursor.next();
            fail( "Advancing the cursor should have thrown" );
        }
        catch ( IllegalStateException e )
        {
            // Yay!
        }
    }

    @Test( timeout = 1000 )
    public void readingAndRetryingOnPageWithOptimisticReadLockingAfterUnmappingMustNotThrow() throws Exception
    {
        generateFileWithRecords( file, recordsPerFilePage * 2, recordSize );

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );

        PagedFile pagedFile = pageCache.map( file, filePageSize );
        PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK );
        assertTrue( cursor.next() );    // fault
        assertTrue( cursor.next() );    // fault + unpin page 0
        assertTrue( cursor.next( 0 ) ); // potentially optimistic read lock page 0

        fork( $unmap( pageCache, file ) ).join();
        pageCache.close();
        pageCache = null;

        cursor.getByte();
        assertFalse( cursor.shouldRetry() );
        try {
            cursor.next();
            fail( "Advancing the cursor should have thrown" );
        }
        catch ( IllegalStateException e )
        {
            // Yay!
        }
    }

    private static interface PageCursorAction
    {
        public void apply( PageCursor cursor );
    }

    @Test( timeout = 1000, expected = IndexOutOfBoundsException.class )
    public void getByteBeyondPageEndMustThrow() throws IOException
    {
        verifyPageBounds( new PageCursorAction()
        {
            @Override
            public void apply( PageCursor cursor )
            {
                cursor.getByte();
            }
        } );
    }

    @Test( timeout = 1000, expected = IndexOutOfBoundsException.class )
    public void putByteBeyondPageEndMustThrow() throws IOException
    {
        verifyPageBounds( new PageCursorAction()
        {
            @Override
            public void apply( PageCursor cursor )
            {
                cursor.putByte( (byte) 42 );
            }
        } );
    }

    @Test( timeout = 1000, expected = IndexOutOfBoundsException.class )
    public void getShortBeyondPageEndMustThrow() throws IOException
    {
        verifyPageBounds( new PageCursorAction()
        {
            @Override
            public void apply( PageCursor cursor )
            {
                cursor.getShort();
            }
        } );
    }

    @Test( timeout = 1000, expected = IndexOutOfBoundsException.class )
    public void putShortBeyondPageEndMustThrow() throws IOException
    {
        verifyPageBounds( new PageCursorAction()
        {
            @Override
            public void apply( PageCursor cursor )
            {
                cursor.putShort( (short) 42 );
            }
        } );
    }

    @Test( timeout = 1000, expected = IndexOutOfBoundsException.class )
    public void getIntBeyondPageEndMustThrow() throws IOException
    {
        verifyPageBounds( new PageCursorAction()
        {
            @Override
            public void apply( PageCursor cursor )
            {
                cursor.getInt();
            }
        } );
    }

    @Test( timeout = 1000, expected = IndexOutOfBoundsException.class )
    public void putIntBeyondPageEndMustThrow() throws IOException
    {
        verifyPageBounds( new PageCursorAction()
        {
            @Override
            public void apply( PageCursor cursor )
            {
                cursor.putInt( 42 );
            }
        } );
    }

    @Test( timeout = 1000, expected = IndexOutOfBoundsException.class )
    public void getUnsignedIntBeyondPageEndMustThrow() throws IOException
    {
        verifyPageBounds( new PageCursorAction()
        {
            @Override
            public void apply( PageCursor cursor )
            {
                cursor.getUnsignedInt();
            }
        } );
    }

    @Test( timeout = 1000, expected = IndexOutOfBoundsException.class )
    public void putLongBeyondPageEndMustThrow() throws IOException
    {
        verifyPageBounds( new PageCursorAction()
        {
            @Override
            public void apply( PageCursor cursor )
            {
                cursor.putLong( 42 );
            }
        } );
    }

    @Test( timeout = 1000, expected = IndexOutOfBoundsException.class )
    public void getLongBeyondPageEndMustThrow() throws IOException
    {
        verifyPageBounds( new PageCursorAction()
        {
            @Override
            public void apply( PageCursor cursor )
            {
                cursor.getLong();
            }
        } );
    }

    @Test( timeout = 1000, expected = IndexOutOfBoundsException.class )
    public void putBytesBeyondPageEndMustThrow() throws IOException
    {
        final byte[] bytes = new byte[] { 1, 2, 3 };
        verifyPageBounds( new PageCursorAction()
        {
            @Override
            public void apply( PageCursor cursor )
            {
                cursor.putBytes( bytes );
            }
        } );
    }

    @Test( timeout = 1000, expected = IndexOutOfBoundsException.class )
    public void getBytesBeyondPageEndMustThrow() throws IOException
    {
        final byte[] bytes = new byte[3];
        verifyPageBounds( new PageCursorAction()
        {
            @Override
            public void apply( PageCursor cursor )
            {
                cursor.getBytes( bytes );
            }
        } );
    }

    private void verifyPageBounds( PageCursorAction action ) throws IOException
    {
        generateFileWithRecords( file, 1, recordSize );

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = pageCache.map( file, filePageSize );

        try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK ) )
        {
            cursor.next();
            for ( int i = 0; i < 100000; i++ )
            {
                action.apply( cursor );
            }
        }
        finally
        {
            pageCache.unmap( file );
        }
    }

    @Test( timeout = 1000, expected = IndexOutOfBoundsException.class )
    public void settingNegativeCursorOffsetMustThrow() throws IOException
    {
        generateFileWithRecords( file, 1, recordSize );

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = pageCache.map( file, filePageSize );
        try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK ) )
        {
            cursor.setOffset( -1 );
        }
        finally
        {
            pageCache.unmap( file );
        }
    }

    // TODO future change: we don't want out of space errors to kill the sweeper thread -- change this to throw and Error instead
    @Test( timeout = 1000, expected = IOException.class )
    public void pageFaultForWriteMustThrowIfSweeperThreadIsDead() throws IOException
    {
        final AtomicInteger writeCounter = new AtomicInteger();
        FileSystemAbstraction fs = new DelegatingFileSystemAbstraction( this.fs )
        {
            @Override
            public StoreChannel open( File fileName, String mode ) throws IOException
            {
                return new DelegatingStoreChannel( super.open( fileName, mode ) )
                {
                    @Override
                    public void writeAll( ByteBuffer src, long position ) throws IOException
                    {
                        if ( writeCounter.incrementAndGet() > 10 )
                        {
                            throw new IOException( "No space left on device" );
                        }
                        super.writeAll( src, position );
                    }
                };
            }
        };

        fs.create( file ).close();

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = pageCache.map( file, filePageSize );

        try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            while ( cursor.next() )
            {
                // Profound and interesting I/O.
            }
        }
        finally
        {
            // Unmapping and closing the PageCache will want to flush,
            // but we can't do that with a full drive.
            pageCache = null;
        }
    }

    // TODO future change: we don't want out of space errors to kill the sweeper thread -- change this to throw and Error instead
    @Test( timeout = 1000, expected = IOException.class )
    public void pageFaultForReadMustThrowIfSweeperThreadIsDead() throws IOException
    {
        generateFileWithRecords( file, recordCount, recordSize );

        final AtomicInteger writeCounter = new AtomicInteger();
        FileSystemAbstraction fs = new DelegatingFileSystemAbstraction( this.fs )
        {
            @Override
            public StoreChannel open( File fileName, String mode ) throws IOException
            {
                return new DelegatingStoreChannel( super.open( fileName, mode ) )
                {
                    @Override
                    public void writeAll( ByteBuffer src, long position ) throws IOException
                    {
                        if ( writeCounter.incrementAndGet() >= 1 )
                        {
                            throw new IOException( "No space left on device" );
                        }
                        super.writeAll( src, position );
                    }
                };
            }
        };

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = pageCache.map( file, filePageSize );

        // Create 1 dirty page
        try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            assertTrue( cursor.next() );
        }

        // Read pages until the dirty page gets flushed
        try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK ) )
        {
            for (;;)
            {
                while ( cursor.next() )
                {
                    // Profound and interesting I/O.
                }
                // Use rewind if we get to the end, because it is non-
                // deterministic which pages get evicted and when.
                cursor.rewind();
            }
        }
        finally
        {
            // Unmapping and closing the PageCache will want to flush,
            // but we can't do that with a full drive.
            pageCache = null;
        }
    }
    // TODO if the storage devise runs out of space, eviction must stop and faulting must throw, until more space becomes available
    // TODO page faulting with a terminated eviction thread must throw (still important even if running out of space is not enough to kill the sweeper thread)

    @Test( timeout = 1000 )
    public void dataFromDifferentFilesMustNotBleedIntoEachOther() throws IOException
    {
        // The idea with this test is, that the pages for fileA are larger than
        // the pages for fileB, so we can put A-data beyond the end of the B
        // file pages.
        // Furthermore, our writes to the B-pages do not overwrite the entire page.
        // In those cases, the bytes not written to must be zeros.

        File fileA = new File( "a" );
        File fileB = new File( "b" );
        fs.create( fileA ).close();
        fs.create( fileB ).close();
        int filePageSizeA = pageCachePageSize - 2;
        int filePageSizeB = pageCachePageSize - 6;
        int pagesToWriteA = 100;
        int pagesToWriteB = 3;

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFileA = pageCache.map( fileA, filePageSizeA );

        try ( PageCursor cursor = pagedFileA.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            for ( int i = 0; i < pagesToWriteA; i++ )
            {
                assertTrue( cursor.next() );
                for ( int j = 0; j < filePageSizeA; j++ )
                {
                    cursor.putByte( (byte) 42 );
                }
            }
        }

        PagedFile pagedFileB = pageCache.map( fileB, filePageSizeB );

        try ( PageCursor cursor = pagedFileB.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            for ( int i = 0; i < pagesToWriteB; i++ )
            {
                assertTrue( cursor.next() );
                cursor.putByte( (byte) 63 );
            }
        }

        pageCache.unmap( fileA );
        pageCache.unmap( fileB );

        InputStream inputStream = fs.openAsInputStream( fileB );
        assertThat( "first page first byte", inputStream.read(), is( 63 ) );
        for ( int i = 0; i < filePageSizeB - 1; i++ )
        {
            assertThat( "page 0 byte pos " + i, inputStream.read(), is( 0 ) );
        }
        assertThat( "second page first byte", inputStream.read(), is( 63 ) );
        for ( int i = 0; i < filePageSizeB - 1; i++ )
        {
            assertThat( "page 1 byte pos " + i, inputStream.read(), is( 0 ) );
        }
        assertThat( "third page first byte", inputStream.read(), is( 63 ) );
        for ( int i = 0; i < filePageSizeB - 1; i++ )
        {
            assertThat( "page 2 byte pos " + i, inputStream.read(), is( 0 ) );
        }
        assertThat( "expect EOF", inputStream.read(), is( -1 ) );
    }

    @Test( timeout = 1000 )
    public void freshlyCreatedPagesMustContainAllZeros() throws IOException
    {
        File fileA = new File( "a" );
        File fileB = new File( "b" );
        fs.create( fileA ).close();
        ThreadLocalRandom rng = ThreadLocalRandom.current();

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = pageCache.map( fileA, filePageSize );

        try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            for ( int i = 0; i < 100; i++ )
            {
                assertTrue( cursor.next() );
                for ( int j = 0; j < filePageSize; j++ )
                {
                    cursor.putByte( (byte) rng.nextInt() );
                }
            }
        }
        pageCache.unmap( fileA );
        pageCache.close();
        pageCache = null;
        System.gc(); // make sure underlying pages are finalizable
        System.gc(); // make sure underlying pages are finally collected

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        pagedFile = pageCache.map( fileB, filePageSize );

        try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            for ( int i = 0; i < 100; i++ )
            {
                assertTrue( cursor.next() );
                for ( int j = 0; j < filePageSize; j++ )
                {
                    assertThat( cursor.getByte(), is( (byte) 0 ) );
                }
            }
        }
        finally
        {
            pageCache.unmap( fileB );
        }
    }

    @Test( timeout = 10000 )
    public void optimisticReadLockMustFaultOnRetryIfPageHasBeenEvicted() throws Exception
    {
        final byte a = 'a';
        final byte b = 'b';
        final File fileA = new File( "a" );
        final File fileB = new File( "b" );

        fs.create( fileA ).close();
        fs.create( fileB ).close();
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );

        final PagedFile pagedFileA = pageCache.map( fileA, filePageSize );
        final PagedFile pagedFileB = pageCache.map( fileB, filePageSize );

        // Fill fileA with some predicable data
        try ( PageCursor cursor = pagedFileA.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            for ( int i = 0; i < maxPages; i++ )
            {
                assertTrue( cursor.next() );
                for ( int j = 0; j < filePageSize; j++ )
                {
                    cursor.putByte( a );
                }
            }
        }

        Runnable fillPagedFileB = new Runnable()
        {
            @Override
            public void run()
            {
                try ( PageCursor cursor = pagedFileB.io( 0, PF_EXCLUSIVE_LOCK ) )
                {
                    for ( int i = 0; i < maxPages * 30; i++ )
                    {
                        assertTrue( cursor.next() );
                        for ( int j = 0; j < filePageSize; j++ )
                        {
                            cursor.putByte( b );
                        }
                    }
                }
                catch ( IOException e )
                {
                    e.printStackTrace();
                }
            }
        };

        try ( PageCursor cursor = pagedFileA.io( 0, PF_SHARED_LOCK ) )
        {
            // First, make sure page 0 is in the cache:
            assertTrue( cursor.next( 0 ) );
            // If we took a page fault, we'd have a pessimistic lock on page 0.
            // Move to the next page to release that lock:
            assertTrue( cursor.next() );
            // Now go back to page 0. It's still in the cache, so we should get
            // an optimistic lock, if that's available:
            assertTrue( cursor.next( 0 ) );

            // Verify the page is all 'a's:
            for ( int i = 0; i < filePageSize; i++ )
            {
                assertThat( cursor.getByte(), is( a ) );
            }

            // Now fill file B with 'b's... this will cause our current page to be evicted
            fork( fillPagedFileB ).join();
            // So if we had an optimistic lock, we should be asked to retry:
            if ( cursor.shouldRetry() )
            {
                // When we do reads after the shouldRetry() call, we should fault our page back
                // and get consistent reads (assuming we don't race any further with eviction)
                int expected = a * filePageSize;
                int actual;
                do
                {
                    actual = 0;
                    for ( int i = 0; i < filePageSize; i++ )
                    {
                        actual += cursor.getByte();
                    }
                }
                while ( cursor.shouldRetry() );
                assertThat( actual, is( expected ) );
            }
        }

        pageCache.unmap( fileA );
        pageCache.unmap( fileB );
    }

    @Test
    public void concurrentPageFaultingMustNotPutInterleavedDataIntoPages() throws Exception
    {
        getPageCache( fs, 3, pageCachePageSize, PageCacheMonitor.NULL );

        final File fileA = new File( "a" );

        int pageCount = 11;

        try (StoreChannel storeChannel = fs.create( fileA ))
        {
            for ( byte i = 0; i < pageCount; i++ )
            {
                byte[] data = new byte[pageCachePageSize];
                Arrays.fill(data, (byte) (i+1) );
                storeChannel.write( ByteBuffer.wrap( data ) );
            }
        }

        final int COUNT = 100000;
        final CountDownLatch readyLatch = new CountDownLatch( 11 );
        final PagedFile pagedFile = pageCache.map( fileA, pageCachePageSize );

        List<Future<?>> futures = new ArrayList<>(  );
        for ( int pageId = 0; pageId < pageCount; pageId++ )
        {
            final int finalPageId = pageId;
            futures.add( executor.submit( new Callable<Void>()
            {
                @Override
                public Void call() throws Exception
                {
                    readyLatch.countDown();
                    readyLatch.await();
                    byte[] byteCheck = new byte[pageCachePageSize];
                    for ( int c = 0; c < COUNT; c++ )
                    {
                        try ( PageCursor cursor = pagedFile.io( finalPageId, PagedFile.PF_SHARED_LOCK ) )
                        {
                            if ( cursor.next() )
                            {
                                do
                                {
                                    cursor.getBytes( byteCheck );
                                } while ( cursor.shouldRetry() );
                            }
                        }

                        for ( int i = 0; i < pageCachePageSize; i++ )
                        {
                            assertThat( byteCheck[i], equalTo( (byte) (1 + finalPageId) ) );
                        }
                    }
                    return null;
                }
            } ) );
        }

        try
        {
            for ( Future<?> future : futures )
            {
                future.get(); // This must not throw an ExecutionException
            }
        }
        finally
        {
            pageCache.unmap( fileA );
        }
    }

    @Test
    public void concurrentFlushingMustNotPutInterleavedDataIntoFile() throws Exception
    {
        generateFileWithRecords( file, recordCount, recordSize );

        getPageCache( fs, 500, pageCachePageSize, PageCacheMonitor.NULL );

        final PagedFile pagedFile = pageCache.map( file, filePageSize );

        for ( int i = 0; i < 100; i++ )
        {
            try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
            {
                while ( cursor.next() )
                {
                    // Advance the cursor through all the pages in the file,
                    // faulting them into memory and marking them as dirty
                    verifyRecordsMatchExpected( cursor );
                }
            }

            int threads = 2;
            final CountDownLatch readyLatch = new CountDownLatch( threads );
            List<Future<?>> futures = new ArrayList<>();
            for ( int j = 0; j < threads; j++ )
            {
                futures.add( executor.submit( new Callable<Object>()
                {
                    @Override
                    public Object call() throws Exception
                    {
                        readyLatch.countDown();
                        readyLatch.await();

                        pagedFile.flush();

                        return null;
                    }
                } ) );
            }

            for ( Future<?> future : futures )
            {
                future.get(); // Must not throw an ExecutionException
            }
        }

        pageCache.unmap( file );
    }

    @Test
    public void evictionThreadMustGracefullyShutDown() throws Exception
    {
        // TODO this test takes significantly longer to complete with Muninn, than it does with the StandardPageCache -- investigate why!
        int iterations = 1000;
        final AtomicReference<Throwable> caughtException = new AtomicReference<>();
        Thread.UncaughtExceptionHandler exceptionHandler = new Thread.UncaughtExceptionHandler()
        {
            @Override
            public void uncaughtException( Thread t, Throwable e )
            {
                e.printStackTrace();
                caughtException.set( e );
            }
        };

        generateFileWithRecords( file, recordCount, recordSize );
        int filePagesInTotal = recordCount / recordsPerFilePage;

        for ( int i = 0; i < iterations; i++ )
        {
            RunnablePageCache cache = createPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
            String evictionThreadName = cache.getClass().getSimpleName() + "-Eviction-Thread-" + i;
            Thread evictionThread = new Thread( cache, evictionThreadName );
            evictionThread.setUncaughtExceptionHandler( exceptionHandler );
            evictionThread.start();

            // Touch all the pages
            PagedFile pagedFile = cache.map( file, filePageSize );
            try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK ) )
            {
                for ( int j = 0; j < filePagesInTotal; j++ )
                {
                    assertTrue( cursor.next() );
                }
            }

            // We're now likely racing with the eviction thread
            cache.unmap( file );
            cache.close();
            evictionThread.interrupt();
            evictionThread.join();

            assertThat( caughtException.get(), is( nullValue() ) );
        }
    }
}
