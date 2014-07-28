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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.neo4j.graphdb.mockfs.DelegatingFileSystemAbstraction;
import org.neo4j.graphdb.mockfs.DelegatingStoreChannel;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import static org.neo4j.io.pagecache.PagedFile.PF_EXCLUSIVE_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_NO_FAULT;
import static org.neo4j.io.pagecache.PagedFile.PF_NO_GROW;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_LOCK;
import static org.neo4j.test.ByteArrayMatcher.byteArray;

public abstract class PageCacheTest<T extends PageCache>
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

    protected final T getPageCache(
            FileSystemAbstraction fs,
            int maxPages,
            int pageSize,
            PageCacheMonitor monitor ) throws IOException
    {
        if ( pageCache != null )
        {
            tearDownPageCache( pageCache );
        }
        pageCache = createPageCache( fs, maxPages, pageSize, monitor );
        return pageCache;
    }

    @Before
    public void setUp()
    {
        fs = new EphemeralFileSystemAbstraction();
    }

    @After
    public void tearDown() throws IOException
    {
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
    private void verifyRecordsMatchExpected( PageCursor cursor )
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
            } while ( cursor.retry() );
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
                        Math.abs(pageId - estimatedPageId) + ")",
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

        assertThat( recordId, is( recordCount ) );
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

        assertThat( recordId, is( recordCount - (10 * recordsPerFilePage) ) );
    }

    @Test( timeout = 1000 )
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
                } while ( cursor.retry() );
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
            // TODO racy: might not observe changes to later pages
            assertRecord( i, observation, buf );
        }
        channel.close();
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
                } while ( cursor.retry() );
            }
        }

        cache.flush();

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
    public void firstNextCallMustReturnFalseWhenTheFileIsEmptyAndNoGrowIsSpecified() throws IOException
    {
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );

        try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
        {
            assertFalse( cursor.next() );
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
    }

    @Test( timeout = 1000 )
    public void closingWithoutCallingNextMustLeavePageUnpinnedAndUntouched() throws IOException
    {
        int numberOfRecordsToGenerate = recordsPerFilePage; // one page worth
        generateFileWithRecords( file, numberOfRecordsToGenerate, recordSize );

        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );

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

    @Test//( timeout = 1000 )
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
                } while ( cursor.retry() );
            }
        }

        cache.close();

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
        pagedFile.io( 0, 0 ); // this must throw
    }

    @Test( timeout = 1000, expected = IllegalArgumentException.class )
    public void notSpecifyingAnyPfLockFlagsMustThrow() throws IOException
    {
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );
        pagedFile.io( 0, PF_NO_FAULT ); // this must throw
    }

    @Test( timeout = 1000, expected = IllegalArgumentException.class )
    public void specifyingBothSharedAndExclusiveLocksMustThrow() throws IOException
    {
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );
        pagedFile.io( 0, PF_EXCLUSIVE_LOCK | PF_SHARED_LOCK ); // this must throw
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
        }
        unpinLatch.countDown();
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
            } while ( cursor.retry() );
            assertTrue( cursor.next() );
            do
            {
                cursor.setOffset( 0 );
                cursor.putByte( (byte) 5 );
                cursor.putByte( (byte) 6 );
                cursor.putByte( (byte) 7 );
                cursor.putByte( (byte) 8 );
            } while ( cursor.retry() );
        }

        try ( PageCursor cursor = pagedFile.io( 0L, PF_EXCLUSIVE_LOCK ) )
        {
            byte[] bytes = new byte[4];
            assertTrue( cursor.next() );
            do
            {
                cursor.getBytes( bytes );
            } while ( cursor.retry() );
            assertThat( bytes, byteArray( new byte[]{1, 2, 3, 4} ) );
            assertTrue( cursor.next() );
            do
            {
                cursor.getBytes( bytes );
            } while ( cursor.retry() );
            assertThat( bytes, byteArray( new byte[]{5, 6, 7, 8} ) );
        }
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
    }

    @Test( timeout = 1000 )
    public void nextToSpecificPageIdMustAdvanceFromThatPointOn() throws IOException
    {
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );

        try ( PageCursor cursor = pagedFile.io( 1L, PF_EXCLUSIVE_LOCK ) )
        {
            assertTrue( cursor.next() );
            assertThat( cursor.getCurrentPageId(), is( 1L ) );
            assertTrue( cursor.next( 4L ) );
            assertThat( cursor.getCurrentPageId(), is( 4L ) );
            assertTrue( cursor.next() );
            assertThat( cursor.getCurrentPageId(), is( 5L ) );
        }
    }

    @Test( timeout = 1000 )
    public void currentPageIdIsUnboundBeforeFirstNextAndAfterRewind() throws IOException
    {
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );

        try ( PageCursor cursor = pagedFile.io( 0L, PF_EXCLUSIVE_LOCK ) )
        {
            assertThat( cursor.getCurrentPageId(), is( PageCursor.UNBOUND_PAGE_ID ) );
            assertTrue( cursor.next() );
            assertThat( cursor.getCurrentPageId(), is( 0L ) );
            cursor.rewind();
            assertThat( cursor.getCurrentPageId(), is( PageCursor.UNBOUND_PAGE_ID ) );
        }
    }

    @Test( timeout = 1000 )
    public void lastPageMustBeAccessibleWithNoGrowSpecified() throws IOException
    {
        generateFileWithRecords( file, recordsPerFilePage * 2, recordSize );

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
    }

    @Test( timeout = 1000 )
    public void firstPageMustBeAccessibleWithNoGrowSpecifiedIfItIsTheOnlyPage() throws IOException
    {
        generateFileWithRecords( file, recordsPerFilePage, recordSize );

        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );

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

    @Test( timeout = 1000 )
    public void firstPageMustBeAccessibleEvenIfTheFileIsNonEmptyButSmallerThanFilePageSize() throws IOException
    {
        generateFileWithRecords( file, 1, recordSize );

        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );

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

    @Test( timeout = 1000 )
    public void firstPageMustNotBeAccessibleIfFileIsEmptyAndNoGrowSpecified() throws IOException
    {
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );

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
                } while ( cursor.retry() );
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
                } while ( cursor.retry() );
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
                            } while ( cursor.retry() );
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
                } while ( cursor.retry() );
            }
        }

        writerFuture.cancel( true );
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
    }

    @Test( timeout = 1000 )
    public void nextWithPageIdMustReturnFalseIfPageIdIsBeyondFilePageRangeAndNoGrowSpecified() throws IOException
    {
        generateFileWithRecords( file, recordsPerFilePage * 2, recordSize );

        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );

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
            } while ( cursor.retry() );
            assertTrue( cursor.next( 0 ) );
            do
            {
                writeRecords( cursor );
            } while ( cursor.retry() );
            assertTrue( cursor.next( 1 ) );
            do
            {
                writeRecords( cursor );
            } while ( cursor.retry() );
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
    }

    @Test( timeout = 10000 )
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
        List<Future<?>> writers = new ArrayList<>();

        final PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );

        PagedFile pagedFile = cache.map( file, filePageSize );
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
                    PagedFile pagedFile = cache.map( file, filePageSize );
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
                                } while ( cursor.retry() );
                            }
                        }
                    }

                    cache.unmap( file );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            }
        };

        for ( int i = 0; i < writerThreads; i++ )
        {
            writers.add( executor.submit( writer ) );
        }

        startLatch.await();

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
                    } while ( cursor.retry() );
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
        pageCache.map( file, filePageSize - 1 );
    }

    @Test( timeout = 1000, expected = IllegalArgumentException.class )
    public void mappingFileSecondTimeWithGreaterPageSizeMutThrow() throws Exception
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        pageCache.map( file, filePageSize );
        pageCache.map( file, filePageSize + 1 );
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
                    while ( cursor2.retry() );
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
        // We might be unlucky and fault in the second next call, on the page
        // we brought up in the first next call. That's why we assert that we
        // have observed *at least* the countedPages number of faults.
        assertThat( monitor.countFaults(), greaterThanOrEqualTo( countedPages ) );
        assertThat( monitor.countEvictions(),
                both( greaterThanOrEqualTo( countedPages - maxPages ) )
                        .and( lessThan( countedPages ) ) );
    }

    // TODO tests that use the monitor
    // TODO lots of tests where more than one file is mapped
    // TODO must collect all exceptions from closing file channels when the cache is closed
    // TODO figure out what should happen when the last reference to a file is unmapped, while pages are still pinned
    // TODO figure out how closing the cache should work when there are still mapped files



    @Test( timeout = 1000 )
    public void shouldCloseAllFilesWhenClosingThePageCache() throws Exception
    {
        // TODO really? close files that have not been unmapped?
        // Given
        FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        File file1Name = new File( "file1" );
        File file2Name = new File( "file2" );

        StoreChannel channel1 = mock( StoreChannel.class );
        StoreChannel channel2 = mock( StoreChannel.class );
        when( fs.open( file1Name, "rw" ) ).thenReturn( channel1 );
        when( fs.open( file2Name, "rw" ) ).thenReturn( channel2 );

        // When
        cache.map( file1Name, filePageSize );
        cache.map( file1Name, filePageSize );
        cache.map( file2Name, filePageSize );
        cache.unmap( file2Name );

        // Then
        verify( fs ).open( file1Name, "rw" );
        verify( fs ).open( file2Name, "rw" );
        verify( channel2, atLeast( 1 ) ).force( false );
        verify( channel2 ).close();
        verify( channel1, atLeast( 0 ) ).size();
        verify( channel2, atLeast( 0 ) ).size();
        verifyNoMoreInteractions( channel1, channel2, fs );

        // And When
        cache.close();

        // Then
        verify( channel1, atLeast( 1 ) ).force( false );
        verify( channel1 ).close();
        verifyNoMoreInteractions( channel1, channel2, fs );
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

    @Ignore( "A meta-test that verifies that the byteArray matcher works as expected." +
             "Ignored because it is intentionally failing." )
    @Test
    public void metatestForByteArrayMatcher()
    {
        byte[] a = new byte[] { 1, -2, 3 };
        byte[] b = new byte[] { 1, 3, -2 };
        assertThat( "a == a", a, byteArray( a ) );
        assertThat( "a != b", a, byteArray( b ) ); // this must fail
    }
}
