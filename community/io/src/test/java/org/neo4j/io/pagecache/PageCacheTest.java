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
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.AssumptionViolatedException;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.adversaries.RandomAdversary;
import org.neo4j.adversaries.fs.AdversarialFileSystemAbstraction;
import org.neo4j.graphdb.mockfs.DelegatingFileSystemAbstraction;
import org.neo4j.graphdb.mockfs.DelegatingStoreChannel;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.randomharness.Command;
import org.neo4j.io.pagecache.randomharness.PageCountRecordFormat;
import org.neo4j.io.pagecache.randomharness.Phase;
import org.neo4j.io.pagecache.randomharness.RandomPageCacheTestHarness;
import org.neo4j.io.pagecache.randomharness.Record;
import org.neo4j.io.pagecache.randomharness.RecordFormat;
import org.neo4j.io.pagecache.randomharness.StandardRecordFormat;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.PinEvent;
import org.neo4j.test.LinearHistoryPageCacheTracer;
import org.neo4j.test.RepeatRule;

import static java.lang.Long.toHexString;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.io.pagecache.PagedFile.PF_EXCLUSIVE_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_NO_FAULT;
import static org.neo4j.io.pagecache.PagedFile.PF_NO_GROW;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_LOCK;
import static org.neo4j.test.ByteArrayMatcher.byteArray;
import static org.neo4j.test.ThreadTestUtils.awaitThreadState;
import static org.neo4j.test.ThreadTestUtils.fork;

public abstract class PageCacheTest<T extends PageCache>
{
    protected static final long SHORT_TIMEOUT_MILLIS = 10_000;
    protected static final long SEMI_LONG_TIMEOUT_MILLIS = 120_000;
    protected static final long LONG_TIMEOUT_MILLIS = 360_000;

    protected static ExecutorService executor;

    @BeforeClass
    public static void enablePinUnpinMonitoring()
    {
        DefaultPageCacheTracer.enablePinUnpinTracing();
    }

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

    @Rule
    public RepeatRule repeatRule = new RepeatRule();

    protected int recordSize = 9;
    protected int maxPages = 20;
    protected int pageCachePageSize = 32;
    protected int recordsPerFilePage = pageCachePageSize / recordSize;
    protected int recordCount = 25 * maxPages * recordsPerFilePage;
    protected int filePageSize = recordsPerFilePage * recordSize;
    protected ByteBuffer bufA = ByteBuffer.allocate( recordSize );

    protected FileSystemAbstraction fs;

    private T pageCache;

    protected File file( String pathname )
    {
        return new File( pathname );
    }

    protected abstract T createPageCache(
            PageSwapperFactory swapperFactory,
            int maxPages,
            int pageSize,
            PageCacheTracer tracer );

    protected T createPageCache(
            FileSystemAbstraction fs,
            int maxPages,
            int pageSize,
            PageCacheTracer tracer )
    {
        PageSwapperFactory swapperFactory = new SingleFilePageSwapperFactory();
        swapperFactory.setFileSystemAbstraction( fs );
        return createPageCache( swapperFactory, maxPages, pageSize, tracer );
    }

    protected FileSystemAbstraction createFileSystemAbstraction()
    {
        return new EphemeralFileSystemAbstraction();
    }

    protected abstract void tearDownPageCache( T pageCache ) throws IOException;

    protected final T getPageCache(
            FileSystemAbstraction fs,
            int maxPages,
            int pageSize,
            PageCacheTracer tracer ) throws IOException
    {
        if ( pageCache != null )
        {
            tearDownPageCache( pageCache );
        }
        pageCache = createPageCache( fs, maxPages, pageSize, tracer );
        return pageCache;
    }

    @Before
    public void setUp() throws IOException
    {
        Thread.interrupted(); // Clear stray interrupts
        fs = createFileSystemAbstraction();
        ensureExists( file( "a" ) );
    }

    protected void ensureExists( File file ) throws IOException
    {
        fs.create( file ).close();
    }

    protected File existingFile( String name ) throws IOException
    {
        File file = file( name );
        ensureExists( file );
        return file;
    }

    @After
    public void tearDown() throws IOException
    {
        Thread.interrupted(); // Clear stray interrupts

        if ( pageCache != null )
        {
            tearDownPageCache( pageCache );
        }

        if ( fs instanceof EphemeralFileSystemAbstraction )
        {
            ((EphemeralFileSystemAbstraction) fs).shutdown();
        }
    }

    /**
     * Verifies the records on the current page of the cursor.
     * <p>
     * This does the do-while-retry loop internally.
     */
    protected void verifyRecordsMatchExpected( PageCursor cursor ) throws IOException
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
                cursor.setOffset( recordSize * i );
                cursor.getBytes( record );
            } while ( cursor.shouldRetry() );
            actualPageContents.position( recordSize * i );
            actualPageContents.put( record );
        }
        assertRecord( pageId, actualPageContents, expectedPageContents );
    }

    protected void assertRecord( long pageId, ByteBuffer actualPageContents, ByteBuffer expectedPageContents )
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

    protected int estimateId( byte[] record )
    {
        return ByteBuffer.wrap( record ).getInt() - 1;
    }

    /**
     * Fill the page bound by the cursor with records that can be verified with
     * {@link #verifyRecordsMatchExpected(PageCursor)} or {@link #verifyRecordsInFile(java.io.File, int)}.
     */
    protected void writeRecords( PageCursor cursor )
    {
        cursor.setOffset( 0 );
        for ( int i = 0; i < recordsPerFilePage; i++ )
        {
            long recordId = (cursor.getCurrentPageId() * recordsPerFilePage) + i;
            generateRecordForId( recordId, bufA );
            cursor.putBytes( bufA.array() );
        }
    }

    protected void generateFileWithRecords(
            File file,
            int recordCount,
            int recordSize ) throws IOException
    {
        StoreChannel channel = fs.open( file, "rw" );
        ByteBuffer buf = ByteBuffer.allocate( recordSize );
        for ( int i = 0; i < recordCount; i++ )
        {
            generateRecordForId( i, buf );
            channel.writeAll( buf );
        }
        channel.close();
    }

    protected static void generateRecordForId( long id, ByteBuffer buf )
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

    protected void verifyRecordsInFile( File file, int recordCount ) throws IOException
    {
        StoreChannel channel = fs.open( file, "r" );
        ByteBuffer buf = ByteBuffer.allocate( recordSize );
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

    protected Runnable $close( final PagedFile file )
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    file.close();
                }
                catch ( IOException e )
                {
                    throw new AssertionError( e );
                }
            }
        };
    }

    protected Runnable $writeLock( final PagedFile file, final long pageId, final CountDownLatch latch )
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                try ( PageCursor cursor = file.io( pageId, PF_EXCLUSIVE_LOCK ) )
                {
                    assertTrue( cursor.next() );
                    latch.await();
                }
                catch ( IOException e )
                {
                    throw new AssertionError( e );
                }
                catch ( InterruptedException e )
                {
                    throw new AssertionError( e );
                }
            }
        };
    }

    /**
     * We implement 'assumeTrue' ourselves because JUnit insist on adding hamcrest matchers to the
     * AssumptionViolatedException instances it throws. This is a problem because those matchers are not serializable,
     * so they cannot be used together with the BootClassPathRunner, because it uses RMI under the hood.
     */
    protected void assumeTrue( String description, boolean test )
    {
        if ( !test )
        {
            throw new AssumptionViolatedException( description );
        }
    }

    @Test
    public void mustReportConfiguredMaxPages() throws IOException
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        assertThat( pageCache.maxCachedPages(), is( maxPages ) );
    }

    @Test
    public void mustReportConfiguredCachePageSize() throws IOException
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        assertThat( pageCache.pageSize(), is( pageCachePageSize ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void cachePageSizeMustBePowerOfTwo() throws IOException
    {
        getPageCache( fs, maxPages, 31, PageCacheTracer.NULL );
    }

    @Test( expected = IllegalArgumentException.class )
    public void mustHaveAtLeastTwoPages() throws Exception
    {
        getPageCache( fs, 1, pageCachePageSize, PageCacheTracer.NULL );
    }

    @Test
    public void mustAcceptTwoPagesAsMinimumConfiguration() throws Exception
    {
        getPageCache( fs, 2, pageCachePageSize, PageCacheTracer.NULL );
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void mustReadExistingData() throws IOException
    {
        generateFileWithRecords( file( "a" ), recordCount, recordSize );

        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        int recordId = 0;
        try ( PagedFile pagedFile = cache.map( file( "a" ), filePageSize );
              PageCursor cursor = pagedFile.io( 0L, PF_SHARED_LOCK ) )
        {
            while ( cursor.next() )
            {
                verifyRecordsMatchExpected( cursor );
                recordId += recordsPerFilePage;
            }
        }

        assertThat( recordId, is( recordCount ) );
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void mustScanInTheMiddleOfTheFile() throws IOException
    {
        long startPage = 10;
        long endPage = (recordCount / recordsPerFilePage) - 10;
        generateFileWithRecords( file( "a" ), recordCount, recordSize );

        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        int recordId = (int) (startPage * recordsPerFilePage);
        try ( PagedFile pagedFile = cache.map( file( "a" ), filePageSize );
              PageCursor cursor = pagedFile.io( startPage, PF_SHARED_LOCK ) )
        {
            while ( cursor.next() && cursor.getCurrentPageId() < endPage )
            {
                verifyRecordsMatchExpected( cursor );
                recordId += recordsPerFilePage;
            }
        }

        assertThat( recordId, is( recordCount - (10 * recordsPerFilePage) ) );
    }

    @Test( timeout = SEMI_LONG_TIMEOUT_MILLIS )
    public void writesFlushedFromPageFileMustBeExternallyObservable() throws IOException
    {
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        PagedFile pagedFile = cache.map( file( "a" ), filePageSize );

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

        pagedFile.flushAndForce();

        verifyRecordsInFile( file( "a" ), recordCount );
        pagedFile.close();
    }

    @Test( timeout = SEMI_LONG_TIMEOUT_MILLIS )
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

    @Test( timeout = LONG_TIMEOUT_MILLIS )
    public void writesFlushedFromPageFileMustBeObservableEvenWhenRacingWithEviction() throws IOException
    {
        PageCache cache = getPageCache( fs, 20, pageCachePageSize, PageCacheTracer.NULL );

        long startPageId = 0;
        long endPageId = 21;
        int iterations = 10000;
        int shortsPerPage = pageCachePageSize / 2;

        try ( PagedFile pagedFile = cache.map( file( "a" ), pageCachePageSize ) )
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
                pagedFile.flushAndForce();

                // Race or not, a flush should still put all changes in storage,
                // so we should be able to verify the contents of the file.
                try ( DataInputStream stream = new DataInputStream( fs.openAsInputStream( file( "a" ) ) ) )
                {
                    for ( int j = 0; j < shortsPerPage; j++ )
                    {
                        int value = stream.readShort();
                        assertThat( "short pos = " + j + ", iteration = " + i, value, is( i ) );
                    }
                }
            }
        }
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void writesFlushedFromPageCacheMustBeExternallyObservable() throws IOException
    {
        ByteBuffer buf = ByteBuffer.allocate( recordSize );
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        long startPageId = 0;
        long endPageId = recordCount / recordsPerFilePage;
        try ( PagedFile pagedFile = cache.map( file( "a" ), filePageSize );
              PageCursor cursor = pagedFile.io( startPageId, PF_EXCLUSIVE_LOCK ) )
        {
            while ( cursor.getCurrentPageId() < endPageId && cursor.next() )
            {
                do
                {
                    writeRecords( cursor );
                } while ( cursor.shouldRetry() );
            }
        } // closing the PagedFile implies flushing because it was the last reference

        StoreChannel channel = fs.open( file( "a" ), "r" );
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

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void writesToPagesMustNotBleedIntoAdjacentPages() throws IOException
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        // Write the pageId+1 to every byte in the file
        try ( PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );
              PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
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

        // Then check that none of those writes ended up in adjacent pages
        InputStream inputStream = fs.openAsInputStream( file( "a" ) );
        for ( int i = 1; i <= 100; i++ )
        {
            for ( int j = 0; j < filePageSize; j++ )
            {
                assertThat( inputStream.read(), is( i ) );
            }
        }
        inputStream.close();
    }

    @Test
    public void channelMustBeForcedAfterPagedFileFlushAndForce() throws Exception
    {
        final AtomicInteger writeCounter = new AtomicInteger();
        final AtomicInteger forceCounter = new AtomicInteger();
        FileSystemAbstraction fs = writeAndForceCountingFs( writeCounter, forceCounter );

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        try ( PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize ) )
        {
            try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
            {
                assertTrue( cursor.next() );
                cursor.putInt( 1 );
                assertTrue( cursor.next() );
                cursor.putInt( 1 );
            }

            pagedFile.flushAndForce();

            assertThat( writeCounter.get(), greaterThanOrEqualTo( 2 ) ); // we might race with background flushing
            assertThat( forceCounter.get(), is( 1 ) );
        }
    }

    @Test
    public void channelsMustBeForcedAfterPageCacheFlushAndForce() throws Exception
    {
        final AtomicInteger writeCounter = new AtomicInteger();
        final AtomicInteger forceCounter = new AtomicInteger();
        FileSystemAbstraction fs = writeAndForceCountingFs( writeCounter, forceCounter );

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        try ( PagedFile pagedFileA = pageCache.map( existingFile( "a" ), filePageSize );
              PagedFile pagedFileB = pageCache.map( existingFile( "b" ), filePageSize ) )
        {
            try ( PageCursor cursor = pagedFileA.io( 0, PF_EXCLUSIVE_LOCK ) )
            {
                assertTrue( cursor.next() );
                cursor.putInt( 1 );
                assertTrue( cursor.next() );
                cursor.putInt( 1 );
            }
            try ( PageCursor cursor = pagedFileB.io( 0, PF_EXCLUSIVE_LOCK ) )
            {
                assertTrue( cursor.next() );
                cursor.putInt( 1 );
            }

            pageCache.flushAndForce();

            assertThat( writeCounter.get(), greaterThanOrEqualTo( 3 ) ); // we might race with background flushing
            assertThat( forceCounter.get(), is( 2 ) );
        }
    }

    private DelegatingFileSystemAbstraction writeAndForceCountingFs( final AtomicInteger writeCounter,
                                                                     final AtomicInteger forceCounter )
    {
        return new DelegatingFileSystemAbstraction( fs )
        {
            @Override
            public StoreChannel open( File fileName, String mode ) throws IOException
            {
                return new DelegatingStoreChannel( super.open( fileName, mode ) )
                {
                    @Override
                    public void writeAll( ByteBuffer src, long position ) throws IOException
                    {
                        writeCounter.getAndIncrement();
                        super.writeAll( src, position );
                    }

                    @Override
                    public void force( boolean metaData ) throws IOException
                    {
                        forceCounter.getAndIncrement();
                        super.force( metaData );
                    }
                };
            }
        };
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void firstNextCallMustReturnFalseWhenTheFileIsEmptyAndNoGrowIsSpecified() throws IOException
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        try ( PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );
              PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
        {
            assertFalse( cursor.next() );
        }
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void nextMustReturnTrueThenFalseWhenThereIsOnlyOnePageInTheFileAndNoGrowIsSpecified() throws IOException
    {
        int numberOfRecordsToGenerate = recordsPerFilePage; // one page worth
        generateFileWithRecords( file( "a" ), numberOfRecordsToGenerate, recordSize );

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        try ( PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );
              PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
        {
            assertTrue( cursor.next() );
            verifyRecordsMatchExpected( cursor );
            assertFalse( cursor.next() );
        }
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void closingWithoutCallingNextMustLeavePageUnpinnedAndUntouched() throws IOException
    {
        int numberOfRecordsToGenerate = recordsPerFilePage; // one page worth
        generateFileWithRecords( file( "a" ), numberOfRecordsToGenerate, recordSize );

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        try ( PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize ) )
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
    }

    @Test( timeout = SEMI_LONG_TIMEOUT_MILLIS )
    public void rewindMustStartScanningOverFromTheBeginning() throws IOException
    {
        int numberOfRewindsToTest = 10;
        generateFileWithRecords( file( "a" ), recordCount, recordSize );
        int actualPageCounter = 0;
        int filePageCount = recordCount / recordsPerFilePage;
        int expectedPageCounterResult = numberOfRewindsToTest * filePageCount;

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        try ( PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );
              PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK ) )
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

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void mustCloseFileChannelWhenTheLastHandleIsUnmapped() throws Exception
    {
        assumeTrue( "This depends on EphemeralFSA specific features",
                fs.getClass() == EphemeralFileSystemAbstraction.class );

        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        PagedFile a = cache.map( file( "a" ), filePageSize );
        PagedFile b = cache.map( file( "a" ), filePageSize );
        a.close();
        b.close();
        ((EphemeralFileSystemAbstraction) fs).assertNoOpenFiles();
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void dirtyPagesMustBeFlushedWhenTheCacheIsClosed() throws IOException
    {
        ByteBuffer buf = ByteBuffer.allocate( recordSize );
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        long startPageId = 0;
        long endPageId = recordCount / recordsPerFilePage;
        try ( PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );
              PageCursor cursor = pagedFile.io( startPageId, PF_EXCLUSIVE_LOCK ) )
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
            pageCache.close();
        }

        StoreChannel channel = fs.open( file( "a" ), "r" );
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

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void flushingDuringPagedFileCloseMustRetryUntilItSucceeds() throws IOException
    {
        FileSystemAbstraction fs = new DelegatingFileSystemAbstraction( this.fs )
        {
            @Override
            public StoreChannel open( File fileName, String mode ) throws IOException
            {
                return new DelegatingStoreChannel( super.open( fileName, mode ) )
                {
                    private int writeCount = 0;

                    @Override
                    public void writeAll( ByteBuffer src, long position ) throws IOException
                    {
                        if ( writeCount++ < 10 )
                        {
                            throw new IOException( "This is a benign exception that we expect to be thrown " +
                                                   "during a flush of a PagedFile." );
                        }
                        super.writeAll( src, position );
                    }
                };
            }
        };
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        PrintStream oldSystemErr = System.err;

        try ( PagedFile pf = pageCache.map( file( "a" ), filePageSize );
              PageCursor cursor = pf.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            assertTrue( cursor.next() );
            writeRecords( cursor );

            // Silence any stack traces the failed flushes might print.
            System.setErr( new PrintStream( new ByteArrayOutputStream() ) );
        }
        finally
        {
            System.setErr( oldSystemErr );
        }

        verifyRecordsInFile( file( "a" ), recordsPerFilePage );
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS, expected = IllegalStateException.class )
    public void mappingFilesInClosedCacheMustThrow() throws IOException
    {
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        cache.close();
        cache.map( file( "a" ), filePageSize );
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS, expected = IllegalStateException.class )
    public void flushingClosedCacheMustThrow() throws IOException
    {
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        cache.close();
        cache.flushAndForce();
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS, expected = IllegalArgumentException.class )
    public void mappingFileWithPageSizeGreaterThanCachePageSizeMustThrow() throws IOException
    {
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        cache.map( file( "a" ), pageCachePageSize + 1 ); // this must throw
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void mappingFileWithPageSizeEqualToCachePageSizeMustNotThrow() throws IOException
    {
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        PagedFile pagedFile = cache.map( file( "a" ), pageCachePageSize );// this must NOT throw
        pagedFile.close();
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS, expected = IllegalArgumentException.class )
    public void notSpecifyingAnyPfFlagsMustThrow() throws IOException
    {
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        try ( PagedFile pagedFile = cache.map( file( "a" ), filePageSize ) )
        {
            pagedFile.io( 0, 0 ); // this must throw
        }
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS, expected = IllegalArgumentException.class )
    public void notSpecifyingAnyPfLockFlagsMustThrow() throws IOException
    {
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        try ( PagedFile pagedFile = cache.map( file( "a" ), filePageSize ) )
        {
            pagedFile.io( 0, PF_NO_FAULT ); // this must throw
        }
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS, expected = IllegalArgumentException.class )
    public void specifyingBothSharedAndExclusiveLocksMustThrow() throws IOException
    {
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        try ( PagedFile pagedFile = cache.map( file( "a" ), filePageSize ) )
        {
            pagedFile.io( 0, PF_EXCLUSIVE_LOCK | PF_SHARED_LOCK ); // this must throw
        }
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void mustNotPinPagesAfterNextReturnsFalse() throws Exception
    {
        final CountDownLatch startLatch = new CountDownLatch( 1 );
        final CountDownLatch unpinLatch = new CountDownLatch( 1 );
        final AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        generateFileWithRecords( file( "a" ), recordsPerFilePage, recordSize );
        final PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        final PagedFile pagedFile = cache.map( file( "a" ), filePageSize );

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
            pagedFile.close();
        }
        Exception e = exceptionRef.get();
        if ( e != null )
        {
            throw new Exception( "Child thread got exception", e );
        }
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void nextMustResetTheCursorOffset() throws IOException
    {
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        PagedFile pagedFile = cache.map( file( "a" ), filePageSize );

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
        pagedFile.close();
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void nextMustAdvanceCurrentPageId() throws IOException
    {
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        try ( PagedFile pagedFile = cache.map( file( "a" ), filePageSize );
              PageCursor cursor = pagedFile.io( 0L, PF_EXCLUSIVE_LOCK ) )
        {
            assertTrue( cursor.next() );
            assertThat( cursor.getCurrentPageId(), is( 0L ) );
            assertTrue( cursor.next() );
            assertThat( cursor.getCurrentPageId(), is( 1L ) );
        }
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void nextToSpecificPageIdMustAdvanceFromThatPointOn() throws IOException
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        try ( PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );
              PageCursor cursor = pagedFile.io( 1L, PF_EXCLUSIVE_LOCK ) )
        {
            assertTrue( cursor.next() );
            assertThat( cursor.getCurrentPageId(), is( 1L ) );
            assertTrue( cursor.next( 4L ) );
            assertThat( cursor.getCurrentPageId(), is( 4L ) );
            assertTrue( cursor.next() );
            assertThat( cursor.getCurrentPageId(), is( 5L ) );
        }
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void currentPageIdIsUnboundBeforeFirstNextAndAfterRewind() throws IOException
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        try ( PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );
              PageCursor cursor = pagedFile.io( 0L, PF_EXCLUSIVE_LOCK ) )
        {
            assertThat( cursor.getCurrentPageId(), is( PageCursor.UNBOUND_PAGE_ID ) );
            assertTrue( cursor.next() );
            assertThat( cursor.getCurrentPageId(), is( 0L ) );
            cursor.rewind();
            assertThat( cursor.getCurrentPageId(), is( PageCursor.UNBOUND_PAGE_ID ) );
        }
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void pageCursorMustKnowCurrentFilePageSize() throws IOException
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        try ( PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );
              PageCursor cursor = pagedFile.io( 0L, PF_EXCLUSIVE_LOCK ) )
        {
            assertThat( cursor.getCurrentPageSize(), is( PageCursor.UNBOUND_PAGE_SIZE ) );
            assertTrue( cursor.next() );
            assertThat( cursor.getCurrentPageSize(), is( filePageSize ) );
            cursor.rewind();
            assertThat( cursor.getCurrentPageSize(), is( PageCursor.UNBOUND_PAGE_SIZE ) );
        }
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void pageCursorMustKnowCurrentFile() throws Exception
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        try ( PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );
              PageCursor cursor = pagedFile.io( 0L, PF_EXCLUSIVE_LOCK ) )
        {
            assertThat( cursor.getCurrentFile(), nullValue() );
            assertTrue( cursor.next() );
            assertThat( cursor.getCurrentFile(), is( file( "a" ) ) );
            cursor.rewind();
            assertThat( cursor.getCurrentFile(), nullValue() );
        }
    }

    @Test( expected = NullPointerException.class )
    public void readingFromUnboundCursorMustThrow() throws IOException
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        try ( PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );
              PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            cursor.getByte();
        }
    }

    @Test( expected = NullPointerException.class )
    public void writingFromUnboundCursorMustThrow() throws IOException
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        try ( PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );
              PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            cursor.putInt( 1 );
        }
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void lastPageMustBeAccessibleWithNoGrowSpecified() throws IOException
    {
        generateFileWithRecords( file( "a" ), recordsPerFilePage * 2, recordSize );

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        try ( PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize ) )
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
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void lastPageMustBeAccessibleWithNoGrowSpecifiedEvenIfLessThanFilePageSize() throws IOException
    {
        generateFileWithRecords( file( "a" ), (recordsPerFilePage * 2) - 1, recordSize );

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        try ( PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize ) )
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
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void firstPageMustBeAccessibleWithNoGrowSpecifiedIfItIsTheOnlyPage() throws IOException
    {
        generateFileWithRecords( file( "a" ), recordsPerFilePage, recordSize );

        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        try ( PagedFile pagedFile = cache.map( file( "a" ), filePageSize ) )
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
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void firstPageMustBeAccessibleEvenIfTheFileIsNonEmptyButSmallerThanFilePageSize() throws IOException
    {
        generateFileWithRecords( file( "a" ), 1, recordSize );

        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        try ( PagedFile pagedFile = cache.map( file( "a" ), filePageSize ) )
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
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void firstPageMustNotBeAccessibleIfFileIsEmptyAndNoGrowSpecified() throws IOException
    {
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        try ( PagedFile pagedFile = cache.map( file( "a" ), filePageSize ) )
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
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void newlyWrittenPagesMustBeAccessibleWithNoGrow() throws IOException
    {
        int initialPages = 1;
        int pagesToAdd = 3;
        generateFileWithRecords( file( "a" ), recordsPerFilePage * initialPages, recordSize );

        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        PagedFile pagedFile = cache.map( file( "a" ), filePageSize );

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
        pagedFile.close();
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void sharedLockImpliesNoGrow() throws IOException
    {
        int initialPages = 3;
        generateFileWithRecords( file( "a" ), recordsPerFilePage * initialPages, recordSize );

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        int pagesChecked = 0;
        try ( PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );
              PageCursor cursor = pagedFile.io( 0L, PF_SHARED_LOCK ) )
        {
            while ( cursor.next() )
            {
                pagesChecked++;
            }
        }
        assertThat( pagesChecked, is( initialPages ) );
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void retryMustResetCursorOffset() throws Exception
    {
        // The general idea here, is that we have a page with a particular value in its 0th position.
        // We also have a thread that constantly writes to the middle of the page, so it modifies
        // the page, but does not change the value in the 0th position. This thread will in principle
        // mean that it is possible for a reader to get an inconsistent view and must retry.
        // We then check that every retry iteration will read the special value in the 0th position.
        // We repeat the experiment a couple of times to make sure we didn't succeed by chance.

        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        final PagedFile pagedFile = cache.map( file( "a" ), filePageSize );
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

        final AtomicBoolean end = new AtomicBoolean( false );
        Runnable writer = new Runnable()
        {
            @Override
            public void run()
            {
                while ( !end.get() )
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

        end.set( true );
        writerFuture.get();
        pagedFile.close();
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void nextWithPageIdMustAllowTraversingInReverse() throws IOException
    {
        generateFileWithRecords( file( "a" ), recordCount, recordSize );
        long lastFilePageId = (recordCount / recordsPerFilePage) - 1;

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        try ( PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );
              PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK ) )
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

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void nextWithPageIdMustReturnFalseIfPageIdIsBeyondFilePageRangeAndNoGrowSpecified() throws IOException
    {
        generateFileWithRecords( file( "a" ), recordsPerFilePage * 2, recordSize );

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        try ( PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize ) )
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
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void pagesAddedWithNextWithPageIdMustBeAccessibleWithNoGrowSpecified() throws IOException
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );

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
        pagedFile.close();
    }

    @RepeatRule.Repeat( times = 10 )
    @Test( timeout = SEMI_LONG_TIMEOUT_MILLIS )
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

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        final PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );

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
                                    for ( int offset : offsets )
                                    {
                                        cursor.setOffset( offset );
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
            pagedFile.close();
        }
    }

    @RepeatRule.Repeat( times = 1000 )
    @Test( timeout = SEMI_LONG_TIMEOUT_MILLIS )
    public void mustNotLoseUpdates() throws Exception
    {
        // Another test that tries to squeeze out data race bugs. The idea is
        // the following:
        // We have a number of threads that are going to perform one of two
        // operations on randomly chosen pages. The first operation is this:
        // They are going to pin a random page, and then scan through it to
        // find a record that is their own. A record has a thread-id and a
        // counter, both 32-bit integers. If the record is not found, it will
        // be added after all the other existing records on that page, if any.
        // The last 32-bit word on a page is a sum of all the counters, and it
        // will be updated. Then it will verify that the sum matches the
        // counters.
        // The second operation is read-only, where only the verification is
        // performed.
        // The kicker is this: the threads will also keep track of which of
        // their counters on what pages are at what value, by maintaining
        // mirror counters in memory. The threads will continuously check if
        // these stay in sync with the data on the page cache. If they go out
        // of sync, then we have a data race bug where we either pin the wrong
        // pages or somehow lose updates to the pages.
        // This is somewhat similar to what the PageCacheStressTest does.

        final AtomicBoolean shouldStop = new AtomicBoolean();
        final int cachePages = 20;
        final int filePages = cachePages * 2;
        final int threadCount = 8;
        final int pageSize = threadCount * 4;

        getPageCache( fs, cachePages, pageSize, PageCacheTracer.NULL );
        final PagedFile pagedFile = pageCache.map( file( "a" ), pageSize );

        // Ensure all the pages exist
        try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            for ( int i = 0; i < filePages; i++ )
            {
                assertTrue( "failed to initialise file page " + i, cursor.next() );
                for ( int j = 0; j < pageSize; j++ )
                {
                    cursor.putByte( (byte) 0 );
                }
            }
        }
        pageCache.flushAndForce();

        class Result
        {
            final int threadId;
            final int[] pageCounts;

            Result( int threadId, int[] pageCounts )
            {
                this.threadId = threadId;
                this.pageCounts = pageCounts;
            }
        }

        class Worker implements Callable<Result>
        {
            final int threadId;

            Worker( int threadId )
            {
                this.threadId = threadId;
            }

            @Override
            public Result call() throws Exception
            {
                int[] pageCounts = new int[filePages];
                ThreadLocalRandom rng = ThreadLocalRandom.current();

                while ( !shouldStop.get() )
                {
                    int pageId = rng.nextInt( 0, filePages );
                    int offset = threadId * 4;
                    boolean updateCounter = rng.nextBoolean();
                    int pf_flags = updateCounter? PF_EXCLUSIVE_LOCK : PF_SHARED_LOCK;
                    try ( PageCursor cursor = pagedFile.io( pageId, pf_flags ) )
                    {
                        int counter;
                        try
                        {
                            assertTrue( cursor.next() );
                            do
                            {
                                cursor.setOffset( offset );
                                counter = cursor.getInt();
                            }
                            while ( cursor.shouldRetry() );
                            String lockName = updateCounter ? "PF_EXCLUSIVE_LOCK" : "PF_SHARED_LOCK";
                            assertThat( "inconsistent page read from filePageId = " + pageId + ", with " + lockName +
                                            ", workerId = " + threadId + " [t:" + Thread.currentThread().getId() + "]",
                                    counter, is( pageCounts[pageId] ) );
                        }
                        catch ( Throwable throwable )
                        {
                            shouldStop.set( true );
                            throw throwable;
                        }
                        if ( updateCounter )
                        {
                            counter++;
                            pageCounts[pageId]++;
                            cursor.setOffset( offset );
                            cursor.putInt( counter );
                        }
                    }
                }

                return new Result( threadId, pageCounts );
            }
        }

        List<Future<Result>> futures = new ArrayList<>();
        for ( int i = 0; i < threadCount; i++ )
        {
            futures.add( executor.submit( new Worker( i ) ) );
        }

        Thread.sleep( 10 );
        shouldStop.set( true );

        for ( Future<Result> future : futures )
        {
            Result result = future.get();
            try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK ) )
            {
                for ( int i = 0; i < filePages; i++ )
                {
                    assertTrue( cursor.next() );

                    int threadId = result.threadId;
                    int expectedCount = result.pageCounts[i];
                    int actualCount;
                    do
                    {
                        cursor.setOffset( threadId * 4 );
                        actualCount = cursor.getInt();
                    }
                    while ( cursor.shouldRetry() );

                    assertThat( "wrong count for threadId = " + threadId + ", pageId = " + i,
                            actualCount, is( expectedCount ) );
                }
            }
        }
        pagedFile.close();
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void writesOfDifferentUnitsMustHaveCorrectEndianess() throws Exception
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        PagedFile pagedFile = pageCache.map( file( "a" ), 20 );

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
            for ( byte d : data )
            {
                d++;
                cursor.putByte( d );
            }
        }

        pagedFile.close();

        StoreChannel channel = fs.open( file( "a" ), "r" );
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

    @Test( timeout = SHORT_TIMEOUT_MILLIS, expected = IllegalArgumentException.class )
    public void mappingFileSecondTimeWithLesserPageSizeMustThrow() throws Exception
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        try ( PagedFile ignore = pageCache.map( file( "a" ), filePageSize ) )
        {
            pageCache.map( file( "a" ), filePageSize - 1 );
        }
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS, expected = IllegalArgumentException.class )
    public void mappingFileSecondTimeWithGreaterPageSizeMustThrow() throws Exception
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        try ( PagedFile ignore = pageCache.map( file( "a" ), filePageSize ) )
        {
            pageCache.map( file( "a" ), filePageSize + 1 );
        }
    }

    @SuppressWarnings( "unused" )
    @Test( expected = IllegalStateException.class )
    public void mustThrowWhenClaimingExclusivelyMoreThanOneCursorFromSamePagedFile() throws IOException
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        try ( PagedFile pf = pageCache.map( file( "a" ), filePageSize );
              PageCursor a = pf.io( 0, PF_EXCLUSIVE_LOCK );
              PageCursor b = pf.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            fail( "The second io() call should have thrown" );
        }
    }

    @SuppressWarnings( "unused" )
    @Test( expected = IllegalStateException.class )
    public void mustThrowWhenClaimingExclusivelyMoreThanOneCursorFromSamePageCacheButDifferentPagedFiles() throws Exception
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        try ( PagedFile pfA = pageCache.map( existingFile( "a" ), filePageSize );
              PagedFile pfB = pageCache.map( existingFile( "b" ), filePageSize );
              PageCursor a = pfA.io( 0, PF_EXCLUSIVE_LOCK );
              PageCursor b = pfB.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            fail( "The second io() call should have thrown" );
        }
    }

    @SuppressWarnings( "unused" )
    @Test( expected = IllegalStateException.class )
    public void mustThrowWhenClaimingWithSharedLockMoreThanOneCursorFromSamePagedFile() throws IOException
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        try ( PagedFile pf = pageCache.map( file( "a" ), filePageSize );
              PageCursor a = pf.io( 0, PF_SHARED_LOCK );
              PageCursor b = pf.io( 0, PF_SHARED_LOCK ) )
        {
            fail( "The second io() call should have thrown" );
        }
    }

    @SuppressWarnings( "unused" )
    @Test( expected = IllegalStateException.class )
    public void mustThrowWhenClaimingWithSharedLockMoreThanOneCursorFromSamePageCacheButDifferentPagedFiles() throws Exception
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        try ( PagedFile pfA = pageCache.map( existingFile( "a" ), filePageSize );
              PagedFile pfB = pageCache.map( existingFile( "b" ), filePageSize );
              PageCursor a = pfA.io( 0, PF_SHARED_LOCK );
              PageCursor b = pfB.io( 0, PF_SHARED_LOCK ) )
        {
            fail( "The second io() call should have thrown" );
        }
    }

    @SuppressWarnings( "unused" )
    @Test( expected = IllegalStateException.class )
    public void mustThrowWhenClaimingReadCursorWhileHavingWriteCursor() throws Exception
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        try ( PagedFile pfA = pageCache.map( existingFile( "a" ), filePageSize );
              PagedFile pfB = pageCache.map( existingFile( "b" ), filePageSize );
              PageCursor a = pfA.io( 0, PF_EXCLUSIVE_LOCK );
              PageCursor b = pfB.io( 0, PF_SHARED_LOCK ) )
        {
            fail( "The second io() call should have thrown" );
        }
    }

    @SuppressWarnings( "unused" )
    @Test( expected = IllegalStateException.class )
    public void mustThrowWhenClaimingWriteCursorWhileHavingReadCursor() throws Exception
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        try ( PagedFile pfA = pageCache.map( existingFile( "a" ), filePageSize );
              PagedFile pfB = pageCache.map( existingFile( "b" ), filePageSize );
              PageCursor a = pfA.io( 0, PF_SHARED_LOCK );
              PageCursor b = pfB.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            fail( "The second io() call should have thrown" );
        }
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS)
    public void mustNotFlushCleanPagesWhenEvicting() throws Exception
    {
        generateFileWithRecords( file( "a" ), recordCount, recordSize );

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
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        try( PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );
             PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK ) )
        {
            while ( cursor.next() )
            {
                verifyRecordsMatchExpected( cursor );
            }
        }
        assertFalse( observedWrite.get() );
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void evictionMustFlushPagesToTheRightFiles() throws IOException
    {
        generateFileWithRecords( file( "a" ), recordCount, recordSize );

        int filePageSize2 = filePageSize - 3; // diff. page size just to be difficult
        long maxPageIdCursor1 = recordCount / recordsPerFilePage;
        File file2 = file( "b" );
        OutputStream outputStream = fs.openAsOutputStream( file2, false );
        long file2sizeBytes = (maxPageIdCursor1 + 17) * filePageSize2;
        for ( int i = 0; i < file2sizeBytes; i++ )
        {
            // We will ues the page cache to change these 'a's into 'b's.
            outputStream.write( 'a' );
        }
        outputStream.flush();
        outputStream.close();

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        PagedFile pagedFile1 = pageCache.map( file( "a" ), filePageSize );
        PagedFile pagedFile2 = pageCache.map( file2, filePageSize2 );

        long pageId1 = 0;
        long pageId2 = 0;
        boolean moreWorkToDo;
        do {
            boolean cursorReady1;
            boolean cursorReady2;

            try ( PageCursor cursor = pagedFile1.io( pageId1, PF_EXCLUSIVE_LOCK ) )
            {
                cursorReady1 = cursor.next() && cursor.getCurrentPageId() < maxPageIdCursor1;
                if ( cursorReady1 )
                {
                    writeRecords( cursor );
                    pageId1++;
                }
            }

            try ( PageCursor cursor = pagedFile2.io( pageId2, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
            {
                cursorReady2 = cursor.next();
                if ( cursorReady2 )
                {
                    do {
                        for ( int i = 0; i < filePageSize2; i++ )
                        {
                            cursor.putByte( (byte) 'b' );
                        }
                    }
                    while ( cursor.shouldRetry() );
                }
                pageId2++;
            }

            moreWorkToDo = cursorReady1 || cursorReady2;
        }
        while ( moreWorkToDo );

        pagedFile1.close();
        pagedFile2.close();

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

        StoreChannel channel = fs.open( file( "a" ), "r" );
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

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void tracerMustBeNotifiedAboutPinUnpinFaultAndEvictEventsWhenReading() throws IOException
    {
        DefaultPageCacheTracer tracer = new DefaultPageCacheTracer();
        generateFileWithRecords( file( "a" ), recordCount, recordSize );

        getPageCache( fs, maxPages, pageCachePageSize, tracer );

        long countedPages = 0;
        try ( PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );
              PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK ) )
        {
            while ( cursor.next() )
            {
                assertTrue( cursor.next( cursor.getCurrentPageId() ) );
                countedPages++;
            }
        }

        assertThat( "wrong count of pins", tracer.countPins(), is( countedPages * 2 ) );
        assertThat( "wrong count of unpins", tracer.countUnpins(), is( countedPages * 2 ) );

        // We might be unlucky and fault in the second next call, on the page
        // we brought up in the first next call. That's why we assert that we
        // have observed *at least* the countedPages number of faults.
        long faults = tracer.countFaults();
        long bytesRead = tracer.countBytesRead();
        assertThat( "wrong count of faults", faults, greaterThanOrEqualTo( countedPages ) );
        assertThat( "wrong number of bytes read",
                bytesRead, greaterThanOrEqualTo( countedPages * filePageSize ) );
        // Every page we move forward can put the freelist behind so the cache
        // wants to evict more pages. Plus, every page fault we do could also
        // block and get a page directly transferred to it, and these kinds of
        // evictions can count in addition to the evictions we do when the
        // cache is behind on keeping the freelist full.
        assertThat( "wrong count of evictions", tracer.countEvictions(),
                both( greaterThanOrEqualTo( countedPages - maxPages ) )
                        .and( lessThanOrEqualTo( countedPages + faults ) ) );
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void tracerMustBeNotifiedAboutPinUnpinFaultFlushAndEvictionEventsWhenWriting() throws IOException
    {
        long pagesToGenerate = 142;
        DefaultPageCacheTracer tracer = new DefaultPageCacheTracer();

        getPageCache( fs, maxPages, pageCachePageSize, tracer );

        try ( PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );
              PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
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

        assertThat( "wrong count of pins", tracer.countPins(), is( pagesToGenerate * 2 ) );
        assertThat( "wrong count of unpins", tracer.countUnpins(), is( pagesToGenerate * 2 ) );

        // We might be unlucky and fault in the second next call, on the page
        // we brought up in the first next call. That's why we assert that we
        // have observed *at least* the countedPages number of faults.
        long faults = tracer.countFaults();
        assertThat( "wrong count of faults", faults, greaterThanOrEqualTo( pagesToGenerate ) );
        // Every page we move forward can put the freelist behind so the cache
        // wants to evict more pages. Plus, every page fault we do could also
        // block and get a page directly transferred to it, and these kinds of
        // evictions can count in addition to the evictions we do when the
        // cache is behind on keeping the freelist full.
        assertThat( "wrong count of evictions", tracer.countEvictions(),
                both( greaterThanOrEqualTo( pagesToGenerate - maxPages ) )
                        .and( lessThanOrEqualTo( pagesToGenerate + faults ) ) );

        // We use greaterThanOrEqualTo because we visit each page twice, and
        // that leaves a small window wherein we can race with eviction, have
        // the evictor flush the page, and then fault it back and mark it as
        // dirty again.
        // We also subtract 'maxPages' from the expected flush count, because
        // vectored IO may coalesce all the flushes we do as part of unmapping
        // the file, into a single flush.
        long flushes = tracer.countFlushes();
        long bytesWritten = tracer.countBytesWritten();
        assertThat( "wrong count of flushes",
                flushes, greaterThanOrEqualTo( pagesToGenerate - maxPages ) );
        assertThat( "wrong count of bytes written",
                bytesWritten, greaterThanOrEqualTo( pagesToGenerate * filePageSize ) );
    }

    @Test
    public void tracerMustBeNotifiedOfSharedAndExclusivePins() throws Exception
    {
        final AtomicInteger exclusiveCount = new AtomicInteger();
        final AtomicInteger sharedCount = new AtomicInteger();

        DefaultPageCacheTracer tracer = new DefaultPageCacheTracer()
        {
            @Override
            public PinEvent beginPin( boolean exclusiveLock, long filePageId, PageSwapper swapper )
            {
                (exclusiveLock? exclusiveCount : sharedCount).getAndIncrement();
                return super.beginPin( exclusiveLock, filePageId, swapper );
            }
        };
        generateFileWithRecords( file( "a" ), recordCount, recordSize );

        getPageCache( fs, maxPages, pageCachePageSize, tracer );

        int pinsForSharing = 13;
        int pinsForExclusive = 42;

        try ( PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize ) )
        {
            try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK ) )
            {
                for ( int i = 0; i < pinsForSharing; i++ )
                {
                    assertTrue( cursor.next() );
                }
            }

            try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
            {
                for ( int i = 0; i < pinsForExclusive; i++ )
                {
                    assertTrue( cursor.next() );
                }
            }
        }

        assertThat( "wrong shared pin count", sharedCount.get(), is( pinsForSharing ) );
        assertThat( "wrong exclusive pin count", exclusiveCount.get(), is( pinsForExclusive ) );
    }

    @Test
    public void lastPageIdOfEmptyFileIsMinusOne() throws IOException
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        try ( PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize ) )
        {
            assertThat( pagedFile.getLastPageId(), is( -1L ) );
        }
    }

    @Test
    public void lastPageIdOfFileWithOneByteIsZero() throws IOException
    {
        StoreChannel channel = fs.create( file( "a" ) );
        channel.write( ByteBuffer.wrap( new byte[]{1} ) );
        channel.close();

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );
        assertThat( pagedFile.getLastPageId(), is( 0L ) );
        pagedFile.close();
    }

    @Test
    public void lastPageIdOfFileWithExactlyTwoPagesWorthOfDataIsOne() throws IOException
    {
        int twoPagesWorthOfRecords = recordsPerFilePage * 2;
        generateFileWithRecords( file( "a" ), twoPagesWorthOfRecords, recordSize );

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        try ( PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize ) )
        {
            assertThat( pagedFile.getLastPageId(), is( 1L ) );
        }
    }

    @Test
    public void lastPageIdOfFileWithExactlyTwoPagesAndOneByteWorthOfDataIsTwo() throws IOException
    {
        int twoPagesWorthOfRecords = recordsPerFilePage * 2;
        generateFileWithRecords( file( "a" ), twoPagesWorthOfRecords, recordSize );
        OutputStream outputStream = fs.openAsOutputStream( file( "a" ), true );
        outputStream.write( 'a' );
        outputStream.close();

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        try ( PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize ) )
        {
            assertThat( pagedFile.getLastPageId(), is( 2L ) );
        }
    }

    @Test
    public void lastPageIdMustNotIncreaseWhenReadingToEndWithSharedLock() throws IOException
    {
        generateFileWithRecords( file( "a" ), recordCount, recordSize );
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );

        long initialLastPageId = pagedFile.getLastPageId();
        try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK ) )
        {
            //noinspection StatementWithEmptyBody
            while ( cursor.next() )
            {
                // scan through the lot
            }
        }
        long resultingLastPageId = pagedFile.getLastPageId();
        pagedFile.close();
        assertThat( resultingLastPageId, is( initialLastPageId ) );
    }

    @Test
    public void lastPageIdMustNotIncreaseWhenReadingToEndWithNoGrowAndExclusiveLock()
            throws IOException
    {
        generateFileWithRecords( file( "a" ), recordCount, recordSize );
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );

        long initialLastPageId = pagedFile.getLastPageId();
        try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
        {
            //noinspection StatementWithEmptyBody
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
            pagedFile.close();
        }
    }

    @Test
    public void lastPageIdMustIncreaseWhenScanningPastEndWithExclusiveLock()
            throws IOException
    {
        generateFileWithRecords( file( "a" ), recordsPerFilePage * 10, recordSize );
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );

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
            pagedFile.close();
        }
    }

    @Test
    public void lastPageIdMustIncreaseWhenJumpingPastEndWithExclusiveLock()
            throws IOException
    {
        generateFileWithRecords( file( "a" ), recordsPerFilePage * 10, recordSize );
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );

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
            pagedFile.close();
        }
    }

    @Test
    public void cursorOffsetMustBeUpdatedReadAndWrite() throws IOException
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        try ( PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize ) )
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
        cursor.putBytes( new byte[]{1, 2, 3} );
        assertThat( cursor.getOffset(), is( 18 ) );
        cursor.putBytes( new byte[]{1, 2, 3}, 1, 1 );
        assertThat( cursor.getOffset(), is( 19 ) );
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
        cursor.getBytes( new byte[3], 1, 1 );
        assertThat( cursor.getOffset(), is( 19 ) );

        byte[] expectedBytes = new byte[] {
                0, 0, 0, 0, 0, 0, 0, 1, // first; long
                0, 0, 0, 1, // second; int
                0, 1, // third; short
                1, // fourth; byte
                1, 2, 3, // lastly; more bytes
                2
        };
        byte[] actualBytes = new byte[19];
        cursor.setOffset( 0 );
        cursor.getBytes( actualBytes );
        assertThat( actualBytes, byteArray( expectedBytes ) );
    }

    @Test
    public void cursorCanReadUnsignedIntGreaterThanMaxInt() throws IOException
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        try ( PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );
              PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            assertTrue( cursor.next() );
            long greaterThanMaxInt = (1L << 40) - 1;
            cursor.putLong( greaterThanMaxInt );
            cursor.setOffset( 0 );
            assertThat( cursor.getInt(), is( (1 << 8) - 1 ) );
            assertThat( cursor.getUnsignedInt(), is( (1L << 32) - 1 ) );
        }
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS, expected = IllegalStateException.class )
    public void pageCacheCloseMustThrowIfFilesAreStillMapped() throws IOException
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        try ( PagedFile ignore = pageCache.map( file( "a" ), filePageSize ) )
        {
            pageCache.close();
        }
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS, expected = IllegalStateException.class )
    public void pagedFileIoMustThrowIfFileIsUnmapped() throws IOException
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );
        pagedFile.close();

        pagedFile.io( 0, PF_EXCLUSIVE_LOCK );
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS, expected = IllegalStateException.class )
    public void writeLockedPageCursorNextMustThrowIfFileIsUnmapped() throws IOException
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );
        PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK );
        pagedFile.close();

        cursor.next();
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS, expected = IllegalStateException.class )
    public void writeLockedPageCursorNextWithIdMustThrowIfFileIsUnmapped() throws IOException
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );
        PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK );
        pagedFile.close();

        cursor.next( 1 );
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS, expected = IllegalStateException.class )
    public void readLockedPageCursorNextMustThrowIfFileIsUnmapped() throws IOException
    {
        generateFileWithRecords( file( "a" ), 1, recordSize );

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );
        PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK );
        pagedFile.close();

        cursor.next();
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS, expected = IllegalStateException.class )
    public void readLockedPageCursorNextWithIdMustThrowIfFileIsUnmapped() throws IOException
    {
        generateFileWithRecords( file( "a" ), recordsPerFilePage * 2, recordSize );

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );
        PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK );
        pagedFile.close();

        cursor.next( 1 );
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void writeLockedPageMustBlockFileUnmapping() throws Exception
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );
        PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK );
        assertTrue( cursor.next() );

        Thread unmapper = fork( $close( pagedFile ) );
        awaitThreadState( unmapper, 1000,
                Thread.State.BLOCKED, Thread.State.WAITING, Thread.State.TIMED_WAITING );

        assertFalse( cursor.shouldRetry() );
        cursor.close();

        unmapper.join();
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void writeLockedPageNextOnUnmappedFileMustNotBlockFileUnmapping() throws Exception
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );
        //Get a write lock
        PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK );
        assertTrue( cursor.next() );
        //Try to unmap the paged file
        Thread unmapper = fork( $close( pagedFile ) );
        awaitThreadState( unmapper, 1000,
                Thread.State.BLOCKED, Thread.State.WAITING, Thread.State.TIMED_WAITING );

        try
        {
            // This needs to unpin before checking if file is still mapped, otherwise the unmapper will be unable to
            // unmap the file due to us still having a writelock on a page.
            cursor.next();
            fail( "Should have thrown" );
        }
        catch ( IllegalStateException e )
        {
            unmapper.join();
        }
        cursor.close();
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void readLockedPageNextOnUnmappedFileMustNotBlockFileUnmapping() throws Exception
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        File a = file( "a" );
        generateFileWithRecords( a, 1, recordSize );
        PagedFile pagedFile = pageCache.map( a, filePageSize );
        //Get a pessimistic read lock
        PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK );
        assertTrue( cursor.next() );
        //Get a write lock to pause the unmapper
        CountDownLatch writeLockLatch = new CountDownLatch( 1 );
        Thread writeLockFork = fork( $writeLock( pagedFile, 2, writeLockLatch ) );
        awaitThreadState( writeLockFork, 1000,
                Thread.State.BLOCKED, Thread.State.WAITING, Thread.State.TIMED_WAITING );
        //Try to unmap the file.
        Thread unmapper = fork( $close( pagedFile ) );
        awaitThreadState( unmapper, 1000,
                Thread.State.BLOCKED, Thread.State.WAITING, Thread.State.TIMED_WAITING );

        try
        {
            //This must unpin the current page before checking if the page is still mapped, otherwise we will be left
            // with a pessimistic read lock with no associated cursor.
            cursor.next();
            fail( "Should have thrown" );
        }
        catch ( IllegalStateException e )
        {
            writeLockLatch.countDown();
            writeLockFork.join();
            unmapper.join();
        }
        //Try to take a write lock on all pages in the page cache to make sure that there is no rouge read lock.
        //If unsuccessful we will be pagefaulting forever.
        pagedFile = pageCache.map( a, filePageSize );
        writeLockLatch = new CountDownLatch( 1 );
        Thread[] writeLockers = new Thread[maxPages];
        for ( int i = 0; i < maxPages; i++ )
        {
            writeLockers[i] = fork( $writeLock( pagedFile, i, writeLockLatch ) );
            awaitThreadState( writeLockers[i], 1000,
                    Thread.State.BLOCKED, Thread.State.WAITING, Thread.State.TIMED_WAITING );
        }
        writeLockLatch.countDown();
        try
        {
            for ( Thread writeLocker : writeLockers )
            {
                writeLocker.join();
            }
        }
        finally
        {
            pagedFile.close();
        }
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void pessimisticReadLockedPageMustNotBlockFileUnmapping() throws Exception
    {
        generateFileWithRecords( file( "a" ), 1, recordSize );

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );
        PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK );
        assertTrue( cursor.next() ); // Got a pessimistic read lock

        fork( $close( pagedFile ) ).join();

        assertFalse( cursor.shouldRetry() );
        cursor.close();
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS, expected = IllegalStateException.class )
    public void advancingPessimisticReadLockingCursorAfterUnmappingMustThrow() throws Exception
    {
        generateFileWithRecords( file( "a" ), recordsPerFilePage * 2, recordSize );

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );
        PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK );
        assertTrue( cursor.next() ); // Got a pessimistic read lock

        fork( $close( pagedFile ) ).join();

        cursor.next();
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void advancingOptimisticReadLockingCursorAfterUnmappingMustThrow() throws Exception
    {
        generateFileWithRecords( file( "a" ), recordsPerFilePage * 2, recordSize );

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );
        PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK );
        assertTrue( cursor.next() );    // fault
        assertTrue( cursor.next() );    // fault + unpin page 0
        assertTrue( cursor.next( 0 ) ); // potentially optimistic read lock page 0

        fork( $close( pagedFile ) ).join();

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

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void readingAndRetryingOnPageWithOptimisticReadLockingAfterUnmappingMustNotThrow() throws Exception
    {
        generateFileWithRecords( file( "a" ), recordsPerFilePage * 2, recordSize );

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );
        PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK );
        assertTrue( cursor.next() );    // fault
        assertTrue( cursor.next() );    // fault + unpin page 0
        assertTrue( cursor.next( 0 ) ); // potentially optimistic read lock page 0

        fork( $close( pagedFile ) ).join();
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

    @RepeatRule.Repeat( times = 100 )
    @Test( timeout = SEMI_LONG_TIMEOUT_MILLIS )
    public void writeLockingCursorMustThrowWhenLockingPageRacesWithUnmapping() throws Exception
    {
        // Even if we block in pin, waiting to grab a lock on a page that is
        // already locked, and the PagedFile is concurrently closed, then we
        // want to have an exception thrown, such that we race and get a
        // page that is writable after the PagedFile has been closed.
        // This is important because closing a PagedFile implies flushing, thus
        // ensuring that all changes make it to storage.
        // Conversely, we don't have to go to the same lengths for read locked
        // pages, because those are never changed. Not by us, anyway.

        File file = file( "a" );
        generateFileWithRecords( file, recordsPerFilePage * 2, recordSize );

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        final PagedFile pf = pageCache.map( file, filePageSize );
        final CountDownLatch hasLockLatch = new CountDownLatch( 1 );
        final CountDownLatch unlockLatch = new CountDownLatch( 1 );
        final CountDownLatch secondThreadGotLockLatch = new CountDownLatch( 1 );

        executor.submit( new Callable<Object>()
        {
            @Override
            public Object call() throws Exception
            {
                try ( PageCursor cursor = pf.io( 0, PF_EXCLUSIVE_LOCK ) )
                {
                    cursor.next();
                    hasLockLatch.countDown();
                    unlockLatch.await();
                }
                return null;
            }
        } );

        hasLockLatch.await(); // An exclusive lock is now held on page 0.

        Future<Object> takeLockFuture = executor.submit( new Callable<Object>()
        {
            @Override
            public Object call() throws Exception
            {
                try ( PageCursor cursor = pf.io( 0, PF_EXCLUSIVE_LOCK ) )
                {
                    cursor.next();
                    secondThreadGotLockLatch.await();
                }
                return null;
            }
        } );

        Future<Object> closeFuture = executor.submit( new Callable<Object>()
        {
            @Override
            public Object call() throws Exception
            {
                pf.close();
                return null;
            }
        } );

        try
        {
            closeFuture.get( 100, TimeUnit.MILLISECONDS );
            fail( "Expected a TimeoutException here" );
        }
        catch ( TimeoutException e )
        {
            // As expected, the close cannot not complete while an exclusive
            // lock is held
        }

        // Now, both the close action and a grab for an exclusive page lock is
        // waiting for our first thread.
        // When we release that lock, we should see that either close completes
        // and our second thread, the one blocked on the write lock, gets an
        // exception, or we should see that the second thread gets the lock,
        // and then close has to wait for that thread as well.

        unlockLatch.countDown(); // The race is on.

        try
        {
            closeFuture.get( 1000, TimeUnit.MILLISECONDS );
            // The closeFuture got it first, so the takeLockFuture should throw.
            try
            {
                secondThreadGotLockLatch.countDown(); // only to prevent incorrect programs from deadlocking
                takeLockFuture.get();
                fail( "Expected takeLockFuture.get() to throw an ExecutionException" );
            }
            catch ( ExecutionException e )
            {
                Throwable cause = e.getCause();
                assertThat( cause, instanceOf( IllegalStateException.class ) );
                assertThat( cause.getMessage(), startsWith( "File has been unmapped" ) );
            }
        }
        catch ( TimeoutException e )
        {
            // The takeLockFuture got it first, so the closeFuture should
            // complete when we release the latch.
            secondThreadGotLockLatch.countDown();
            closeFuture.get( 20000, TimeUnit.MILLISECONDS );
        }
    }

    private interface PageCursorAction
    {
        void apply( PageCursor cursor );
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS, expected = IndexOutOfBoundsException.class )
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

    @Test( timeout = SHORT_TIMEOUT_MILLIS, expected = IndexOutOfBoundsException.class )
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

    @Test( timeout = SHORT_TIMEOUT_MILLIS, expected = IndexOutOfBoundsException.class )
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

    @Test( timeout = SHORT_TIMEOUT_MILLIS, expected = IndexOutOfBoundsException.class )
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

    @Test( timeout = SHORT_TIMEOUT_MILLIS, expected = IndexOutOfBoundsException.class )
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

    @Test( timeout = SHORT_TIMEOUT_MILLIS, expected = IndexOutOfBoundsException.class )
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

    @Test( timeout = SHORT_TIMEOUT_MILLIS, expected = IndexOutOfBoundsException.class )
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

    @Test( timeout = SHORT_TIMEOUT_MILLIS, expected = IndexOutOfBoundsException.class )
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

    @Test( timeout = SHORT_TIMEOUT_MILLIS, expected = IndexOutOfBoundsException.class )
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

    @Test( timeout = SHORT_TIMEOUT_MILLIS, expected = IndexOutOfBoundsException.class )
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

    @Test( timeout = SHORT_TIMEOUT_MILLIS, expected = IndexOutOfBoundsException.class )
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

    @Test( timeout = SHORT_TIMEOUT_MILLIS, expected = IndexOutOfBoundsException.class )
    public void putBytesWithOffsetAndLengthBeyondPageEndMustThrow() throws IOException
    {
        final byte[] bytes = new byte[] { 1, 2, 3 };
        verifyPageBounds( new PageCursorAction()
        {
            @Override
            public void apply( PageCursor cursor )
            {
                cursor.putBytes( bytes, 1, 1 );
            }
        } );
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS, expected = IndexOutOfBoundsException.class )
    public void getBytesWithOffsetAndLengthBeyondPageEndMustThrow() throws IOException
    {
        final byte[] bytes = new byte[3];
        verifyPageBounds( new PageCursorAction()
        {
            @Override
            public void apply( PageCursor cursor )
            {
                cursor.getBytes( bytes, 1, 1 );
            }
        } );
    }

    private void verifyPageBounds( PageCursorAction action ) throws IOException
    {
        generateFileWithRecords( file( "a" ), 1, recordSize );

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        try ( PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );
              PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            cursor.next();
            for ( int i = 0; i < 100000; i++ )
            {
                action.apply( cursor );
            }
        }
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS, expected = IndexOutOfBoundsException.class )
    public void settingNegativeCursorOffsetMustThrow() throws IOException
    {
        generateFileWithRecords( file( "a" ), 1, recordSize );

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        try ( PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );
              PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK ) )
        {
            cursor.setOffset( -1 );
        }
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS, expected = IOException.class )
    public void pageFaultForWriteMustThrowIfOutOfStorageSpace() throws IOException
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

        fs.create( file( "a" ) ).close();

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );

        try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            //noinspection StatementWithEmptyBody
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

    @Test( timeout = SEMI_LONG_TIMEOUT_MILLIS, expected = IOException.class )
    public void pageFaultForReadMustThrowIfOutOfStorageSpace() throws IOException
    {
        generateFileWithRecords( file( "a" ), recordCount, recordSize );

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

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );

        // Create 1 dirty page
        try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            assertTrue( cursor.next() );
        }

        // Read pages until the dirty page gets flushed
        try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK ) )
        {
            //noinspection InfiniteLoopStatement
            for (;;)
            {
                //noinspection StatementWithEmptyBody
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

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void mustRecoverFromFullDriveWhenMoreStorageBecomesAvailable() throws IOException
    {
        final AtomicBoolean hasSpace = new AtomicBoolean();
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
                        if ( !hasSpace.get() )
                        {
                            throw new IOException( "No space left on device" );
                        }
                        super.writeAll( src, position );
                    }
                };
            }
        };

        fs.create( file( "a" ) ).close();

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );

        try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            //noinspection InfiniteLoopStatement
            for (;;) // Keep writing until we get an exception! (when the cache starts evicting stuff)
            {
                assertTrue( cursor.next() );
                writeRecords( cursor );
            }
        }
        catch ( IOException ignore )
        {
            // We're not out of space! Salty tears...
        }

        // Fix the situation:
        hasSpace.set( true );

        // Closing the last reference of a paged file implies a flush, and it mustn't throw:
        pagedFile.close();
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void dataFromDifferentFilesMustNotBleedIntoEachOther() throws IOException
    {
        // The idea with this test is, that the pages for fileA are larger than
        // the pages for fileB, so we can put A-data beyond the end of the B
        // file pages.
        // Furthermore, our writes to the B-pages do not overwrite the entire page.
        // In those cases, the bytes not written to must be zeros.

        File fileB = existingFile( "b" );
        int filePageSizeA = pageCachePageSize - 2;
        int filePageSizeB = pageCachePageSize - 6;
        int pagesToWriteA = 100;
        int pagesToWriteB = 3;

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        PagedFile pagedFileA = pageCache.map( existingFile( "a" ), filePageSizeA );

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

        pagedFileA.close();
        pagedFileB.close();

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

    @Test( timeout = SEMI_LONG_TIMEOUT_MILLIS )
    public void freshlyCreatedPagesMustContainAllZeros() throws IOException
    {
        ThreadLocalRandom rng = ThreadLocalRandom.current();

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        try ( PagedFile pagedFile = pageCache.map( existingFile( "a" ), filePageSize );
              PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
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
        pageCache.close();
        pageCache = null;
        System.gc(); // make sure underlying pages are finalizable
        System.gc(); // make sure underlying pages are finally collected

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

        try ( PagedFile pagedFile = pageCache.map( existingFile( "b" ), filePageSize );
              PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
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
    }

    @Test( timeout = SEMI_LONG_TIMEOUT_MILLIS )
    public void optimisticReadLockMustFaultOnRetryIfPageHasBeenEvicted() throws Exception
    {
        final byte a = 'a';
        final byte b = 'b';
        final File fileA = existingFile( "a" );
        final File fileB = existingFile( "b" );

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );

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

        pagedFileA.close();
        pagedFileB.close();
    }

    @Test( timeout = LONG_TIMEOUT_MILLIS )
    public void concurrentPageFaultingMustNotPutInterleavedDataIntoPages() throws Exception
    {
        final int filePageCount = 11;
        final RecordFormat recordFormat = new PageCountRecordFormat();
        RandomPageCacheTestHarness harness = new RandomPageCacheTestHarness();
        harness.setConcurrencyLevel( 11 );
        harness.setUseAdversarialIO( false );
        harness.setCachePageCount( 3 );
        harness.setCachePageSize( pageCachePageSize );
        harness.setFilePageCount( filePageCount );
        harness.setFilePageSize( pageCachePageSize );
        harness.setInitialMappedFiles( 1 );
        harness.setCommandCount( 10000 );
        harness.setRecordFormat( recordFormat );
        harness.setFileSystem( fs );
        harness.disableCommands(
                Command.FlushCache, Command.FlushFile, Command.MapFile, Command.UnmapFile, Command.WriteRecord );
        harness.setPreparation( new Phase()
        {
            @Override
            public void run(
                    PageCache pageCache, FileSystemAbstraction fs, Set<File> filesTouched ) throws Exception
            {
                File file = filesTouched.iterator().next();
                try ( PagedFile pf = pageCache.map( file, pageCachePageSize );
                      PageCursor cursor = pf.io( 0, PF_EXCLUSIVE_LOCK ) )
                {
                    for ( int pageId = 0; pageId < filePageCount; pageId++ )
                    {
                        cursor.next();
                        recordFormat.fillWithRecords( cursor );
                    }
                }
            }
        } );

        harness.run( LONG_TIMEOUT_MILLIS, MILLISECONDS );
    }

    @Test( timeout = LONG_TIMEOUT_MILLIS )
    public void concurrentFlushingMustNotPutInterleavedDataIntoFile() throws Exception
    {
        final RecordFormat recordFormat = new StandardRecordFormat();
        final int filePageCount = 2_000;
        RandomPageCacheTestHarness harness = new RandomPageCacheTestHarness();
        harness.setConcurrencyLevel( 16 );
        harness.setUseAdversarialIO( false );
        harness.setCachePageCount( filePageCount / 2 );
        harness.setFilePageCount( filePageCount );
        harness.setCachePageSize( pageCachePageSize );
        harness.setFilePageSize( pageCachePageSize );
        harness.setInitialMappedFiles( 3 );
        harness.setCommandCount( 15_000 );
        harness.setFileSystem( fs );
        harness.disableCommands( Command.MapFile, Command.UnmapFile, Command.ReadRecord );
        harness.setVerification( filesAreCorrectlyWrittenVerification( recordFormat, filePageCount ) );

        harness.run( LONG_TIMEOUT_MILLIS, MILLISECONDS );
    }

    @Test( timeout = LONG_TIMEOUT_MILLIS )
    public void concurrentFlushingWithMischiefMustNotPutInterleavedDataIntoFile() throws Exception
    {
        final RecordFormat recordFormat = new StandardRecordFormat();
        final int filePageCount = 2_000;
        RandomPageCacheTestHarness harness = new RandomPageCacheTestHarness();
        harness.setConcurrencyLevel( 16 );
        harness.setUseAdversarialIO( true );
        harness.setMischiefRate( 0.5 );
        harness.setFailureRate( 0.0 );
        harness.setErrorRate( 0.0 );
        harness.setCachePageCount( filePageCount / 2 );
        harness.setFilePageCount( filePageCount );
        harness.setCachePageSize( pageCachePageSize );
        harness.setFilePageSize( pageCachePageSize );
        harness.setInitialMappedFiles( 3 );
        harness.setCommandCount( 15_000 );
        harness.setFileSystem( fs );
        harness.disableCommands( Command.MapFile, Command.UnmapFile, Command.ReadRecord );
        harness.setVerification( filesAreCorrectlyWrittenVerification( recordFormat, filePageCount ) );

        harness.run( LONG_TIMEOUT_MILLIS, MILLISECONDS );
    }

    @Test( timeout = LONG_TIMEOUT_MILLIS )
    public void concurrentFlushingWithFailuresMustNotPutInterleavedDataIntoFile() throws Exception
    {
        final RecordFormat recordFormat = new StandardRecordFormat();
        final int filePageCount = 20_000;
        RandomPageCacheTestHarness harness = new RandomPageCacheTestHarness();
        harness.setConcurrencyLevel( 16 );
        harness.setUseAdversarialIO( true );
        harness.setMischiefRate( 0.0 );
        harness.setFailureRate( 0.5 );
        harness.setErrorRate( 0.0 );
        harness.setCachePageCount( filePageCount / 2 );
        harness.setFilePageCount( filePageCount );
        harness.setCachePageSize( pageCachePageSize );
        harness.setFilePageSize( pageCachePageSize );
        harness.setInitialMappedFiles( 3 );
        harness.setCommandCount( 150_000 );
        harness.setFileSystem( fs );
        harness.disableCommands( Command.MapFile, Command.UnmapFile, Command.ReadRecord );
        harness.setVerification( filesAreCorrectlyWrittenVerification( recordFormat, filePageCount ) );

        harness.run( LONG_TIMEOUT_MILLIS, MILLISECONDS );
    }

    private Phase filesAreCorrectlyWrittenVerification( final RecordFormat recordFormat, final int filePageCount )
    {
        return new Phase()
        {
            @Override
            public void run(
                    PageCache pageCache, FileSystemAbstraction fs, Set<File> filesTouched ) throws Exception
            {
                for ( File file : filesTouched )
                {
                    try ( PagedFile pf = pageCache.map( file, pageCachePageSize );
                          PageCursor cursor = pf.io( 0, PF_SHARED_LOCK ) )
                    {
                        for ( int pageId = 0; pageId < filePageCount && cursor.next(); pageId++ )
                        {
                            try
                            {
                                recordFormat.assertRecordsWrittenCorrectly( cursor );
                            }
                            catch ( Throwable th )
                            {
                                th.addSuppressed( new Exception( "pageId = " + pageId ) );
                                throw th;
                            }
                        }
                    }
                    try ( StoreChannel channel = fs.open( file, "r" ) )
                    {
                        recordFormat.assertRecordsWrittenCorrectly( file, channel );
                    }
                }
            }
        };
    }

    @Test( timeout = SHORT_TIMEOUT_MILLIS )
    public void pagesMustReturnToFreelistIfSwapInThrows() throws IOException
    {
        generateFileWithRecords( file( "a" ), recordCount, recordSize );

        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );

        int iterations = maxPages * 2;
        accessPagesWhileInterrupted( pagedFile, PF_SHARED_LOCK, iterations );
        accessPagesWhileInterrupted( pagedFile, PF_EXCLUSIVE_LOCK, iterations );

        // Verify that after all those troubles, page faulting starts working again
        // as soon as our thread is no longer interrupted and the PageSwapper no
        // longer throws.
        Thread.interrupted(); // make sure to clear our interruption status

        try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK ) )
        {
            assertTrue( cursor.next() );
            verifyRecordsMatchExpected( cursor );
        }
        pagedFile.close();
    }

    private void accessPagesWhileInterrupted(
            PagedFile pagedFile,
            int pf_flags,
            int iterations ) throws IOException
    {
        try ( PageCursor cursor = pagedFile.io( 0, pf_flags ) )
        {
            for ( int i = 0; i < iterations; i++ )
            {
                Thread.currentThread().interrupt();
                try
                {
                    cursor.next( 0 );
                }
                catch ( IOException ignored )
                {
                    // We don't care about the exception per se.
                    // We just want lots of failed page faults.
                }
            }
        }
    }

    @RepeatRule.Repeat( times = 3000 )
    @Test( timeout = LONG_TIMEOUT_MILLIS )
    public void pageCacheMustRemainInternallyConsistentWhenGettingRandomFailures() throws Exception
    {
        // NOTE: This test is inherently non-deterministic. This means that every failure must be
        // thoroughly investigated, since they have a good chance of being a real issue.
        // This is effectively a targeted robustness test.

        RandomAdversary adversary = new RandomAdversary( 0.5, 0.2, 0.2 );
        adversary.setProbabilityFactor( 0.0 );
        FileSystemAbstraction fs = new AdversarialFileSystemAbstraction( adversary, this.fs );
        ThreadLocalRandom rng = ThreadLocalRandom.current();

        // Because our test failures are non-deterministic, we use this tracer to capture a full history of the
        // events leading up to any given failure.
        LinearHistoryPageCacheTracer tracer = new LinearHistoryPageCacheTracer();
        getPageCache( fs, maxPages, pageCachePageSize, tracer );

        PagedFile pfA = pageCache.map( existingFile( "a" ), filePageSize );
        PagedFile pfB = pageCache.map( existingFile( "b" ), filePageSize / 2 + 1 );
        adversary.setProbabilityFactor( 1.0 );

        for ( int i = 0; i < 1000; i++ )
        {
            PagedFile pagedFile = rng.nextBoolean()? pfA : pfB;
            long maxPageId = pagedFile.getLastPageId();
            boolean performingRead = rng.nextBoolean() && maxPageId != -1;
            long startingPage = maxPageId == -1? 0 : rng.nextLong( maxPageId + 1 );
            int pf_flags = performingRead ? PF_SHARED_LOCK : PF_EXCLUSIVE_LOCK;
            int pageSize = pagedFile.pageSize();

            try ( PageCursor cursor = pagedFile.io( startingPage, pf_flags ) )
            {
                if ( performingRead )
                {
                    performConsistentAdversarialRead( cursor, maxPageId, startingPage, pageSize );
                }
                else
                {
                    performConsistentAdversarialWrite( cursor, rng, pageSize );
                }
            }
            catch ( AssertionError error )
            {
                // Capture any exception that might have hit the eviction thread.
                adversary.setProbabilityFactor( 0.0 );
                try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
                {
                    for ( int j = 0; j < 100; j++ )
                    {
                        cursor.next( rng.nextLong( maxPageId + 1 ) );
                    }
                }
                catch ( Throwable throwable )
                {
                    error.addSuppressed( throwable );
                }

                throw error;
            }
            catch ( Throwable throwable )
            {
                // Don't worry about it... it's fine!
//                throwable.printStackTrace(); // only enable this when debugging test failures.
            }
        }

        // Unmapping will cause pages to be flushed.
        // We don't want that to fail, since it will upset the test tear-down.
        adversary.setProbabilityFactor( 0.0 );
        try
        {
            // Flushing all pages, if successful, should clear any internal
            // exception.
            pageCache.flushAndForce();

            // Do some post-chaos verification of what has been written.
            verifyAdversarialPagedContent( pfA );
            verifyAdversarialPagedContent( pfB );

            pfA.close();
            pfB.close();
        }
        catch ( Throwable e )
        {
            tracer.printHistory( System.err );
            throw e;
        }
    }

    private void performConsistentAdversarialRead( PageCursor cursor, long maxPageId, long startingPage,
                                                   int pageSize ) throws IOException
    {
        long pagesToLookAt = Math.min( maxPageId, startingPage + 3 ) - startingPage + 1;
        for ( int j = 0; j < pagesToLookAt; j++ )
        {
            assertTrue( cursor.next() );
            readAndVerifyAdversarialPage( cursor, pageSize );
        }
    }

    private void readAndVerifyAdversarialPage( PageCursor cursor, int pageSize ) throws IOException
    {
        byte[] actualPage = new byte[pageSize];
        byte[] expectedPage = new byte[pageSize];
        do
        {
            cursor.getBytes( actualPage );
        }
        while ( cursor.shouldRetry() );
        Arrays.fill( expectedPage, actualPage[0] );
        String msg = String.format(
                "filePageId = %s, pageSize = %s",
                cursor.getCurrentPageId(), pageSize );
        assertThat( msg, actualPage, byteArray( expectedPage ) );
    }

    private void performConsistentAdversarialWrite( PageCursor cursor, ThreadLocalRandom rng, int pageSize ) throws IOException
    {
        for ( int j = 0; j < 3; j++ )
        {
            assertTrue( cursor.next() );
            // Avoid generating zeros, so we can tell them apart from the
            // absence of a write:
            byte b = (byte) rng.nextInt( 1, 127 );
            for ( int k = 0; k < pageSize; k++ )
            {
                cursor.putByte( b );
            }
            assertFalse( cursor.shouldRetry() );
        }
    }

    private void verifyAdversarialPagedContent( PagedFile pagedFile ) throws IOException
    {
        try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK ) )
        {
            while ( cursor.next() )
            {
                readAndVerifyAdversarialPage( cursor, pagedFile.pageSize() );
            }
        }
    }

    // NOTE: This test is CPU architecture dependent, but it should fail on no
    // architecture that we support.
    // This test has no timeout because one may want to run it on a CPU
    // emulator, where it's not unthinkable for it to take minutes.
    @Test( timeout = SEMI_LONG_TIMEOUT_MILLIS )
    public void mustSupportUnalignedWordAccesses() throws Exception
    {
        // 8 MB pages, 10 of them for 80 MB.
        // This way we are sure to write across OS page boundaries. The default
        // size of Huge Pages on Linux is 2 MB, but it can be configured to be
        // as large as 1 GB - at least I have not heard of anyone trying to
        // configure it to be more than that.
        int pageSize = 1024 * 1024 * 8;
        getPageCache( fs, 10, pageSize, PageCacheTracer.NULL );

        ThreadLocalRandom rng = ThreadLocalRandom.current();

        try ( PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );
              PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            assertTrue( cursor.next() );

            for ( int i = 0; i < pageSize - 8; i++ )
            {
                cursor.setOffset( i );
                long x = rng.nextLong();
                cursor.putLong( x );
                cursor.setOffset( i );
                String reason =
                        "Failed to read back the value that was written at " +
                        "offset " + toHexString( i );
                assertThat( reason,
                        toHexString( cursor.getLong() ),
                        is( toHexString( x ) ) );
            }
        }
    }

    @RepeatRule.Repeat( times = 50 )
    @Test( timeout = SEMI_LONG_TIMEOUT_MILLIS )
    public void mustEvictPagesFromUnmappedFiles() throws Exception
    {
        // GIVEN mapping then unmapping
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        try ( PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );
              PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            assertTrue( cursor.next() );
        }

        // WHEN using all pages, so that eviction of some pages will happen
        try ( PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize );
              PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            for ( int i = 0; i < maxPages+5; i++ )
            {
                // THEN eviction happening here should not result in any exception
                assertTrue( cursor.next() );
            }
        }
    }

    @Test( timeout = SEMI_LONG_TIMEOUT_MILLIS )
    public void mustFlushDirtyPagesInTheBackground() throws Exception
    {
        final CountDownLatch swapOutLatch = new CountDownLatch( 1 );
        PageSwapperFactory swapperFactory = new SingleFilePageSwapperFactory()
        {
            @Override
            public PageSwapper createPageSwapper(
                    File file, int filePageSize, PageEvictionCallback onEviction, boolean createIfNotExist ) throws IOException
            {
                PageSwapper delegate = super.createPageSwapper( file, filePageSize, onEviction, createIfNotExist );
                return new DelegatingPageSwapper( delegate )
                {
                    @Override
                    public long write( long filePageId, Page page ) throws IOException
                    {
                        try
                        {
                            return super.write( filePageId, page );
                        }
                        finally
                        {
                            swapOutLatch.countDown();
                        }
                    }
                };
            }
        };
        swapperFactory.setFileSystemAbstraction( fs );

        try ( PageCache pageCache = createPageCache(
                swapperFactory, maxPages, pageCachePageSize, PageCacheTracer.NULL );
              PagedFile pagedFile = pageCache.map( file( "a" ), filePageSize ) )
        {
            try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
            {
                assertTrue( cursor.next() );
                writeRecords( cursor );
            }

            swapOutLatch.await();
            verifyRecordsInFile( file( "a" ), recordsPerFilePage );
        }
    }

    @Test( timeout = SEMI_LONG_TIMEOUT_MILLIS )
    public void mustReadZerosFromBeyondEndOfFile() throws Exception
    {
        StandardRecordFormat recordFormat = new StandardRecordFormat();
        File[] files = {
                file( "1" ), file( "2" ), file( "3" ), file( "4" ), file( "5" ), file( "6" ),
                file( "7" ), file( "8" ), file( "9" ), file( "0" ), file( "A" ), file( "B" ),
        };
        for ( int fileId = 0; fileId < files.length; fileId++ )
        {
            File file = files[fileId];
            StoreChannel channel = fs.open( file, "rw" );
            for ( int recordId = 0; recordId < fileId + 1; recordId++ )
            {
                Record record = recordFormat.createRecord( file, recordId );
                recordFormat.writeRecord( record, channel );
            }
            channel.close();
        }

        int pageSize = nextPowerOf2( recordFormat.getRecordSize() * (files.length + 1) );
        getPageCache( fs, 2, pageSize, PageCacheTracer.NULL );

        int fileId = files.length;
        while ( fileId --> 0 )
        {
            File file = files[fileId];
            try ( PagedFile pf = pageCache.map( file, pageSize );
                  PageCursor cursor = pf.io( 0, PF_SHARED_LOCK ) )
            {
                int pageCount = 0;
                while( cursor.next() )
                {
                    pageCount++;
                    recordFormat.assertRecordsWrittenCorrectly( cursor );
                }
                assertThat( "pages in file " + file, pageCount, greaterThan( 0 ) );
            }
        }
    }

    private int nextPowerOf2( int i )
    {
        return 1 << (32 - Integer.numberOfLeadingZeros( i ) );
    }

    private PageSwapperFactory factoryCountingSyncDevice(
            final AtomicInteger syncDeviceCounter,
            final Queue<Integer> expectedCountsInForce )
    {
        SingleFilePageSwapperFactory factory = new SingleFilePageSwapperFactory()
        {
            @Override
            public void syncDevice()
            {
                super.syncDevice();
                syncDeviceCounter.getAndIncrement();
            }

            @Override
            public PageSwapper createPageSwapper(
                    File file, int filePageSize, PageEvictionCallback onEviction, boolean createIfNotExist ) throws IOException
            {
                PageSwapper delegate = super.createPageSwapper( file, filePageSize, onEviction, createIfNotExist );
                return new DelegatingPageSwapper( delegate )
                {
                    @Override
                    public void force() throws IOException
                    {
                        super.force();
                        assertThat( syncDeviceCounter.get(), is( expectedCountsInForce.poll() ) );
                    }
                };
            }
        };
        factory.setFileSystemAbstraction( fs );
        return factory;
    }

    @SafeVarargs
    private static <E> Queue<E> queue( E... items )
    {
        Queue<E> queue = new ConcurrentLinkedQueue<>();
        for ( E item : items )
        {
            queue.offer( item );
        }
        return queue;
    }

    @Test( timeout = SEMI_LONG_TIMEOUT_MILLIS )
    public void mustSyncDeviceWhenFlushAndForcingPagedFile() throws Exception
    {
        AtomicInteger syncDeviceCounter = new AtomicInteger();
        AtomicInteger expectedCountInForce = new AtomicInteger();
        Queue<Integer> expectedCountsInForce = queue(
                0,      // at `p1.flushAndForce` no `syncDevice` has happened before the force
                1, 2 ); // closing+forcing the files one by one, we get 2 more `syncDevice`
        PageSwapperFactory factory = factoryCountingSyncDevice( syncDeviceCounter, expectedCountsInForce );
        try ( PageCache cache = createPageCache( factory, maxPages, pageCachePageSize, PageCacheTracer.NULL );
              PagedFile p1 = cache.map( existingFile( "a" ), filePageSize );
              PagedFile p2 = cache.map( existingFile( "b" ), filePageSize ) )
        {
            try ( PageCursor cursor = p1.io( 0, PF_EXCLUSIVE_LOCK ) )
            {
                assertTrue( cursor.next() );
            }
            try ( PageCursor cursor = p2.io( 0, PF_EXCLUSIVE_LOCK ) )
            {
                assertTrue( cursor.next() );
            }

            p1.flushAndForce();
            expectedCountInForce.set( 1 );
            assertThat( syncDeviceCounter.get(), is( 1 ) );
        }
    }

    @Test( timeout = SEMI_LONG_TIMEOUT_MILLIS )
    public void mustSyncDeviceWhenFlushAndForcingPageCache() throws Exception
    {
        AtomicInteger syncDeviceCounter = new AtomicInteger();
        AtomicInteger expectedCountInForce = new AtomicInteger();
        Queue<Integer> expectedCountsInForce = queue(
                0, 0,   // `cache.flushAndForce` forces the individual files, no `syncDevice` yet
                1, 2 ); // after test, files are closed+forced one by one
        PageSwapperFactory factory = factoryCountingSyncDevice( syncDeviceCounter, expectedCountsInForce );
        try ( PageCache cache = createPageCache( factory, maxPages, pageCachePageSize, PageCacheTracer.NULL );
              PagedFile p1 = cache.map( existingFile( "a" ), filePageSize );
              PagedFile p2 = cache.map( existingFile( "b" ), filePageSize ) )
        {
            try ( PageCursor cursor = p1.io( 0, PF_EXCLUSIVE_LOCK ) )
            {
                assertTrue( cursor.next() );
            }
            try ( PageCursor cursor = p2.io( 0, PF_EXCLUSIVE_LOCK ) )
            {
                assertTrue( cursor.next() );
            }

            cache.flushAndForce();
            expectedCountInForce.set( 1 );
            assertThat( syncDeviceCounter.get(), is( 1 ) );
        }
    }

    @Test(expected = NoSuchFileException.class)
    public void mustThrowWhenMappingNonExistingFile() throws Exception
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        pageCache.map( file( "does not exist" ), filePageSize );
    }

    @Test( timeout = SEMI_LONG_TIMEOUT_MILLIS )
    public void mustCreateNonExistingFileWithCreateOption() throws Exception
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        try ( PagedFile pf = pageCache.map( file( "does not exist" ), filePageSize, StandardOpenOption.CREATE );
              PageCursor cursor = pf.io( 0, PF_EXCLUSIVE_LOCK ))
        {
            assertTrue( cursor.next() );
        }
    }

    @Test( timeout = SEMI_LONG_TIMEOUT_MILLIS )
    public void mustIgnoreCreateOptionIfFileAlreadyExists() throws Exception
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        try ( PagedFile pf = pageCache.map( file( "a" ), filePageSize, StandardOpenOption.CREATE );
              PageCursor cursor = pf.io( 0, PF_EXCLUSIVE_LOCK ))
        {
            assertTrue( cursor.next() );
        }
    }

    @Test( timeout = SEMI_LONG_TIMEOUT_MILLIS )
    public void mustIgnoreCertainOpenOptions() throws Exception
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        try ( PagedFile pf = pageCache.map( file( "a" ), filePageSize,
                StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.APPEND,
                StandardOpenOption.SPARSE );
              PageCursor cursor = pf.io( 0, PF_EXCLUSIVE_LOCK ))
        {
            assertTrue( cursor.next() );
        }
    }

    @Test( timeout = SEMI_LONG_TIMEOUT_MILLIS )
    public void mustThrowOnUnsupportedOpenOptions() throws Exception
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        verifyMappingWithOpenOptionThrows( StandardOpenOption.CREATE_NEW );
        verifyMappingWithOpenOptionThrows( StandardOpenOption.DELETE_ON_CLOSE );
        verifyMappingWithOpenOptionThrows( StandardOpenOption.SYNC );
        verifyMappingWithOpenOptionThrows( StandardOpenOption.DSYNC );
        verifyMappingWithOpenOptionThrows( new OpenOption()
        {
            @Override
            public String toString()
            {
                return "NonStandardOpenOption";
            }
        } );
    }

    private void verifyMappingWithOpenOptionThrows( OpenOption option ) throws IOException
    {
        try
        {
            pageCache.map( file( "a" ), filePageSize, option ).close();
            fail( "Expected PageCache.map() to throw when given the OpenOption " + option );
        }
        catch ( IllegalArgumentException | UnsupportedOperationException e )
        {
            // good
        }
    }

    @Test( timeout = SEMI_LONG_TIMEOUT_MILLIS )
    public void mappingFileWithTruncateOptionMustTruncateFile() throws Exception
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        try ( PagedFile pf = pageCache.map( file( "a" ), filePageSize );
              PageCursor cursor = pf.io( 10, PF_EXCLUSIVE_LOCK ) )
        {
            assertThat( pf.getLastPageId(), is( -1L ) );
            assertTrue( cursor.next() );
            cursor.putInt( 0xcafebabe );
        }
        try ( PagedFile pf = pageCache.map( file( "a" ), filePageSize, StandardOpenOption.TRUNCATE_EXISTING );
              PageCursor cursor = pf.io( 0, PF_SHARED_LOCK ) )
        {
            assertThat( pf.getLastPageId(), is( -1L ) );
            assertFalse( cursor.next() );
        }
    }

    @SuppressWarnings( "unused" )
    @Test( expected = UnsupportedOperationException.class )
    public void mappingAlreadyMappedFileWithTruncateOptionMustThrow() throws Exception
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
        try ( PagedFile first = pageCache.map( file( "a" ), filePageSize );
              PagedFile second = pageCache.map( file( "a" ), filePageSize, StandardOpenOption.TRUNCATE_EXISTING ) )
        {
            fail( "the second map call should have thrown" );
        }
    }
}
