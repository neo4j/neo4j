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
package org.neo4j.io.pagecache;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.adversaries.RandomAdversary;
import org.neo4j.adversaries.fs.AdversarialFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.test.LinearHistoryPageCacheTracer;
import org.neo4j.test.RepeatRule;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.io.pagecache.PagedFile.PF_EXCLUSIVE_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_LOCK;
import static org.neo4j.test.ByteArrayMatcher.byteArray;

public abstract class PageCacheSlowTest<T extends PageCache> extends PageCacheTestSupport<T>
{
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

        executor.submit( () -> {
            try ( PageCursor cursor = pf.io( 0, PF_EXCLUSIVE_LOCK ) )
            {
                cursor.next();
                hasLockLatch.countDown();
                unlockLatch.await();
            }
            return null;
        } );

        hasLockLatch.await(); // An exclusive lock is now held on page 0.

        Future<Object> takeLockFuture = executor.submit( () -> {
            try ( PageCursor cursor = pf.io( 0, PF_EXCLUSIVE_LOCK ) )
            {
                cursor.next();
                secondThreadGotLockLatch.await();
            }
            return null;
        } );

        Future<Object> closeFuture = executor.submit( () -> {
            pf.close();
            return null;
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
            closeFuture.get( 2000, TimeUnit.MILLISECONDS );
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
            long startingPage = maxPageId < 0? 0 : rng.nextLong( maxPageId + 1 );
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
}
