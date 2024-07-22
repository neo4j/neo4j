/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.io.pagecache;

import static java.time.Duration.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_WRITE_LOCK;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.tracing.linear.LinearHistoryTracerFactory.pageCacheTracer;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
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
import java.util.stream.IntStream;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.ImmutableSet;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.neo4j.adversaries.RandomAdversary;
import org.neo4j.adversaries.fs.AdversarialFileSystemAbstraction;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.impl.FileIsNotMappedException;
import org.neo4j.io.pagecache.tracing.DatabaseFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.linear.LinearTracers;

public abstract class PageCacheSlowTest<T extends PageCache> extends PageCacheTestSupport<T> {
    private static class UpdateResult {
        final int threadId;
        final long realThreadId;
        final int[] pageCounts;

        UpdateResult(int threadId, int[] pageCounts) {
            this.threadId = threadId;
            this.realThreadId = Thread.currentThread().getId();
            this.pageCounts = pageCounts;
        }
    }

    private abstract static class UpdateWorker implements Callable<UpdateResult> {
        final int threadId;
        final int filePages;
        final AtomicBoolean shouldStop;
        final PagedFile pagedFile;
        final int[] pageCounts;
        final int offset;

        UpdateWorker(int threadId, int filePages, AtomicBoolean shouldStop, PagedFile pagedFile) {
            this.threadId = threadId;
            this.filePages = filePages;
            this.shouldStop = shouldStop;
            this.pagedFile = pagedFile;
            pageCounts = new int[filePages];
            offset = threadId * 4;
        }

        @Override
        public UpdateResult call() throws Exception {
            ThreadLocalRandom rng = ThreadLocalRandom.current();

            while (!shouldStop.get()) {
                boolean updateCounter = rng.nextBoolean();
                int pfFlags = updateCounter ? PF_SHARED_WRITE_LOCK : PF_SHARED_READ_LOCK;
                performReadOrUpdate(rng, updateCounter, pfFlags);
            }

            return new UpdateResult(threadId, pageCounts);
        }

        protected abstract void performReadOrUpdate(ThreadLocalRandom rng, boolean updateCounter, int pf_flags)
                throws IOException;
    }

    @RepeatedTest(50)
    void mustNotLoseUpdates() {
        assertTimeoutPreemptively(ofMillis(10 * LONG_TIMEOUT_MILLIS), () -> {
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
            final int threadCount = 4;

            // For debugging via the linear tracers:
            //        LinearTracers linearTracers = LinearHistoryTracerFactory.pageCacheTracer();
            //        getPageCache( fs, cachePages, pageSize, linearTracers.getPageCacheTracer(),
            //                linearTracers.getCursorTracerSupplier() );
            getPageCache(fs, cachePages, PageCacheTracer.NULL);
            final int pageSize = getReservedBytes(pageCache) + threadCount * 4;
            try (var pagedFile = pageCache.map(file("a"), pageSize, DEFAULT_DATABASE_NAME, getOpenOptions())) {
                ensureAllPagesExists(filePages, pagedFile);

                var futures = new ArrayList<Future<UpdateResult>>();
                for (int i = 0; i < threadCount; i++) {
                    var worker = new UpdateWorker(i, filePages, shouldStop, pagedFile) {
                        @Override
                        protected void performReadOrUpdate(ThreadLocalRandom rng, boolean updateCounter, int pf_flags)
                                throws IOException {
                            int pageId = rng.nextInt(0, filePages);
                            try (var cursor = pagedFile.io(pageId, pf_flags, NULL_CONTEXT)) {
                                int counter;
                                try {
                                    assertTrue(cursor.next());
                                    do {
                                        cursor.setOffset(offset);
                                        counter = cursor.getInt();
                                    } while (cursor.shouldRetry());
                                    String lockName = updateCounter ? "PF_SHARED_WRITE_LOCK" : "PF_SHARED_READ_LOCK";
                                    String reason = String.format(
                                            "inconsistent page read from filePageId:%s, with %s, threadId:%s",
                                            pageId,
                                            lockName,
                                            Thread.currentThread().getId());
                                    assertThat(counter).as(reason).isEqualTo(pageCounts[pageId]);
                                } catch (Throwable throwable) {
                                    shouldStop.set(true);
                                    throw throwable;
                                }
                                if (updateCounter) {
                                    counter++;
                                    pageCounts[pageId]++;
                                    cursor.setOffset(offset);
                                    cursor.putInt(counter);
                                }
                                if (cursor.checkAndClearBoundsFlag()) {
                                    shouldStop.set(true);
                                    throw new IndexOutOfBoundsException("offset = " + offset + ", filPageId:" + pageId
                                            + ", threadId: " + threadId + ", updateCounter = " + updateCounter);
                                }
                            }
                        }
                    };
                    futures.add(executor.submit(worker));
                }

                Thread.sleep(10);
                shouldStop.set(true);

                try {
                    verifyUpdateResults(filePages, pagedFile, futures);
                } catch (Throwable e) {
                    // For debugging via linear tracers:
                    //            synchronized ( System.err )
                    //            {
                    //                System.err.flush();
                    //                linearTracers.printHistory( System.err );
                    //                System.err.flush();
                    //            }
                    //            try ( PrintStream out = new PrintStream( "trace.log" ) )
                    //            {
                    //                linearTracers.printHistory( out );
                    //                out.flush();
                    //            }
                    throw e;
                }
            }
        });
    }

    protected ImmutableSet<OpenOption> getOpenOptions() {
        return Sets.immutable.empty();
    }

    private int getReservedBytes(PageCache pageCache) {
        return pageCache.pageReservedBytes(getOpenOptions());
    }

    private void ensureAllPagesExists(int filePages, PagedFile pagedFile) throws IOException {
        try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            for (int i = 0; i < filePages; i++) {
                assertTrue(cursor.next(), "failed to initialise file page " + i);
            }
        }
        pageCache.flushAndForce(DatabaseFlushEvent.NULL);
    }

    private static void verifyUpdateResults(int filePages, PagedFile pagedFile, List<Future<UpdateResult>> futures)
            throws InterruptedException, ExecutionException, IOException {
        UpdateResult[] results = new UpdateResult[futures.size()];
        for (int i = 0; i < results.length; i++) {
            results[i] = futures.get(i).get();
        }
        for (UpdateResult result : results) {
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                for (int i = 0; i < filePages; i++) {
                    assertTrue(cursor.next());

                    int threadId = result.threadId;
                    int expectedCount = result.pageCounts[i];
                    int actualCount;
                    do {
                        cursor.setOffset(threadId * 4);
                        actualCount = cursor.getInt();
                    } while (cursor.shouldRetry());

                    assertThat(actualCount)
                            .as("wrong count for threadId:" + threadId + ", aka. real threadId:" + result.realThreadId
                                    + ", filePageId:" + i)
                            .isEqualTo(expectedCount);
                }
            }
        }
    }

    protected IntStream maybeDistinct(IntStream stream) {
        return stream;
    }

    @RepeatedTest(100)
    void mustNotLoseUpdatesWhenOpeningMultiplePageCursorsPerThread() {
        assertTimeoutPreemptively(ofMillis(SEMI_LONG_TIMEOUT_MILLIS), () -> {
            // Similar to the test above, except the threads will have multiple page cursors opened at a time.

            final AtomicBoolean shouldStop = new AtomicBoolean();
            final int cachePages = 40;
            final int filePages = cachePages * 2;
            final int threadCount = 8;

            // It's very important that even if all threads grab their maximum number of pages at the same time, there
            // will
            // still be free pages left in the cache. If we don't keep this invariant, then there's a chance that our
            // test
            // will run into live-locks, where a page fault will try to find a page to cooperatively evict, but all
            // pages
            // in cache are already taken.
            final int maxCursorsPerThread = cachePages / (1 + threadCount);
            assertThat(maxCursorsPerThread * threadCount).isLessThan(cachePages);

            getPageCache(fs, cachePages, PageCacheTracer.NULL);
            final int pageSize = getReservedBytes(pageCache) + threadCount * 4;
            try (PagedFile pagedFile = pageCache.map(file("a"), pageSize, DEFAULT_DATABASE_NAME, getOpenOptions())) {

                ensureAllPagesExists(filePages, pagedFile);

                List<Future<UpdateResult>> futures = new ArrayList<>();
                for (int i = 0; i < threadCount; i++) {
                    UpdateWorker worker = new UpdateWorker(i, filePages, shouldStop, pagedFile) {
                        @Override
                        protected void performReadOrUpdate(ThreadLocalRandom rng, boolean updateCounter, int pf_flags)
                                throws IOException {
                            try {
                                int pageCount = rng.nextInt(1, maxCursorsPerThread);
                                // in multiversion mode page write locks are not reentrant and can cause deadlock, so
                                // pageIds should be unique and sorted
                                int[] pageIds = maybeDistinct(rng.ints(0, filePages))
                                        .limit(pageCount)
                                        .sorted()
                                        .toArray();

                                PageCursor[] cursors = new PageCursor[pageCount];
                                try {

                                    for (int j = 0; j < pageCount; j++) {
                                        cursors[j] = pagedFile.io(pageIds[j], pf_flags, NULL_CONTEXT);
                                        assertTrue(cursors[j].next());
                                    }
                                    for (int j = 0; j < pageCount; j++) {
                                        int pageId = pageIds[j];
                                        PageCursor cursor = cursors[j];
                                        int counter;
                                        do {
                                            cursor.setOffset(offset);
                                            counter = cursor.getInt();
                                        } while (cursor.shouldRetry());
                                        String lockName =
                                                updateCounter ? "PF_SHARED_WRITE_LOCK" : "PF_SHARED_READ_LOCK";
                                        String reason = String.format(
                                                "inconsistent page read from filePageId = %s, with %s, workerId = %s [t:%s]",
                                                pageId,
                                                lockName,
                                                threadId,
                                                Thread.currentThread().getId());
                                        assertThat(counter).as(reason).isEqualTo(pageCounts[pageId]);
                                        if (updateCounter) {
                                            counter++;
                                            pageCounts[pageId]++;
                                            cursor.setOffset(offset);
                                            cursor.putInt(counter);
                                        }
                                    }
                                } finally {
                                    for (PageCursor cursor : cursors) {
                                        if (cursor != null) {
                                            cursor.close();
                                        }
                                    }
                                }
                            } catch (Throwable throwable) {
                                shouldStop.set(true);
                                throwable.printStackTrace();
                                throw throwable;
                            }
                        }
                    };
                    futures.add(executor.submit(worker));
                }

                Thread.sleep(40);
                shouldStop.set(true);

                verifyUpdateResults(filePages, pagedFile, futures);
            }
        });
    }

    @RepeatedTest(50)
    void writeLockingCursorMustThrowWhenLockingPageRacesWithUnmapping() {
        assertTimeoutPreemptively(ofMillis(SEMI_LONG_TIMEOUT_MILLIS), () -> {
            // Even if we block in pin, waiting to grab a lock on a page that is
            // already locked, and the PagedFile is concurrently closed, then we
            // want to have an exception thrown, such that we race and get a
            // page that is writable after the PagedFile has been closed.
            // This is important because closing a PagedFile implies flushing, thus
            // ensuring that all changes make it to storage.
            // Conversely, we don't have to go to the same lengths for read locked
            // pages, because those are never changed. Not by us, anyway.

            getPageCache(fs, maxPages, PageCacheTracer.NULL);

            Path file = file("a");
            generateFileWithRecords(
                    file,
                    recordsPerFilePage * 2,
                    recordSize,
                    recordsPerFilePage,
                    getReservedBytes(pageCache),
                    pageCachePageSize);

            final PagedFile pf = pageCache.map(file, filePageSize, DEFAULT_DATABASE_NAME, getOpenOptions());
            final CountDownLatch hasLockLatch = new CountDownLatch(1);
            final CountDownLatch unlockLatch = new CountDownLatch(1);
            final CountDownLatch secondThreadGotLockLatch = new CountDownLatch(1);
            final AtomicBoolean doneWriteSignal = new AtomicBoolean();
            final AtomicBoolean doneCloseSignal = new AtomicBoolean();

            executor.submit(() -> {
                try (PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                    cursor.next();
                    hasLockLatch.countDown();
                    unlockLatch.await();
                }
                return null;
            });

            hasLockLatch.await(); // A write lock is now held on page 0.

            Future<Object> takeLockFuture = executor.submit(() -> {
                try (PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                    cursor.next();
                    doneWriteSignal.set(true);
                    secondThreadGotLockLatch.await();
                }
                return null;
            });

            Future<Object> closeFuture = executor.submit(() -> {
                pf.close();
                doneCloseSignal.set(true);
                return null;
            });

            try {
                Thread.yield();
                closeFuture.get(50, TimeUnit.MILLISECONDS);
                fail("Expected a TimeoutException here");
            } catch (TimeoutException e) {
                // As expected, the close cannot not complete while an write
                // lock is held
            }

            // Now, both the close action and a grab for an write page lock is
            // waiting for our first thread.
            // When we release that lock, we should see that either close completes
            // and our second thread, the one blocked on the write lock, gets an
            // exception, or we should see that the second thread gets the lock,
            // and then close has to wait for that thread as well.

            unlockLatch.countDown(); // The race is on.

            boolean anyDone;
            do {
                Thread.yield();
                anyDone = doneWriteSignal.get() | doneCloseSignal.get();
            } while (!anyDone);

            if (doneCloseSignal.get()) {
                closeFuture.get(1000, TimeUnit.MILLISECONDS);
                // The closeFuture got it first, so the takeLockFuture should throw.
                try {
                    secondThreadGotLockLatch.countDown(); // only to prevent incorrect programs from deadlocking
                    takeLockFuture.get();
                    fail("Expected takeLockFuture.get() to throw an ExecutionException");
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    assertThat(cause).isInstanceOf(FileIsNotMappedException.class);
                    assertThat(cause.getMessage()).startsWith("File has been unmapped");
                }
            } else {
                assertTrue(doneWriteSignal.get());
                // The takeLockFuture got it first, so the closeFuture should
                // complete when we release the latch.
                secondThreadGotLockLatch.countDown();
                closeFuture.get(1, TimeUnit.MINUTES);
            }
        });
    }

    @RepeatedTest(20)
    void pageCacheMustRemainInternallyConsistentWhenGettingRandomFailures() {
        assertTimeoutPreemptively(ofMillis(LONG_TIMEOUT_MILLIS), () -> {
            // NOTE: This test is inherently non-deterministic. This means that every failure must be
            // thoroughly investigated, since they have a good chance of being a real issue.
            // This is effectively a targeted robustness test.

            RandomAdversary adversary = new RandomAdversary(0.5, 0.2, 0.2);
            adversary.enableAdversary(false);
            FileSystemAbstraction fs = new AdversarialFileSystemAbstraction(adversary, this.fs);
            ThreadLocalRandom rng = ThreadLocalRandom.current();

            // Because our test failures are non-deterministic, we use this tracer to capture a full history of the
            // events leading up to any given failure.
            LinearTracers linearTracers = pageCacheTracer();
            getPageCache(fs, maxPages, linearTracers.getPageCacheTracer());

            try (PagedFile pfA =
                            pageCache.map(existingFile("a"), filePageSize, DEFAULT_DATABASE_NAME, getOpenOptions());
                    PagedFile pfB = pageCache.map(
                            existingFile("b"), filePageSize / 2 + 1, DEFAULT_DATABASE_NAME, getOpenOptions())) {
                adversary.enableAdversary(true);

                for (int i = 0; i < 1000; i++) {
                    PagedFile pagedFile = rng.nextBoolean() ? pfA : pfB;
                    long maxPageId = pagedFile.getLastPageId();
                    boolean performingRead = rng.nextBoolean() && maxPageId != -1;
                    long startingPage = maxPageId < 0 ? 0 : rng.nextLong(maxPageId + 1);
                    int pfFlags = performingRead ? PF_SHARED_READ_LOCK : PF_SHARED_WRITE_LOCK;
                    int payload = pagedFile.payloadSize();

                    try (PageCursor cursor = pagedFile.io(startingPage, pfFlags, NULL_CONTEXT)) {
                        if (performingRead) {
                            performConsistentAdversarialRead(cursor, maxPageId, startingPage, payload);
                        } else {
                            performConsistentAdversarialWrite(cursor, rng, payload);
                        }
                    } catch (AssertionError error) {
                        // Capture any exception that might have hit the eviction thread.
                        adversary.enableAdversary(false);
                        try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                            for (int j = 0; j < 100; j++) {
                                cursor.next(rng.nextLong(maxPageId + 1));
                            }
                        } catch (Throwable throwable) {
                            error.addSuppressed(throwable);
                        }

                        throw error;
                    } catch (Throwable throwable) {
                        // Don't worry about it... it's fine!
                        //                throwable.printStackTrace(); // only enable this when debugging test failures.
                    }
                }

                // Unmapping will cause pages to be flushed.
                // We don't want that to fail, since it will upset the test tear-down.
                adversary.enableAdversary(false);
                try {
                    // Flushing all pages, if successful, should clear any internal
                    // exception.
                    pageCache.flushAndForce(DatabaseFlushEvent.NULL);

                    // Do some post-chaos verification of what has been written.
                    verifyAdversarialPagedContent(pfA);
                    verifyAdversarialPagedContent(pfB);
                } catch (Throwable e) {
                    linearTracers.printHistory(System.err);
                    throw e;
                }
            }
        });
    }

    private static void performConsistentAdversarialRead(
            PageCursor cursor, long maxPageId, long startingPage, int payloadSize) throws IOException {
        long pagesToLookAt = Math.min(maxPageId, startingPage + 3) - startingPage + 1;
        for (int j = 0; j < pagesToLookAt; j++) {
            assertTrue(cursor.next());
            readAndVerifyAdversarialPage(cursor, payloadSize);
        }
    }

    private static void readAndVerifyAdversarialPage(PageCursor cursor, int payloadSize) throws IOException {
        byte[] actualPage = new byte[payloadSize];
        byte[] expectedPage = new byte[payloadSize];
        do {
            cursor.getBytes(actualPage);
        } while (cursor.shouldRetry());
        Arrays.fill(expectedPage, actualPage[0]);
        String msg = String.format("filePageId = %s, payloadSize = %s", cursor.getCurrentPageId(), payloadSize);
        assertThat(actualPage).as(msg).containsExactly(expectedPage);
    }

    private static void performConsistentAdversarialWrite(PageCursor cursor, ThreadLocalRandom rng, int pageSize)
            throws IOException {
        for (int j = 0; j < 3; j++) {
            assertTrue(cursor.next());
            // Avoid generating zeros, so we can tell them apart from the
            // absence of a write:
            byte b = (byte) rng.nextInt(1, 127);
            for (int k = 0; k < pageSize; k++) {
                cursor.putByte(b);
            }
            assertFalse(cursor.shouldRetry());
        }
    }

    private static void verifyAdversarialPagedContent(PagedFile pagedFile) throws IOException {
        try (PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
            while (cursor.next()) {
                readAndVerifyAdversarialPage(cursor, pagedFile.payloadSize());
            }
        }
    }

    @Test
    void mustNotRunOutOfSwapperAllocationSpace() throws Exception {
        assumeTrue(
                fs instanceof EphemeralFileSystemAbstraction,
                "This test is file system agnostic, and too slow on a real file system");
        configureStandardPageCache();

        Path file = file("a");
        int iterations = Short.MAX_VALUE * 3;
        for (int i = 0; i < iterations; i++) {
            try (PagedFile pagedFile = pageCache.map(file, filePageSize, DEFAULT_DATABASE_NAME, getOpenOptions())) {
                try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                    assertTrue(cursor.next());
                }
            }
        }
    }
}
