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

import static java.lang.Long.toHexString;
import static java.lang.System.currentTimeMillis;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.DELETE_ON_CLOSE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.time.Duration.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.eclipse.collections.api.factory.Sets.immutable;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_flush_buffer_size_in_pages;
import static org.neo4j.internal.helpers.Numbers.ceilingPowerOfTwo;
import static org.neo4j.io.memory.ByteBuffers.allocateDirect;
import static org.neo4j.io.memory.ByteBuffers.releaseBuffer;
import static org.neo4j.io.pagecache.PageCache.PAGE_SIZE;
import static org.neo4j.io.pagecache.PagedFile.PF_EAGER_FLUSH;
import static org.neo4j.io.pagecache.PagedFile.PF_NO_FAULT;
import static org.neo4j.io.pagecache.PagedFile.PF_NO_GROW;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_WRITE_LOCK;
import static org.neo4j.io.pagecache.buffer.IOBufferFactory.DISABLED_BUFFER_FACTORY;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.test.ThreadTestUtils.fork;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ReadOnlyBufferException;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.eclipse.collections.api.set.ImmutableSet;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.adversaries.RandomAdversary;
import org.neo4j.adversaries.fs.AdversarialFileSystemAbstraction;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.DelegatingFileSystemAbstraction;
import org.neo4j.io.fs.DelegatingStoreChannel;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.impl.FileIsNotMappedException;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.CacheLiveLockException;
import org.neo4j.io.pagecache.impl.muninn.EvictionBouncer;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCursor;
import org.neo4j.io.pagecache.impl.muninn.SwapperSet;
import org.neo4j.io.pagecache.randomharness.Record;
import org.neo4j.io.pagecache.randomharness.StandardRecordFormat;
import org.neo4j.io.pagecache.tracing.DatabaseFlushEvent;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.PinEvent;
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracer;
import org.neo4j.io.pagecache.tracing.version.DefaultVersionStorageTracer;
import org.neo4j.io.pagecache.tracing.version.FileTruncateEvent;
import org.neo4j.io.pagecache.tracing.version.RegionCollectionEvent;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.util.concurrent.BinaryLatch;
import org.neo4j.util.concurrent.Futures;

public abstract class PageCacheTest<T extends PageCache> extends PageCacheTestSupport<T> {

    protected ImmutableSet<OpenOption> getOpenOptions() {
        return immutable.empty();
    }

    protected PagedFile map(PageCache pageCache, Path file, int filePageSize, ImmutableSet<OpenOption> options)
            throws IOException {
        return pageCache.map(
                file, filePageSize, DEFAULT_DATABASE_NAME, getOpenOptions().newWithAll(options));
    }

    protected PagedFile map(PageCache pageCache, Path file, int filePageSize) throws IOException {
        return map(pageCache, file, filePageSize, immutable.empty());
    }

    protected PagedFile map(Path file, int filePageSize) throws IOException {
        return map(pageCache, file, filePageSize, immutable.empty());
    }

    protected PagedFile map(Path file, int filePageSize, ImmutableSet<OpenOption> options) throws IOException {
        return map(pageCache, file, filePageSize, options);
    }

    @Test
    void mustReportConfiguredMaxPages() {
        configureStandardPageCache();
        assertThat(pageCache.maxCachedPages()).isEqualTo(maxPages);
    }

    @Test
    void mustReportConfiguredCachePageSize() {
        configureStandardPageCache();
        assertThat(pageCache.pageSize()).isEqualTo(pageCachePageSize);
    }

    @Test
    void mustHaveAtLeastTwoPages() {
        assertThrows(IllegalArgumentException.class, () -> getPageCache(fs, 1, PageCacheTracer.NULL));
    }

    @Test
    void mustAcceptTwoPagesAsMinimumConfiguration() {
        getPageCache(fs, 2, PageCacheTracer.NULL);
    }

    @Test
    void gettingNameFromMappedFileMustMatchMappedFileName() throws Exception {
        configureStandardPageCache();
        Path file = file("a");
        try (PagedFile pf = map(file, filePageSize)) {
            assertThat(pf.path()).isEqualTo(file);
        }
    }

    @Test
    void mustReadExistingData() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            generateFileWithRecords(
                    file("a"), recordCount, recordSize, recordsPerFilePage, reservedBytes, filePageSize);

            int recordId = 0;
            try (PagedFile pagedFile = map(file("a"), filePageSize);
                    PageCursor cursor = pagedFile.io(0L, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                while (cursor.next()) {
                    verifyRecordsMatchExpected(cursor);
                    recordId += recordsPerFilePage;
                }
            }

            assertThat(recordId).isEqualTo(recordCount);
        });
    }

    @Test
    void mustScanInTheMiddleOfTheFile() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            long startPage = 10;
            long endPage = (recordCount / recordsPerFilePage) - 10;
            generateFileWithRecords(
                    file("a"), recordCount, recordSize, recordsPerFilePage, reservedBytes, filePageSize);

            int recordId = (int) (startPage * recordsPerFilePage);
            try (PagedFile pagedFile = map(file("a"), filePageSize);
                    PageCursor cursor = pagedFile.io(startPage, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                while (cursor.next() && cursor.getCurrentPageId() < endPage) {
                    verifyRecordsMatchExpected(cursor);
                    recordId += recordsPerFilePage;
                }
            }

            assertThat(recordId).isEqualTo(recordCount - (10 * recordsPerFilePage));
        });
    }

    @Test
    void writesFlushedFromPageFileMustBeExternallyObservable() {
        assertTimeoutPreemptively(ofMillis(SEMI_LONG_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            PagedFile pagedFile = map(file("a"), filePageSize);

            long startPageId = 0;
            long endPageId = recordCount / recordsPerFilePage;
            try (PageCursor cursor = pagedFile.io(startPageId, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                while (cursor.getCurrentPageId() < endPageId && cursor.next()) {
                    writeRecords(cursor);
                }
            }

            pagedFile.flushAndForce(FileFlushEvent.NULL);

            verifyRecordsInFile(file("a"), recordCount);
            pagedFile.close();
        });
    }

    @Test
    void pagedFileFlushAndForceMustThrowOnNullIOPSLimiter() {
        configureStandardPageCache();
        assertThrows(NullPointerException.class, () -> {
            try (PagedFile pf =
                    pageCache.map(file("a"), filePageSize, DEFAULT_DATABASE_NAME, immutable.empty(), null)) {
                // empty
            }
        });
    }

    @Test
    void wholePageCacheFlushAndForceExecutedWithoutIOController() throws Exception {
        int pagesToDirty = 10_000;
        PageCache cache = getPageCache(fs, ceilingPowerOfTwo(2 * pagesToDirty), PageCacheTracer.NULL);
        int pagesPerFlush = DISABLED_BUFFER_FACTORY.equals(cache.getBufferFactory())
                ? 1
                : pagecache_flush_buffer_size_in_pages.defaultValue();
        AtomicInteger callbackCounter = new AtomicInteger();
        AtomicInteger ioCounter = new AtomicInteger();
        PageCacheIOController ioController = new PageCacheIOController(ioCounter, pagesPerFlush, callbackCounter);
        PagedFile pfA =
                cache.map(existingFile("a"), filePageSize, DEFAULT_DATABASE_NAME, immutable.empty(), ioController);
        PagedFile pfB =
                cache.map(existingFile("b"), filePageSize, DEFAULT_DATABASE_NAME, immutable.empty(), ioController);

        dirtyManyPages(pfA, pagesToDirty);
        dirtyManyPages(pfB, pagesToDirty);

        cache.flushAndForce(DatabaseFlushEvent.NULL);
        pfA.close();
        pfB.close();

        assertThat(callbackCounter.get()).isEqualTo(0);
        assertThat(ioCounter.get()).isEqualTo(0);
    }

    @Test
    void pagedFileFlushAndForceMustQueryTheGivenIOPSLimiter() throws Exception {
        int pagesToDirty = 10_000;
        PageCache cache = getPageCache(fs, ceilingPowerOfTwo(pagesToDirty), PageCacheTracer.NULL);
        int pagesPerFlush = DISABLED_BUFFER_FACTORY.equals(cache.getBufferFactory())
                ? 1
                : pagecache_flush_buffer_size_in_pages.defaultValue();

        AtomicInteger callbackCounter = new AtomicInteger();
        AtomicInteger ioCounter = new AtomicInteger();
        PageCacheIOController ioController = new PageCacheIOController(ioCounter, pagesPerFlush, callbackCounter);

        PagedFile pf = cache.map(file("a"), filePageSize, DEFAULT_DATABASE_NAME, immutable.empty(), ioController);

        // Dirty a bunch of data
        dirtyManyPages(pf, pagesToDirty);

        pf.flushAndForce(FileFlushEvent.NULL);
        pf.close();

        assertThat(callbackCounter.get()).isGreaterThan(0);
        assertThat(ioCounter.get()).isGreaterThanOrEqualTo(pagesToDirty - 30); // -30 because of the eviction thread
    }

    private static void dirtyManyPages(PagedFile pf, int pagesToDirty) throws IOException {
        dirtyManyPages(pf, pagesToDirty, NULL_CONTEXT);
    }

    private static void dirtyManyPages(PagedFile pf, int pagesToDirty, CursorContext cursorContext) throws IOException {
        try (PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, cursorContext)) {
            for (int i = 0; i < pagesToDirty; i++) {
                assertTrue(cursor.next());
            }
        }
    }

    @Test
    void writesFlushedFromPageFileMustBeObservableEvenWhenRacingWithEviction() {
        assertTimeoutPreemptively(ofMillis(LONG_TIMEOUT_MILLIS), () -> {
            getPageCache(fs, 20, PageCacheTracer.NULL);

            long startPageId = 0;
            long endPageId = 21;
            int iterations = 500;
            int shortsPerPage = pageCachePayloadSize / 2;

            try (PagedFile pagedFile = map(file("a"), pageCachePageSize)) {
                for (int i = 1; i <= iterations; i++) {
                    try (PageCursor cursor = pagedFile.io(startPageId, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                        while (cursor.getCurrentPageId() < endPageId && cursor.next()) {
                            for (int j = 0; j < shortsPerPage; j++) {
                                cursor.putShort((short) i);
                            }
                        }
                    }

                    // There are 20 pages in the cache and we've overwritten 20 pages.
                    // This means eviction has probably fallen behind and is likely
                    // running concurrently right now.
                    // Therefor, a flush right now would have a high chance of racing
                    // with eviction.
                    pagedFile.flushAndForce(FileFlushEvent.NULL);

                    // Race or not, a flush should still put all changes in storage,
                    // so we should be able to verify the contents of the file.
                    try (DataInputStream stream = new DataInputStream(fs.openAsInputStream(file("a")))) {
                        stream.readNBytes(reservedBytes);
                        for (int j = 0; j < shortsPerPage; j++) {
                            int value = Short.reverseBytes(stream.readShort());
                            assertThat(value)
                                    .as("short pos = " + j + ", iteration = " + i)
                                    .isEqualTo(i);
                        }
                    }
                }
            }
        });
    }

    @Test
    void flushAndForceMustNotLockPageCacheForWholeDuration() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            maxPages = 5000;
            configureStandardPageCache();
            Path a = existingFile("a");
            Path b = existingFile("b");
            BinaryLatch limiterStartLatch = new BinaryLatch();
            BinaryLatch limiterBlockLatch = new BinaryLatch();
            var ioController = new EmptyIOController() {
                @Override
                public void maybeLimitIO(int recentlyCompletedIOs, FileFlushEvent flushEvent) {
                    limiterStartLatch.release();
                    limiterBlockLatch.await();
                    super.maybeLimitIO(recentlyCompletedIOs, flushEvent);
                }
            };
            try (PagedFile pfA =
                    pageCache.map(a, filePageSize, DEFAULT_DATABASE_NAME, immutable.empty(), ioController)) {
                // Dirty a bunch of pages.
                try (PageCursor cursor = pfA.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                    for (int i = 0; i < maxPages; i++) {
                        assertTrue(cursor.next());
                    }
                }

                Future<?> flusher = executor.submit(() -> {
                    pfA.flushAndForce(FileFlushEvent.NULL);
                    return null;
                });

                limiterStartLatch.await(); // Flusher is now stuck inside flushAndForce.

                // We should be able to map and close paged files.
                map(pageCache, b, filePageSize).close();
                // We should be able to get and list existing mappings.
                pageCache.listExistingMappings();
                pageCache.getExistingMapping(a).ifPresent(PagedFile::close);

                limiterBlockLatch.release();
                flusher.get();
            }
        });
    }

    @Test
    void flushAndForceMustTolerateAsynchronousFileUnmapping() throws Exception {
        configureStandardPageCache();
        Path a = existingFile("a");
        Path b = existingFile("b");
        Path c = existingFile("c");

        CountDownLatch closeFilesLatch = new CountDownLatch(3);
        BinaryLatch limiterBlockLatch = new BinaryLatch();
        var ioController = new LatchedIOController(closeFilesLatch, limiterBlockLatch);
        List<Future<?>> flushers = new ArrayList<>();

        try (PagedFile pfA = pageCache.map(a, filePageSize, DEFAULT_DATABASE_NAME, immutable.empty(), ioController);
                PagedFile pfB = pageCache.map(b, filePageSize, DEFAULT_DATABASE_NAME, immutable.empty(), ioController);
                PagedFile pfC =
                        pageCache.map(c, filePageSize, DEFAULT_DATABASE_NAME, immutable.empty(), ioController)) {
            // Dirty a bunch of pages.
            try (PageCursor cursor = pfA.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
            }
            try (PageCursor cursor = pfB.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
            }
            try (PageCursor cursor = pfC.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
            }

            flushers.add(executor.submit(() -> {
                pfA.flushAndForce(FileFlushEvent.NULL);
                return null;
            }));
            flushers.add(executor.submit(() -> {
                pfB.flushAndForce(FileFlushEvent.NULL);
                return null;
            }));
            flushers.add(executor.submit(() -> {
                pfC.flushAndForce(FileFlushEvent.NULL);
                return null;
            }));

            closeFilesLatch.await(); // Flusher is now stuck inside flushAndForce.
        } // We should be able to unmap all the files.
        // And then when the flusher resumes again, it should not throw any exceptions from the asynchronously
        // closed files.
        limiterBlockLatch.release();
        Futures.getAll(flushers); // This must not throw.
    }

    @Test
    void writesFlushedFromPageCacheMustBeExternallyObservable() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            long startPageId = 0;
            long endPageId = recordCount / recordsPerFilePage;
            Path file = file("a");
            try (PagedFile pagedFile = map(file, filePageSize);
                    PageCursor cursor = pagedFile.io(startPageId, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                while (cursor.getCurrentPageId() < endPageId && cursor.next()) {
                    writeRecords(cursor);
                }
            } // closing the PagedFile implies flushing because it was the last reference

            verifyRecordsInFile(file, recordCount);
        });
    }

    @Test
    void writesToPagesMustNotBleedIntoAdjacentPages() {
        assertTimeoutPreemptively(ofMillis(SEMI_LONG_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            // Write the pageId+1 to every byte in the file
            try (PagedFile pagedFile = map(file("a"), filePageSize);
                    PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                for (int i = 1; i <= 100; i++) {
                    assertTrue(cursor.next());
                    for (int j = 0; j < filePayloadSize; j++) {
                        cursor.putByte((byte) i);
                    }
                }
            }

            // Then check that none of those writes ended up in adjacent pages
            InputStream inputStream = fs.openAsInputStream(file("a"));
            for (int i = 1; i <= 100; i++) {
                inputStream.readNBytes(reservedBytes);
                for (int j = 0; j < filePayloadSize; j++) {
                    assertThat(inputStream.read()).isEqualTo(i);
                }
            }
            inputStream.close();
        });
    }

    @Test
    void channelMustBeForcedAfterPagedFileFlushAndForceWithNoBuffers() throws Exception {
        final AtomicInteger writeCounter = new AtomicInteger();
        final AtomicInteger forceCounter = new AtomicInteger();
        FileSystemAbstraction fs = writeAndForceCountingFs(writeCounter, forceCounter);

        getPageCache(fs, maxPages, PageCacheTracer.NULL);
        assumeTrue(DISABLED_BUFFER_FACTORY.equals(pageCache.getBufferFactory()));

        try (PagedFile pagedFile = map(file("a"), filePageSize)) {
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putInt(1);
                assertTrue(cursor.next());
                cursor.putInt(1);
            }

            pagedFile.flushAndForce(FileFlushEvent.NULL);

            assertThat(writeCounter.get()).isGreaterThanOrEqualTo(2); // We might race with background flushing.
            assertThat(forceCounter.get()).isEqualTo(1);
        }
    }

    @Test
    void channelMustBeForcedAfterPagedFileFlushAndForceWithBuffers() throws Exception {
        final AtomicInteger writeCounter = new AtomicInteger();
        final AtomicInteger forceCounter = new AtomicInteger();
        FileSystemAbstraction fs = writeAndForceCountingFs(writeCounter, forceCounter);

        getPageCache(fs, maxPages, PageCacheTracer.NULL);
        assumeFalse(DISABLED_BUFFER_FACTORY.equals(pageCache.getBufferFactory()));

        try (PagedFile pagedFile = map(file("a"), filePageSize)) {
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putInt(1);
                assertTrue(cursor.next());
                cursor.putInt(1);
            }

            pagedFile.flushAndForce(FileFlushEvent.NULL);

            assertThat(writeCounter.get()).isGreaterThanOrEqualTo(1); // We might race with background flushing.
            assertThat(forceCounter.get()).isEqualTo(1);
        }
    }

    @Test
    void channelsMustBeForcedAfterPageCacheFlushAndForceWithNoBuffers() throws Exception {
        final AtomicInteger writeCounter = new AtomicInteger();
        final AtomicInteger forceCounter = new AtomicInteger();
        FileSystemAbstraction fs = writeAndForceCountingFs(writeCounter, forceCounter);

        getPageCache(fs, maxPages, PageCacheTracer.NULL);
        assumeTrue(DISABLED_BUFFER_FACTORY.equals(pageCache.getBufferFactory()));

        try (PagedFile pagedFileA = map(existingFile("a"), filePageSize);
                PagedFile pagedFileB = map(existingFile("b"), filePageSize)) {
            try (PageCursor cursor = pagedFileA.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putInt(1);
                assertTrue(cursor.next());
                cursor.putInt(1);
            }
            try (PageCursor cursor = pagedFileB.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putInt(1);
            }

            pageCache.flushAndForce(DatabaseFlushEvent.NULL);

            assertThat(writeCounter.get()).isGreaterThanOrEqualTo(3); // We might race with background flushing.
            assertThat(forceCounter.get()).isEqualTo(2);
        }
    }

    @Test
    void channelsMustBeForcedAfterPageCacheFlushAndForceWithBuffers() throws Exception {
        final AtomicInteger writeCounter = new AtomicInteger();
        final AtomicInteger forceCounter = new AtomicInteger();
        FileSystemAbstraction fs = writeAndForceCountingFs(writeCounter, forceCounter);

        getPageCache(fs, maxPages, PageCacheTracer.NULL);
        assumeFalse(DISABLED_BUFFER_FACTORY.equals(pageCache.getBufferFactory()));

        try (PagedFile pagedFileA = map(existingFile("a"), filePageSize);
                PagedFile pagedFileB = map(existingFile("b"), filePageSize)) {
            try (PageCursor cursor = pagedFileA.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putInt(1);
                assertTrue(cursor.next());
                cursor.putInt(1);
            }
            try (PageCursor cursor = pagedFileB.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putInt(1);
            }

            pageCache.flushAndForce(DatabaseFlushEvent.NULL);

            assertThat(writeCounter.get()).isGreaterThanOrEqualTo(2); // We might race with background flushing.
            assertThat(forceCounter.get()).isEqualTo(2);
        }
    }

    private DelegatingFileSystemAbstraction writeAndForceCountingFs(
            final AtomicInteger writeCounter, final AtomicInteger forceCounter) {
        return new DelegatingFileSystemAbstraction(fs) {
            @Override
            public StoreChannel open(Path fileName, Set<OpenOption> options) throws IOException {
                return new DelegatingStoreChannel(super.open(fileName, options)) {
                    @Override
                    public void writeAll(ByteBuffer src, long position) throws IOException {
                        writeCounter.getAndIncrement();
                        super.writeAll(src, position);
                    }

                    @Override
                    public long write(ByteBuffer[] srcs) throws IOException {
                        writeCounter.getAndAdd(srcs.length);
                        return super.write(srcs);
                    }

                    @Override
                    public void force(boolean metaData) throws IOException {
                        forceCounter.getAndIncrement();
                        super.force(metaData);
                    }
                };
            }
        };
    }

    @Test
    void firstNextCallMustReturnFalseWhenTheFileIsEmptyAndNoGrowIsSpecified() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            try (PagedFile pagedFile = map(file("a"), filePageSize);
                    PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK | PF_NO_GROW, NULL_CONTEXT)) {
                assertFalse(cursor.next());
            }
        });
    }

    @Test
    void nextMustReturnTrueThenFalseWhenThereIsOnlyOnePageInTheFileAndNoGrowIsSpecified() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            int numberOfRecordsToGenerate = recordsPerFilePage; // one page worth
            generateFileWithRecords(
                    file("a"),
                    numberOfRecordsToGenerate,
                    recordSize,
                    recordsPerFilePage,
                    reservedBytes,
                    pageCachePageSize);

            try (PagedFile pagedFile = map(file("a"), filePageSize);
                    PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK | PF_NO_GROW, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                verifyRecordsMatchExpected(cursor);
                assertFalse(cursor.next());
            }
        });
    }

    @Test
    void closingWithoutCallingNextMustLeavePageUnpinnedAndUntouched() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            int numberOfRecordsToGenerate = recordsPerFilePage; // one page worth
            generateFileWithRecords(
                    file("a"),
                    numberOfRecordsToGenerate,
                    recordSize,
                    recordsPerFilePage,
                    reservedBytes,
                    pageCachePageSize);

            try (PagedFile pagedFile = map(file("a"), filePageSize)) {
                //noinspection EmptyTryBlock
                try (PageCursor ignore = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                    // No call to next, so the page should never get pinned in the first place, nor
                    // should the page corruption take place.
                }

                try (PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                    // We didn't call next before, so the page and its records should still be fine
                    cursor.next();
                    verifyRecordsMatchExpected(cursor);
                }
            }
        });
    }

    @Test
    void nextWithNegativeInitialPageIdMustReturnFalse() throws Exception {
        configureStandardPageCache();
        Path file = file("a");
        generateFileWithRecords(file, recordCount, recordSize, recordsPerFilePage, reservedBytes, pageCachePageSize);
        try (PagedFile pf = map(file, filePageSize)) {
            try (PageCursor cursor = pf.io(-1, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertFalse(cursor.next());
            }
            try (PageCursor cursor = pf.io(-1, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                assertFalse(cursor.next());
            }
        }
    }

    @Test
    void nextWithNegativePageIdMustReturnFalse() throws Exception {
        Path file = file("a");
        generateFileWithRecords(file, recordCount, recordSize, recordsPerFilePage, reservedBytes, pageCachePageSize);
        configureStandardPageCache();
        try (PagedFile pf = map(file, filePageSize)) {
            long pageId = 12;
            try (PageCursor cursor = pf.io(pageId, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                assertFalse(cursor.next(-1));
                assertThat(cursor.getCurrentPageId()).isEqualTo(PageCursor.UNBOUND_PAGE_ID);
            }
            try (PageCursor cursor = pf.io(pageId, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                assertFalse(cursor.next(-1));
                assertThat(cursor.getCurrentPageId()).isEqualTo(PageCursor.UNBOUND_PAGE_ID);
            }
        }
    }

    @Test
    void mustCloseFileChannelWhenTheLastHandleIsUnmapped() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            assumeTrue(
                    fs.getClass() == EphemeralFileSystemAbstraction.class,
                    "This depends on EphemeralFSA specific features");

            configureStandardPageCache();
            PagedFile a = map(file("a"), filePageSize);
            PagedFile b = map(file("a"), filePageSize);
            a.close();
            b.close();
            ((EphemeralFileSystemAbstraction) fs).assertNoOpenFiles();
        });
    }

    @Test
    void dirtyPagesMustBeFlushedWhenTheCacheIsClosed() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            long startPageId = 0;
            long endPageId = recordCount / recordsPerFilePage;
            Path file = file("a");
            try (PagedFile pagedFile = map(file, filePageSize);
                    PageCursor cursor = pagedFile.io(startPageId, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                while (cursor.getCurrentPageId() < endPageId && cursor.next()) {
                    writeRecords(cursor);
                }
            } finally {
                pageCache.close();
            }

            verifyRecordsInFile(file, recordCount);
        });
    }

    @Test
    void dirtyPagesMustBeFlushedWhenThePagedFileIsClosed() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            long startPageId = 0;
            long endPageId = recordCount / recordsPerFilePage;
            Path file = file("a");
            try (PagedFile pagedFile = map(file, filePageSize);
                    PageCursor cursor = pagedFile.io(startPageId, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                while (cursor.getCurrentPageId() < endPageId && cursor.next()) {
                    writeRecords(cursor);
                }
            }

            verifyRecordsInFile(file, recordCount);
        });
    }

    @RepeatedTest(100)
    void flushingDuringPagedFileCloseMustRetryUntilItSucceeds() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            FileSystemAbstraction fs = new DelegatingFileSystemAbstraction(this.fs) {
                @Override
                public StoreChannel open(Path fileName, Set<OpenOption> options) throws IOException {
                    return new DelegatingStoreChannel(super.open(fileName, options)) {
                        private int writeCount;

                        @Override
                        public void writeAll(ByteBuffer src, long position) throws IOException {
                            if (writeCount++ < 10) {
                                throw new IOException("This is a benign exception that we expect to be thrown "
                                        + "during a flush of a PagedFile.");
                            }
                            super.writeAll(src, position);
                        }
                    };
                }
            };
            getPageCache(fs, maxPages, PageCacheTracer.NULL);
            PrintStream oldSystemErr = System.err;

            try (PagedFile pf = map(file("a"), filePageSize);
                    PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                writeRecords(cursor);

                // Silence any stack traces the failed flushes might print.
                System.setErr(new PrintStream(new ByteArrayOutputStream()));
            } finally {
                System.setErr(oldSystemErr);
            }

            verifyRecordsInFile(file("a"), recordsPerFilePage);
        });
    }

    @Test
    void mappingFilesInClosedCacheMustThrow() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            pageCache.close();
            assertThrows(IllegalStateException.class, () -> map(file("a"), filePageSize));
        });
    }

    @Test
    void flushingClosedCacheMustThrow() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            pageCache.close();
            assertThrows(IllegalStateException.class, () -> pageCache.flushAndForce(DatabaseFlushEvent.NULL));
        });
    }

    @Test
    void mappingFileWithPageSizeGreaterThanCachePageSizeMustThrow() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            assertThrows(
                    IllegalArgumentException.class, () -> map(file("a"), pageCachePageSize + 1)); // this must throw;
        });
    }

    @Test
    void mappingFileWithPageSizeSmallerThanLongSizeBytesMustThrow() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            // Because otherwise we cannot ensure that our branch-free bounds checking always lands within a page
            // boundary.
            configureStandardPageCache();
            assertThrows(IllegalArgumentException.class, () -> map(file("a"), Long.BYTES - 1)); // this must throw;
        });
    }

    @Test
    void mappingFileWithPageSizeSmallerThanLongSizeBytesMustThrowEvenWithAnyPageSizeOpenOptionAndNoExistingMapping() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            // Because otherwise we cannot ensure that our branch-free bounds checking always lands within a page
            // boundary.
            configureStandardPageCache();
            assertThrows(
                    IllegalArgumentException.class,
                    () -> map(
                            file("a"),
                            Long.BYTES - 1,
                            immutable.of(PageCacheOpenOptions.ANY_PAGE_SIZE))); // this must throw;
        });
    }

    @Test
    void mappingFileWithPageZeroPageSizeMustThrowEvenWithExistingMapping() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            Path file = file("a");
            //noinspection unused
            try (PagedFile oldMapping = map(file, filePageSize)) {
                assertThrows(IllegalArgumentException.class, () -> map(file, Long.BYTES - 1)); // this must throw
            }
        });
    }

    @Test
    void mappingFileWithPageZeroPageSizeAndAnyPageSizeOpenOptionMustNotThrowGivenExistingMapping() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            Path file = file("a");
            //noinspection unused,EmptyTryBlock
            try (PagedFile oldMapping = map(file, filePageSize);
                    PagedFile newMapping = map(file, 0, immutable.of(PageCacheOpenOptions.ANY_PAGE_SIZE))) {
                // All good
            }
        });
    }

    @Test
    void mappingFileWithPageSizeEqualToCachePageSizeMustNotThrow() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            PagedFile pagedFile = map(file("a"), pageCachePageSize); // this must NOT throw
            pagedFile.close();
        });
    }

    @Test
    void flushAndForceAfterCloseAndEvictionMustNotGetStuckOnEvictedPages() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            PagedFile pagedFile = pageCache.map(file("a"), pageCachePageSize, DEFAULT_DATABASE_NAME);
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                for (int i = 0; i < 20; i++) {
                    cursor.next();
                    writeRecords(cursor);
                }
            }
            pagedFile.close();
            try (PagedFile b = pageCache.map(existingFile("b"), pageCachePageSize, DEFAULT_DATABASE_NAME);
                    PageCursor cursor = b.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                for (int i = 0; i < 200; i++) {
                    cursor.next();
                }
            }

            pagedFile.flushAndForce(FileFlushEvent.NULL);
        });
    }

    @Test
    void notSpecifyingAnyPfFlagsMustThrow() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            try (PagedFile pagedFile = map(file("a"), filePageSize)) {
                assertThrows(IllegalArgumentException.class, () -> pagedFile.io(0, 0, NULL_CONTEXT)); // this must throw
            }
        });
    }

    @Test
    void notSpecifyingAnyPfLockFlagsMustThrow() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            try (PagedFile pagedFile = map(file("a"), filePageSize)) {
                assertThrows(
                        IllegalArgumentException.class,
                        () -> pagedFile.io(0, PF_NO_FAULT, NULL_CONTEXT)); // this must throw
            }
        });
    }

    @Test
    void specifyingBothReadAndWriteLocksMustThrow() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            try (PagedFile pagedFile = map(file("a"), filePageSize)) {
                assertThrows(
                        IllegalArgumentException.class,
                        () -> pagedFile.io(
                                0, PF_SHARED_WRITE_LOCK | PF_SHARED_READ_LOCK, NULL_CONTEXT)); // this must throw
            }
        });
    }

    @Test
    void mustNotPinPagesAfterNextReturnsFalse() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            final CountDownLatch startLatch = new CountDownLatch(1);
            final CountDownLatch unpinLatch = new CountDownLatch(1);
            final AtomicReference<Exception> exceptionRef = new AtomicReference<>();
            configureStandardPageCache();
            generateFileWithRecords(
                    file("a"), recordsPerFilePage, recordSize, recordsPerFilePage, reservedBytes, filePageSize);
            final PagedFile pagedFile = map(file("a"), filePageSize);

            Runnable runnable = () -> {
                try (PageCursor cursorA = pagedFile.io(0, PF_SHARED_WRITE_LOCK | PF_NO_GROW, NULL_CONTEXT)) {
                    assertTrue(cursorA.next());
                    assertFalse(cursorA.next());
                    startLatch.countDown();
                    unpinLatch.await();
                } catch (Exception e) {
                    exceptionRef.set(e);
                }
            };
            executor.submit(runnable);

            startLatch.await();
            try (PageCursor cursorB = pagedFile.io(1, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursorB.next());
                unpinLatch.countDown();
            } finally {
                pagedFile.close();
            }
            Exception e = exceptionRef.get();
            if (e != null) {
                throw new Exception("Child thread got exception", e);
            }
        });
    }

    @Test
    void nextMustResetTheCursorOffset() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            PagedFile pagedFile = map(file("a"), filePageSize);

            try (PageCursor cursor = pagedFile.io(0L, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.setOffset(0);
                cursor.putByte((byte) 1);
                cursor.putByte((byte) 2);
                cursor.putByte((byte) 3);
                cursor.putByte((byte) 4);

                assertTrue(cursor.next());
                cursor.setOffset(0);
                cursor.putByte((byte) 5);
                cursor.putByte((byte) 6);
                cursor.putByte((byte) 7);
                cursor.putByte((byte) 8);
            }

            try (PageCursor cursor = pagedFile.io(0L, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                byte[] bytes = new byte[4];
                assertTrue(cursor.next());
                cursor.getBytes(bytes);
                assertThat(bytes).containsExactly(1, 2, 3, 4);

                assertTrue(cursor.next());
                cursor.getBytes(bytes);
                assertThat(bytes).containsExactly(5, 6, 7, 8);
            }
            pagedFile.close();
        });
    }

    @Test
    void nextMustAdvanceCurrentPageId() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            try (PagedFile pagedFile = map(file("a"), filePageSize);
                    PageCursor cursor = pagedFile.io(0L, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                assertThat(cursor.getCurrentPageId()).isEqualTo(0L);
                assertTrue(cursor.next());
                assertThat(cursor.getCurrentPageId()).isEqualTo(1L);
            }
        });
    }

    @Test
    void nextToSpecificPageIdMustAdvanceFromThatPointOn() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            try (PagedFile pagedFile = map(file("a"), filePageSize);
                    PageCursor cursor = pagedFile.io(1L, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                assertThat(cursor.getCurrentPageId()).isEqualTo(1L);
                assertTrue(cursor.next(4L));
                assertThat(cursor.getCurrentPageId()).isEqualTo(4L);
                assertTrue(cursor.next());
                assertThat(cursor.getCurrentPageId()).isEqualTo(5L);
            }
        });
    }

    @Test
    void currentPageIdIsUnboundBeforeFirstNext() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            try (PagedFile pagedFile = map(file("a"), filePageSize);
                    PageCursor cursor = pagedFile.io(0L, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertThat(cursor.getCurrentPageId()).isEqualTo(PageCursor.UNBOUND_PAGE_ID);
                assertTrue(cursor.next());
                assertThat(cursor.getCurrentPageId()).isEqualTo(0L);
            }
        });
    }

    @Test
    void currentPageIdIsUnboundAfterUnpin() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            try (PagedFile pagedFile = map(file("a"), filePageSize);
                    PageCursor cursor = pagedFile.io(0L, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                assertThat(cursor.getCurrentPageId()).isEqualTo(0L);
                cursor.unpin();
                assertThat(cursor.getCurrentPageId()).isEqualTo(PageCursor.UNBOUND_PAGE_ID);
            }
        });
    }

    @Test
    void currentPageIdIsUnboundAfterUnpinForReadCursor() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            try (PagedFile pagedFile = map(file("a"), filePageSize)) {
                try (PageCursor writeCursor = pagedFile.io(0L, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                    assertTrue(writeCursor.next());
                }
                try (PageCursor cursor = pagedFile.io(0L, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                    assertTrue(cursor.next());
                    assertThat(cursor.getCurrentPageId()).isEqualTo(0L);
                    cursor.unpin();
                    assertThat(cursor.getCurrentPageId()).isEqualTo(PageCursor.UNBOUND_PAGE_ID);
                }
            }
        });
    }

    @Test
    void shouldPinPageAfterUnpinWriteCursor() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            try (var pagedFile = map(file("a"), filePageSize)) {
                int offset;
                var pageId = 0;
                try (var cursor = pagedFile.io(pageId, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                    assertTrue(cursor.next());
                    cursor.putInt(5);
                    offset = cursor.getOffset();
                    cursor.putInt(10);
                    cursor.unpin();

                    assertTrue(cursor.next(pageId));
                    cursor.setOffset(offset);
                    int read = cursor.getInt();
                    assertThat(read).isEqualTo(10);
                }
            }
        });
    }

    @Test
    void shouldPinPageAfterUnpinReadCursor() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            try (var pagedFile = map(file("a"), filePageSize)) {
                int offset;
                var pageId = 0;
                try (var cursor = pagedFile.io(pageId, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                    assertTrue(cursor.next());
                    cursor.putInt(5);
                    offset = cursor.getOffset();
                    cursor.putInt(10);
                }
                try (var readCursor = pagedFile.io(pageId, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                    assertTrue(readCursor.next());
                    readCursor.unpin();
                    assertTrue(readCursor.next(pageId));
                    int read;
                    do {
                        readCursor.setOffset(offset);
                        read = readCursor.getInt();
                    } while (readCursor.shouldRetry());
                    assertThat(read).isEqualTo(10);
                }
            }
        });
    }

    @Test
    void pageCursorMustKnowCurrentFilePageSize() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            try (PagedFile pagedFile = map(file("a"), filePageSize);
                    MuninnPageCursor cursor = (MuninnPageCursor) pagedFile.io(0L, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertThat(cursor.getPageSize()).isEqualTo(0);
                assertTrue(cursor.next());
                assertThat(cursor.getPageSize()).isEqualTo(filePageSize);
            }
        });
    }

    @Test
    void pageCursorMustKnowCurrentFile() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            try (PagedFile pagedFile = map(file("a"), filePageSize);
                    PageCursor cursor = pagedFile.io(0L, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertThat(cursor.getCurrentFile()).isNull();
                assertTrue(cursor.next());
                assertThat(cursor.getCurrentFile()).isEqualTo(file("a"));
            }
        });
    }

    @Test
    void readingFromUnboundReadCursorMustThrow() {
        assertTimeoutPreemptively(
                ofMillis(SHORT_TIMEOUT_MILLIS), () -> verifyOnReadCursor(this::checkUnboundReadCursorAccess));
    }

    @Test
    void readingFromUnboundWriteCursorMustThrow() {
        assertTimeoutPreemptively(
                ofMillis(SHORT_TIMEOUT_MILLIS), () -> verifyOnReadCursor(this::checkUnboundWriteCursorAccess));
    }

    @Test
    void readingFromPreviouslyBoundCursorMustThrow() {
        assertTimeoutPreemptively(
                ofMillis(SHORT_TIMEOUT_MILLIS), () -> verifyOnReadCursor(this::checkPreviouslyBoundWriteCursorAccess));
    }

    @Test
    void writingToUnboundCursorMustThrow() {
        assertTimeoutPreemptively(
                ofMillis(SHORT_TIMEOUT_MILLIS), () -> verifyOnWriteCursor(this::checkUnboundWriteCursorAccess));
    }

    @Test
    void writingToPreviouslyBoundCursorMustThrow() {
        assertTimeoutPreemptively(
                ofMillis(SHORT_TIMEOUT_MILLIS), () -> verifyOnWriteCursor(this::checkPreviouslyBoundWriteCursorAccess));
    }

    @Test
    void readFromReadCursorAfterNextReturnsFalseMustThrow() {
        assertTimeoutPreemptively(
                ofMillis(SHORT_TIMEOUT_MILLIS), () -> verifyOnReadCursor(this::checkReadCursorAfterFailedNext));
    }

    @Test
    void readFromPreviouslyBoundReadCursorAfterNextReturnsFalseMustThrow() {
        assertTimeoutPreemptively(
                ofMillis(SHORT_TIMEOUT_MILLIS),
                () -> verifyOnReadCursor(this::checkPreviouslyBoundReadCursorAfterFailedNext));
    }

    @Test
    void readFromWriteCursorAfterNextReturnsFalseMustThrow() {
        assertTimeoutPreemptively(
                ofMillis(SHORT_TIMEOUT_MILLIS), () -> verifyOnReadCursor(this::checkWriteCursorAfterFailedNext));
    }

    @Test
    void readFromPreviouslyBoundWriteCursorAfterNextReturnsFalseMustThrow() {
        assertTimeoutPreemptively(
                ofMillis(SHORT_TIMEOUT_MILLIS),
                () -> verifyOnReadCursor(this::checkPreviouslyBoundWriteCursorAfterFailedNext));
    }

    @Test
    void writeAfterNextReturnsFalseMustThrow() {
        assertTimeoutPreemptively(
                ofMillis(SHORT_TIMEOUT_MILLIS), () -> verifyOnWriteCursor(this::checkWriteCursorAfterFailedNext));
    }

    @Test
    void writeToPreviouslyBoundCursorAfterNextReturnsFalseMustThrow() {
        assertTimeoutPreemptively(
                ofMillis(SHORT_TIMEOUT_MILLIS),
                () -> verifyOnWriteCursor(this::checkPreviouslyBoundWriteCursorAfterFailedNext));
    }

    private static void verifyOnReadCursor(ThrowingConsumer<PageCursorAction, IOException> testTemplate)
            throws IOException {
        testTemplate.accept(PageCursor::getByte);
        testTemplate.accept(PageCursor::getInt);
        testTemplate.accept(PageCursor::getLong);
        testTemplate.accept(PageCursor::getShort);
        testTemplate.accept(cursor -> cursor.getByte(0));
        testTemplate.accept(cursor -> cursor.getInt(0));
        testTemplate.accept(cursor -> cursor.getLong(0));
        testTemplate.accept(cursor -> cursor.getShort(0));
    }

    private static void verifyOnWriteCursor(ThrowingConsumer<PageCursorAction, IOException> testTemplate)
            throws IOException {
        testTemplate.accept(cursor -> cursor.putByte((byte) 1));
        testTemplate.accept(cursor -> cursor.putInt(1));
        testTemplate.accept(cursor -> cursor.putLong(1));
        testTemplate.accept(cursor -> cursor.putShort((short) 1));
        testTemplate.accept(cursor -> cursor.putByte(0, (byte) 1));
        testTemplate.accept(cursor -> cursor.putInt(0, 1));
        testTemplate.accept(cursor -> cursor.putLong(0, 1));
        testTemplate.accept(cursor -> cursor.putShort(0, (short) 1));
        testTemplate.accept(PageCursor::zapPage);
    }

    private void checkUnboundReadCursorAccess(PageCursorAction action) throws IOException {
        configureStandardPageCache();

        try (PagedFile pagedFile = map(file("a"), filePageSize);
                PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
            action.apply(cursor);
            assertTrue(cursor.checkAndClearBoundsFlag());
        }
    }

    private void checkUnboundWriteCursorAccess(PageCursorAction action) throws IOException {
        configureStandardPageCache();

        try (PagedFile pagedFile = map(file("a"), filePageSize);
                PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            action.apply(cursor);
            assertTrue(cursor.checkAndClearBoundsFlag());
        }
    }

    private void checkPreviouslyBoundWriteCursorAccess(PageCursorAction action) throws IOException {
        configureStandardPageCache();

        try (PagedFile pagedFile = map(file("a"), filePageSize)) {
            PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
            assertTrue(cursor.next());
            action.apply(cursor);
            assertFalse(cursor.checkAndClearBoundsFlag());
            cursor.close();
            action.apply(cursor);
            assertTrue(cursor.checkAndClearBoundsFlag());
        }
    }

    private void checkReadCursorAfterFailedNext(PageCursorAction action) throws IOException {
        configureStandardPageCache();

        try (PagedFile pagedFile = map(file("a"), filePageSize);
                PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
            assertFalse(cursor.next());
            action.apply(cursor);
            assertTrue(cursor.checkAndClearBoundsFlag());
        }
    }

    private void checkPreviouslyBoundReadCursorAfterFailedNext(PageCursorAction action) throws IOException {
        configureStandardPageCache();

        try (PagedFile pagedFile = map(file("a"), filePageSize);
                PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            assertTrue(cursor.next());
        }

        try (PagedFile pagedFile = map(file("a"), filePageSize);
                PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
            assertTrue(cursor.next());
            assertFalse(cursor.next());
            action.apply(cursor);
            assertTrue(cursor.checkAndClearBoundsFlag());
        }
    }

    private void checkWriteCursorAfterFailedNext(PageCursorAction action) throws IOException {
        configureStandardPageCache();

        try (PagedFile pagedFile = map(file("a"), filePageSize);
                PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK | PF_NO_GROW, NULL_CONTEXT)) {
            assertFalse(cursor.next());
            action.apply(cursor);
            assertTrue(cursor.checkAndClearBoundsFlag());
        }
    }

    private void checkPreviouslyBoundWriteCursorAfterFailedNext(PageCursorAction action) throws IOException {
        configureStandardPageCache();

        try (PagedFile pagedFile = map(file("a"), filePageSize);
                PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            assertTrue(cursor.next());
        }

        try (PagedFile pagedFile = map(file("a"), filePageSize);
                PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK | PF_NO_GROW, NULL_CONTEXT)) {
            assertTrue(cursor.next());
            assertFalse(cursor.next());
            action.apply(cursor);
            assertTrue(cursor.checkAndClearBoundsFlag());
        }
    }

    @Test
    void tryMappedPagedFileShouldReportMappedFilePresent() throws Exception {
        configureStandardPageCache();
        final Path file = file("a");
        try (PagedFile pf = map(file, filePageSize)) {
            final Optional<PagedFile> optional = pageCache.getExistingMapping(file);
            assertTrue(optional.isPresent());
            final PagedFile actual = optional.get();
            assertThat(actual).isSameAs(pf);
            actual.close();
        }
    }

    @Test
    void tryMappedPagedFileShouldReportNonMappedFileNotPresent() throws Exception {
        configureStandardPageCache();
        final Optional<PagedFile> dontExist = pageCache.getExistingMapping(Path.of("dont_exist"));
        assertFalse(dontExist.isPresent());
    }

    @Test
    void mustListExistingMappings() throws Exception {
        configureStandardPageCache();
        Path f1 = existingFile("1");
        Path f2 = existingFile("2");
        Path f3 = existingFile("3"); // Not mapped at the time of calling listExistingMappings.
        existingFile("4"); // Never mapped.
        try (PagedFile pf1 = map(f1, filePageSize);
                PagedFile pf2 = map(f2, filePageSize)) {
            map(f3, filePageSize).close();
            List<PagedFile> existingMappings = pageCache.listExistingMappings();
            assertThat(existingMappings.size()).isEqualTo(2);
            assertThat(existingMappings).contains(pf1, pf2);
        }
    }

    @Test
    void listExistingMappingsMustNotIncrementPagedFileReferenceCount() throws Exception {
        configureStandardPageCache();
        Path file = file("a");
        PagedFile existingMapping;
        try (PagedFile pf = map(file, filePageSize)) {
            existingMapping = pageCache.listExistingMappings().get(0);
            try (PageCursor cursor = existingMapping.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
            }
        }
        // Now the mapping should be closed, which is signalled as an illegal state.
        assertThrows(
                FileIsNotMappedException.class,
                () -> existingMapping.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT).next());
    }

    @Test
    void listExistingMappingsMustThrowOnClosedPageCache() {
        configureStandardPageCache();
        T pc = pageCache;
        pageCache = null;
        pc.close();
        assertThrows(IllegalStateException.class, pc::listExistingMappings);
    }

    @Test
    void lastPageMustBeAccessibleWithNoGrowSpecified() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            generateFileWithRecords(
                    file("a"), recordsPerFilePage * 2, recordSize, recordsPerFilePage, reservedBytes, filePageSize);

            try (PagedFile pagedFile = map(file("a"), filePageSize)) {
                try (PageCursor cursor = pagedFile.io(0L, PF_SHARED_WRITE_LOCK | PF_NO_GROW, NULL_CONTEXT)) {
                    assertTrue(cursor.next());
                    assertTrue(cursor.next());
                    assertFalse(cursor.next());
                }

                try (PageCursor cursor = pagedFile.io(0L, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                    assertTrue(cursor.next());
                    assertTrue(cursor.next());
                    assertFalse(cursor.next());
                }

                try (PageCursor cursor = pagedFile.io(1L, PF_SHARED_WRITE_LOCK | PF_NO_GROW, NULL_CONTEXT)) {
                    assertTrue(cursor.next());
                    assertFalse(cursor.next());
                }

                try (PageCursor cursor = pagedFile.io(1L, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                    assertTrue(cursor.next());
                    assertFalse(cursor.next());
                }

                try (PageCursor cursor = pagedFile.io(2L, PF_SHARED_WRITE_LOCK | PF_NO_GROW, NULL_CONTEXT)) {
                    assertFalse(cursor.next());
                }

                try (PageCursor cursor = pagedFile.io(2L, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                    assertFalse(cursor.next());
                }

                try (PageCursor cursor = pagedFile.io(3L, PF_SHARED_WRITE_LOCK | PF_NO_GROW, NULL_CONTEXT)) {
                    assertFalse(cursor.next());
                }

                try (PageCursor cursor = pagedFile.io(3L, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                    assertFalse(cursor.next());
                }
            }
        });
    }

    @Test
    void lastPageMustBeAccessibleWithNoGrowSpecifiedEvenIfLessThanFilePageSize() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            generateFileWithRecords(
                    file("a"),
                    (recordsPerFilePage * 2) - 1,
                    recordSize,
                    recordsPerFilePage,
                    reservedBytes,
                    filePageSize);
            try (PagedFile pagedFile = map(file("a"), filePageSize)) {
                try (PageCursor cursor = pagedFile.io(0L, PF_SHARED_WRITE_LOCK | PF_NO_GROW, NULL_CONTEXT)) {
                    assertTrue(cursor.next());
                    assertTrue(cursor.next());
                    assertFalse(cursor.next());
                }

                try (PageCursor cursor = pagedFile.io(0L, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                    assertTrue(cursor.next());
                    assertTrue(cursor.next());
                    assertFalse(cursor.next());
                }

                try (PageCursor cursor = pagedFile.io(1L, PF_SHARED_WRITE_LOCK | PF_NO_GROW, NULL_CONTEXT)) {
                    assertTrue(cursor.next());
                    assertFalse(cursor.next());
                }

                try (PageCursor cursor = pagedFile.io(1L, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                    assertTrue(cursor.next());
                    assertFalse(cursor.next());
                }

                try (PageCursor cursor = pagedFile.io(2L, PF_SHARED_WRITE_LOCK | PF_NO_GROW, NULL_CONTEXT)) {
                    assertFalse(cursor.next());
                }

                try (PageCursor cursor = pagedFile.io(2L, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                    assertFalse(cursor.next());
                }

                try (PageCursor cursor = pagedFile.io(3L, PF_SHARED_WRITE_LOCK | PF_NO_GROW, NULL_CONTEXT)) {
                    assertFalse(cursor.next());
                }

                try (PageCursor cursor = pagedFile.io(3L, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                    assertFalse(cursor.next());
                }
            }
        });
    }

    @Test
    void firstPageMustBeAccessibleWithNoGrowSpecifiedIfItIsTheOnlyPage() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            generateFileWithRecords(
                    file("a"), recordsPerFilePage, recordSize, recordsPerFilePage, reservedBytes, pageCachePageSize);

            try (PagedFile pagedFile = map(file("a"), filePageSize)) {
                try (PageCursor cursor = pagedFile.io(0L, PF_SHARED_WRITE_LOCK | PF_NO_GROW, NULL_CONTEXT)) {
                    assertTrue(cursor.next());
                    assertFalse(cursor.next());
                }

                try (PageCursor cursor = pagedFile.io(0L, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                    assertTrue(cursor.next());
                    assertFalse(cursor.next());
                }

                try (PageCursor cursor = pagedFile.io(1L, PF_SHARED_WRITE_LOCK | PF_NO_GROW, NULL_CONTEXT)) {
                    assertFalse(cursor.next());
                }

                try (PageCursor cursor = pagedFile.io(1L, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                    assertFalse(cursor.next());
                }
            }
        });
    }

    @Test
    void firstPageMustBeAccessibleEvenIfTheFileIsNonEmptyButSmallerThanFilePageSize() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            generateFileWithRecords(file("a"), 1, recordSize, recordsPerFilePage, reservedBytes, pageCachePageSize);

            try (PagedFile pagedFile = map(file("a"), filePageSize)) {
                try (PageCursor cursor = pagedFile.io(0L, PF_SHARED_WRITE_LOCK | PF_NO_GROW, NULL_CONTEXT)) {
                    assertTrue(cursor.next());
                    assertFalse(cursor.next());
                }

                try (PageCursor cursor = pagedFile.io(0L, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                    assertTrue(cursor.next());
                    assertFalse(cursor.next());
                }

                try (PageCursor cursor = pagedFile.io(1L, PF_SHARED_WRITE_LOCK | PF_NO_GROW, NULL_CONTEXT)) {
                    assertFalse(cursor.next());
                }

                try (PageCursor cursor = pagedFile.io(1L, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                    assertFalse(cursor.next());
                }
            }
        });
    }

    @Test
    void firstPageMustNotBeAccessibleIfFileIsEmptyAndNoGrowSpecified() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            try (PagedFile pagedFile = map(file("a"), filePageSize)) {
                try (PageCursor cursor = pagedFile.io(0L, PF_SHARED_WRITE_LOCK | PF_NO_GROW, NULL_CONTEXT)) {
                    assertFalse(cursor.next());
                    assertFalse(cursor.next());
                }

                try (PageCursor cursor = pagedFile.io(0L, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                    assertFalse(cursor.next());
                    assertFalse(cursor.next());
                }

                try (PageCursor cursor = pagedFile.io(1L, PF_SHARED_WRITE_LOCK | PF_NO_GROW, NULL_CONTEXT)) {
                    assertFalse(cursor.next());
                }

                try (PageCursor cursor = pagedFile.io(1L, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                    assertFalse(cursor.next());
                }
            }
        });
    }

    @Test
    void newlyWrittenPagesMustBeAccessibleWithNoGrow() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            int initialPages = 1;
            int pagesToAdd = 3;
            generateFileWithRecords(
                    file("a"),
                    recordsPerFilePage * initialPages,
                    recordSize,
                    recordsPerFilePage,
                    reservedBytes,
                    pageCachePageSize);

            PagedFile pagedFile = map(file("a"), filePageSize);

            try (PageCursor cursor = pagedFile.io(1L, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                for (int i = 0; i < pagesToAdd; i++) {
                    assertTrue(cursor.next());
                    writeRecords(cursor);
                }
            }

            int pagesChecked = 0;
            try (PageCursor cursor = pagedFile.io(0L, PF_SHARED_WRITE_LOCK | PF_NO_GROW, NULL_CONTEXT)) {
                while (cursor.next()) {
                    verifyRecordsMatchExpected(cursor);
                    pagesChecked++;
                }
            }
            assertThat(pagesChecked).isEqualTo(initialPages + pagesToAdd);

            pagesChecked = 0;
            try (PageCursor cursor = pagedFile.io(0L, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                while (cursor.next()) {
                    verifyRecordsMatchExpected(cursor);
                    pagesChecked++;
                }
            }
            assertThat(pagesChecked).isEqualTo(initialPages + pagesToAdd);
            pagedFile.close();
        });
    }

    @Test
    void readLockImpliesNoGrow() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            int initialPages = 3;
            generateFileWithRecords(
                    file("a"),
                    recordsPerFilePage * initialPages,
                    recordSize,
                    recordsPerFilePage,
                    reservedBytes,
                    filePageSize);

            int pagesChecked = 0;
            try (PagedFile pagedFile = map(file("a"), filePageSize);
                    PageCursor cursor = pagedFile.io(0L, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                while (cursor.next()) {
                    pagesChecked++;
                }
            }
            assertThat(pagesChecked).isEqualTo(initialPages);
        });
    }

    // This test has an internal timeout in that it tries to verify 1000 reads within SHORT_TIMEOUT_MILLIS,
    // although this is a soft limit in that it may abort if number of verifications isn't reached.
    // This is so because on some machines this test takes a very long time to run. Verifying in the end
    // that at least there were some correct reads is good enough.
    @Test
    void retryMustResetCursorOffset() throws Exception {
        // The general idea here, is that we have a page with a particular value in its 0th position.
        // We also have a thread that constantly writes to the middle of the page, so it modifies
        // the page, but does not change the value in the 0th position. This thread will in principle
        // mean that it is possible for a reader to get an inconsistent view and must retry.
        // We then check that every retry iteration will read the special value in the 0th position.
        // We repeat the experiment a couple of times to make sure we didn't succeed by chance.

        configureStandardPageCache();
        final PagedFile pagedFile = map(file("a"), filePageSize);
        final AtomicReference<Exception> caughtWriterException = new AtomicReference<>();
        final CountDownLatch startLatch = new CountDownLatch(1);
        final byte expectedByte = (byte) 13;

        try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            if (cursor.next()) {
                cursor.putByte(expectedByte);
            }
        }

        AtomicBoolean end = new AtomicBoolean(false);
        Runnable writer = () -> {
            while (!end.get()) {
                try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                    if (cursor.next()) {
                        cursor.setOffset(recordSize);
                        cursor.putByte((byte) 14);
                    }
                    startLatch.countDown();
                } catch (IOException e) {
                    caughtWriterException.set(e);
                    throw new RuntimeException(e);
                }
            }
        };
        Future<?> writerFuture = executor.submit(writer);

        startLatch.await();

        long timeout = currentTimeMillis() + SHORT_TIMEOUT_MILLIS;
        int i = 0;
        for (; i < 1000 && currentTimeMillis() < timeout; i++) {
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                do {
                    assertThat(cursor.getByte()).isEqualTo(expectedByte);
                } while (cursor.shouldRetry() && currentTimeMillis() < timeout);
            }
        }

        end.set(true);
        writerFuture.get();
        assertTrue(i > 1);
        pagedFile.close();
    }

    @Test
    void nextWithPageIdMustAllowTraversingInReverse() {
        assertTimeoutPreemptively(ofMillis(SEMI_LONG_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            generateFileWithRecords(
                    file("a"), recordCount, recordSize, recordsPerFilePage, reservedBytes, filePageSize);
            long lastFilePageId = (recordCount / recordsPerFilePage) - 1;

            try (PagedFile pagedFile = map(file("a"), filePageSize);
                    PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                for (long currentPageId = lastFilePageId; currentPageId >= 0; currentPageId--) {
                    assertTrue(cursor.next(currentPageId), "next( currentPageId = " + currentPageId + " )");
                    assertThat(cursor.getCurrentPageId()).isEqualTo(currentPageId);
                    verifyRecordsMatchExpected(cursor);
                }
            }
        });
    }

    @Test
    void nextWithPageIdMustReturnFalseIfPageIdIsBeyondFilePageRangeAndNoGrowSpecified() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            generateFileWithRecords(
                    file("a"), recordsPerFilePage * 2, recordSize, recordsPerFilePage, reservedBytes, filePageSize);

            try (PagedFile pagedFile = map(file("a"), filePageSize)) {
                try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK | PF_NO_GROW, NULL_CONTEXT)) {
                    assertFalse(cursor.next(2));
                    assertTrue(cursor.next(1));
                }

                try (PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                    assertFalse(cursor.next(2));
                    assertTrue(cursor.next(1));
                }
            }
        });
    }

    @Test
    void pagesAddedWithNextWithPageIdMustBeAccessibleWithNoGrowSpecified() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            PagedFile pagedFile = map(file("a"), filePageSize);

            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next(2));
                writeRecords(cursor);
                assertTrue(cursor.next(0));
                writeRecords(cursor);
                assertTrue(cursor.next(1));
                writeRecords(cursor);
            }

            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK | PF_NO_GROW, NULL_CONTEXT)) {
                while (cursor.next()) {
                    verifyRecordsMatchExpected(cursor);
                }
            }

            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                while (cursor.next()) {
                    verifyRecordsMatchExpected(cursor);
                }
            }
            pagedFile.close();
        });
    }

    @Test
    void writesOfDifferentUnitsMustHaveCorrectEndianness() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            try (PagedFile pagedFile = map(file("a"), 23 + reservedBytes)) {
                try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                    assertTrue(cursor.next());
                    byte[] data = {42, 43, 44, 45, 46};

                    cursor.putLong(41); //  0+8 = 8
                    cursor.putInt(41); //  8+4 = 12
                    cursor.putShort((short) 41); // 12+2 = 14
                    cursor.putByte((byte) 41); // 14+1 = 15
                    cursor.putBytes(data); // 15+5 = 20
                    cursor.putBytes(3, (byte) 47); // 20+3 = 23
                }
                try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                    assertTrue(cursor.next());

                    long a = cursor.getLong(); //  8
                    int b = cursor.getInt(); // 12
                    short c = cursor.getShort(); // 14
                    byte[] data = new byte[] {
                        cursor.getByte(), // 15
                        cursor.getByte(), // 16
                        cursor.getByte(), // 17
                        cursor.getByte(), // 18
                        cursor.getByte(), // 19
                        cursor.getByte() // 20
                    };
                    byte d = cursor.getByte(); // 21
                    byte e = cursor.getByte(); // 22
                    byte f = cursor.getByte(); // 23
                    cursor.setOffset(0);
                    cursor.putLong(1 + a);
                    cursor.putInt(1 + b);
                    cursor.putShort((short) (1 + c));
                    for (byte g : data) {
                        g++;
                        cursor.putByte(g);
                    }
                    cursor.putByte((byte) (1 + d));
                    cursor.putByte((byte) (1 + e));
                    cursor.putByte((byte) (1 + f));
                }
            }

            ByteBuffer buf = ByteBuffers.allocate(23, ByteOrder.LITTLE_ENDIAN, INSTANCE);
            try (StoreChannel channel = fs.read(file("a"))) {
                channel.position(reservedBytes);
                channel.readAll(buf);
            }
            buf.flip();

            assertThat(buf.getLong()).isEqualTo(42L);
            assertThat(buf.getInt()).isEqualTo(42);
            assertThat(buf.getShort()).isEqualTo((short) 42);
            assertThat(buf.get()).isEqualTo((byte) 42);
            assertThat(buf.get()).isEqualTo((byte) 43);
            assertThat(buf.get()).isEqualTo((byte) 44);
            assertThat(buf.get()).isEqualTo((byte) 45);
            assertThat(buf.get()).isEqualTo((byte) 46);
            assertThat(buf.get()).isEqualTo((byte) 47);
            assertThat(buf.get()).isEqualTo((byte) 48);
            assertThat(buf.get()).isEqualTo((byte) 48);
            assertThat(buf.get()).isEqualTo((byte) 48);
        });
    }

    @Test
    void mappingFileSecondTimeWithLesserPageSizeMustThrow() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            try (PagedFile ignore = map(file("a"), filePageSize)) {
                assertThrows(IllegalArgumentException.class, () -> map(file("a"), filePageSize - 1));
            }
        });
    }

    @Test
    void mappingFileSecondTimeWithGreaterPageSizeMustThrow() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            try (PagedFile ignore = map(file("a"), filePageSize)) {
                assertThrows(IllegalArgumentException.class, () -> map(file("a"), filePageSize + 1));
            }
        });
    }

    @Test
    void allowOpeningMultipleReadAndWriteCursorsPerThread() {
        assumeFalse(multiVersioned);
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            Path fileA = existingFile("a");
            Path fileB = existingFile("b");

            generateFileWithRecords(fileA, 1, 16, recordsPerFilePage, reservedBytes, pageCachePageSize);
            generateFileWithRecords(fileB, 1, 16, recordsPerFilePage, reservedBytes, pageCachePageSize);

            try (PagedFile pfA = map(fileA, filePageSize);
                    PagedFile pfB = map(fileB, filePageSize);
                    PageCursor a = pfA.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT);
                    PageCursor b = pfA.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT);
                    PageCursor c = pfA.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
                    PageCursor d = pfA.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
                    PageCursor e = pfB.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT);
                    PageCursor f = pfB.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT);
                    PageCursor g = pfB.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
                    PageCursor h = pfB.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(a.next());
                assertTrue(b.next());
                assertTrue(c.next());
                assertTrue(d.next());
                assertTrue(e.next());
                assertTrue(f.next());
                assertTrue(g.next());
                assertTrue(h.next());
            }
        });
    }

    @Test
    void allowOpeningMultipleNonInterleavingWriteCursorsPerThread() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            Path fileA = existingFile("a");
            Path fileB = existingFile("b");

            generateFileWithRecords(fileA, 1, 16, recordsPerFilePage, reservedBytes, pageCachePageSize);
            generateFileWithRecords(fileB, 1, 16, recordsPerFilePage, reservedBytes, pageCachePageSize);

            try (PagedFile pfA = map(fileA, filePageSize);
                    PagedFile pfB = map(fileB, filePageSize);
                    PageCursor c = pfA.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
                    PageCursor d = pfA.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
                    PageCursor g = pfB.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
                    PageCursor h = pfB.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(c.next(0));
                assertTrue(d.next(1));
                assertTrue(g.next(0));
                assertTrue(h.next(1));
            }
        });
    }

    @Test
    void mustNotLiveLockIfWeRunOutOfEvictablePages() {
        assertTimeoutPreemptively(ofMillis(SEMI_LONG_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            List<PageCursor> cursors = new LinkedList<>();
            try (PagedFile pf = map(existingFile("a"), filePageSize)) {
                try {
                    assertThrows(IOException.class, () -> {
                        //noinspection InfiniteLoopStatement
                        for (long i = 0; ; i++) {
                            PageCursor cursor = pf.io(i, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
                            cursors.add(cursor);
                            assertTrue(cursor.next());
                        }
                    });
                } finally {
                    for (PageCursor cursor : cursors) {
                        cursor.close();
                    }
                }
            }
        });
    }

    @Test
    void writeLocksMustNotBeExclusive() {
        assumeFalse(multiVersioned);
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            try (PagedFile pf = map(existingFile("a"), filePageSize);
                    PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());

                executor.submit(() -> {
                            try (PageCursor innerCursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                                assertTrue(innerCursor.next());
                            }
                            return null;
                        })
                        .get();
            }
        });
    }

    @Test
    void writeLockMustInvalidateInnerReadLock() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            try (PagedFile pf = map(existingFile("a"), filePageSize);
                    PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());

                executor.submit(() -> {
                            try (PageCursor innerCursor = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                                assertTrue(innerCursor.next());
                                assertTrue(innerCursor.shouldRetry());
                            }
                            return null;
                        })
                        .get();
            }
        });
    }

    @Test
    void writeLockMustInvalidateExistingReadLock() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            BinaryLatch startLatch = new BinaryLatch();
            BinaryLatch continueLatch = new BinaryLatch();

            try (PagedFile pf = map(existingFile("a"), filePageSize);
                    PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next()); // Ensure that page 0 exists so the read cursor can get it
                assertTrue(cursor.next()); // Then unlock it

                Future<Object> read = executor.submit(() -> {
                    try (PageCursor innerCursor = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                        assertTrue(innerCursor.next());
                        assertFalse(innerCursor.shouldRetry());
                        startLatch.release();
                        continueLatch.await();
                        assertTrue(innerCursor.shouldRetry());
                    }
                    return null;
                });

                startLatch.await();
                assertTrue(cursor.next(0)); // Re-take the write lock on page 0.
                continueLatch.release();
                read.get();
            }
        });
    }

    @Test
    void writeUnlockMustInvalidateReadLocks() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            BinaryLatch startLatch = new BinaryLatch();
            BinaryLatch continueLatch = new BinaryLatch();

            try (PagedFile pf = map(existingFile("a"), filePageSize);
                    PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next()); // Lock page 0

                Future<Object> read = executor.submit(() -> {
                    try (PageCursor innerCursor = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                        assertTrue(innerCursor.next());
                        assertTrue(innerCursor.shouldRetry());
                        startLatch.release();
                        continueLatch.await();
                        assertTrue(innerCursor.shouldRetry());
                    }
                    return null;
                });

                startLatch.await();
                assertTrue(cursor.next()); // Unlock page 0
                continueLatch.release();
                read.get();
            }
        });
    }

    @Test
    void mustNotFlushCleanPagesWhenEvicting() {
        assertTimeoutPreemptively(ofMillis(SEMI_LONG_TIMEOUT_MILLIS), () -> {
            final AtomicBoolean observedWrite = new AtomicBoolean();
            FileSystemAbstraction fs = new DelegatingFileSystemAbstraction(this.fs) {
                @Override
                public StoreChannel open(Path fileName, Set<OpenOption> options) throws IOException {
                    StoreChannel channel = super.open(fileName, options);
                    return new DelegatingStoreChannel(channel) {
                        @Override
                        public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
                            observedWrite.set(true);
                            throw new IOException("not allowed");
                        }

                        @Override
                        public void writeAll(ByteBuffer src, long position) throws IOException {
                            observedWrite.set(true);
                            throw new IOException("not allowed");
                        }

                        @Override
                        public void writeAll(ByteBuffer src) throws IOException {
                            observedWrite.set(true);
                            throw new IOException("not allowed");
                        }

                        @Override
                        public int write(ByteBuffer src) throws IOException {
                            observedWrite.set(true);
                            throw new IOException("not allowed");
                        }

                        @Override
                        public long write(ByteBuffer[] srcs) throws IOException {
                            observedWrite.set(true);
                            throw new IOException("not allowed");
                        }
                    };
                }
            };
            getPageCache(fs, maxPages, PageCacheTracer.NULL);
            generateFileWithRecords(
                    file("a"), recordCount, recordSize, recordsPerFilePage, reservedBytes, filePageSize);

            try (PagedFile pagedFile = map(file("a"), filePageSize);
                    PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                while (cursor.next()) {
                    verifyRecordsMatchExpected(cursor);
                }
            }
            assertFalse(observedWrite.get());
        });
    }

    @Test
    void evictionMustFlushPagesToTheRightFiles() {
        assertTimeoutPreemptively(ofMillis(SEMI_LONG_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            generateFileWithRecords(
                    file("a"), recordCount, recordSize, recordsPerFilePage, reservedBytes, filePageSize);

            int file2PageSize = filePageSize - 3; // diff. page size just to be difficult
            int file2Payload = file2PageSize - reservedBytes;
            long maxPageIdCursor1 = recordCount / recordsPerFilePage;

            Path file2 = file("b");
            long file2sizeBytes = (maxPageIdCursor1 + 17) * file2PageSize;
            long file2PayloadBytes = (maxPageIdCursor1 + 17) * file2Payload;

            try (OutputStream outputStream = fs.openAsOutputStream(file2, false)) {
                for (int i = 0; i < file2PayloadBytes; i++) {
                    if (i % file2Payload == 0) {
                        outputStream.write(new byte[reservedBytes]);
                    }
                    // We will use the page cache to change these 'a's into 'b's.
                    outputStream.write('a');
                }
                outputStream.flush();
            }

            try (PagedFile pagedFile1 = map(file("a"), filePageSize);
                    PagedFile pagedFile2 = map(file2, file2PageSize)) {
                long pageId1 = 0;
                long pageId2 = 0;
                boolean moreWorkToDo;
                do {
                    boolean cursorReady1;
                    boolean cursorReady2;

                    try (PageCursor cursor = pagedFile1.io(pageId1, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                        cursorReady1 = cursor.next() && cursor.getCurrentPageId() < maxPageIdCursor1;
                        if (cursorReady1) {
                            writeRecords(cursor);
                            pageId1++;
                        }
                    }

                    try (PageCursor cursor = pagedFile2.io(pageId2, PF_SHARED_WRITE_LOCK | PF_NO_GROW, NULL_CONTEXT)) {
                        cursorReady2 = cursor.next();
                        if (cursorReady2) {
                            for (int i = 0; i < file2Payload; i++) {
                                cursor.putByte((byte) 'b');
                            }
                            assertFalse(cursor.shouldRetry());
                        }
                        pageId2++;
                    }

                    moreWorkToDo = cursorReady1 || cursorReady2;
                } while (moreWorkToDo);
            }

            // Verify the file contents
            assertThat(fs.getFileSize(file2)).isEqualTo(file2sizeBytes);
            try (InputStream inputStream = fs.openAsInputStream(file2)) {
                for (int i = 0; i < file2PayloadBytes; i++) {
                    if (i % file2Payload == 0) {
                        inputStream.readNBytes(reservedBytes);
                    }
                    int b = inputStream.read();
                    assertThat(b).isEqualTo('b');
                }
                assertThat(inputStream.read()).isEqualTo(-1);
            }

            try (StoreChannel channel = fs.read(file("a"))) {
                ByteBuffer bufB = ByteBuffers.allocate(recordSize, ByteOrder.LITTLE_ENDIAN, INSTANCE);
                for (int i = 0; i < recordCount; i++) {
                    if (i % recordsPerFilePage == 0) {
                        channel.position(channel.position() + reservedBytes);
                    }
                    bufA.clear();
                    channel.readAll(bufA);
                    bufA.flip();
                    bufB.clear();
                    generateRecordForId(i, bufB);
                    assertThat(bufB.array()).containsExactly(bufA.array());
                }
            }
        });
    }

    @Test
    void tracerMustBeNotifiedAboutPinUnpinFaultAndEvictEventsWhenReading() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            DefaultPageCacheTracer tracer = new DefaultPageCacheTracer(true);
            var contextFactory = new CursorContextFactory(tracer, EMPTY_CONTEXT_SUPPLIER);
            getPageCache(fs, maxPages, tracer);

            generateFileWithRecords(
                    file("a"), recordCount, recordSize, recordsPerFilePage, reservedBytes, filePageSize);

            long initialPins = tracer.pins();
            long initialUnpins = tracer.unpins();
            long countedPages = 0;
            long countedFaults = 0;
            try (CursorContext cursorContext =
                            contextFactory.create("tracerMustBeNotifiedAboutPinUnpinFaultAndEvictEventsWhenReading");
                    PagedFile pagedFile = map(file("a"), filePageSize);
                    PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, cursorContext)) {
                while (cursor.next()) {
                    countedPages++;
                    countedFaults++;
                }

                // Using next( pageId ) to the already-pinned page id does not count,
                // so we only increment once for this section
                countedPages++;
                for (int i = 0; i < 20; i++) {
                    assertTrue(cursor.next(1));
                }

                // But if we use next( pageId ) to a page that is different from the one already pinned,
                // then it counts
                for (int i = 0; i < 20; i++) {
                    assertTrue(cursor.next(i));
                    countedPages++;
                }

                assertThat(pagedFile.pageFileCounters().pins())
                        .as("wrong count of pins")
                        .isEqualTo(countedPages + initialPins);
                // we substract 1 here since cursor still not unpinned last page at this point
                assertThat(pagedFile.pageFileCounters().unpins())
                        .as("wrong count of unpins")
                        .isEqualTo(countedPages + initialUnpins - 1);
            }

            assertThat(tracer.pins()).as("wrong count of pins").isEqualTo(countedPages + initialPins);
            assertThat(tracer.unpins()).as("wrong count of unpins").isEqualTo(countedPages + initialUnpins);

            // We might be unlucky and fault in the second next call, on the page
            // we brought up in the first next call. That's why we assert that we
            // have observed *at least* the countedPages number of faults.
            long faults = tracer.faults();
            long bytesRead = tracer.bytesRead();
            assertThat(faults).as("wrong count of faults").isGreaterThanOrEqualTo(countedFaults);
            assertThat(bytesRead).as("wrong number of bytes read").isGreaterThanOrEqualTo(countedFaults * filePageSize);
            // Every page we move forward can put the freelist behind so the cache
            // wants to evict more pages. Plus, every page fault we do could also
            // block and get a page directly transferred to it, and these kinds of
            // evictions can count in addition to the evictions we do when the
            // cache is behind on keeping the freelist full.
            assertThat(tracer.evictions())
                    .as("wrong count of evictions")
                    .isGreaterThanOrEqualTo(countedFaults - maxPages)
                    .isLessThanOrEqualTo(countedPages + faults);
        });
    }

    @Test
    void tracerMustBeNotifiedAboutPinUnpinFaultFlushAndEvictionEventsWhenWriting() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            long pagesToGenerate = 142;
            DefaultPageCacheTracer tracer = new DefaultPageCacheTracer(true);
            var contextFactory = new CursorContextFactory(tracer, EMPTY_CONTEXT_SUPPLIER);
            getPageCache(fs, maxPages, tracer);

            long initialPins = tracer.pins();
            long initialUnpins = tracer.unpins();

            try (CursorContext cursorContext = contextFactory.create(
                            "tracerMustBeNotifiedAboutPinUnpinFaultFlushAndEvictionEventsWhenWriting");
                    PagedFile pagedFile = map(file("a"), filePageSize);
                    PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, cursorContext)) {
                for (long i = 0; i < pagesToGenerate; i++) {
                    assertTrue(cursor.next());
                    assertThat(cursor.getCurrentPageId()).isEqualTo(i);
                    assertTrue(cursor.next(i)); // This does not count as a pin
                    assertThat(cursor.getCurrentPageId()).isEqualTo(i);

                    writeRecords(cursor);
                }

                // This counts as a single pin
                assertTrue(cursor.next(0));
                assertTrue(cursor.next(0));

                assertThat(pagedFile.pageFileCounters().pins())
                        .as("wrong count of pins")
                        .isEqualTo(pagesToGenerate + 1 + initialPins);
                // we do not have +1 here since cursor still not unpinned last page at this point
                assertThat(pagedFile.pageFileCounters().unpins())
                        .as("wrong count of unpins")
                        .isEqualTo(pagesToGenerate + initialPins);
            }

            assertThat(tracer.pins()).as("wrong count of pins").isEqualTo(pagesToGenerate + 1 + initialPins);
            assertThat(tracer.unpins()).as("wrong count of unpins").isEqualTo(pagesToGenerate + 1 + initialUnpins);

            // We might be unlucky and fault in the second next call, on the page
            // we brought up in the first next call. That's why we assert that we
            // have observed *at least* the countedPages number of faults.
            long faults = tracer.faults();
            assertThat(faults).as("wrong count of faults").isGreaterThanOrEqualTo(pagesToGenerate);
            // Every page we move forward can put the freelist behind so the cache
            // wants to evict more pages. Plus, every page fault we do could also
            // block and get a page directly transferred to it, and these kinds of
            // evictions can count in addition to the evictions we do when the
            // cache is behind on keeping the freelist full.
            assertThat(tracer.evictions())
                    .as("wrong count of evictions")
                    .isGreaterThanOrEqualTo(pagesToGenerate - maxPages)
                    .isLessThanOrEqualTo(pagesToGenerate + faults);

            // We use greaterThanOrEqualTo because we visit each page twice, and
            // that leaves a small window wherein we can race with eviction, have
            // the evictor flush the page, and then fault it back and mark it as
            // dirty again.
            // We also subtract 'maxPages' from the expected flush count, because
            // vectored IO may coalesce all the flushes we do as part of unmapping
            // the file, into a single flush.
            long flushes = tracer.flushes();
            long bytesWritten = tracer.bytesWritten();
            assertThat(flushes).as("wrong count of flushes").isGreaterThanOrEqualTo(pagesToGenerate - maxPages);
            assertThat(bytesWritten)
                    .as("wrong count of bytes written")
                    .isGreaterThanOrEqualTo(pagesToGenerate * filePageSize);
        });
    }

    @Test
    void tracerMustBeNotifiedOfReadAndWritePins() throws Exception {
        final AtomicInteger writeCount = new AtomicInteger();
        final AtomicInteger readCount = new AtomicInteger();

        DefaultPageCacheTracer tracer = new DefaultPageCacheTracer();
        DefaultPageCursorTracer pageCursorTracer = new DefaultPageCursorTracer(tracer, "test") {
            @Override
            public PinEvent beginPin(boolean writeLock, long filePageId, PageSwapper swapper) {
                (writeLock ? writeCount : readCount).getAndIncrement();
                return super.beginPin(writeLock, filePageId, swapper);
            }
        };
        var contextFactory = new CursorContextFactory(tracer, EMPTY_CONTEXT_SUPPLIER);
        getPageCache(fs, maxPages, tracer);

        generateFileWithRecords(file("a"), recordCount, recordSize, recordsPerFilePage, reservedBytes, filePageSize);

        int pinsForRead = 13;
        int pinsForWrite = 42;

        try (PagedFile pagedFile = map(file("a"), filePageSize)) {
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, contextFactory.create(pageCursorTracer))) {
                for (int i = 0; i < pinsForRead; i++) {
                    assertTrue(cursor.next());
                }
            }

            dirtyManyPages(pagedFile, pinsForWrite, contextFactory.create(pageCursorTracer));
        }

        assertThat(readCount.get()).as("wrong read pin count").isEqualTo(pinsForRead);
        assertThat(writeCount.get()).as("wrong write pin count").isEqualTo(pinsForWrite);
    }

    @Test
    void restartByShouldRetryMustCarryOverExistingPin() throws IOException {
        DefaultPageCacheTracer tracer = new DefaultPageCacheTracer();
        var contextFactory = new CursorContextFactory(tracer, EMPTY_CONTEXT_SUPPLIER);
        getPageCache(fs, maxPages, tracer);
        generateFileWithRecords(file("a"), recordCount, recordSize, recordsPerFilePage, reservedBytes, filePageSize);

        try (PagedFile pagedFile = map(file("a"), filePageSize);
                CursorContext cursorContext = contextFactory.create("test")) {
            try (PageCursor reader = pagedFile.io(0, PF_SHARED_READ_LOCK, cursorContext)) {
                assertTrue(reader.next());

                // Cause the page under the reader cursor to be evicted.
                try (PagedFile otherPagedFile = map(existingFile("b"), filePageSize);
                        PageCursor writer = otherPagedFile.io(0, PF_SHARED_WRITE_LOCK, cursorContext)) {
                    while (!reader.shouldRetry()) {
                        for (int i = 0; i < maxPages * 10; i++) {
                            assertTrue(writer.next(i));
                        }
                    }
                }
            }
        }
        // Then we should see pins and unpins pair up exactly.
        assertThat(tracer.unpins()).isEqualTo(tracer.pins());
    }

    @Test
    void pinEventShouldCompleteIfRepinFromShouldRetryThrows() throws IOException {
        DefaultPageCacheTracer tracer = new DefaultPageCacheTracer();
        RandomAdversary adversary = new RandomAdversary(0.0, 0.9, 0.0);
        adversary.setProbabilityFactor(0.0);
        AdversarialFileSystemAbstraction afs = new AdversarialFileSystemAbstraction(adversary, fs);
        var contextFactory = new CursorContextFactory(tracer, EMPTY_CONTEXT_SUPPLIER);
        getPageCache(afs, maxPages, tracer);
        generateFileWithRecords(file("a"), recordCount, recordSize, recordsPerFilePage, reservedBytes, filePageSize);

        try (PagedFile pagedFile = map(file("a"), filePageSize);
                CursorContext cursorContext = contextFactory.create("test")) {
            try (PageCursor reader = pagedFile.io(0, PF_SHARED_READ_LOCK, cursorContext)) {
                assertTrue(reader.next());

                // Cause the page under the reader cursor to be evicted.
                try (PagedFile otherPagedFile = map(existingFile("b"), filePageSize)) {
                    assertThrows(IOException.class, () -> {
                        //noinspection InfiniteLoopStatement
                        for (; ; ) {
                            adversary.setProbabilityFactor(1.0);
                            try {
                                reader.shouldRetry();
                            } finally {
                                adversary.setProbabilityFactor(0.0);
                            }
                            try (PageCursor writer = otherPagedFile.io(0, PF_SHARED_WRITE_LOCK, cursorContext)) {
                                for (int i = 0; i < maxPages * 10; i++) {
                                    assertTrue(writer.next(i));
                                }
                            }
                            otherPagedFile.flushAndForce(FileFlushEvent.NULL);
                        }
                    });
                }
            }
        }
        // Then we should see pins and unpins pair up exactly.
        assertThat(tracer.pins()).isEqualTo(tracer.unpins() + tracer.failedFaults());
    }

    @Test
    void lastPageIdOfEmptyFileIsLessThanZero() throws IOException {
        configureStandardPageCache();

        try (PagedFile pagedFile = map(file("a"), filePageSize)) {
            assertThat(pagedFile.getLastPageId()).isLessThan(0L);
        }
    }

    @Test
    void lastPageIdOfFileWithOneByteIsZero() throws IOException {
        StoreChannel channel = fs.write(file("a"));
        channel.write(ByteBuffer.wrap(new byte[] {1}));
        channel.close();

        configureStandardPageCache();

        try (PagedFile pagedFile = map(file("a"), filePageSize)) {
            assertThat(pagedFile.getLastPageId()).isEqualTo(0L);
        }
    }

    @Test
    void lastPageIdOfFileWithExactlyTwoPagesWorthOfDataIsOne() throws IOException {
        configureStandardPageCache();

        int twoPagesWorthOfRecords = recordsPerFilePage * 2;
        generateFileWithRecords(
                file("a"), twoPagesWorthOfRecords, recordSize, recordsPerFilePage, reservedBytes, filePageSize);

        try (PagedFile pagedFile = map(file("a"), filePageSize)) {
            assertThat(pagedFile.getLastPageId()).isEqualTo(1L);
        }
    }

    @Test
    void lastPageIdOfFileWithExactlyTwoPagesAndOneByteWorthOfDataIsTwo() throws IOException {
        configureStandardPageCache();

        int twoPagesWorthOfRecords = recordsPerFilePage * 2;
        generateFileWithRecords(
                file("a"), twoPagesWorthOfRecords, recordSize, recordsPerFilePage, reservedBytes, filePageSize);
        OutputStream outputStream = fs.openAsOutputStream(file("a"), true);
        outputStream.write('a');
        outputStream.close();

        try (PagedFile pagedFile = map(file("a"), filePageSize)) {
            assertThat(pagedFile.getLastPageId()).isEqualTo(2L);
        }
    }

    @Test
    void lastPageIdMustNotIncreaseWhenReadingToEndWithReadLock() throws IOException {
        configureStandardPageCache();
        generateFileWithRecords(file("a"), recordCount, recordSize, recordsPerFilePage, reservedBytes, filePageSize);
        PagedFile pagedFile = map(file("a"), filePageSize);

        long initialLastPageId = pagedFile.getLastPageId();
        try (PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
            //noinspection StatementWithEmptyBody
            while (cursor.next()) {
                // scan through the lot
            }
        }
        long resultingLastPageId = pagedFile.getLastPageId();
        pagedFile.close();
        assertThat(resultingLastPageId).isEqualTo(initialLastPageId);
    }

    @Test
    void lastPageIdMustNotIncreaseWhenReadingToEndWithNoGrowAndWriteLock() throws IOException {
        configureStandardPageCache();
        generateFileWithRecords(file("a"), recordCount, recordSize, recordsPerFilePage, reservedBytes, filePageSize);
        PagedFile pagedFile = map(file("a"), filePageSize);

        long initialLastPageId = pagedFile.getLastPageId();
        try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK | PF_NO_GROW, NULL_CONTEXT)) {
            //noinspection StatementWithEmptyBody
            while (cursor.next()) {
                // scan through the lot
            }
        }
        long resultingLastPageId = pagedFile.getLastPageId();

        try {
            assertThat(resultingLastPageId).isEqualTo(initialLastPageId);
        } finally {
            IOUtils.closeAllSilently(pagedFile);
        }
    }

    @Test
    void lastPageIdMustIncreaseWhenScanningPastEndWithWriteLock() throws IOException {
        configureStandardPageCache();
        generateFileWithRecords(
                file("a"), recordsPerFilePage * 10, recordSize, recordsPerFilePage, reservedBytes, filePageSize);
        PagedFile pagedFile = map(file("a"), filePageSize);

        assertThat(pagedFile.getLastPageId()).isEqualTo(9L);
        dirtyManyPages(pagedFile, 15);
        try {
            assertThat(pagedFile.getLastPageId()).isEqualTo(14L);
        } finally {
            IOUtils.closeAllSilently(pagedFile);
        }
    }

    @Test
    void lastPageIdMustIncreaseWhenJumpingPastEndWithWriteLock() throws IOException {
        configureStandardPageCache();
        generateFileWithRecords(
                file("a"), recordsPerFilePage * 10, recordSize, recordsPerFilePage, reservedBytes, filePageSize);
        PagedFile pagedFile = map(file("a"), filePageSize);

        assertThat(pagedFile.getLastPageId()).isEqualTo(9L);
        try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            assertTrue(cursor.next(15));
        }
        try {
            assertThat(pagedFile.getLastPageId()).isEqualTo(15L);
        } finally {
            IOUtils.closeAllSilently(pagedFile);
        }
    }

    @Test
    void lastPageIdFromUnmappedFileMustThrow() throws IOException {
        configureStandardPageCache();

        PagedFile file;
        try (PagedFile pf = map(file("a"), filePageSize, immutable.of(StandardOpenOption.CREATE))) {
            file = pf;
        }

        assertThrows(FileIsNotMappedException.class, file::getLastPageId);
    }

    @Test
    void cursorOffsetMustBeUpdatedReadAndWrite() throws IOException {
        configureStandardPageCache();

        try (PagedFile pagedFile = map(file("a"), filePageSize, immutable.empty())) {
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                verifyWriteOffsets(cursor);

                cursor.setOffset(0);
                verifyReadOffsets(cursor);
            }

            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                verifyReadOffsets(cursor);
            }
        }
    }

    private void verifyWriteOffsets(PageCursor cursor) {
        cursor.setOffset(filePageSize / 2);
        cursor.zapPage();
        assertThat(cursor.getOffset()).isEqualTo(filePageSize / 2);
        cursor.setOffset(0);
        cursor.putLong(1);
        assertThat(cursor.getOffset()).isEqualTo(8);
        cursor.putInt(1);
        assertThat(cursor.getOffset()).isEqualTo(12);
        cursor.putShort((short) 1);
        assertThat(cursor.getOffset()).isEqualTo(14);
        cursor.putByte((byte) 1);
        assertThat(cursor.getOffset()).isEqualTo(15);
        cursor.putBytes(new byte[] {1, 2, 3});
        assertThat(cursor.getOffset()).isEqualTo(18);
        cursor.putBytes(new byte[] {1, 2, 3}, 1, 1);
        assertThat(cursor.getOffset()).isEqualTo(19);
        cursor.putBytes(5, (byte) 1);
        assertThat(cursor.getOffset()).isEqualTo(24);
    }

    private static void verifyReadOffsets(PageCursor cursor) {
        assertThat(cursor.getOffset()).isEqualTo(0);
        cursor.getLong();
        assertThat(cursor.getOffset()).isEqualTo(8);
        cursor.getInt();
        assertThat(cursor.getOffset()).isEqualTo(12);
        cursor.getShort();
        assertThat(cursor.getOffset()).isEqualTo(14);
        cursor.getByte();
        assertThat(cursor.getOffset()).isEqualTo(15);
        cursor.getBytes(new byte[3]);
        assertThat(cursor.getOffset()).isEqualTo(18);
        cursor.getBytes(new byte[3], 1, 1);
        assertThat(cursor.getOffset()).isEqualTo(19);
        cursor.getBytes(new byte[5]);
        assertThat(cursor.getOffset()).isEqualTo(24);

        byte[] expectedBytes = new byte[] {
            1, 0, 0, 0, 0, 0, 0, 0, // first; long
            1, 0, 0, 0, // second; int
            1, 0, // third; short
            1, // fourth; byte
            1, 2, 3, // fifth; more bytes
            2, // sixth; additional bytes
            1, 1, 1, 1, 1, // lastly; more bytes
        };
        byte[] actualBytes = new byte[24];
        cursor.setOffset(0);
        cursor.getBytes(actualBytes);
        assertThat(actualBytes).containsExactly(expectedBytes);
    }

    @Test
    void testLitteEndianCursor() throws IOException {
        configureStandardPageCache();

        try (var pagedFile = map(file("a"), filePageSize, immutable.empty())) {
            try (var cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                cursor.next();
                cursor.putLong(0x1020304050607055L);
                cursor.putInt(0x10203040);
                cursor.putShort((short) 0x1020);
            }

            try (var cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                cursor.next();
                assertThat(cursor.getLong()).isEqualTo(0x1020304050607055L);
                assertThat(cursor.getInt()).isEqualTo(0x10203040);
                assertThat(cursor.getShort()).isEqualTo((short) 0x1020);
                cursor.setOffset(0);
                var actualBytes = new byte[Long.BYTES + Integer.BYTES + Short.BYTES];
                cursor.getBytes(actualBytes);
                // little-endian bytes layout
                assertThat(actualBytes).containsExactly(new byte[] {
                    0x55, 0x70, 0x60, 0x50, 0x40, 0x30, 0x20, 0x10, 0x40, 0x30, 0x20, 0x10, 0x20, 0x10
                });
            }
        }
    }

    @Test
    void testBigEndianCursor() throws IOException {
        configureStandardPageCache();

        try (var pagedFile = map(file("a"), filePageSize, immutable.of(PageCacheOpenOptions.BIG_ENDIAN))) {
            try (var cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                cursor.next();
                cursor.putLong(0x1020304050607055L);
                cursor.putInt(0x10203040);
                cursor.putShort((short) 0x1020);
            }

            try (var cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                cursor.next();
                assertThat(cursor.getLong()).isEqualTo(0x1020304050607055L);
                assertThat(cursor.getInt()).isEqualTo(0x10203040);
                assertThat(cursor.getShort()).isEqualTo((short) 0x1020);
                cursor.setOffset(0);
                var actualBytes = new byte[Long.BYTES + Integer.BYTES + Short.BYTES];
                cursor.getBytes(actualBytes);
                // little-endian bytes layout
                assertThat(actualBytes).containsExactly(new byte[] {
                    0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x55, 0x10, 0x20, 0x30, 0x40, 0x10, 0x20
                });
            }
        }
    }

    @Test
    void getBytesMustRespectOffsets() throws IOException {
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize);
                PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            assertTrue(cursor.next());
            cursor.putByte((byte) 1);
            cursor.putByte((byte) 2);
            cursor.putByte((byte) 3);
            cursor.putByte((byte) 4);
            cursor.putByte((byte) 5);
            cursor.putByte((byte) 6);
            cursor.putByte((byte) 7);
            cursor.putByte((byte) 8);
            byte[] data = {42, 42, 42, 42, 42};
            cursor.setOffset(1);
            cursor.getBytes(data, 1, 3);
            byte[] expected = {42, 2, 3, 4, 42};
            assertArrayEquals(expected, data);
        }
    }

    @Test
    void putBytesMustRespectOffsets() throws IOException {
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize);
                PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            assertTrue(cursor.next());
            cursor.putByte((byte) 1);
            cursor.putByte((byte) 2);
            cursor.putByte((byte) 3);
            cursor.putByte((byte) 4);
            cursor.putByte((byte) 5);
            cursor.putByte((byte) 6);
            cursor.putByte((byte) 7);
            cursor.putByte((byte) 8);
            byte[] data = {42, 41, 40, 39, 38};
            cursor.setOffset(1);
            cursor.putBytes(data, 1, 3);
            cursor.setOffset(0);
            assertThat(cursor.getByte()).isEqualTo((byte) 1);
            assertThat(cursor.getByte()).isEqualTo((byte) 41);
            assertThat(cursor.getByte()).isEqualTo((byte) 40);
            assertThat(cursor.getByte()).isEqualTo((byte) 39);
            assertThat(cursor.getByte()).isEqualTo((byte) 5);
            assertThat(cursor.getByte()).isEqualTo((byte) 6);
            assertThat(cursor.getByte()).isEqualTo((byte) 7);
            assertThat(cursor.getByte()).isEqualTo((byte) 8);
        }
    }

    @Test
    void getBytesMustRespectLargeOffsets() throws IOException {
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize);
                PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            assertTrue(cursor.next());
            for (int i = 0; i < 200; i++) {
                cursor.putByte((byte) (i + 1));
            }
            byte[] data = new byte[200];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) (data.length - i);
            }
            cursor.setOffset(1);
            cursor.getBytes(data, 1, data.length - 2);
            assertThat(data[0]).isEqualTo((byte) 200);
            for (int i = 1; i < 199; i++) {
                assertThat(data[i]).isEqualTo((byte) (i + 1));
            }
            assertThat(data[199]).isEqualTo((byte) 1);
        }
    }

    @Test
    void putBytesMustRespectLargeOffsets() throws IOException {
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize);
                PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            assertTrue(cursor.next());
            for (int i = 0; i < 200; i++) {
                cursor.putByte((byte) (i + 1));
            }
            byte[] data = new byte[200];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) (data.length - i);
            }
            cursor.setOffset(1);
            cursor.putBytes(data, 1, data.length - 2);
            cursor.setOffset(0);
            assertThat(cursor.getByte()).isEqualTo((byte) 1);
            for (int i = 0; i < 198; i++) {
                assertThat(cursor.getByte()).isEqualTo((byte) (199 - i));
            }
            assertThat(cursor.getByte()).isEqualTo((byte) 200);
        }
    }

    @Test
    void getBytesMustThrowArrayIndexOutOfBounds() throws Exception {
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize);
                PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            assertTrue(cursor.next());
            byte[] bytes = new byte[3];
            assertThrows(ArrayIndexOutOfBoundsException.class, () -> cursor.getBytes(bytes, 1, 3));
        }
    }

    @Test
    void putBytesMustThrowArrayIndexOutOfBounds() throws Exception {
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize);
                PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            assertTrue(cursor.next());
            byte[] bytes = new byte[3];
            assertThrows(ArrayIndexOutOfBoundsException.class, () -> cursor.putBytes(bytes, 1, 3));
        }
    }

    @Test
    void closeOnPageCacheMustThrowIfFilesAreStillMapped() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            try (PagedFile ignore = map(file("a"), filePageSize)) {
                assertThrows(IllegalStateException.class, () -> pageCache.close());
            }
        });
    }

    @Test
    void pagedFileIoMustThrowIfFileIsUnmapped() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            PagedFile pagedFile = map(file("a"), filePageSize);
            pagedFile.close();

            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                FileIsNotMappedException exception = assertThrows(FileIsNotMappedException.class, cursor::next);
                assertThat(Exceptions.stringify(exception)).contains("tracing paged file closing");
            }
        });
    }

    @Test
    void writeLockedPageCursorNextMustThrowIfFileIsUnmapped() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            PagedFile pagedFile = map(file("a"), filePageSize);
            PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
            pagedFile.close();

            FileIsNotMappedException exception = assertThrows(FileIsNotMappedException.class, cursor::next);
            assertThat(Exceptions.stringify(exception)).contains("tracing paged file closing");
        });
    }

    @Test
    void writeLockedPageCursorNextWithIdMustThrowIfFileIsUnmapped() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            PagedFile pagedFile = map(file("a"), filePageSize);
            PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
            pagedFile.close();

            assertThrows(FileIsNotMappedException.class, () -> cursor.next(1));
        });
    }

    @Test
    void readLockedPageCursorNextMustThrowIfFileIsUnmapped() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            generateFileWithRecords(file("a"), 1, recordSize, recordsPerFilePage, reservedBytes, filePageSize);

            PagedFile pagedFile = map(file("a"), filePageSize);
            PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT);
            pagedFile.close();

            assertThrows(FileIsNotMappedException.class, cursor::next);
        });
    }

    @Test
    void readLockedPageCursorNextWithIdMustThrowIfFileIsUnmapped() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            generateFileWithRecords(
                    file("a"), recordsPerFilePage * 2, recordSize, recordsPerFilePage, reservedBytes, filePageSize);

            PagedFile pagedFile = map(file("a"), filePageSize);
            PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT);
            pagedFile.close();

            assertThrows(FileIsNotMappedException.class, () -> cursor.next(1));
        });
    }

    @Test
    void writeLockedPageMustBlockFileUnmapping() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            PagedFile pagedFile = map(file("a"), filePageSize);
            PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
            assertTrue(cursor.next());

            Thread unmapper = fork(closePageFile(pagedFile));
            unmapper.join(100);

            cursor.close();
            unmapper.join();
        });
    }

    @Test
    void optimisticReadLockedPageMustNotBlockFileUnmapping() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            generateFileWithRecords(file("a"), 1, recordSize, recordsPerFilePage, reservedBytes, filePageSize);

            PagedFile pagedFile = map(file("a"), filePageSize);
            PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT);
            assertTrue(cursor.next()); // Got a read lock

            fork(closePageFile(pagedFile)).join();

            cursor.close();
        });
    }

    @Test
    void advancingPessimisticReadLockingCursorAfterUnmappingMustThrow() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            generateFileWithRecords(
                    file("a"), recordsPerFilePage * 2, recordSize, recordsPerFilePage, reservedBytes, filePageSize);

            PagedFile pagedFile = map(file("a"), filePageSize);
            PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT);
            assertTrue(cursor.next()); // Got a pessimistic read lock

            fork(closePageFile(pagedFile)).join();

            assertThrows(FileIsNotMappedException.class, cursor::next);
        });
    }

    @Test
    void advancingOptimisticReadLockingCursorAfterUnmappingMustThrow() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            generateFileWithRecords(
                    file("a"), recordsPerFilePage * 2, recordSize, recordsPerFilePage, reservedBytes, filePageSize);

            PagedFile pagedFile = map(file("a"), filePageSize);
            PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT);
            assertTrue(cursor.next()); // fault
            assertTrue(cursor.next()); // fault + unpin page 0
            assertTrue(cursor.next(0)); // potentially optimistic read lock page 0

            fork(closePageFile(pagedFile)).join();

            assertThrows(FileIsNotMappedException.class, cursor::next);
        });
    }

    @Test
    void readingAndRetryingOnPageWithOptimisticReadLockingAfterUnmappingMustThrow() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            generateFileWithRecords(
                    file("a"), recordsPerFilePage * 2, recordSize, recordsPerFilePage, reservedBytes, filePageSize);

            PagedFile pagedFile = map(file("a"), filePageSize);
            PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT);
            assertTrue(cursor.next()); // fault
            assertTrue(cursor.next()); // fault + unpin page 0
            assertTrue(cursor.next(0)); // potentially optimistic read lock page 0

            fork(closePageFile(pagedFile)).join();
            pageCache.close();
            pageCache = null;

            cursor.getByte();
            assertThrows(FileIsNotMappedException.class, cursor::shouldRetry);
        });
    }

    @Test
    void shouldRetryFromUnboundReadCursorMustNotThrow() throws Exception {
        Path file = file("a");
        generateFileWithRecords(file, recordsPerFilePage, recordSize, recordsPerFilePage, reservedBytes, filePageSize);
        configureStandardPageCache();
        try (PagedFile pf = map(file, filePageSize);
                PageCursor cursor = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
            assertFalse(cursor.shouldRetry());
        }
    }

    @Test
    void shouldRetryFromUnboundWriteCursorMustNotThrow() throws Exception {
        configureStandardPageCache();
        Path file = file("a");
        generateFileWithRecords(file, recordsPerFilePage, recordSize, recordsPerFilePage, reservedBytes, filePageSize);
        try (PagedFile pf = map(file, filePageSize);
                PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            assertFalse(cursor.shouldRetry());
        }
    }

    @Test
    void shouldRetryFromUnboundLinkedReadCursorMustNotThrow() throws Exception {
        configureStandardPageCache();
        Path file = file("a");
        generateFileWithRecords(
                file, recordsPerFilePage * 2, recordSize, recordsPerFilePage, reservedBytes, filePageSize);
        try (PagedFile pf = map(file, filePageSize);
                PageCursor cursor = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
            assertTrue(cursor.next());
            //noinspection unused
            try (PageCursor linked = cursor.openLinkedCursor(1)) {
                assertFalse(cursor.shouldRetry());
            }
        }
    }

    @Test
    void shouldRetryFromUnboundLinkedWriteCursorMustNotThrow() throws Exception {
        configureStandardPageCache();
        Path file = file("a");
        generateFileWithRecords(
                file, recordsPerFilePage * 2, recordSize, recordsPerFilePage, reservedBytes, filePageSize);
        try (PagedFile pf = map(file, filePageSize);
                PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            assertTrue(cursor.next());
            //noinspection unused
            try (PageCursor linked = cursor.openLinkedCursor(1)) {
                assertFalse(cursor.shouldRetry());
            }
        }
    }

    @Test
    void shouldRetryOnWriteParentOfClosedLinkedCursorMustNotThrow() throws Exception {
        configureStandardPageCache();
        Path file = file("a");
        generateFileWithRecords(
                file, recordsPerFilePage * 2, recordSize, recordsPerFilePage, reservedBytes, filePageSize);
        try (PagedFile pf = map(file, filePageSize);
                PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            assertTrue(cursor.next());
            try (PageCursor linked = cursor.openLinkedCursor(1)) {
                assertTrue(linked.next());
            }
            cursor.shouldRetry();
        }
    }

    @Test
    void shouldRetryOnReadParentOfClosedLinkedCursorMustNotThrow() throws Exception {
        configureStandardPageCache();
        Path file = file("a");
        generateFileWithRecords(
                file, recordsPerFilePage * 2, recordSize, recordsPerFilePage, reservedBytes, filePageSize);
        try (PagedFile pf = map(file, filePageSize);
                PageCursor cursor = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
            assertTrue(cursor.next());
            try (PageCursor linked = cursor.openLinkedCursor(1)) {
                assertTrue(linked.next());
            }
            cursor.shouldRetry();
        }
    }

    @Test
    void shouldRetryOnReadParentOnDirtyPageOfClosedLinkedCursorMustNotThrow() throws Exception {
        configureStandardPageCache();
        Path file = file("a");
        generateFileWithRecords(
                file, recordsPerFilePage * 2, recordSize, recordsPerFilePage, reservedBytes, filePageSize);
        try (PagedFile pf = map(file, filePageSize);
                PageCursor reader = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
            assertTrue(reader.next());
            try (PageCursor writer = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(writer.next());
            }
            try (PageCursor linked = reader.openLinkedCursor(1)) {
                assertTrue(linked.next());
            }
            assertTrue(reader.shouldRetry());
        }
    }

    @Test
    void pageCursorCloseShouldNotReturnAlreadyClosedLinkedCursorToPool() throws Exception {
        getPageCache(fs, maxPages, PageCacheTracer.NULL);
        Path file = file("a");
        generateFileWithRecords(
                file, recordsPerFilePage * 2, recordSize, recordsPerFilePage, reservedBytes, filePageSize);
        try (PagedFile pf = map(file, filePageSize)) {
            PageCursor a = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
            PageCursor b = a.openLinkedCursor(0);
            b.close();
            PageCursor c = a.openLinkedCursor(0); // Will close b again, creating a loop in the CursorPool
            PageCursor d = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT); // Same object as c because of loop in pool
            assertNotSame(c, d);
            c.close();
            d.close();
        }
    }

    @Test
    void pageCursorCloseShouldNotReturnSameObjectToCursorPoolTwice() throws Exception {
        getPageCache(fs, maxPages, PageCacheTracer.NULL);
        Path file = file("a");
        generateFileWithRecords(
                file, recordsPerFilePage * 2, recordSize, recordsPerFilePage, reservedBytes, filePageSize);
        try (PagedFile pf = map(file, filePageSize)) {
            PageCursor a = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
            a.close();
            a.close(); // Return same object to CursorPool again, creating a Loop
            PageCursor b = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
            PageCursor c = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
            assertNotSame(b, c);
            b.close();
            c.close();
        }
    }

    @Test
    void pageCursorCloseMustNotClosePreviouslyLinkedCursorThatGotReused() throws Exception {
        Path file = file("a");
        generateFileWithRecords(
                file, recordsPerFilePage * 2, recordSize, recordsPerFilePage, reservedBytes, filePageSize);
        getPageCache(fs, maxPages, PageCacheTracer.NULL);
        try (PagedFile pf = map(file, filePageSize)) {
            PageCursor a = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
            a.openLinkedCursor(0).close();
            PageCursor x = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
            a.close();
            assertTrue(x.next(1));
            x.close();
        }
    }

    @FunctionalInterface
    private interface PageCursorAction {
        void apply(PageCursor cursor);
    }

    @Test
    void getByteBeyondPageEndMustThrow() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> verifyPageBounds(PageCursor::getByte));
    }

    @Test
    void putByteBeyondPageEndMustThrow() {
        assertTimeoutPreemptively(
                ofMillis(SHORT_TIMEOUT_MILLIS), () -> verifyPageBounds(cursor -> cursor.putByte((byte) 42)));
    }

    @Test
    void getShortBeyondPageEndMustThrow() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> verifyPageBounds(PageCursor::getShort));
    }

    @Test
    void putShortBeyondPageEndMustThrow() {
        assertTimeoutPreemptively(
                ofMillis(SHORT_TIMEOUT_MILLIS), () -> verifyPageBounds(cursor -> cursor.putShort((short) 42)));
    }

    @Test
    void getIntBeyondPageEndMustThrow() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> verifyPageBounds(PageCursor::getInt));
    }

    @Test
    void putIntBeyondPageEndMustThrow() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> verifyPageBounds(cursor -> cursor.putInt(42)));
    }

    @Test
    void putLongBeyondPageEndMustThrow() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> verifyPageBounds(cursor -> cursor.putLong(42)));
    }

    @Test
    void getLongBeyondPageEndMustThrow() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> verifyPageBounds(PageCursor::getLong));
    }

    @Test
    void putBytesBeyondPageEndMustThrow() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            final byte[] bytes = new byte[] {1, 2, 3};
            verifyPageBounds(cursor -> cursor.putBytes(bytes));
        });
    }

    @Test
    void putBytesRepeatedByteBeyondPageEndMustThrow() {
        assertTimeoutPreemptively(
                ofMillis(SHORT_TIMEOUT_MILLIS), () -> verifyPageBounds(cursor -> cursor.putBytes(3, (byte) 1)));
    }

    @Test
    void getBytesBeyondPageEndMustThrow() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            final byte[] bytes = new byte[3];
            verifyPageBounds(cursor -> cursor.getBytes(bytes));
        });
    }

    @Test
    void putBytesWithOffsetAndLengthBeyondPageEndMustThrow() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            final byte[] bytes = new byte[] {1, 2, 3};
            verifyPageBounds(cursor -> cursor.putBytes(bytes, 1, 1));
        });
    }

    @Test
    void getBytesWithOffsetAndLengthBeyondPageEndMustThrow() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            final byte[] bytes = new byte[3];
            verifyPageBounds(cursor -> cursor.getBytes(bytes, 1, 1));
        });
    }

    private void verifyPageBounds(PageCursorAction action) throws IOException {
        configureStandardPageCache();

        generateFileWithRecords(file("a"), 1, recordSize, recordsPerFilePage, reservedBytes, filePageSize);

        try (PagedFile pagedFile = map(file("a"), filePageSize);
                PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            cursor.next();
            assertThrows(IndexOutOfBoundsException.class, () -> {
                for (int i = 0; i < 100000; i++) {
                    action.apply(cursor);
                    if (cursor.checkAndClearBoundsFlag()) {
                        throw new IndexOutOfBoundsException();
                    }
                }
            });
        }
    }

    @Test
    void shouldRetryMustClearBoundsFlagWhenReturningTrue() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            try (PagedFile pf = map(file("a"), filePageSize);
                    PageCursor reader = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                PageCursor writer = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
                assertTrue(writer.next());

                assertTrue(reader.next());
                reader.getByte(-1); // out-of-bounds flag now raised
                writer.close(); // reader overlapped with writer, so must retry
                assertTrue(reader.shouldRetry());

                // shouldRetry returned 'true', so it must clear the out-of-bounds flag
                assertFalse(reader.checkAndClearBoundsFlag());
            }
        });
    }

    @Test
    void shouldRetryMustNotClearBoundsFlagWhenReturningFalse() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            try (PagedFile pf = map(file("a"), filePageSize);
                    PageCursor reader = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                PageCursor writer = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
                assertTrue(writer.next());
                writer.close(); // writer closed before reader comes to this page, so no need for retry

                assertTrue(reader.next());
                reader.getByte(-1); // out-of-bounds flag now raised
                assertFalse(reader.shouldRetry());

                // shouldRetry returned 'true', so it must clear the out-of-bounds flag
                assertTrue(reader.checkAndClearBoundsFlag());
            }
        });
    }

    @Test
    void nextThatReturnsTrueMustNotClearBoundsFlagOnReadCursor() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            try (PagedFile pf = map(file("a"), filePageSize);
                    PageCursor reader = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                PageCursor writer = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
                assertTrue(writer.next());

                assertTrue(reader.next());
                reader.getByte(-1); // out-of-bounds flag now raised
                writer.next(); // make sure there's a next page for the reader to move to
                writer.close(); // reader overlapped with writer, so must retry
                assertTrue(reader.next());

                assertTrue(reader.checkAndClearBoundsFlag());
            }
        });
    }

    @Test
    void nextThatReturnsTrueMustNotClearBoundsFlagOnWriteCursor() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            try (PagedFile pf = map(file("a"), filePageSize);
                    PageCursor writer = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(writer.next());
                writer.getByte(-1); // out-of-bounds flag now raised
                assertTrue(writer.next());

                assertTrue(writer.checkAndClearBoundsFlag());
            }
        });
    }

    @Test
    void nextThatReturnsFalseMustNotClearBoundsFlagOnReadCursor() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            try (PagedFile pf = map(file("a"), filePageSize);
                    PageCursor reader = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                PageCursor writer = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
                assertTrue(writer.next());

                assertTrue(reader.next());
                reader.getByte(-1); // out-of-bounds flag now raised
                // don't call next of the writer, so there won't be a page for the reader to move onto
                writer.close(); // reader overlapped with writer, so must retry
                assertFalse(reader.next());

                assertTrue(reader.checkAndClearBoundsFlag());
            }
        });
    }

    @Test
    void nextThatReturnsFalseMustNotClearBoundsFlagOnWriteCursor() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            Path file = file("a");
            generateFileWithRecords(
                    file, recordsPerFilePage, recordSize, recordsPerFilePage, reservedBytes, filePageSize);

            try (PagedFile pf = map(file, filePageSize);
                    PageCursor writer = pf.io(0, PF_SHARED_WRITE_LOCK | PF_NO_GROW, NULL_CONTEXT)) {
                assertTrue(writer.next());
                writer.getByte(-1); // out-of-bounds flag now raised
                assertFalse(writer.next());

                assertTrue(writer.checkAndClearBoundsFlag());
            }
        });
    }

    @Test
    void nextWithPageIdThatReturnsTrueMustNotClearBoundsFlagOnReadCursor() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            try (PagedFile pf = map(file("a"), filePageSize);
                    PageCursor reader = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                PageCursor writer = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
                assertTrue(writer.next());

                assertTrue(reader.next());
                reader.getByte(-1); // out-of-bounds flag now raised
                writer.next(3); // make sure there's a next page for the reader to move to
                writer.close(); // reader overlapped with writer, so must retry
                assertTrue(reader.next(3));

                assertTrue(reader.checkAndClearBoundsFlag());
            }
        });
    }

    @Test
    void nextWithPageIdMustNotClearBoundsFlagOnWriteCursor() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            try (PagedFile pf = map(file("a"), filePageSize);
                    PageCursor writer = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(writer.next());
                writer.getByte(-1); // out-of-bounds flag now raised
                assertTrue(writer.next(3));

                assertTrue(writer.checkAndClearBoundsFlag());
            }
        });
    }

    @Test
    void settingOutOfBoundsCursorOffsetMustRaiseBoundsFlag() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            generateFileWithRecords(file("a"), 1, recordSize, recordsPerFilePage, reservedBytes, filePageSize);
            try (PagedFile pagedFile = map(file("a"), filePageSize);
                    PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                cursor.setOffset(-1);
                assertTrue(cursor.checkAndClearBoundsFlag());
                assertFalse(cursor.checkAndClearBoundsFlag());

                cursor.setOffset(filePageSize + 1);
                assertTrue(cursor.checkAndClearBoundsFlag());
                assertFalse(cursor.checkAndClearBoundsFlag());

                cursor.setOffset(pageCachePageSize + 1);
                assertTrue(cursor.checkAndClearBoundsFlag());
                assertFalse(cursor.checkAndClearBoundsFlag());
            }
        });
    }

    @Test
    void pageFaultForWriteMustThrowIfOutOfStorageSpace() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            final AtomicInteger writeCounter = new AtomicInteger();
            AtomicBoolean restrictWrites = new AtomicBoolean(true);
            FileSystemAbstraction fs = new DelegatingFileSystemAbstraction(this.fs) {
                private final List<StoreChannel> channels = new CopyOnWriteArrayList<>();

                @Override
                public StoreChannel open(Path fileName, Set<OpenOption> options) throws IOException {
                    StoreChannel channel = new DelegatingStoreChannel(super.open(fileName, options)) {
                        @Override
                        public void writeAll(ByteBuffer src, long position) throws IOException {
                            if (restrictWrites.get() && writeCounter.incrementAndGet() > 10) {
                                throw new IOException("No space left on device");
                            }
                            super.writeAll(src, position);
                        }
                    };
                    channels.add(channel);
                    return channel;
                }

                @Override
                public void close() throws IOException {
                    IOUtils.closeAll(channels);
                    super.close();
                }
            };

            fs.write(file("a")).close();

            getPageCache(fs, maxPages, PageCacheTracer.NULL);
            PagedFile pagedFile = map(file("a"), filePageSize);

            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertThrows(IOException.class, () -> {
                    //noinspection StatementWithEmptyBody
                    while (cursor.next()) {
                        // Profound and interesting I/O.
                    }
                });
            } finally {
                restrictWrites.set(false);
                pagedFile.close();
                pageCache.close();
                fs.close();
            }
        });
    }

    @Test
    void pageFaultForReadMustThrowIfOutOfStorageSpace() {
        try {
            assertTimeoutPreemptively(ofMillis(SEMI_LONG_TIMEOUT_MILLIS), () -> {
                final AtomicInteger writeCounter = new AtomicInteger();
                AtomicBoolean restrictWrites = new AtomicBoolean(true);
                FileSystemAbstraction fs = new DelegatingFileSystemAbstraction(this.fs) {
                    private final List<StoreChannel> channels = new CopyOnWriteArrayList<>();

                    @Override
                    public StoreChannel open(Path fileName, Set<OpenOption> options) throws IOException {
                        StoreChannel channel = new DelegatingStoreChannel(super.open(fileName, options)) {
                            @Override
                            public void writeAll(ByteBuffer src, long position) throws IOException {
                                if (restrictWrites.get() && writeCounter.incrementAndGet() >= 1) {
                                    throw new IOException("No space left on device");
                                }
                                super.writeAll(src, position);
                            }
                        };
                        channels.add(channel);
                        return channel;
                    }

                    @Override
                    public void close() throws IOException {
                        IOUtils.closeAll(channels);
                        super.close();
                    }
                };

                getPageCache(fs, maxPages, PageCacheTracer.NULL);
                generateFileWithRecords(
                        file("a"), recordCount, recordSize, recordsPerFilePage, reservedBytes, filePageSize);
                PagedFile pagedFile = map(file("a"), filePageSize);

                // Create 1 dirty page
                try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                    assertTrue(cursor.next());
                }

                // Read pages until the dirty page gets flushed
                try (PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                    //noinspection InfiniteLoopStatement
                    for (; ; ) {
                        //noinspection StatementWithEmptyBody
                        while (cursor.next()) {
                            // Profound and interesting I/O.
                        }
                        // Rewind to the start of the file if we get to the end, because it is non-
                        // deterministic which pages get evicted and when.
                        cursor.next(0);
                    }
                } finally {
                    restrictWrites.set(false);
                    pagedFile.close();
                    pageCache.close();
                    fs.close();
                }
            });
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IOException.class);
        }
    }

    @Test
    void mustRecoverViaFileCloseFromFullDriveWhenMoreStorageBecomesAvailable() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            final AtomicBoolean hasSpace = new AtomicBoolean();
            FileSystemAbstraction fs = new DelegatingFileSystemAbstraction(this.fs) {
                @Override
                public StoreChannel open(Path fileName, Set<OpenOption> options) throws IOException {
                    return new DelegatingStoreChannel(super.open(fileName, options)) {
                        @Override
                        public void writeAll(ByteBuffer src, long position) throws IOException {
                            if (!hasSpace.get()) {
                                throw new IOException("No space left on device");
                            }
                            super.writeAll(src, position);
                        }
                    };
                }
            };

            fs.write(file("a")).close();

            getPageCache(fs, maxPages, PageCacheTracer.NULL);
            PagedFile pagedFile = map(file("a"), filePageSize);

            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                //noinspection InfiniteLoopStatement
                for (; ; ) // Keep writing until we get an exception! (when the cache starts evicting stuff)
                {
                    assertTrue(cursor.next());
                    writeRecords(cursor);
                }
            } catch (IOException ignore) {
                // We're out of space! Salty tears...
            }

            // Fix the situation:
            hasSpace.set(true);

            // Closing the last reference of a paged file implies a flush, and it mustn't throw:
            pagedFile.close();

            try (PagedFile pf = map(file("a"), filePageSize);
                    PageCursor cursor = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next()); // this should not throw
            }
        });
    }

    @Test
    void mustRecoverViaFileFlushFromFullDriveWhenMoreStorageBecomesAvailable() throws Exception {

        final AtomicBoolean hasSpace = new AtomicBoolean();
        final AtomicBoolean hasThrown = new AtomicBoolean();
        FileSystemAbstraction fs = new DelegatingFileSystemAbstraction(this.fs) {
            @Override
            public StoreChannel open(Path fileName, Set<OpenOption> options) throws IOException {
                return new DelegatingStoreChannel(super.open(fileName, options)) {
                    @Override
                    public void writeAll(ByteBuffer src, long position) throws IOException {
                        if (!hasSpace.get()) {
                            hasThrown.set(true);
                            throw new IOException("No space left on device");
                        }
                        super.writeAll(src, position);
                    }
                };
            }
        };

        fs.write(file("a")).close();

        getPageCache(fs, maxPages, PageCacheTracer.NULL);
        PagedFile pagedFile = map(file("a"), filePageSize);

        try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            while (!hasThrown.get()) // Keep writing until we get an exception! (when the cache starts evicting stuff)
            {
                assertTrue(cursor.next());
                writeRecords(cursor);
            }
        } catch (IOException ignore) {
            // We're out of space! Salty tears...
        }

        // Fix the situation:
        hasSpace.set(true);

        // Flushing the paged file implies the eviction exception gets cleared, and mustn't itself throw:
        pagedFile.flushAndForce(FileFlushEvent.NULL);

        try (PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
            assertTrue(cursor.next()); // this should not throw
        }
        pagedFile.close();
    }

    @Test
    void dataFromDifferentFilesMustNotBleedIntoEachOther() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            // The idea with this test is, that the pages for fileA are larger than
            // the pages for fileB, so we can put A-data beyond the end of the B
            // file pages.
            // Furthermore, our writes to the B-pages do not overwrite the entire page.
            // In those cases, the bytes not written to must be zeros.

            configureStandardPageCache();
            Path fileB = existingFile("b");
            int filePageSizeA = pageCachePageSize - 2;
            int filePayloadA = filePageSizeA - reservedBytes;
            int filePageSizeB = pageCachePageSize - 6;
            int filePayloadB = filePageSizeB - reservedBytes;
            int pagesToWriteA = 100;
            int pagesToWriteB = 3;

            PagedFile pagedFileA = map(existingFile("a"), filePageSizeA);

            try (PageCursor cursor = pagedFileA.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                for (int i = 0; i < pagesToWriteA; i++) {
                    assertTrue(cursor.next());
                    for (int j = 0; j < filePayloadA; j++) {
                        cursor.putByte((byte) 42);
                    }
                }
            }

            PagedFile pagedFileB = map(fileB, filePageSizeB);

            try (PageCursor cursor = pagedFileB.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                for (int i = 0; i < pagesToWriteB; i++) {
                    assertTrue(cursor.next());
                    cursor.putByte((byte) 63);
                }
            }

            pagedFileA.close();
            pagedFileB.close();

            try (InputStream inputStream = fs.openAsInputStream(fileB)) {
                readPage(inputStream, "first page first byte", filePayloadB, "page 0 byte pos ");
                readPage(inputStream, "second page first byte", filePayloadB, "page 1 byte pos ");
                readPage(inputStream, "third page first byte", filePayloadB, "page 2 byte pos ");

                assertThat(inputStream.read()).as("expect EOF").isEqualTo(-1);
            }
        });
    }

    private void readPage(InputStream inputStream, String pageDescription, int payload, String pageByteDescription)
            throws IOException {
        inputStream.readNBytes(reservedBytes);
        assertThat(inputStream.read()).as(pageDescription).isEqualTo(63);
        for (int i = 0; i < payload - 1; i++) {
            assertThat(inputStream.read()).as(pageByteDescription + i).isEqualTo(0);
        }
    }

    @Test
    void freshlyCreatedPagesMustContainAllZeros() {
        assertTimeoutPreemptively(ofMillis(SEMI_LONG_TIMEOUT_MILLIS), () -> {
            ThreadLocalRandom rng = ThreadLocalRandom.current();

            configureStandardPageCache();

            try (PagedFile pagedFile = map(existingFile("a"), filePageSize);
                    PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                for (int i = 0; i < 100; i++) {
                    assertTrue(cursor.next());
                    for (int j = 0; j < filePayloadSize; j++) {
                        cursor.putByte((byte) rng.nextInt());
                    }
                }
            }
            pageCache.close();
            pageCache = null;

            configureStandardPageCache();

            try (PagedFile pagedFile = map(existingFile("b"), filePageSize);
                    PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                for (int i = 0; i < 100; i++) {
                    assertTrue(cursor.next());
                    for (int j = 0; j < filePayloadSize; j++) {
                        assertThat(cursor.getByte()).isEqualTo((byte) 0);
                    }
                }
            }
        });
    }

    @Test
    void optimisticReadLockMustFaultOnRetryIfPageHasBeenEvicted() {
        assertTimeoutPreemptively(ofMillis(SEMI_LONG_TIMEOUT_MILLIS), () -> {
            final byte a = 'a';
            final byte b = 'b';
            final Path fileA = existingFile("a");
            final Path fileB = existingFile("b");

            configureStandardPageCache();

            final PagedFile pagedFileA = map(fileA, filePageSize);
            final PagedFile pagedFileB = map(fileB, filePageSize);

            // Fill fileA with some predicable data
            try (PageCursor cursor = pagedFileA.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                for (int i = 0; i < maxPages; i++) {
                    assertTrue(cursor.next());
                    for (int j = 0; j < filePayloadSize; j++) {
                        cursor.putByte(a);
                    }
                }
            }

            Runnable fillPagedFileB = () -> {
                try (PageCursor cursor = pagedFileB.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                    for (int i = 0; i < maxPages * 30; i++) {
                        assertTrue(cursor.next());
                        for (int j = 0; j < filePayloadSize; j++) {
                            cursor.putByte(b);
                        }
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            };

            try (PageCursor cursor = pagedFileA.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                // First, make sure page 0 is in the cache:
                assertTrue(cursor.next(0));
                // If we took a page fault, we'd have a pessimistic lock on page 0.
                // Move to the next page to release that lock:
                assertTrue(cursor.next());
                // Now go back to page 0. It's still in the cache, so we should get
                // an optimistic lock, if that's available:
                assertTrue(cursor.next(0));

                // Verify the page is all 'a's:
                for (int i = 0; i < filePayloadSize; i++) {
                    assertThat(cursor.getByte()).isEqualTo(a);
                }

                // Now fill file B with 'b's... this will cause our current page to be evicted
                fork(fillPagedFileB).join();
                // So if we had an optimistic lock, we should be asked to retry:
                if (cursor.shouldRetry()) {
                    // When we do reads after the shouldRetry() call, we should fault our page back
                    // and get consistent reads (assuming we don't race any further with eviction)
                    int expected = a * filePayloadSize;
                    int actual;
                    do {
                        actual = 0;
                        for (int i = 0; i < filePayloadSize; i++) {
                            actual += cursor.getByte();
                        }
                    } while (cursor.shouldRetry());
                    assertThat(actual).isEqualTo(expected);
                }
            }

            pagedFileA.close();
            pagedFileB.close();
        });
    }

    @Test
    void pagesMustReturnToFreelistIfSwapInThrows() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();

            generateFileWithRecords(
                    file("a"), recordCount, recordSize, recordsPerFilePage, reservedBytes, filePageSize);
            PagedFile pagedFile = map(file("a"), filePageSize);

            int iterations = maxPages * 2;
            accessPagesWhileInterrupted(pagedFile, PF_SHARED_READ_LOCK, iterations);
            accessPagesWhileInterrupted(pagedFile, PF_SHARED_WRITE_LOCK, iterations);

            // Verify that after all those troubles, page faulting starts working again
            // as soon as our thread is no longer interrupted and the PageSwapper no
            // longer throws.
            Thread.interrupted(); // make sure to clear our interruption status

            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                verifyRecordsMatchExpected(cursor);
            }
            pagedFile.close();
        });
    }

    private static void accessPagesWhileInterrupted(PagedFile pagedFile, int pf_flags, int iterations)
            throws IOException {
        try (PageCursor cursor = pagedFile.io(0, pf_flags, NULL_CONTEXT)) {
            for (int i = 0; i < iterations; i++) {
                Thread.currentThread().interrupt();
                try {
                    cursor.next(0);
                } catch (IOException ignored) {
                    // We don't care about the exception per se.
                    // We just want lots of failed page faults.
                }
            }
        }
    }

    // NOTE: This test is CPU architecture dependent, but it should fail on no
    // architecture that we support.
    // This test has no timeout because one may want to run it on a CPU
    // emulator, where it's not unthinkable for it to take minutes.
    @Test
    void mustSupportUnalignedWordAccesses() throws Exception {
        getPageCache(fs, 5, PageCacheTracer.NULL);
        int pageSize = pageCache.pageSize();

        ThreadLocalRandom rng = ThreadLocalRandom.current();

        try (PagedFile pagedFile = map(file("a"), pageSize);
                PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            assertTrue(cursor.next());

            long x = rng.nextLong();
            int limit = filePayloadSize - Long.BYTES;
            for (int i = 0; i < limit; i++) {
                x += i;
                cursor.setOffset(i);
                cursor.putLong(x);
                cursor.setOffset(i);
                long y = cursor.getLong();

                assertFalse(cursor.checkAndClearBoundsFlag(), "Should not have had a page out-of-bounds access!");
                if (x != y) {
                    String reason = "Failed to read back the value that was written at " + "offset " + toHexString(i);
                    assertThat(toHexString(y)).as(reason).isEqualTo(toHexString(x));
                }
            }
        }
    }

    @RepeatedTest(50)
    void mustEvictPagesFromUnmappedFiles() {
        assertTimeoutPreemptively(ofMillis(SEMI_LONG_TIMEOUT_MILLIS), () -> {
            // GIVEN mapping then unmapping
            configureStandardPageCache();
            try (PagedFile pagedFile = map(file("a"), filePageSize);
                    PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
            }

            // WHEN using all pages, so that eviction of some pages will happen
            try (PagedFile pagedFile = map(file("a"), filePageSize);
                    PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                for (int i = 0; i < maxPages + 5; i++) {
                    // THEN eviction happening here should not result in any exception
                    assertTrue(cursor.next());
                }
            }
        });
    }

    @Test
    void mustEvictPagesFromUnmappedFilesWithFewUsages() {
        assertTimeoutPreemptively(ofMillis(SEMI_LONG_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            try (PagedFile pagedFile = map(file("a"), filePageSize);
                    PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next(1));
            }

            try (PagedFile pagedFile = map(file("a"), filePageSize);
                    PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                // touch pages few times to increase their usage
                cursor.next(0);
                cursor.next(1);
                cursor.next(0);
                cursor.next(1);
                cursor.next(0);
                cursor.next(1);
            }
            // all pages must be free in page cache
            assertThat(pageCache.freePages()).isEqualTo(pageCache.maxCachedPages());
        });
    }

    @Test
    void mustEvictPagesFromUnmappedFilesWithHangingReadLock() {
        assertTimeoutPreemptively(ofMillis(SEMI_LONG_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            try (PagedFile pagedFile = map(file("a"), filePageSize);
                    PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next(1));
            }

            PagedFile pagedFile = map(file("a"), filePageSize);
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                // touch pages few times to increase their usage
                cursor.next(0);
                cursor.next(1);
                cursor.next(0);
                cursor.next(1);
                cursor.next(0);
                cursor.next(1);

                pagedFile.close();
                // all pages must be free in page cache
                assertThat(pageCache.freePages()).isEqualTo(pageCache.maxCachedPages());
            }
        });
    }

    @Test
    void mustReadZerosFromBeyondEndOfFile() {
        assertTimeoutPreemptively(ofMillis(SEMI_LONG_TIMEOUT_MILLIS), () -> {
            StandardRecordFormat recordFormat = new StandardRecordFormat();
            Path[] files = {
                file("1"), file("2"), file("3"), file("4"), file("5"), file("6"), file("7"), file("8"), file("9"),
                file("0"), file("A"), file("B"),
            };
            for (int fileId = 0; fileId < files.length; fileId++) {
                Path file = files[fileId];
                StoreChannel channel = fs.write(file);
                channel.position(reservedBytes);
                for (int recordId = 0; recordId < fileId + 1; recordId++) {
                    Record record =
                            recordFormat.createRecord(file, recordId, (int) (channel.position() / PAGE_SIZE), (int)
                                    (channel.position() % PAGE_SIZE));
                    recordFormat.writeRecord(record, channel);
                }
                channel.close();
            }

            getPageCache(fs, 2, PageCacheTracer.NULL);
            int pageSize = pageCache.pageSize();

            int fileId = files.length;
            while (fileId-- > 0) {
                Path file = files[fileId];
                try (PagedFile pf = map(file, pageSize);
                        PageCursor cursor = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                    int pageCount = 0;
                    while (cursor.next()) {
                        pageCount++;
                        recordFormat.assertRecordsWrittenCorrectly(cursor);
                    }
                    assertThat(pageCount).as("pages in file " + file).isGreaterThan(0);
                }
            }
        });
    }

    @Test
    void mustThrowWhenMappingNonExistingFile() {
        assertThrows(NoSuchFileException.class, () -> {
            configureStandardPageCache();
            map(file("does not exist"), filePageSize);
        });
    }

    @Test
    void mustCreateNonExistingFileWithCreateOption() {
        assertTimeoutPreemptively(ofMillis(SEMI_LONG_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            try (PagedFile pf = map(file("does not exist"), filePageSize, immutable.of(StandardOpenOption.CREATE));
                    PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
            }
        });
    }

    @Test
    void mustIgnoreCreateOptionIfFileAlreadyExists() {
        assertTimeoutPreemptively(ofMillis(SEMI_LONG_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            try (PagedFile pf = map(file("a"), filePageSize, immutable.of(StandardOpenOption.CREATE));
                    PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
            }
        });
    }

    @Test
    void mustIgnoreCertainOpenOptions() {
        assertTimeoutPreemptively(ofMillis(SEMI_LONG_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            try (PagedFile pf = map(
                            file("a"),
                            filePageSize,
                            immutable.of(
                                    StandardOpenOption.READ,
                                    StandardOpenOption.WRITE,
                                    StandardOpenOption.APPEND,
                                    StandardOpenOption.SPARSE));
                    PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
            }
        });
    }

    @Test
    void mustThrowOnUnsupportedOpenOptions() {
        assertTimeoutPreemptively(ofMillis(SEMI_LONG_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            verifyMappingWithOpenOptionThrows(StandardOpenOption.CREATE_NEW);
            verifyMappingWithOpenOptionThrows(StandardOpenOption.SYNC);
            verifyMappingWithOpenOptionThrows(StandardOpenOption.DSYNC);
            verifyMappingWithOpenOptionThrows(new OpenOption() {
                @Override
                public String toString() {
                    return "NonStandardOpenOption";
                }
            });
        });
    }

    private void verifyMappingWithOpenOptionThrows(OpenOption option) throws IOException {
        try {
            map(file("a"), filePageSize, immutable.of(option)).close();
            fail("Expected map() to throw when given the OpenOption " + option);
        } catch (IllegalArgumentException | UnsupportedOperationException e) {
            // good
        }
    }

    @Test
    void mappingFileWithTruncateOptionMustTruncateFile() {
        assertTimeoutPreemptively(ofMillis(SEMI_LONG_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            try (PagedFile pf = map(file("a"), filePageSize);
                    PageCursor cursor = pf.io(10, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertThat(pf.getLastPageId()).isLessThan(0L);
                assertTrue(cursor.next());
                cursor.putInt(0xcafebabe);
            }
            try (PagedFile pf = map(file("a"), filePageSize, immutable.of(TRUNCATE_EXISTING));
                    PageCursor cursor = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                assertThat(pf.getLastPageId()).isLessThan(0L);
                assertFalse(cursor.next());
            }
        });
    }

    @Test
    void truncatePageFileTruncatesRealFile() {
        assertTimeoutPreemptively(ofMillis(SEMI_LONG_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            Path file = file("a");
            try (PagedFile pf = map(file, filePageSize)) {
                try (PageCursor cursor = pf.io(20, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                    assertTrue(cursor.next());
                }
                pf.flushAndForce(FileFlushEvent.NULL);
                assertEquals(21L * filePageSize, fs.getFileSize(file));

                pf.truncate(10, FileTruncateEvent.NULL);
                assertEquals(10L * filePageSize, fs.getFileSize(file));
            }
        });
    }

    @Test
    void truncatePageFileTraceEvents() {
        assertTimeoutPreemptively(ofMillis(SEMI_LONG_TIMEOUT_MILLIS), () -> {
            var cacheTracer = new DefaultPageCacheTracer();
            var storageTracer = new DefaultVersionStorageTracer(cacheTracer);
            getPageCache(fs, maxPages, cacheTracer);
            Path file = file("a");
            try (PagedFile pf = map(file, filePageSize)) {
                try (PageCursor cursor = pf.io(25, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                    assertTrue(cursor.next());
                }
                pf.flushAndForce(FileFlushEvent.NULL);

                try (RegionCollectionEvent collectionEvent = storageTracer.beginRegionCollection()) {
                    try (var truncateEvent = collectionEvent.attemptTruncate()) {
                        pf.truncate(22, truncateEvent);
                    }
                    try (var truncateEvent = collectionEvent.attemptTruncate()) {
                        pf.truncate(21, truncateEvent);
                    }
                    try (var truncateEvent = collectionEvent.attemptTruncate()) {
                        pf.truncate(20, truncateEvent);
                    }
                    try (var truncateEvent = collectionEvent.attemptTruncate()) {
                        pf.truncate(19, truncateEvent);
                    }
                    try (var truncateEvent = collectionEvent.attemptTruncate()) {
                        pf.truncate(17, truncateEvent);
                    }
                    try (var truncateEvent = collectionEvent.attemptTruncate()) {
                        pf.truncate(15, truncateEvent);
                    }
                }

                assertEquals(6, cacheTracer.filesTruncated());
                assertEquals(11L * filePageSize, cacheTracer.bytesTruncated());
            }
        });
    }

    @Test
    void truncateSetCorrectLastPageId() {
        assertTimeoutPreemptively(ofMillis(SEMI_LONG_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            Path file = file("a");
            try (PagedFile pf = map(file, filePageSize)) {
                try (PageCursor cursor = pf.io(2, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                    assertTrue(cursor.next());
                }
                pf.flushAndForce(FileFlushEvent.NULL);
                assertEquals(2, pf.getLastPageId());

                pf.truncate(1, FileTruncateEvent.NULL);
                assertEquals(0, pf.getLastPageId());
            }
        });
    }

    @Test
    void truncateEmptyFile() {
        assertTimeoutPreemptively(ofMillis(SEMI_LONG_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            Path file = file("a");
            try (PagedFile pf = map(file, filePageSize)) {
                assertThat(pf.getLastPageId()).isNegative();

                pf.truncate(0, FileTruncateEvent.NULL);
                assertThat(pf.getLastPageId()).isNegative();
            }
        });
    }

    @Test
    void truncateNonEmptyFileToEmpty() {
        assertTimeoutPreemptively(ofMillis(SEMI_LONG_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            Path file = file("a");
            try (PagedFile pf = map(file, filePageSize)) {
                try (PageCursor cursor = pf.io(2, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                    assertTrue(cursor.next());
                }
                pf.flushAndForce(FileFlushEvent.NULL);
                assertEquals(2, pf.getLastPageId());

                pf.truncate(0, FileTruncateEvent.NULL);
                assertThat(pf.getLastPageId()).isNegative();

                try (PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                    assertTrue(cursor.next());
                }
                assertThat(pf.getLastPageId()).isZero();
            }
        });
    }

    @Test
    void truncateWithOngoingEviction() {
        assertTimeoutPreemptively(ofMillis(SEMI_LONG_TIMEOUT_MILLIS), () -> {
            var cacheTracer = new DefaultPageCacheTracer();
            long totalFilePages = 1024;
            int pagesToKeep = 15;
            getPageCache(fs, 100, cacheTracer);
            Path file = file("a");
            try (PagedFile pf = map(file, filePageSize)) {
                for (int iteration = 0; iteration < 1024; iteration++) {
                    try (PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                        for (int i = 0; i < totalFilePages; i++) {
                            assertTrue(cursor.next());
                            cursor.putLong(i);
                        }
                    }

                    pf.truncate(pagesToKeep, FileTruncateEvent.NULL);
                }

                assertEquals(pagesToKeep - 1, pf.getLastPageId());
                assertEquals((long) pagesToKeep * filePageSize, fs.getFileSize(file));
            }
        });
    }

    @Test
    void truncateAndGrowFile() {
        assertTimeoutPreemptively(ofMillis(SEMI_LONG_TIMEOUT_MILLIS), () -> {
            long totalFilePages = 600;
            // number of pages to keep matching flush vector size to make calculation easier
            long pagesToKeep = 128;
            var cacheTracer = new DefaultPageCacheTracer();
            var versionStorageTracer = new DefaultVersionStorageTracer(cacheTracer);
            getPageCache(fs, 1000, cacheTracer);
            Path file = file("a");
            try (PagedFile pf = map(file, filePageSize)) {
                for (int iteration = 0; iteration < 1024; iteration++) {
                    try (PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                        for (int i = 0; i < totalFilePages; i++) {
                            assertTrue(cursor.next());
                            cursor.putLong(i);
                        }
                    }

                    long beforeTruncation = cacheTracer.bytesTruncated();
                    long beforeWriter = cacheTracer.bytesWritten();

                    try (var regionCollection = versionStorageTracer.beginRegionCollection();
                            var truncateEvent = regionCollection.attemptTruncate()) {
                        pf.truncate(pagesToKeep, truncateEvent);
                    }
                    try (FileFlushEvent fileFlushEvent = cacheTracer.beginFileFlush()) {
                        pf.flushAndForce(fileFlushEvent);
                    }

                    long expectedTruncatedBytes = beforeTruncation + (totalFilePages - pagesToKeep) * filePageSize;
                    long expectedWrittenBytes = beforeWriter + pagesToKeep * filePageSize;
                    assertThat(cacheTracer.bytesTruncated()).isEqualTo(expectedTruncatedBytes);
                    assertThat(cacheTracer.bytesWritten()).isEqualTo(expectedWrittenBytes);
                }
            }
        });
    }

    @Test
    void truncateReflectedInLastPageId() {
        assertTimeoutPreemptively(ofMillis(SEMI_LONG_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            Path file = file("a");
            try (PagedFile pf = map(file, filePageSize)) {
                try (PageCursor cursor = pf.io(10, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                    assertTrue(cursor.next());
                }
                assertEquals(10, pf.getLastPageId());

                pf.truncate(4, FileTruncateEvent.NULL);
                assertEquals(3, pf.getLastPageId());
            }
        });
    }

    @Test
    void growRealPageFileAfterTruncation() {
        assertTimeoutPreemptively(ofMillis(SEMI_LONG_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            Path file = file("a");
            try (PagedFile pf = map(file, filePageSize)) {
                for (int i = 0; i < 21; i++) {
                    try (PageCursor cursor = pf.io(i, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                        assertTrue(cursor.next());
                        cursor.putLong(i);
                    }
                }
                pf.flushAndForce(FileFlushEvent.NULL);
                assertEquals(21L * filePageSize, fs.getFileSize(file));

                pf.truncate(7, FileTruncateEvent.NULL);
                assertEquals(7L * filePageSize, fs.getFileSize(file));

                for (int i = 7; i < 25; i++) {
                    try (PageCursor cursor = pf.io(i, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                        assertTrue(cursor.next());
                        cursor.putLong(i);
                    }
                }
                pf.flushAndForce(FileFlushEvent.NULL);
                assertEquals(25L * filePageSize, fs.getFileSize(file));
            }
        });
    }

    @Test
    void touchAllNonTruncatedPagesAfterTruncate() {
        assertTimeoutPreemptively(ofMillis(SEMI_LONG_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            Path file = file("a");
            try (PagedFile pf = map(file, filePageSize)) {
                for (int i = 0; i < 20; i++) {
                    try (PageCursor cursor = pf.io(i, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                        assertTrue(cursor.next());
                        cursor.putLong(i);
                    }
                }
                pf.flushAndForce(FileFlushEvent.NULL);

                pf.truncate(5, FileTruncateEvent.NULL);
                for (int i = 0; i < 4; i++) {
                    try (PageCursor cursor = pf.io(i, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                        assertTrue(cursor.next());
                        assertEquals(i, cursor.getLong());
                    }
                }
            }
        });
    }

    @Test
    void growPageFileAfterTruncation() {
        assertTimeoutPreemptively(ofMillis(SEMI_LONG_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            Path file = file("a");
            try (PagedFile pf = map(file, filePageSize)) {
                for (int i = 0; i < 20; i++) {
                    try (PageCursor cursor = pf.io(i, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                        assertTrue(cursor.next());
                        cursor.putLong(i);
                    }
                }
                pf.flushAndForce(FileFlushEvent.NULL);

                pf.truncate(7, FileTruncateEvent.NULL);

                for (int i = 7; i < 24; i++) {
                    try (PageCursor cursor = pf.io(i, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                        assertTrue(cursor.next());
                        cursor.putLong(i);
                    }
                }

                for (int i = 0; i < 24; i++) {
                    try (PageCursor cursor = pf.io(i, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                        assertTrue(cursor.next());
                        assertEquals(i, cursor.getLong());
                    }
                }
            }
        });
    }

    @SuppressWarnings("unused")
    @Test
    void mappingAlreadyMappedFileWithTruncateOptionMustThrow() throws Exception {
        configureStandardPageCache();
        try (PagedFile first = map(file("a"), filePageSize)) {
            assertThrows(UnsupportedOperationException.class, () -> {
                try (PagedFile second = map(file("a"), filePageSize, immutable.of(TRUNCATE_EXISTING))) {
                    // empty
                }
            });
        }
    }

    @Test
    void mustThrowIfFileIsClosedMoreThanItIsMapped() throws Exception {
        configureStandardPageCache();
        PagedFile pf = map(file("a"), filePageSize);
        pf.close();
        assertThrows(IllegalStateException.class, pf::close);
    }

    @Test
    void fileMappedWithDeleteOnCloseMustNotExistAfterUnmap() throws Exception {
        configureStandardPageCache();
        map(file("a"), filePageSize, immutable.of(DELETE_ON_CLOSE)).close();
        assertThrows(NoSuchFileException.class, () -> map(file("a"), filePageSize));
    }

    @Test
    void fileMappedWithDeleteOnCloseMustNotExistAfterLastUnmap() throws Exception {
        configureStandardPageCache();
        Path file = file("a");
        try (PagedFile ignore = map(file, filePageSize)) {
            map(file, filePageSize, immutable.of(DELETE_ON_CLOSE)).close();
        }
        assertThrows(NoSuchFileException.class, () -> map(file, filePageSize));
    }

    @Test
    void fileMappedWithDeleteOnCloseShouldNotFlushDirtyPagesOnClose() throws Exception {
        PageCacheTracer cacheTracer = PageCacheTracer.NULL;
        AtomicInteger flushCounter = new AtomicInteger();
        PageSwapperFactory swapperFactory = flushCountingPageSwapperFactory(fs, flushCounter, cacheTracer);
        Path file = file("a");
        try (PageCache cache = createPageCache(swapperFactory, maxPages, cacheTracer);
                PagedFile pf = cache.map(file, filePageSize, DEFAULT_DATABASE_NAME, immutable.of(DELETE_ON_CLOSE));
                PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            writeRecords(cursor);
            assertTrue(cursor.next());
        }
        assertThat(flushCounter.get()).isLessThan(recordCount / recordsPerFilePage);
    }

    @Test
    void mustFlushAllDirtyPagesWhenClosingPagedFileThatIsNotMappedWithDeleteOnClose() throws Exception {
        PageCacheTracer cacheTracer = PageCacheTracer.NULL;
        AtomicInteger flushCounter = new AtomicInteger();
        PageSwapperFactory swapperFactory = flushCountingPageSwapperFactory(fs, flushCounter, cacheTracer);
        Path file = file("a");
        try (PageCache cache = createPageCache(swapperFactory, maxPages, cacheTracer);
                PagedFile pf = cache.map(file, filePageSize, DEFAULT_DATABASE_NAME);
                PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            writeRecords(cursor);
            assertTrue(cursor.next());
        }
        assertThat(flushCounter.get()).isEqualTo(1);
    }

    private static SingleFilePageSwapperFactory flushCountingPageSwapperFactory(
            FileSystemAbstraction fs, AtomicInteger flushCounter, PageCacheTracer pageCacheTracer) {
        return new SingleFilePageSwapperFactory(fs, pageCacheTracer, EmptyMemoryTracker.INSTANCE) {
            @Override
            public PageSwapper createPageSwapper(
                    Path path,
                    int filePageSize,
                    PageEvictionCallback onEviction,
                    boolean createIfNotExist,
                    boolean useDirectIO,
                    IOController ioController,
                    EvictionBouncer evictionBouncer,
                    SwapperSet swappers)
                    throws IOException {
                PageSwapper swapper = super.createPageSwapper(
                        path,
                        filePageSize,
                        onEviction,
                        createIfNotExist,
                        useDirectIO,
                        ioController,
                        evictionBouncer,
                        swappers);
                return new DelegatingPageSwapper(swapper) {
                    @Override
                    public long write(long filePageId, long bufferAddress) throws IOException {
                        flushCounter.getAndIncrement();
                        return super.write(filePageId, bufferAddress);
                    }

                    @Override
                    public long write(
                            long startFilePageId,
                            long[] bufferAddresses,
                            int[] bufferLengths,
                            int length,
                            int totalAffectedPages)
                            throws IOException {
                        flushCounter.getAndAdd(totalAffectedPages);
                        return super.write(startFilePageId, bufferAddresses, bufferLengths, length, totalAffectedPages);
                    }
                };
            }
        };
    }

    @Test
    void fileMappedWithDeleteOnCloseMustNotLeakDirtyPages() {
        assertTimeoutPreemptively(ofMillis(SEMI_LONG_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            Path file = file("a");
            int iterations = 50;
            for (int i = 0; i < iterations; i++) {
                ensureExists(file);
                try (PagedFile pf = map(file, filePageSize, immutable.of(DELETE_ON_CLOSE));
                        PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                    writeRecords(cursor);
                    assertTrue(cursor.next());
                }
            }
        });
    }

    @Test
    void mustNotThrowWhenMappingFileWithDifferentFilePageSizeAndAnyPageSizeIsSpecified() throws Exception {
        configureStandardPageCache();
        try (PagedFile ignore = map(file("a"), filePageSize)) {
            map(file("a"), filePageSize + 1, immutable.of(PageCacheOpenOptions.ANY_PAGE_SIZE))
                    .close();
        }
    }

    @Test
    void mustCopyIntoSameSizedWritePageCursor() throws Exception {
        configureStandardPageCache();
        int bytes = 200;

        int pagePayload = 16;
        int pageSize = pagePayload + reservedBytes;

        // Put some data into the file
        try (PagedFile pf = map(file("a"), pageSize);
                PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            for (int i = 0; i < bytes; i++) {
                if ((i % pagePayload) == 0) {
                    assertTrue(cursor.next());
                }
                cursor.putByte((byte) i);
            }
        }

        // Then copy all the pages into another file, with a larger file page size
        try (PagedFile pfA = map(file("a"), pageSize);
                PagedFile pfB = map(existingFile("b"), pageSize);
                PageCursor cursorA = pfA.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT);
                PageCursor cursorB = pfB.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            while (cursorA.next()) {
                assertTrue(cursorB.next());
                int bytesCopied;
                do {
                    bytesCopied =
                            cursorA.copyTo(0, cursorB, 0, cursorA.getPagedFile().payloadSize());
                } while (cursorA.shouldRetry());
                assertThat(bytesCopied).isEqualTo(pagePayload);
            }
        }

        // Finally, verify the contents of file 'b'
        try (PagedFile pf = map(file("b"), pageSize);
                PageCursor cursor = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
            for (int i = 0; i < bytes; i++) {
                if ((i % pagePayload) == 0) {
                    assertTrue(cursor.next());
                }
                int offset = cursor.getOffset();
                byte b;
                do {
                    cursor.setOffset(offset);
                    b = cursor.getByte();
                } while (cursor.shouldRetry());
                assertThat(b).isEqualTo((byte) i);
            }
        }
    }

    @Test
    void mustCopyIntoLargerPageCursor() throws Exception {
        configureStandardPageCache();
        int smallPageSize = 16 + reservedBytes;
        int largePageSize = 17 + reservedBytes;
        try (PagedFile pfA = map(file("a"), smallPageSize);
                PagedFile pfB = map(existingFile("b"), largePageSize);
                PageCursor cursorA = pfA.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
                PageCursor cursorB = pfB.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            assertTrue(cursorA.next());
            for (int i = 0; i < smallPageSize; i++) {
                cursorA.putByte((byte) (i + 1));
            }
            assertTrue(cursorB.next());
            assertThat(cursorA.copyTo(0, cursorB, 0, smallPageSize)).isEqualTo(smallPageSize - reservedBytes);
            for (int i = 0; i < smallPageSize - reservedBytes; i++) {
                assertThat(cursorB.getByte()).isEqualTo((byte) (i + 1));
            }
            assertThat(cursorB.getByte()).isEqualTo((byte) 0);
        }
    }

    @Test
    void mustCopyIntoSmallerPageCursor() throws Exception {
        configureStandardPageCache();
        int smallPagePayload = 16;
        int largePagePayload = 17;
        int smallPageSize = smallPagePayload + reservedBytes;
        int largePageSize = largePagePayload + reservedBytes;
        try (PagedFile pfA = map(file("a"), largePageSize);
                PagedFile pfB = map(existingFile("b"), smallPageSize);
                PageCursor cursorA = pfA.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
                PageCursor cursorB = pfB.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            assertTrue(cursorA.next());
            for (int i = 0; i < largePagePayload; i++) {
                cursorA.putByte((byte) (i + 1));
            }
            assertTrue(cursorB.next());
            assertThat(cursorA.copyTo(0, cursorB, 0, largePagePayload)).isEqualTo(smallPagePayload);
            for (int i = 0; i < smallPagePayload; i++) {
                assertThat(cursorB.getByte()).isEqualTo((byte) (i + 1));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {8, 16, 32, 64, 17, 42, 37})
    void mustCopyOverlappingSourceAndDestination(int payload) throws Exception {
        configureStandardPageCache();

        verifySameCursorCopy(payload, 0, payload / 2);
        verifySameCursorCopy(payload, payload / 2, 0);
    }

    private void verifySameCursorCopy(int payload, int sourceOffset, int targetOffset) throws IOException {
        int pageSize = payload * 2 + reservedBytes;
        try (var pf = map(existingFile("a"), pageSize);
                var cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            assertTrue(cursor.next());
            for (int i = 0; i < payload; i++) {
                cursor.putByte(sourceOffset + i, (byte) (i + 1));
            }
            assertThat(cursor.copyTo(sourceOffset, cursor, targetOffset, payload))
                    .isEqualTo(payload);
            for (int i = 0; i < payload; i++) {
                assertThat(cursor.getByte(targetOffset + i)).isEqualTo((byte) (i + 1));
            }
        }
    }

    @Test
    void mustThrowOnCopyIntoReadPageCursor() throws Exception {
        configureStandardPageCache();
        int pageSize = 17 + reservedBytes;
        try (PagedFile pfA = map(file("a"), pageSize);
                PagedFile pfB = map(existingFile("b"), pageSize)) {
            // Create data
            try (PageCursor cursorA = pfA.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
                    PageCursor cursorB = pfB.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursorA.next());
                assertTrue(cursorB.next());
            }

            // Try copying
            try (PageCursor cursorA = pfA.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
                    PageCursor cursorB = pfB.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                assertTrue(cursorA.next());
                assertTrue(cursorB.next());
                assertThrows(IllegalArgumentException.class, () -> cursorA.copyTo(0, cursorB, 0, pageSize));
            }
        }
    }

    @Test
    void copyToPageCursorMustCheckBounds() throws Exception {
        configureStandardPageCache();
        int payloadSize = 16;
        int pageSize = payloadSize + reservedBytes;
        try (PagedFile pf = map(file("a"), pageSize);
                PageCursor cursorA = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT);
                PageCursor cursorB = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            assertTrue(cursorB.next());
            assertTrue(cursorB.next());
            assertTrue(cursorA.next());

            // source buffer underflow
            cursorA.copyTo(-1, cursorB, 0, 1);
            assertTrue(cursorA.checkAndClearBoundsFlag());
            assertFalse(cursorB.checkAndClearBoundsFlag());

            // target buffer underflow
            cursorA.copyTo(0, cursorB, -1, 1);
            assertTrue(cursorA.checkAndClearBoundsFlag());
            assertFalse(cursorB.checkAndClearBoundsFlag());

            // source buffer offset overflow
            cursorA.copyTo(payloadSize, cursorB, 0, 1);
            assertTrue(cursorA.checkAndClearBoundsFlag());
            assertFalse(cursorB.checkAndClearBoundsFlag());

            // target buffer offset overflow
            cursorA.copyTo(0, cursorB, payloadSize, 1);
            assertTrue(cursorA.checkAndClearBoundsFlag());
            assertFalse(cursorB.checkAndClearBoundsFlag());

            // source buffer length overflow
            assertThat(cursorA.copyTo(1, cursorB, 0, payloadSize)).isEqualTo(payloadSize - 1);
            assertFalse(cursorA.checkAndClearBoundsFlag());
            assertFalse(cursorB.checkAndClearBoundsFlag());

            // target buffer length overflow
            assertThat(cursorA.copyTo(0, cursorB, 1, payloadSize)).isEqualTo(payloadSize - 1);
            assertFalse(cursorA.checkAndClearBoundsFlag());
            assertFalse(cursorB.checkAndClearBoundsFlag());

            // zero length
            assertThat(cursorA.copyTo(0, cursorB, 1, 0)).isEqualTo(0);
            assertFalse(cursorA.checkAndClearBoundsFlag());
            assertFalse(cursorB.checkAndClearBoundsFlag());

            // negative length
            cursorA.copyTo(1, cursorB, 1, -1);
            assertTrue(cursorA.checkAndClearBoundsFlag());
            assertFalse(cursorB.checkAndClearBoundsFlag());
        }
    }

    @Test
    void copyPageChecksForWritableCursor() throws IOException {
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize);
                PageCursor reader = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT);
                PageCursor writer = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            assertTrue(writer.next());
            assertTrue(reader.next());

            assertThrows(IllegalArgumentException.class, () -> writer.copyPage(reader));
        }
    }

    @Test
    void copyPageChecksForDifferentCursorPageSizes() throws IOException {
        configureStandardPageCache();
        ensureExists(file("b"));
        try (PagedFile pfA = map(file("a"), filePageSize);
                PagedFile pfB = map(file("b"), filePageSize + 2);
                PageCursor reader = pfA.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT);
                PageCursor writerA = pfA.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
                PageCursor writerB = pfB.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            assertTrue(writerB.next());
            assertTrue(writerA.next());
            assertTrue(reader.next());

            assertThrows(IllegalArgumentException.class, () -> reader.copyPage(writerB));
            assertDoesNotThrow(() -> reader.copyPage(writerA));
        }
    }

    @Test
    void copyPageContent() throws IOException {
        configureStandardPageCache();
        ensureExists(file("b"));

        try (PagedFile pfA = map(file("a"), filePageSize);
                PagedFile pfB = map(file("b"), filePageSize);
                PageCursor writerB = pfB.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {

            try (PageCursor writerA = pfA.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(writerA.next());

                for (int i = 0; i < filePageSize; i++) {
                    writerA.putByte((byte) 5);
                }
            }

            try (PageCursor reader = pfA.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                assertTrue(reader.next());
                assertTrue(writerB.next());
                do {
                    reader.copyPage(writerB);
                } while (reader.shouldRetry());
            }

            for (int i = 0; i < filePageSize; i++) {
                assertEquals(5, writerB.getByte());
            }
        }
    }

    @Test
    void copyToHeapByteBufferFromReadPageCursorMustCheckBounds() throws Exception {
        configureStandardPageCache();
        ByteBuffer buffer = ByteBuffers.allocate(filePayloadSize, ByteOrder.LITTLE_ENDIAN, INSTANCE);
        Path file = file("a");
        generateFileWithRecords(file, recordsPerFilePage, recordSize, recordsPerFilePage, reservedBytes, filePageSize);
        try (PagedFile pf = map(file, filePageSize);
                PageCursor cursor = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
            assertTrue(cursor.next());
            verifyCopyToBufferBounds(cursor, buffer);
        }
    }

    @Test
    void copyToDirectByteBufferFromReadPageCursorMustCheckBounds() throws Exception {
        configureStandardPageCache();
        ByteBuffer buffer = allocateDirect(filePayloadSize, ByteOrder.LITTLE_ENDIAN, INSTANCE);
        try {
            Path file = file("a");
            generateFileWithRecords(
                    file, recordsPerFilePage, recordSize, recordsPerFilePage, reservedBytes, filePageSize);
            try (PagedFile pf = map(file, filePageSize);
                    PageCursor cursor = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                verifyCopyToBufferBounds(cursor, buffer);
            }
        } finally {
            releaseBuffer(buffer, INSTANCE);
        }
    }

    @Test
    void copyToHeapByteBufferFromWritePageCursorMustCheckBounds() throws Exception {
        configureStandardPageCache();
        ByteBuffer buffer = ByteBuffers.allocate(filePayloadSize, ByteOrder.LITTLE_ENDIAN, INSTANCE);
        Path file = file("a");
        generateFileWithRecords(file, recordsPerFilePage, recordSize, recordsPerFilePage, reservedBytes, filePageSize);
        try (PagedFile pf = map(file, filePageSize);
                PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            assertTrue(cursor.next());
            verifyCopyToBufferBounds(cursor, buffer);
        }
    }

    @Test
    void copyToDirectByteBufferFromWritePageCursorMustCheckBounds() throws Exception {
        configureStandardPageCache();
        ByteBuffer buffer = allocateDirect(filePayloadSize, ByteOrder.LITTLE_ENDIAN, INSTANCE);
        try {
            Path file = file("a");
            generateFileWithRecords(
                    file, recordsPerFilePage, recordSize, recordsPerFilePage, reservedBytes, filePageSize);
            try (PagedFile pf = map(file, filePageSize);
                    PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                verifyCopyToBufferBounds(cursor, buffer);
            }
        } finally {
            releaseBuffer(buffer, INSTANCE);
        }
    }

    private void verifyCopyToBufferBounds(PageCursor cursor, ByteBuffer buffer) throws IOException {
        // Assuming no mistakes, the data must be copied as is.
        int copied;
        do {
            buffer.clear();
            copied = cursor.copyTo(0, buffer);
        } while (cursor.shouldRetry());
        assertThat(copied).isEqualTo(filePayloadSize);
        buffer.clear();
        verifyRecordsMatchExpected(0, 0, buffer);

        // Source buffer underflow.
        buffer.clear();
        cursor.copyTo(-1, buffer);
        assertTrue(cursor.checkAndClearBoundsFlag());

        // Target buffer overflow^W truncation.
        buffer.clear();
        copied = cursor.copyTo(1, buffer);
        assertFalse(cursor.checkAndClearBoundsFlag());
        assertThat(copied).isEqualTo(filePayloadSize - 1);
        assertThat(buffer.position()).isEqualTo(filePayloadSize - 1);
        assertThat(buffer.remaining()).isEqualTo(1);
        buffer.clear();

        // Smaller buffer at offset zero.
        zapBuffer(buffer);
        do {
            buffer.clear();
            buffer.limit(filePayloadSize - recordSize);
            copied = cursor.copyTo(0, buffer);
        } while (cursor.shouldRetry());
        assertThat(copied).isEqualTo(filePayloadSize - recordSize);
        assertThat(buffer.position()).isEqualTo(filePayloadSize - recordSize);
        assertThat(buffer.remaining()).isEqualTo(0);
        buffer.clear();
        buffer.limit(filePayloadSize - recordSize);
        verifyRecordsMatchExpected(0, 0, buffer);

        // Smaller buffer at non-zero offset.
        zapBuffer(buffer);
        do {
            buffer.clear();
            buffer.limit(filePayloadSize - recordSize);
            copied = cursor.copyTo(recordSize, buffer);
        } while (cursor.shouldRetry());
        assertThat(copied).isEqualTo(filePayloadSize - recordSize);
        assertThat(buffer.position()).isEqualTo(filePayloadSize - recordSize);
        assertThat(buffer.remaining()).isEqualTo(0);
        buffer.clear();
        buffer.limit(filePayloadSize - recordSize);
        verifyRecordsMatchExpected(0, recordSize, buffer);
    }

    private static void zapBuffer(ByteBuffer buffer) {
        byte zero = (byte) 0;
        if (buffer.hasArray()) {
            Arrays.fill(buffer.array(), zero);
        } else {
            buffer.clear();
            while (buffer.hasRemaining()) {
                buffer.put(zero);
            }
        }
    }

    @Test
    void copyFromHeapByteBufferToWritePageCursorMustCheckBounds() throws Exception {
        configureStandardPageCache();
        ByteBuffer buffer = ByteBuffers.allocate(filePayloadSize, ByteOrder.LITTLE_ENDIAN, INSTANCE);
        Path file = file("a");
        generateFileWithRecords(file, recordsPerFilePage, recordSize, recordsPerFilePage, reservedBytes, filePageSize);
        try (PagedFile pf = map(file, filePageSize);
                PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            assertTrue(cursor.next());
            cursor.copyTo(0, buffer);
        }
        Path targetFile = file("target");
        try (PagedFile pf = map(targetFile, filePageSize, immutable.of(CREATE, TRUNCATE_EXISTING));
                PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            assertTrue(cursor.next());
            verifyCopyFromBufferBounds(cursor, buffer);
        }
    }

    private void verifyCopyFromBufferBounds(PageCursor cursor, ByteBuffer buffer) throws IOException {
        // Assuming no mistakes, the data must be copied as is
        cursor.zapPage();
        buffer.flip();
        cursor.copyFrom(buffer, 0);
        verifyRecordsMatchExpected(cursor);

        // underflow
        cursor.zapPage();
        buffer.flip();
        cursor.copyFrom(buffer, -1);
        assertTrue(cursor.checkAndClearBoundsFlag());

        // Target buffer overflow^W truncation.
        cursor.zapPage();
        buffer.flip();
        var copied = cursor.copyFrom(buffer, 1);
        assertFalse(cursor.checkAndClearBoundsFlag());
        assertThat(copied).isEqualTo(filePayloadSize - 1);
        assertThat(buffer.position()).isEqualTo(filePayloadSize - 1);
        assertThat(buffer.remaining()).isEqualTo(1);

        // Smaller buffer at offset zero.
        cursor.zapPage();
        buffer.flip();
        buffer.limit(filePayloadSize - recordSize);
        copied = cursor.copyFrom(buffer, 0);
        assertThat(copied).isEqualTo(filePayloadSize - recordSize);
        assertThat(buffer.position()).isEqualTo(filePayloadSize - recordSize);
        assertThat(buffer.remaining()).isEqualTo(0);
        verifyRecordsMatchExpected(cursor, recordsPerFilePage - 1, 0);

        // Smaller buffer at non-zero offset.
        cursor.zapPage();
        buffer.flip();
        buffer.limit(filePayloadSize - recordSize);
        copied = cursor.copyFrom(buffer, recordSize);
        assertThat(copied).isEqualTo(filePayloadSize - recordSize);
        assertThat(buffer.position()).isEqualTo(filePayloadSize - recordSize);
        assertThat(buffer.remaining()).isEqualTo(0);
        verifyRecordsMatchExpected(cursor, recordsPerFilePage - 1, recordSize);
    }

    @Test
    void nextOnClosedWriteCursorMustThrow() throws Exception {
        configureStandardPageCache();
        try (var pf = map(file("a"), filePageSize);
                var cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            assertTrue(cursor.next());
            cursor.putByte((byte) 6);
            cursor.close();
            assertThatThrownBy(cursor::next).isInstanceOf(IllegalStateException.class);
        }
        // verify that we cen read that 6
        try (var pf = map(file("a"), filePageSize);
                var cursor = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
            assertTrue(cursor.next());
            assertThat(cursor.getByte()).isEqualTo((byte) 6);
        }
    }

    @Test
    void nextOnClosedReadCursorMustThrow() throws Exception {
        configureStandardPageCache();
        try (var pf = map(file("a"), filePageSize);
                var cursor = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
            cursor.close();
            assertThatThrownBy(cursor::next).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void copyToReadOnlyHeapByteBufferMustThrow() throws Exception {
        configureStandardPageCache();
        ByteBuffer buf = ByteBuffers.allocate(filePageSize, ByteOrder.LITTLE_ENDIAN, INSTANCE)
                .asReadOnlyBuffer();
        try (PagedFile pf = map(file("a"), filePageSize);
                PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            assertTrue(cursor.next());

            assertThrows(ReadOnlyBufferException.class, () -> cursor.copyTo(0, buf));
        }
    }

    @Test
    void copyToReadOnlyDirectByteBufferMustThrow() throws Exception {
        configureStandardPageCache();
        ByteBuffer allocation = allocateDirect(filePageSize, ByteOrder.LITTLE_ENDIAN, INSTANCE);
        try {
            ByteBuffer buf = allocation.asReadOnlyBuffer();
            try (PagedFile pf = map(file("a"), filePageSize);
                    PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                assertThrows(ReadOnlyBufferException.class, () -> cursor.copyTo(0, buf));
            }
        } finally {
            releaseBuffer(allocation, INSTANCE);
        }
    }

    @Test
    void shiftBytesMustNotRaiseOutOfBoundsOnLengthWithinPageBoundary() throws Exception {
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize);
                PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            assertTrue(cursor.next());
            cursor.shiftBytes(0, filePayloadSize, 0);
            assertFalse(cursor.checkAndClearBoundsFlag());
            cursor.shiftBytes(0, filePayloadSize - 1, 1);
            assertFalse(cursor.checkAndClearBoundsFlag());
            cursor.shiftBytes(1, filePayloadSize - 1, -1);
            assertFalse(cursor.checkAndClearBoundsFlag());
        }
    }

    @Test
    void shiftBytesMustRaiseOutOfBoundsOnLengthLargerThanPageSize() throws Exception {
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize);
                PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            assertTrue(cursor.next());
            cursor.shiftBytes(0, filePageSize + 1, 0);
            assertTrue(cursor.checkAndClearBoundsFlag());
        }
    }

    @Test
    void shiftBytesMustRaiseOutOfBoundsOnNegativeLength() throws Exception {
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize);
                PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            assertTrue(cursor.next());
            cursor.shiftBytes(1, -1, 0);
            assertTrue(cursor.checkAndClearBoundsFlag());
        }
    }

    @Test
    void shiftBytesMustRaiseOutOfBoundsOnNegativeSource() throws Exception {
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize);
                PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            assertTrue(cursor.next());
            cursor.shiftBytes(-1, 10, 0);
            assertTrue(cursor.checkAndClearBoundsFlag());
        }
    }

    @Test
    void shiftBytesMustRaiseOutOfBoundsOnOverSizedSource() throws Exception {
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize);
                PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            assertTrue(cursor.next());
            cursor.shiftBytes(filePageSize, 1, 0);
            assertTrue(cursor.checkAndClearBoundsFlag());
            cursor.shiftBytes(filePageSize + 1, 0, 0);
            assertTrue(cursor.checkAndClearBoundsFlag());
        }
    }

    @Test
    void shiftBytesMustRaiseOutOfBoundsOnBufferUnderflow() throws Exception {
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize);
                PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            assertTrue(cursor.next());
            cursor.shiftBytes(0, 1, -1);
            assertTrue(cursor.checkAndClearBoundsFlag());
        }
    }

    @Test
    void shiftBytesMustRaiseOutOfBoundsOnBufferOverflow() throws Exception {
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize);
                PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            assertTrue(cursor.next());
            cursor.shiftBytes(filePageSize - 1, 1, 1);
            assertTrue(cursor.checkAndClearBoundsFlag());
        }
    }

    @Test
    void shiftBytesMustThrowOnReadCursor() throws Exception {
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize);
                PageCursor writer = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
                PageCursor reader = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
            assertTrue(writer.next());
            assertTrue(reader.next());

            assertThrows(IllegalStateException.class, () -> reader.shiftBytes(0, 0, 0));
        }
    }

    @Test
    void shiftBytesMustShiftBytesToTheRightOverlapping() throws Exception {
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize);
                PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            assertTrue(cursor.next());

            byte[] bytes = new byte[30];
            for (int i = 0; i < bytes.length; i++) {
                bytes[i] = (byte) (i + 1);
            }

            int srcOffset = 10;
            cursor.setOffset(srcOffset);
            cursor.putBytes(bytes);

            int shift = 5;
            assertZeroes(cursor, 0, srcOffset);
            assertZeroes(cursor, srcOffset + bytes.length, filePayloadSize - srcOffset - bytes.length);

            cursor.shiftBytes(srcOffset, bytes.length, shift);

            assertZeroes(cursor, 0, srcOffset);
            assertZeroes(cursor, srcOffset + bytes.length + shift, filePayloadSize - srcOffset - bytes.length - shift);

            cursor.setOffset(srcOffset);
            for (int i = 0; i < shift; i++) {
                assertThat(cursor.getByte()).isEqualTo((byte) (i + 1));
            }
            for (int i = 0; i < bytes.length; i++) {
                assertThat(cursor.getByte()).isEqualTo((byte) (i + 1));
            }

            assertFalse(cursor.checkAndClearBoundsFlag());
        }
    }

    @Test
    void shiftBytesMustShiftBytesToTheRightNotOverlapping() throws Exception {
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize);
                PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            assertTrue(cursor.next());

            byte[] bytes = new byte[30];
            for (int i = 0; i < bytes.length; i++) {
                bytes[i] = (byte) (i + 1);
            }

            int srcOffset = 10;
            cursor.setOffset(srcOffset);
            cursor.putBytes(bytes);

            int gap = 5;
            int shift = bytes.length + gap;
            assertZeroes(cursor, 0, srcOffset);
            assertZeroes(cursor, srcOffset + bytes.length, filePayloadSize - srcOffset - bytes.length);

            cursor.shiftBytes(srcOffset, bytes.length, shift);

            assertZeroes(cursor, 0, srcOffset);
            assertZeroes(cursor, srcOffset + bytes.length + shift, filePayloadSize - srcOffset - bytes.length - shift);

            cursor.setOffset(srcOffset);
            for (int i = 0; i < bytes.length; i++) {
                assertThat(cursor.getByte()).isEqualTo((byte) (i + 1));
            }
            assertZeroes(cursor, srcOffset + bytes.length + shift, gap);
            cursor.setOffset(srcOffset + shift);
            for (int i = 0; i < bytes.length; i++) {
                assertThat(cursor.getByte()).isEqualTo((byte) (i + 1));
            }

            assertFalse(cursor.checkAndClearBoundsFlag());
        }
    }

    @Test
    void shiftBytesMustShiftBytesToTheLeftOverlapping() throws Exception {
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize);
                PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            assertTrue(cursor.next());

            byte[] bytes = new byte[30];
            for (int i = 0; i < bytes.length; i++) {
                bytes[i] = (byte) (i + 1);
            }

            int srcOffset = 10;
            cursor.setOffset(srcOffset);
            cursor.putBytes(bytes);

            int shift = -5;
            assertZeroes(cursor, 0, srcOffset);
            assertZeroes(cursor, srcOffset + bytes.length, filePayloadSize - srcOffset - bytes.length);

            cursor.shiftBytes(srcOffset, bytes.length, shift);

            assertZeroes(cursor, 0, srcOffset + shift);
            assertZeroes(cursor, srcOffset + bytes.length, filePayloadSize - srcOffset - bytes.length);

            cursor.setOffset(srcOffset + shift);
            for (int i = 0; i < bytes.length; i++) {
                assertThat(cursor.getByte()).isEqualTo((byte) (i + 1));
            }
            for (int i = shift; i < 0; i++) {
                assertThat(cursor.getByte()).isEqualTo((byte) (bytes.length + i + 1));
            }

            assertFalse(cursor.checkAndClearBoundsFlag());
        }
    }

    @Test
    void shiftBytesMustShiftBytesToTheLeftNotOverlapping() throws Exception {
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize);
                PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            assertTrue(cursor.next());

            byte[] bytes = new byte[30];
            for (int i = 0; i < bytes.length; i++) {
                bytes[i] = (byte) (i + 1);
            }

            int srcOffset = filePayloadSize - bytes.length - 10;
            cursor.setOffset(srcOffset);
            cursor.putBytes(bytes);

            int gap = 5;
            int shift = -bytes.length - gap;
            assertZeroes(cursor, 0, srcOffset);
            assertZeroes(cursor, srcOffset + bytes.length, filePayloadSize - srcOffset - bytes.length);

            cursor.shiftBytes(srcOffset, bytes.length, shift);

            assertZeroes(cursor, 0, srcOffset + shift);
            assertZeroes(cursor, srcOffset + bytes.length, filePayloadSize - srcOffset - bytes.length);

            cursor.setOffset(srcOffset + shift);
            for (int i = 0; i < bytes.length; i++) {
                assertThat(cursor.getByte()).isEqualTo((byte) (i + 1));
            }
            assertZeroes(cursor, srcOffset + bytes.length + shift, gap);
            cursor.setOffset(srcOffset);
            for (int i = 0; i < bytes.length; i++) {
                assertThat(cursor.getByte()).isEqualTo((byte) (i + 1));
            }

            assertFalse(cursor.checkAndClearBoundsFlag());
        }
    }

    private static void assertZeroes(PageCursor cursor, int offset, int length) {
        for (int i = 0; i < length; i++) {
            assertThat(cursor.getByte(offset + i)).isEqualTo((byte) 0);
        }
    }

    @Test
    void readCursorsCanOpenLinkedCursor() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            generateFileWithRecords(
                    file("a"), recordsPerFilePage * 2, recordSize, recordsPerFilePage, reservedBytes, filePageSize);
            try (PagedFile pf = map(file("a"), filePageSize);
                    PageCursor parent = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                PageCursor linked = parent.openLinkedCursor(1);
                assertTrue(parent.next());
                assertTrue(linked.next());
                verifyRecordsMatchExpected(parent);
                verifyRecordsMatchExpected(linked);
            }
        });
    }

    @Test
    void writeCursorsCanOpenLinkedCursor() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            Path file = file("a");
            try (PagedFile pf = map(file, filePageSize);
                    PageCursor parent = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                PageCursor linked = parent.openLinkedCursor(1);
                assertTrue(parent.next());
                assertTrue(linked.next());
                writeRecords(parent);
                writeRecords(linked);
            }
            verifyRecordsInFile(file, recordsPerFilePage * 2);
        });
    }

    @Test
    void closingParentCursorMustCloseLinkedCursor() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            try (PagedFile pf = map(file("a"), filePageSize)) {
                PageCursor writerParent = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
                PageCursor readerParent = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT);
                assertTrue(writerParent.next());
                assertTrue(readerParent.next());
                PageCursor writerLinked = writerParent.openLinkedCursor(1);
                PageCursor readerLinked = readerParent.openLinkedCursor(1);
                assertTrue(writerLinked.next());
                assertTrue(readerLinked.next());
                writerParent.close();
                readerParent.close();
                writerLinked.getByte(0);
                assertTrue(writerLinked.checkAndClearBoundsFlag());
                readerLinked.getByte(0);
                assertTrue(readerLinked.checkAndClearBoundsFlag());
            }
        });
    }

    @Test
    void writeCursorWithNoGrowCanOpenLinkedCursorWithNoGrow() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            generateFileWithRecords(
                    file("a"), recordsPerFilePage * 2, recordSize, recordsPerFilePage, reservedBytes, filePageSize);
            try (PagedFile pf = map(file("a"), filePageSize);
                    PageCursor parent = pf.io(0, PF_SHARED_WRITE_LOCK | PF_NO_GROW, NULL_CONTEXT)) {
                PageCursor linked = parent.openLinkedCursor(1);
                assertTrue(parent.next());
                assertTrue(linked.next());
                verifyRecordsMatchExpected(parent);
                verifyRecordsMatchExpected(linked);
                assertFalse(linked.next());
            }
        });
    }

    @Test
    void shouldRetryOnParentCursorMustReturnTrueIfLinkedCursorNeedsRetry() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            generateFileWithRecords(
                    file("a"), recordsPerFilePage * 2, recordSize, recordsPerFilePage, reservedBytes, filePageSize);
            try (PagedFile pf = map(file("a"), filePageSize);
                    PageCursor parentReader = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT);
                    PageCursor writer = pf.io(1, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                PageCursor linkedReader = parentReader.openLinkedCursor(1);
                assertTrue(parentReader.next());
                assertTrue(linkedReader.next());
                assertTrue(writer.next());
                assertTrue(writer.next()); // writer now moved on to page 2

                // parentReader shouldRetry should be true because the linked cursor needs retry
                assertTrue(parentReader.shouldRetry());
                // then, the next read should be consistent
                assertFalse(parentReader.shouldRetry());
            }
        });
    }

    @Test
    void checkAndClearBoundsFlagMustCheckAndClearLinkedCursor() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            configureStandardPageCache();
            try (PagedFile pf = map(file("a"), filePageSize);
                    PageCursor parent = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(parent.next());
                PageCursor linked = parent.openLinkedCursor(1);
                linked.getByte(-1); // Do an out of bounds get
                assertTrue(parent.checkAndClearBoundsFlag());
                assertFalse(linked.checkAndClearBoundsFlag());
            }
        });
    }

    @Test
    void shouldRetryMustClearBoundsFlagIfLinkedCursorNeedsRetry() throws Exception {
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize);
                PageCursor writer = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
                PageCursor reader = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
            assertTrue(writer.next()); // now at page id 0
            assertTrue(writer.next()); // now at page id 1, 0 is unlocked
            assertTrue(writer.next()); // now at page id 2, 1 is unlocked
            assertTrue(reader.next()); // reader now at page id 0
            try (PageCursor linkedReader = reader.openLinkedCursor(1)) {
                assertTrue(linkedReader.next()); // linked reader now at page id 1
                assertTrue(writer.next(1)); // invalidate linked readers lock
                assertTrue(writer.next()); // move writer out of the way
                reader.getByte(-1); // raise bounds flag on parent reader
                assertTrue(reader.shouldRetry()); // we must retry because linked reader was invalidated
                assertFalse(reader.checkAndClearBoundsFlag()); // must return false because we are doing a retry
            }
        }
    }

    @Test
    void checkAndClearCursorExceptionMustNotThrowIfNoExceptionIsSet() throws Exception {
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize)) {
            try (PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.checkAndClearCursorException();
            }
            try (PageCursor cursor = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.checkAndClearCursorException();
                //noinspection StatementWithEmptyBody
                do {
                    // nothing
                } while (cursor.shouldRetry());
                cursor.checkAndClearCursorException();
            }
        }
    }

    @Test
    void checkAndClearCursorExceptionMustThrowIfExceptionIsSet() throws Exception {
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize)) {
            String msg = "Boo" + ThreadLocalRandom.current().nextInt();
            try (PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.setCursorException(msg);
                cursor.checkAndClearCursorException();
                fail("checkAndClearError on write cursor should have thrown");
            } catch (CursorException e) {
                assertThat(e.getMessage()).isEqualTo(msg);
            }

            msg = "Boo" + ThreadLocalRandom.current().nextInt();
            try (PageCursor cursor = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.setCursorException(msg);
                cursor.checkAndClearCursorException();
                fail("checkAndClearError on read cursor should have thrown");
            } catch (CursorException e) {
                assertThat(e.getMessage()).isEqualTo(msg);
            }
        }
    }

    @Test
    void checkAndClearCursorExceptionMustClearExceptionIfSet() throws Exception {
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize)) {
            try (PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.setCursorException("boo");
                try {
                    cursor.checkAndClearCursorException();
                    fail("checkAndClearError on write cursor should have thrown");
                } catch (CursorException ignore) {
                }
                cursor.checkAndClearCursorException();
            }

            try (PageCursor cursor = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.setCursorException("boo");
                try {
                    cursor.checkAndClearCursorException();
                    fail("checkAndClearError on read cursor should have thrown");
                } catch (CursorException ignore) {
                }
                cursor.checkAndClearCursorException();
            }
        }
    }

    @Test
    void nextMustClearCursorExceptionIfSet() throws Exception {
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize)) {
            try (PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.setCursorException("boo");
                assertTrue(cursor.next());
                cursor.checkAndClearCursorException();
            }

            try (PageCursor cursor = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.setCursorException("boo");
                assertTrue(cursor.next());
                cursor.checkAndClearCursorException();
            }
        }
    }

    @Test
    void nextWithIdMustClearCursorExceptionIfSet() throws Exception {
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize)) {
            try (PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next(1));
                cursor.setCursorException("boo");
                assertTrue(cursor.next(2));
                cursor.checkAndClearCursorException();
            }

            try (PageCursor cursor = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next(1));
                cursor.setCursorException("boo");
                assertTrue(cursor.next(2));
                cursor.checkAndClearCursorException();
            }
        }
    }

    @Test
    void shouldRetryMustClearCursorExceptionIfItReturnsTrue() throws Exception {
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize);
                PageCursor writer = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
                PageCursor reader = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
            assertTrue(writer.next()); // now at page id 0
            assertTrue(writer.next()); // now at page id 1, 0 is unlocked
            assertTrue(reader.next()); // now at page id 0
            assertTrue(writer.next(0)); // invalidate the readers lock on page 0
            assertTrue(writer.next()); // move writer out of the way
            reader.setCursorException("boo");
            assertTrue(reader.shouldRetry()); // this should clear the cursor error
            reader.checkAndClearCursorException();
        }
    }

    @Test
    void shouldRetryMustNotClearCursorExceptionIfItReturnsFalse() throws Exception {
        configureStandardPageCache();
        Path file = file("a");
        generateFileWithRecords(file, recordCount, recordSize, recordsPerFilePage, reservedBytes, filePageSize);
        try (PagedFile pf = map(file, filePageSize);
                PageCursor cursor = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
            assertTrue(cursor.next());
            do {
                cursor.setCursorException("boo");
            } while (cursor.shouldRetry());
            // The last shouldRetry has obviously returned 'false'
            assertThrows(CursorException.class, cursor::checkAndClearCursorException);
        }
    }

    @Test
    void shouldRetryMustClearCursorExceptionIfLinkedShouldRetryReturnsTrue() throws Exception {
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize);
                PageCursor writer = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
                PageCursor reader = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
            assertTrue(writer.next()); // now at page id 0
            assertTrue(writer.next()); // now at page id 1, 0 is unlocked
            assertTrue(writer.next()); // now at page id 2, 1 is unlocked
            assertTrue(reader.next()); // reader now at page id 0
            try (PageCursor linkedReader = reader.openLinkedCursor(1)) {
                assertTrue(linkedReader.next()); // linked reader now at page id 1
                assertTrue(writer.next(1)); // invalidate linked readers lock
                assertTrue(writer.next()); // move writer out of the way
                reader.setCursorException("boo"); // raise cursor error on parent reader
                assertTrue(reader.shouldRetry()); // we must retry because linked reader was invalidated
                reader.checkAndClearCursorException(); // must not throw because shouldRetry returned true
            }
        }
    }

    @Test
    void shouldRetryMustClearLinkedCursorExceptionIfItReturnsTrue() throws Exception {
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize);
                PageCursor writer = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
                PageCursor reader = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
            assertTrue(writer.next()); // now at page id 0
            assertTrue(writer.next()); // now at page id 1, 0 is unlocked
            assertTrue(writer.next()); // now at page id 2, 1 is unlocked
            assertTrue(reader.next()); // reader now at page id 0
            try (PageCursor linkedReader = reader.openLinkedCursor(1)) {
                assertTrue(linkedReader.next()); // linked reader now at page id 1
                linkedReader.setCursorException("boo");
                assertTrue(writer.next(0)); // invalidate the read lock held by the parent reader
                assertTrue(reader.shouldRetry()); // this should clear the linked cursor error
                linkedReader.checkAndClearCursorException();
                reader.checkAndClearCursorException();
            }
        }
    }

    @Test
    void shouldRetryMustClearLinkedCursorExceptionIfLinkedShouldRetryReturnsTrue() throws Exception {
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize);
                PageCursor writer = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
                PageCursor reader = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
            assertTrue(writer.next()); // now at page id 0
            assertTrue(writer.next()); // now at page id 1, 0 is unlocked
            assertTrue(writer.next()); // now at page id 2, 1 is unlocked
            assertTrue(reader.next()); // reader now at page id 0
            try (PageCursor linkedReader = reader.openLinkedCursor(1)) {
                assertTrue(linkedReader.next()); // linked reader now at page id 1
                linkedReader.setCursorException("boo");
                assertTrue(writer.next(1)); // invalidate the read lock held by the linked reader
                assertTrue(reader.shouldRetry()); // this should clear the linked cursor error
                linkedReader.checkAndClearCursorException();
                reader.checkAndClearCursorException();
            }
        }
    }

    @Test
    void shouldRetryMustNotClearCursorExceptionIfBothItAndLinkedShouldRetryReturnsFalse() throws Exception {
        configureStandardPageCache();
        Path file = file("a");
        generateFileWithRecords(file, recordCount, recordSize, recordsPerFilePage, reservedBytes, filePageSize);
        try (PagedFile pf = map(file, filePageSize);
                PageCursor reader = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT);
                PageCursor linkedReader = reader.openLinkedCursor(1)) {
            assertTrue(reader.next());
            assertTrue(linkedReader.next());
            do {
                reader.setCursorException("boo");
            } while (reader.shouldRetry());
            assertThrows(CursorException.class, reader::checkAndClearCursorException);
        }
    }

    @Test
    void shouldRetryMustNotClearLinkedCursorExceptionIfBothItAndLinkedShouldRetryReturnsFalse() throws Exception {
        configureStandardPageCache();
        Path file = file("a");
        generateFileWithRecords(file, recordCount, recordSize, recordsPerFilePage, reservedBytes, filePageSize);
        try (PagedFile pf = map(file, filePageSize);
                PageCursor reader = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT);
                PageCursor linkedReader = reader.openLinkedCursor(1)) {
            assertTrue(reader.next());
            assertTrue(linkedReader.next());
            do {
                linkedReader.setCursorException("boo");
            } while (reader.shouldRetry());
            assertThrows(CursorException.class, reader::checkAndClearCursorException);
        }
    }

    @Test
    void checkAndClearCursorExceptionMustThrowIfLinkedCursorHasErrorSet() throws Exception {
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize);
                PageCursor writer = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
                PageCursor reader = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
            String msg = "Boo" + ThreadLocalRandom.current().nextInt();
            assertTrue(writer.next());
            try (PageCursor linkedWriter = writer.openLinkedCursor(1)) {
                assertTrue(linkedWriter.next());
                linkedWriter.setCursorException(msg);
                CursorException exception = assertThrows(CursorException.class, writer::checkAndClearCursorException);
                assertThat(exception.getMessage()).isEqualTo(msg);
            }

            msg = "Boo" + ThreadLocalRandom.current().nextInt();
            assertTrue(reader.next());
            try (PageCursor linkedReader = reader.openLinkedCursor(1)) {
                assertTrue(linkedReader.next());
                linkedReader.setCursorException(msg);
                CursorException exception = assertThrows(CursorException.class, reader::checkAndClearCursorException);
                assertThat(exception.getMessage()).isEqualTo(msg);
            }
        }
    }

    @Test
    void checkAndClearCursorMustNotThrowIfErrorHasBeenSetButTheCursorHasBeenClosed() throws Exception {
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize)) {
            PageCursor writer = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
            assertTrue(writer.next());
            writer.setCursorException("boo");
            writer.close();
            writer.checkAndClearCursorException();

            PageCursor reader = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT);
            assertTrue(reader.next());
            reader.setCursorException("boo");
            reader.close();
            reader.checkAndClearCursorException();

            writer = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
            PageCursor linkedWriter = writer.openLinkedCursor(1);
            assertTrue(linkedWriter.next());
            linkedWriter.setCursorException("boo");
            writer.close();
            linkedWriter.checkAndClearCursorException();

            reader = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT);
            PageCursor linkedReader = reader.openLinkedCursor(1);
            assertTrue(linkedReader.next());
            linkedReader.setCursorException("boo");
            reader.close();
            linkedReader.checkAndClearCursorException();
        }
    }

    @Test
    void openingLinkedCursorOnClosedCursorMustThrow() throws Exception {
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize)) {
            PageCursor writer = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
            assertTrue(writer.next());
            writer.close();
            assertThrows(IllegalStateException.class, () -> writer.openLinkedCursor(1));

            PageCursor reader = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT);
            assertTrue(reader.next());
            reader.close();
            assertThrows(IllegalStateException.class, () -> reader.openLinkedCursor(1));
        }
    }

    @Test
    void openingLinkedCursorIfPreviousLinkedCursorIsNotClosedCursorMustThrow() throws Exception {
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize)) {
            try (var writer = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(writer.next());
                writer.openLinkedCursor(0);
                assertThrows(IllegalStateException.class, () -> writer.openLinkedCursor(1));
            }

            try (var reader = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                assertTrue(reader.next());
                reader.openLinkedCursor(0);
                assertThrows(IllegalStateException.class, () -> reader.openLinkedCursor(1));
            }
        }
    }

    @Test
    void settingNullCursorExceptionMustThrow() throws Exception {
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize);
                PageCursor writer = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
                PageCursor reader = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
            assertTrue(writer.next());
            assertThrows(Exception.class, () -> writer.setCursorException(null));

            assertTrue(reader.next());
            assertThrows(Exception.class, () -> reader.setCursorException(null));
        }
    }

    @Test
    void clearCursorExceptionMustUnsetErrorCondition() throws Exception {
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize);
                PageCursor writer = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
                PageCursor reader = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
            assertTrue(writer.next());
            writer.setCursorException("boo");
            writer.clearCursorException();
            writer.checkAndClearCursorException();

            assertTrue(reader.next());
            reader.setCursorException("boo");
            reader.clearCursorException();
            reader.checkAndClearCursorException();
        }
    }

    @Test
    void clearCursorExceptionMustUnsetErrorConditionOnLinkedCursor() throws Exception {
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize);
                PageCursor writer = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
                PageCursor reader = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
            assertTrue(writer.next());
            PageCursor linkedWriter = writer.openLinkedCursor(1);
            assertTrue(linkedWriter.next());
            linkedWriter.setCursorException("boo");
            writer.clearCursorException();
            writer.checkAndClearCursorException();

            assertTrue(reader.next());
            PageCursor linkedReader = reader.openLinkedCursor(1);
            assertTrue(linkedReader.next());
            linkedReader.setCursorException("boo");
            reader.clearCursorException();
            reader.checkAndClearCursorException();
        }
    }

    @Test
    void sizeOfEmptyFileMustBeZero() throws Exception {
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize)) {
            assertThat(pf.fileSize()).isEqualTo(0L);
        }
    }

    @Test
    void fileSizeMustIncreaseInPageIncrements() throws Exception {
        configureStandardPageCache();
        long increment = filePageSize;
        try (PagedFile pf = map(file("a"), filePageSize);
                PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            assertTrue(cursor.next());
            assertThat(pf.fileSize()).isEqualTo(increment);
            assertTrue(cursor.next());
            assertThat(pf.fileSize()).isEqualTo(2 * increment);
        }
    }

    @Test
    void shouldZeroAllBytesOnClear() throws Exception {
        // GIVEN
        configureStandardPageCache();
        try (PagedFile pagedFile = map(file("a"), filePageSize)) {
            long pageId = 0;
            try (PageCursor cursor = pagedFile.io(pageId, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                ThreadLocalRandom rng = ThreadLocalRandom.current();
                byte[] bytes = new byte[filePayloadSize];
                rng.nextBytes(bytes);

                assertTrue(cursor.next());
                cursor.putBytes(bytes);
            }
            // WHEN
            byte[] allZeros = new byte[filePayloadSize];
            try (PageCursor cursor = pagedFile.io(pageId, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.zapPage();

                byte[] read = new byte[filePayloadSize];
                cursor.getBytes(read);
                assertFalse(cursor.checkAndClearBoundsFlag());
                assertArrayEquals(allZeros, read);
            }
            // THEN
            try (PageCursor cursor = pagedFile.io(pageId, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());

                byte[] read = new byte[filePayloadSize];
                do {
                    cursor.getBytes(read);
                } while (cursor.shouldRetry());
                assertFalse(cursor.checkAndClearBoundsFlag());
                assertArrayEquals(allZeros, read);
            }
        }
    }

    @Test
    void isWriteLockingMustBeTrueForCursorOpenedWithSharedWriteLock() throws Exception {
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize);
                PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            assertTrue(cursor.isWriteLocked());
        }
    }

    @Test
    void isWriteLockingMustBeFalseForCursorOpenedWithSharedReadLock() throws Exception {
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize);
                PageCursor cursor = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
            assertFalse(cursor.isWriteLocked());
        }
    }

    @Test
    void eagerFlushMustWriteToFileOnUnpin() throws Exception {
        configureStandardPageCache();
        Path file = file("a");
        try (PagedFile pf = map(file, filePageSize);
                PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK | PF_EAGER_FLUSH, NULL_CONTEXT)) {
            assertTrue(cursor.next());
            writeRecords(cursor);
            assertTrue(cursor.next()); // this will unpin and flush page 0
            verifyRecordsInFile(file, recordsPerFilePage);
        }
    }

    @Test
    void eagerFlushMustWriteToFileOnExplicitUnpin() throws Exception {
        configureStandardPageCache();
        Path file = file("a");
        try (PagedFile pf = map(file, filePageSize);
                PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK | PF_EAGER_FLUSH, NULL_CONTEXT)) {
            assertTrue(cursor.next());
            writeRecords(cursor);
            cursor.unpin();
            verifyRecordsInFile(file, recordsPerFilePage);
        }
    }

    @Test
    void eagerFlushMustWriteToFileOnClose() throws Exception {
        configureStandardPageCache();
        Path file = file("a");
        try (PagedFile pf = map(file, filePageSize)) {
            try (PageCursor cursor = pf.io(0, PF_SHARED_WRITE_LOCK | PF_EAGER_FLUSH, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                writeRecords(cursor);
            } // this will unpin and flush page 0
            // Verify on the file *before* we unmap it, since unmapping will also flush the file.
            verifyRecordsInFile(file, recordsPerFilePage);
        }
    }

    @Test
    void noFaultNextReadOnInMemoryPages() throws Exception {
        assumeFalse(multiVersioned);
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize);
                PageCursor faulter = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
                PageCursor nofault = pf.io(0, PF_SHARED_READ_LOCK | PF_NO_FAULT, NULL_CONTEXT)) {
            verifyNoFaultAccessToInMemoryPages(faulter, nofault);
        }
    }

    @Test
    void noFaultNextWriteOnInMemoryPages() throws Exception {
        assumeFalse(multiVersioned);
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize);
                PageCursor faulter = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
                PageCursor nofault = pf.io(0, PF_SHARED_WRITE_LOCK | PF_NO_FAULT, NULL_CONTEXT)) {
            verifyNoFaultAccessToInMemoryPages(faulter, nofault);
        }
    }

    @Test
    void noFaultNextLinkedReadOnInMemoryPages() throws Exception {
        assumeFalse(multiVersioned);
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize);
                PageCursor faulter = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
                PageCursor nofault = pf.io(0, PF_SHARED_READ_LOCK | PF_NO_FAULT, NULL_CONTEXT);
                PageCursor linkedNoFault = nofault.openLinkedCursor(0)) {
            verifyNoFaultAccessToInMemoryPages(faulter, linkedNoFault);
        }
    }

    @Test
    void noFaultNextLinkedWriteOnInMemoryPages() throws Exception {
        assumeFalse(multiVersioned);
        configureStandardPageCache();
        try (PagedFile pf = map(file("a"), filePageSize);
                PageCursor faulter = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
                PageCursor nofault = pf.io(0, PF_SHARED_WRITE_LOCK | PF_NO_FAULT, NULL_CONTEXT);
                PageCursor linkedNoFault = nofault.openLinkedCursor(0)) {
            verifyNoFaultAccessToInMemoryPages(faulter, linkedNoFault);
        }
    }

    private static void verifyNoFaultAccessToInMemoryPages(PageCursor faulter, PageCursor nofault) throws IOException {
        assertTrue(faulter.next()); // Page 0 now exists.
        assertTrue(nofault.next()); // NO_FAULT next on page that is in memory.
        verifyNoFaultCursorIsInMemory(nofault, 0L); // Page id must be bound for page that is in memory.

        assertTrue(faulter.next()); // Page 1 now exists.
        assertTrue(nofault.next(1)); // NO_FAULT next with page id on page that is in memory.
        verifyNoFaultCursorIsInMemory(nofault, 1L); // Still bound.
    }

    private static void verifyNoFaultCursorIsInMemory(PageCursor nofault, long expectedPageId) {
        assertThat(nofault.getCurrentPageId()).isEqualTo(expectedPageId);
        nofault.getByte();
        assertFalse(nofault.checkAndClearBoundsFlag()); // Access must not be out of bounds.
        nofault.getByte(0);
        assertFalse(nofault.checkAndClearBoundsFlag()); // Access must not be out of bounds.
    }

    @Test
    void noFaultReadOfPagesNotInMemory() throws Exception {
        DefaultPageCacheTracer cacheTracer = new DefaultPageCacheTracer();
        var contextFactory = new CursorContextFactory(cacheTracer, EMPTY_CONTEXT_SUPPLIER);
        getPageCache(fs, maxPages, cacheTracer);

        Path file = file("a");
        generateFileWithRecords(
                file, recordsPerFilePage * 2, recordSize, recordsPerFilePage, reservedBytes, filePageSize);
        long initialFaults = cacheTracer.faults();
        try (PagedFile pf = map(file, filePageSize);
                CursorContext cursorContext = contextFactory.create("noFaultReadOfPagesNotInMemory");
                PageCursor nofault = pf.io(0, PF_SHARED_READ_LOCK | PF_NO_FAULT, cursorContext)) {
            verifyNoFaultAccessToPagesNotInMemory(cacheTracer, cursorContext, nofault, initialFaults);
        }
    }

    @Test
    void noFaultWriteOnPagesNotInMemory() throws Exception {
        DefaultPageCacheTracer cacheTracer = new DefaultPageCacheTracer();
        var contextFactory = new CursorContextFactory(cacheTracer, EMPTY_CONTEXT_SUPPLIER);
        getPageCache(fs, maxPages, cacheTracer);

        Path file = file("a");
        generateFileWithRecords(
                file, recordsPerFilePage * 2, recordSize, recordsPerFilePage, reservedBytes, filePageSize);
        long initialFaults = cacheTracer.faults();
        try (PagedFile pf = map(file, filePageSize);
                CursorContext cursorContext = contextFactory.create("noFaultWriteOnPagesNotInMemory");
                PageCursor nofault = pf.io(0, PF_SHARED_WRITE_LOCK | PF_NO_FAULT, cursorContext)) {
            verifyNoFaultAccessToPagesNotInMemory(cacheTracer, cursorContext, nofault, initialFaults);
            verifyNoFaultWriteIsOutOfBounds(nofault);
        }
    }

    @Test
    void noFaultLinkedReadOfPagesNotInMemory() throws Exception {
        DefaultPageCacheTracer cacheTracer = new DefaultPageCacheTracer();
        var contextFactory = new CursorContextFactory(cacheTracer, EMPTY_CONTEXT_SUPPLIER);
        getPageCache(fs, maxPages, cacheTracer);

        Path file = file("a");
        generateFileWithRecords(
                file, recordsPerFilePage * 2, recordSize, recordsPerFilePage, reservedBytes, filePageSize);
        long initialFaults = cacheTracer.faults();
        try (PagedFile pf = map(file, filePageSize);
                CursorContext cursorContext = contextFactory.create("noFaultLinkedReadOfPagesNotInMemory");
                PageCursor nofault = pf.io(0, PF_SHARED_READ_LOCK | PF_NO_FAULT, cursorContext);
                PageCursor linkedNoFault = nofault.openLinkedCursor(0)) {
            verifyNoFaultAccessToPagesNotInMemory(cacheTracer, cursorContext, linkedNoFault, initialFaults);
        }
    }

    @Test
    void noFaultLinkedWriteOnPagesNotInMemory() throws Exception {
        DefaultPageCacheTracer cacheTracer = new DefaultPageCacheTracer();
        var contextFactory = new CursorContextFactory(cacheTracer, EMPTY_CONTEXT_SUPPLIER);
        getPageCache(fs, maxPages, cacheTracer);

        Path file = file("a");
        generateFileWithRecords(
                file, recordsPerFilePage * 2, recordSize, recordsPerFilePage, reservedBytes, filePageSize);
        long initialFaults = cacheTracer.faults();
        try (PagedFile pf = map(file, filePageSize);
                CursorContext cursorContext = contextFactory.create("noFaultLinkedWriteOnPagesNotInMemory");
                PageCursor nofault = pf.io(0, PF_SHARED_WRITE_LOCK | PF_NO_FAULT, cursorContext);
                PageCursor linkedNoFault = nofault.openLinkedCursor(0)) {
            verifyNoFaultAccessToPagesNotInMemory(cacheTracer, cursorContext, linkedNoFault, initialFaults);
            verifyNoFaultWriteIsOutOfBounds(linkedNoFault);
        }
    }

    private static void verifyNoFaultAccessToPagesNotInMemory(
            DefaultPageCacheTracer cacheTracer, CursorContext cursorContext, PageCursor nofault, long initialFaults)
            throws IOException {
        assertTrue(nofault.next()); // File contains a page id 0.
        verifyNoFaultReadIsNotInMemory(nofault); // But page is not in memory.
        assertTrue(nofault.next()); // File contains a page id 1.
        verifyNoFaultReadIsNotInMemory(nofault); // Also not in memory.
        assertFalse(nofault.next()); // But there is no page id 2.
        assertTrue(nofault.next(0)); // File has page id 0, even when using next with id.
        verifyNoFaultReadIsNotInMemory(nofault); // Still not in memory.
        assertFalse(nofault.next(2)); // Still no page id two, even when using next with id.

        // Also check that no faults happened.
        ((DefaultPageCursorTracer) cursorContext.getCursorTracer()).setIgnoreCounterCheck(true);
        cursorContext.getCursorTracer().reportEvents();
        assertThat(cacheTracer.faults() - initialFaults).isEqualTo(0L);
    }

    private static void verifyNoFaultReadIsNotInMemory(PageCursor nofault) {
        assertThat(nofault.getCurrentPageId()).isEqualTo(PageCursor.UNBOUND_PAGE_ID);
        nofault.getByte();
        assertTrue(nofault.checkAndClearBoundsFlag()); // Access must be out of bounds.
        nofault.getByte(0);
        assertTrue(nofault.checkAndClearBoundsFlag()); // Access must be out of bounds.
    }

    private static void verifyNoFaultWriteIsOutOfBounds(PageCursor nofault) throws IOException {
        assertTrue(nofault.next(0));
        assertThat(nofault.getCurrentPageId()).isEqualTo(PageCursor.UNBOUND_PAGE_ID);
        nofault.putByte((byte) 1);
        assertTrue(nofault.checkAndClearBoundsFlag()); // Access must be out of bounds.
        nofault.putByte(0, (byte) 1);
        assertTrue(nofault.checkAndClearBoundsFlag()); // Access must be out of bounds.
    }

    @Test
    void noFaultNextReadMustStrideForwardMonotonically() throws Exception {
        configureStandardPageCache();
        Path file = file("a");
        generateFileWithRecords(
                file, recordsPerFilePage * 6, recordSize, recordsPerFilePage, reservedBytes, filePageSize);
        try (PagedFile pf = map(file, filePageSize);
                PageCursor faulter = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT);
                PageCursor nofault = pf.io(0, PF_SHARED_READ_LOCK | PF_NO_FAULT, NULL_CONTEXT)) {
            assertTrue(faulter.next(1));
            assertTrue(faulter.next(3));
            assertTrue(faulter.next(5));
            assertTrue(nofault.next()); // Page id 0.
            verifyNoFaultReadIsNotInMemory(nofault);
            assertTrue(nofault.next()); // Page id 1.
            verifyNoFaultCursorIsInMemory(nofault, 1);
            assertTrue(nofault.next()); // Page id 2.
            verifyNoFaultReadIsNotInMemory(nofault);
            assertTrue(nofault.next()); // Page id 3.
            verifyNoFaultCursorIsInMemory(nofault, 3);
            assertTrue(nofault.next()); // Page id 4.
            verifyNoFaultReadIsNotInMemory(nofault);
            assertTrue(nofault.next()); // Page id 5.
            verifyNoFaultCursorIsInMemory(nofault, 5);
            assertFalse(nofault.next()); // There's no page id 6..
        }
    }

    @Test
    void noFaultReadCursorMustCopeWithPageEviction() throws Exception {
        configureStandardPageCache();
        Path file = file("a");
        try (PagedFile pf = map(file, filePageSize);
                PageCursor faulter = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
                PageCursor nofault = pf.io(0, PF_SHARED_READ_LOCK | PF_NO_FAULT, NULL_CONTEXT)) {
            assertTrue(faulter.next()); // Page id 0 now exists.
            assertTrue(faulter.next()); // And page id 1 now exists.
            assertTrue(nofault.next()); // No_FAULT cursor parked on page id 0.
            verifyNoFaultCursorIsInMemory(nofault, 0);
            PageCursor[] writerArray = new PageCursor[maxPages - 1]; // The `- 1` to leave our `faulter` cursor.
            try {
                for (int i = 0; i < writerArray.length; i++) {
                    writerArray[i] = pf.io(2 + i, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
                    assertTrue(writerArray[i].next());
                }
            } finally {
                // The page the nofault cursor is bound to should now be evicted.
                IOUtils.closeAll(writerArray);
            }
            // If the page is evicted, then our read must have been inconsistent.
            assertTrue(nofault.shouldRetry());
            // However, we are no longer in memory, because the page we had earlier got evicted.
            verifyNoFaultReadIsNotInMemory(nofault);
        } catch (CacheLiveLockException ignored) {
            // when pinning the last of (maxPages - 1) pages write cursor tries cooperatively evict the only available
            // page
            // on very rare occasions it can race with evictor thread and pass enough iterations to trigger livelock
            // exception while evictor thread is flushing and evicting that page
            // this is fine, just ignore it
        }
    }

    private static class PageCacheIOController extends EmptyIOController {
        private final AtomicInteger ioCounter;
        private final int pagesPerFlush;
        private final AtomicInteger callbackCounter;

        private PageCacheIOController(AtomicInteger ioCounter, int pagesPerFlush, AtomicInteger callbackCounter) {
            this.ioCounter = ioCounter;
            this.pagesPerFlush = pagesPerFlush;
            this.callbackCounter = callbackCounter;
        }

        @Override
        public void maybeLimitIO(int recentlyCompletedIOs, FileFlushEvent flushEvent) {
            ioCounter.addAndGet(recentlyCompletedIOs * pagesPerFlush);
            callbackCounter.getAndIncrement();
        }
    }

    private static class LatchedIOController extends EmptyIOController {
        private final CountDownLatch closeLatch;
        private final BinaryLatch limiterBlockLatch;

        LatchedIOController(CountDownLatch closeLatch, BinaryLatch limiterBlockLatch) {
            this.closeLatch = closeLatch;
            this.limiterBlockLatch = limiterBlockLatch;
        }

        @Override
        public void maybeLimitIO(int recentlyCompletedIOs, FileFlushEvent flushEvent) {
            closeLatch.countDown();
            limiterBlockLatch.await();
            super.maybeLimitIO(recentlyCompletedIOs, flushEvent);
        }
    }
}
