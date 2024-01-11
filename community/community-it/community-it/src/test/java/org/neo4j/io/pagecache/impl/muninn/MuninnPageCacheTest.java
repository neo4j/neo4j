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
package org.neo4j.io.pagecache.impl.muninn;

import static java.time.Duration.ofMillis;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_LONG_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.eclipse.collections.api.factory.Sets.immutable;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_buffered_flush_enabled;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_flush_buffer_size_in_pages;
import static org.neo4j.io.pagecache.PageCache.PAGE_SIZE;
import static org.neo4j.io.pagecache.PageCacheOpenOptions.MULTI_VERSIONED;
import static org.neo4j.io.pagecache.PagedFile.PF_NO_FAULT;
import static org.neo4j.io.pagecache.PagedFile.PF_NO_GROW;
import static org.neo4j.io.pagecache.PagedFile.PF_NO_LOAD;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_WRITE_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_TRANSIENT;
import static org.neo4j.io.pagecache.buffer.IOBufferFactory.DISABLED_BUFFER_FACTORY;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.io.pagecache.impl.muninn.PageList.getPageHorizon;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.io.pagecache.tracing.recording.RecordingPageCacheTracer.Evict;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.test.assertion.Assert.assertEventually;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedChannelException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntSupplier;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.pagecache.ConfigurableIOBufferFactory;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.DelegatingFileSystemAbstraction;
import org.neo4j.io.fs.DelegatingStoreChannel;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.io.pagecache.DelegatingPageSwapper;
import org.neo4j.io.pagecache.IOController;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCacheTest;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageEvictionCallback;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.context.OldestTransactionIdFactory;
import org.neo4j.io.pagecache.context.TransactionIdSnapshotFactory;
import org.neo4j.io.pagecache.context.VersionContext;
import org.neo4j.io.pagecache.context.VersionContextSupplier;
import org.neo4j.io.pagecache.impl.FileIsNotMappedException;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.tracing.DatabaseFlushEvent;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.DelegatingPageCacheTracer;
import org.neo4j.io.pagecache.tracing.EvictionEvent;
import org.neo4j.io.pagecache.tracing.EvictionRunEvent;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.io.pagecache.tracing.FlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageReferenceTranslator;
import org.neo4j.io.pagecache.tracing.PinEvent;
import org.neo4j.io.pagecache.tracing.PinPageFaultEvent;
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.io.pagecache.tracing.recording.RecordingPageCacheTracer;
import org.neo4j.io.pagecache.tracing.recording.RecordingPageCursorTracer;
import org.neo4j.io.pagecache.tracing.recording.RecordingPageCursorTracer.Fault;
import org.neo4j.io.pagecache.tracing.version.FileTruncateEvent;
import org.neo4j.memory.DefaultScopedMemoryTracker;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.Race;

public class MuninnPageCacheTest extends PageCacheTest<MuninnPageCache> {
    private static final long X = 0xCAFEBABEDEADBEEFL;
    private static final long Y = 0xDECAFC0FFEEDECAFL;
    private MuninnPageCacheFixture fixture;

    @Override
    protected Fixture<MuninnPageCache> createFixture() {
        ConfigurableIOBufferFactory bufferFactory = new ConfigurableIOBufferFactory(
                Config.defaults(pagecache_buffered_flush_enabled, true), new DefaultScopedMemoryTracker(INSTANCE));
        fixture = new MuninnPageCacheFixture();
        fixture.withBufferFactory(bufferFactory);
        return fixture;
    }

    private PageCacheTracer blockCacheFlush(PageCacheTracer delegate) {
        fixture.backgroundFlushLatch = new CountDownLatch(1);
        return new DelegatingPageCacheTracer(delegate) {
            @Override
            public FileFlushEvent beginFileFlush() {
                try {
                    fixture.backgroundFlushLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return super.beginFileFlush();
            }
        };
    }

    @Test
    void payloadSizeEqualsPageSizePlusReservedBytes() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, new DefaultPageCacheTracer());
                var pageFile = map(pageCache, file("a"), pageCache.pageSize())) {
            assertEquals(pageFile.pageSize(), pageFile.payloadSize() + pageFile.pageReservedBytes());
        }
    }

    @Test
    void payloadSizeForCacheWithCustomConfiguration() throws IOException {
        int reservedBytes = 24;
        var customFixture = new MuninnPageCacheFixture();
        customFixture.withReservedBytes(reservedBytes);
        var cacheTracer = PageCacheTracer.NULL;
        try (var pageCache = customFixture.createPageCache(
                        new SingleFilePageSwapperFactory(fs, cacheTracer, EmptyMemoryTracker.INSTANCE),
                        10,
                        cacheTracer,
                        jobScheduler,
                        DISABLED_BUFFER_FACTORY);
                var pageFile = map(pageCache, file("a"), pageCache.pageSize(), immutable.of(MULTI_VERSIONED))) {
            assertEquals(reservedBytes, pageFile.pageReservedBytes());
            assertEquals(PAGE_SIZE, pageFile.pageSize());
            assertEquals(PAGE_SIZE - reservedBytes, pageFile.payloadSize());
            assertEquals(reservedBytes, pageFile.pageSize() - pageFile.payloadSize());
        }
    }

    @Test
    void writeUntilPayloadDoesNotCauseOverflow() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, new DefaultPageCacheTracer());
                var pageFile = map(pageCache, file("a"), pageCache.pageSize());
                var pageCursor = pageFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            assertTrue(pageCursor.next());
            for (int i = 0; i < pageFile.payloadSize(); i++) {
                pageCursor.putByte((byte) 1);
            }
            assertFalse(pageCursor.checkAndClearBoundsFlag());
        }
    }

    @Test
    void readWriteUninitialisedPage() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, new DefaultPageCacheTracer());
                var pageFile = map(pageCache, file("a"), pageCache.pageSize())) {

            try (var reader = pageFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                assertFalse(reader.next());
            }

            try (var reader = pageFile.io(10, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                assertFalse(reader.next());
            }

            try (var mutator = pageFile.io(20, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(mutator.next());
                for (int i = 0; i < pageFile.payloadSize(); i++) {
                    mutator.putByte((byte) 1);
                }
            }

            try (var reader = pageFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                assertTrue(reader.next());
                for (int i = 0; i < pageFile.payloadSize(); i++) {
                    assertEquals(0, reader.getByte());
                }
            }

            try (var reader = pageFile.io(10, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                assertTrue(reader.next());
                for (int i = 0; i < pageFile.payloadSize(); i++) {
                    assertEquals(0, reader.getByte());
                }
            }
        }
    }

    @Test
    void reusePagesOverPageListOnFileTruncation() throws IOException {
        int pageCachePages = 20;
        int pagesToKeep = 5;
        try (var pageCache = createPageCache(fs, pageCachePages, new DefaultPageCacheTracer())) {
            Path file = file("a");
            try (PagedFile pf = map(pageCache, file, filePageSize)) {
                for (int i = 0; i < 10; i++) {
                    try (PageCursor cursor = pf.io(i, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                        assertTrue(cursor.next());
                        cursor.putLong(i);
                    }
                }

                for (int iteration = 0; iteration < 10; iteration++) {
                    pf.truncate(pagesToKeep, FileTruncateEvent.NULL);
                    assertEquals(5, pageCache.tryGetNumberOfPagesToEvict(pageCachePages));
                    for (int i = 0; i < 5; i++) {
                        try (PageCursor cursor = pf.io(pagesToKeep + i, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                            assertTrue(cursor.next());
                            cursor.putLong(i);
                        }
                        pf.flushAndForce(FileFlushEvent.NULL);
                    }
                    assertEquals(10, pageCache.tryGetNumberOfPagesToEvict(pageCachePages));
                }

                assertEquals(10, pageCache.tryGetNumberOfPagesToEvict(pageCachePages));
                assertEquals(9, pf.getLastPageId());
                assertEquals(pageCachePages, pageCache.maxCachedPages());
            }
        }
    }

    @Test
    void countPagesToEvictOnEmptyPageCache() {
        try (var pageCache = createPageCache(fs, 1024, new DefaultPageCacheTracer())) {
            assertEquals(-1, pageCache.tryGetNumberOfPagesToEvict(10));
        }
    }

    @Test
    void countPagesToEvictOnAllPagesLocked() throws IOException {
        int maxPages = 1024;
        try (var pageCache = createPageCache(fs, maxPages, new DefaultPageCacheTracer())) {
            for (int i = 0; i < maxPages; i++) {
                pageCache.grabFreeAndExclusivelyLockedPage(PinPageFaultEvent.NULL);
            }
            assertEquals(12, pageCache.tryGetNumberOfPagesToEvict(12));
        }
    }

    @Test
    void countPagesToEvictWhenLiveLocked() {
        int maxPages = 1024;
        try (var pageCache = createPageCache(fs, maxPages, new DefaultPageCacheTracer())) {
            assertThrows(CacheLiveLockException.class, () -> {
                for (int i = 0; i < maxPages + 1; i++) {
                    pageCache.grabFreeAndExclusivelyLockedPage(PinPageFaultEvent.NULL);
                }
            });
            assertEquals(12, pageCache.tryGetNumberOfPagesToEvict(12));
            assertEquals(1024, pageCache.tryGetNumberOfPagesToEvict(1024));
        }
    }

    @Test
    void countPagesToEvictWithReleasedPages() throws IOException {
        int maxPages = 1024;
        try (var pageCache = createPageCache(fs, maxPages, new DefaultPageCacheTracer())) {
            long pageRef = pageCache.grabFreeAndExclusivelyLockedPage(PinPageFaultEvent.NULL);
            pageCache.addFreePageToFreelist(pageRef, EvictionRunEvent.NULL);
            assertEquals(-1, pageCache.tryGetNumberOfPagesToEvict(12));
        }
    }

    @Test
    void countPagesToEvictWithAllPagesAcquiredAndReleased() throws IOException {
        int maxPages = 1024;
        try (var pageCache = createPageCache(fs, maxPages, new DefaultPageCacheTracer())) {
            for (int i = 0; i < maxPages; i++) {
                long pageRef = pageCache.grabFreeAndExclusivelyLockedPage(PinPageFaultEvent.NULL);
                pageCache.addFreePageToFreelist(pageRef, EvictionRunEvent.NULL);
            }
            assertEquals(-1, pageCache.tryGetNumberOfPagesToEvict(12));
        }
    }

    @Test
    void countPagesToEvictWithPagesAcquiredSomeReleased() throws IOException {
        int maxPages = 1024;
        try (var pageCache = createPageCache(fs, maxPages, new DefaultPageCacheTracer())) {
            for (int i = 0; i < maxPages - 20; i++) {
                pageCache.grabFreeAndExclusivelyLockedPage(PinPageFaultEvent.NULL);
            }
            for (int i = maxPages - 20; i < maxPages; i++) {
                var page = pageCache.grabFreeAndExclusivelyLockedPage(PinPageFaultEvent.NULL);
                pageCache.addFreePageToFreelist(page, EvictionRunEvent.NULL);
            }

            assertEquals(-1, pageCache.tryGetNumberOfPagesToEvict(12));
            assertEquals(-1, pageCache.tryGetNumberOfPagesToEvict(20));
            assertEquals(1, pageCache.tryGetNumberOfPagesToEvict(21));
        }
    }

    @Test
    void countPagesToEvictWithPagesAcquiredOneReleasedInLoop() throws IOException {
        int maxPages = 1024;
        try (var pageCache = createPageCache(fs, maxPages, new DefaultPageCacheTracer())) {
            for (int i = 0; i < maxPages - 5; i++) {
                pageCache.grabFreeAndExclusivelyLockedPage(PinPageFaultEvent.NULL);
            }
            for (int i = maxPages - 5; i < maxPages; i++) {
                var page = pageCache.grabFreeAndExclusivelyLockedPage(PinPageFaultEvent.NULL);
                pageCache.addFreePageToFreelist(page, EvictionRunEvent.NULL);
            }

            assertEquals(7, pageCache.tryGetNumberOfPagesToEvict(12));
            assertEquals(maxPages - 5, pageCache.tryGetNumberOfPagesToEvict(maxPages));
        }
    }

    @Test
    void countPagesToEvictWithAllPagesAcquiredAndReleasedLater() throws IOException {
        int maxPages = 1024;
        try (var pageCache = createPageCache(fs, maxPages, new DefaultPageCacheTracer())) {
            var pages = LongLists.mutable.withInitialCapacity(maxPages);
            for (int i = 0; i < maxPages; i++) {
                pages.add(pageCache.grabFreeAndExclusivelyLockedPage(PinPageFaultEvent.NULL));
            }
            pages.forEach(page -> pageCache.addFreePageToFreelist(page, EvictionRunEvent.NULL));

            assertEquals(-1, pageCache.tryGetNumberOfPagesToEvict(12));
            assertEquals(-1, pageCache.tryGetNumberOfPagesToEvict(maxPages));
            assertEquals(1, pageCache.tryGetNumberOfPagesToEvict(maxPages + 1));
        }
    }

    @Test
    void countPagesToEvictWithAllWithExceptionPagesAcquiredAndReleased() {
        int maxPages = 1024;
        try (var pageCache = createPageCache(fs, maxPages, new DefaultPageCacheTracer())) {
            var pages = LongLists.mutable.withInitialCapacity(maxPages);
            assertThrows(CacheLiveLockException.class, () -> {
                for (int i = 0; i <= maxPages; i++) {
                    pages.add(pageCache.grabFreeAndExclusivelyLockedPage(PinPageFaultEvent.NULL));
                }
            });
            pages.forEach(page -> pageCache.addFreePageToFreelist(page, EvictionRunEvent.NULL));

            assertEquals(-1, pageCache.tryGetNumberOfPagesToEvict(12));
            assertEquals(-1, pageCache.tryGetNumberOfPagesToEvict(maxPages));
            assertEquals(1, pageCache.tryGetNumberOfPagesToEvict(maxPages + 1));
        }
    }

    @Test
    void reuseSwapperIdOnFileClose() throws IOException {
        try (MuninnPageCache pageCache = createPageCache(fs, 50, new DefaultPageCacheTracer())) {
            int swapperId = pagedFileSwapperId(pageCache);
            for (int i = 0; i < 10; i++) {
                assertEquals(swapperId, pagedFileSwapperId(pageCache));
            }
        }
    }

    @Test
    void doNotReuseSwapperIdWhenThereAreOpenCursors() throws IOException {
        try (MuninnPageCache pageCache = createPageCache(fs, 50, new DefaultPageCacheTracer())) {
            MutableIntSet swapperIds = IntSets.mutable.empty();
            ArrayList<CursorSwapperId> cursorWithIds = new ArrayList<>();
            SwapperSet swapperSet = extractSwapperSet(pageCache);

            while (!swapperSet.skipSweep()) {
                CursorSwapperId cursorSwapperId = pagedFileCursorSwapperId(pageCache);
                assertTrue(
                        swapperIds.add(cursorSwapperId.cursorId), "swapper id with open cursors should not be reused");

                cursorWithIds.add(cursorSwapperId);
            }

            for (CursorSwapperId cursorWithId : cursorWithIds) {
                cursorWithId.pageCursor.close();
            }

            var sweeppedIds = IntSets.mutable.empty();
            swapperSet.sweep(sweeppedIds::addAll);
            for (CursorSwapperId cursorWithId : cursorWithIds) {
                assertTrue(swapperIds.contains(cursorWithId.cursorId));
            }
        }
    }

    @Test
    void evaluateNumberOfPagesToKeepFree() {
        try (MuninnPageCache pageCache = createPageCache(fs, 2, new DefaultPageCacheTracer())) {
            assertEquals(1, pageCache.getKeepFree());
        }

        try (MuninnPageCache pageCache = createPageCache(fs, 30, new DefaultPageCacheTracer())) {
            assertEquals(15, pageCache.getKeepFree());
        }

        try (MuninnPageCache pageCache = createPageCache(fs, 50, new DefaultPageCacheTracer())) {
            assertEquals(25, pageCache.getKeepFree());
        }

        try (MuninnPageCache pageCache = createPageCache(fs, 100, new DefaultPageCacheTracer())) {
            assertEquals(30, pageCache.getKeepFree());
        }

        try (MuninnPageCache pageCache = createPageCache(fs, 10_000, new DefaultPageCacheTracer())) {
            assertEquals(500, pageCache.getKeepFree());
        }
    }

    @Test
    void countOpenedAndClosedCursors() throws IOException {
        DefaultPageCacheTracer defaultPageCacheTracer = new DefaultPageCacheTracer();
        var contextFactory = new CursorContextFactory(defaultPageCacheTracer, EMPTY_CONTEXT_SUPPLIER);
        try (MuninnPageCache pageCache = createPageCache(fs, 42, defaultPageCacheTracer)) {
            int iterations = 14;
            for (int i = 0; i < iterations; i++) {
                writeInitialDataTo(file("a" + i), reservedBytes);
                try (PagedFile pagedFile = map(pageCache, file("a" + i), 8 + reservedBytes)) {
                    try (var cursorContext = contextFactory.create("countOpenedAndClosedCursors");
                            PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, cursorContext)) {
                        assertEquals(1, cursorContext.getCursorTracer().openedCursors());
                        assertEquals(0, cursorContext.getCursorTracer().closedCursors());
                        assertTrue(cursor.next());
                        cursor.putLong(0L);
                    }
                    assertEquals(i * 3 + 1, defaultPageCacheTracer.openedCursors());
                    assertEquals(i * 3 + 1, defaultPageCacheTracer.closedCursors());
                    try (var cursorContext = contextFactory.create("countOpenedAndClosedCursors");
                            PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, cursorContext)) {
                        assertEquals(1, cursorContext.getCursorTracer().openedCursors());
                        assertEquals(0, cursorContext.getCursorTracer().closedCursors());
                    }
                    assertEquals(i * 3 + 2, defaultPageCacheTracer.openedCursors());
                    assertEquals(i * 3 + 2, defaultPageCacheTracer.closedCursors());
                    try (var cursorContext = contextFactory.create("countOpenedAndClosedCursors");
                            PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, cursorContext)) {
                        assertTrue(cursor.next());
                        assertEquals(1, cursorContext.getCursorTracer().openedCursors());
                        assertEquals(0, cursorContext.getCursorTracer().closedCursors());
                    }
                    assertEquals(i * 3 + 3, defaultPageCacheTracer.openedCursors());
                    assertEquals(i * 3 + 3, defaultPageCacheTracer.closedCursors());
                    pagedFile.setDeleteOnClose(true);
                }
            }

            assertEquals(iterations * 3, defaultPageCacheTracer.openedCursors());
            assertEquals(iterations * 3, defaultPageCacheTracer.closedCursors());
        }
    }

    @Test
    void countOpenedAndClosedLinkedCursors() throws IOException {
        DefaultPageCacheTracer defaultPageCacheTracer = new DefaultPageCacheTracer();
        var contextFactory = new CursorContextFactory(defaultPageCacheTracer, EMPTY_CONTEXT_SUPPLIER);
        try (MuninnPageCache pageCache = createPageCache(fs, 42, defaultPageCacheTracer)) {
            int iterations = 14;
            for (int i = 0; i < iterations; i++) {
                writeInitialDataTo(file("a" + i), reservedBytes);
                try (PagedFile pagedFile = map(pageCache, file("a" + i), 8 + reservedBytes)) {
                    try (var cursorContext = contextFactory.create("countOpenedAndClosedCursors");
                            PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, cursorContext)) {
                        cursor.openLinkedCursor(1);
                        assertEquals(2, cursorContext.getCursorTracer().openedCursors());
                        assertEquals(0, cursorContext.getCursorTracer().closedCursors());

                        assertTrue(cursor.next());
                        cursor.putLong(0L);
                    }
                    assertEquals(i * 7 + 2, defaultPageCacheTracer.openedCursors());
                    assertEquals(i * 7 + 2, defaultPageCacheTracer.closedCursors());
                    try (var cursorContext = contextFactory.create("countOpenedAndClosedCursors");
                            PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, cursorContext)) {
                        cursor.openLinkedCursor(1);
                        assertEquals(2, cursorContext.getCursorTracer().openedCursors());
                        assertEquals(0, cursorContext.getCursorTracer().closedCursors());
                    }
                    assertEquals(i * 7 + 4, defaultPageCacheTracer.openedCursors());
                    assertEquals(i * 7 + 4, defaultPageCacheTracer.closedCursors());
                    try (var cursorContext = contextFactory.create("countOpenedAndClosedCursors");
                            PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, cursorContext)) {
                        assertTrue(cursor.next());
                        cursor.openLinkedCursor(1).close();
                        cursor.openLinkedCursor(1);

                        assertEquals(3, cursorContext.getCursorTracer().openedCursors());
                        assertEquals(1, cursorContext.getCursorTracer().closedCursors());
                    }
                    assertEquals(i * 7 + 7, defaultPageCacheTracer.openedCursors());
                    assertEquals(i * 7 + 7, defaultPageCacheTracer.closedCursors());
                    pagedFile.setDeleteOnClose(true);
                }
            }

            assertEquals(iterations * 7, defaultPageCacheTracer.openedCursors());
            assertEquals(iterations * 7, defaultPageCacheTracer.closedCursors());
        }
    }

    @Test
    void shouldBeAbleToSetDeleteOnCloseFileAfterItWasMapped() throws IOException {
        DefaultPageCacheTracer defaultPageCacheTracer = new DefaultPageCacheTracer();
        var contextFactory = new CursorContextFactory(defaultPageCacheTracer, EMPTY_CONTEXT_SUPPLIER);
        Path fileForDeletion = file("fileForDeletion");
        writeInitialDataTo(fileForDeletion, reservedBytes);
        long initialFlushes = defaultPageCacheTracer.flushes();
        try (MuninnPageCache pageCache = createPageCache(fs, 2, defaultPageCacheTracer)) {
            try (var cursorContext = contextFactory.create("shouldBeAbleToSetDeleteOnCloseFileAfterItWasMapped");
                    PagedFile pagedFile = map(pageCache, fileForDeletion, 8 + reservedBytes)) {
                try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, cursorContext)) {
                    assertTrue(cursor.next());
                    cursor.putLong(0L);
                }
                pagedFile.setDeleteOnClose(true);
            }
            assertFalse(fs.fileExists(fileForDeletion));
            assertEquals(0, defaultPageCacheTracer.flushes() - initialFlushes);
        }
    }

    @Test
    void ableToEvictAllPageInAPageCache() throws IOException {
        writeInitialDataTo(file("a"), reservedBytes);
        RecordingPageCacheTracer tracer = new RecordingPageCacheTracer();
        RecordingPageCursorTracer recordingCursor =
                new RecordingPageCursorTracer(tracer, "ableToEvictAllPageInAPageCache");
        var contextFactory = new CursorContextFactory(tracer, EMPTY_CONTEXT_SUPPLIER);
        try (MuninnPageCache pageCache = createPageCache(fs, 2, blockCacheFlush(tracer));
                PagedFile pagedFile = map(pageCache, file("a"), 8 + reservedBytes);
                CursorContext context = contextFactory.create(recordingCursor)) {
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, context)) {
                assertTrue(cursor.next());
            }
            try (PageCursor cursor = pagedFile.io(1, PF_SHARED_READ_LOCK, context)) {
                assertTrue(cursor.next());
            }
            evictAllPages(pageCache);
        }
    }

    @Test
    void mustEvictCleanPageWithoutFlushing() throws Exception {
        writeInitialDataTo(file("a"), reservedBytes);
        RecordingPageCacheTracer tracer = new RecordingPageCacheTracer();
        RecordingPageCursorTracer recordingTracer =
                new RecordingPageCursorTracer(tracer, "mustEvictCleanPageWithoutFlushing");
        var contextFactory = new CursorContextFactory(tracer, EMPTY_CONTEXT_SUPPLIER);

        try (MuninnPageCache pageCache = createPageCache(fs, 10, blockCacheFlush(tracer));
                PagedFile pagedFile = map(pageCache, file("a"), 8 + reservedBytes)) {
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, contextFactory.create(recordingTracer))) {
                assertTrue(cursor.next());
            }
            recordingTracer.reportEvents();
            assertNotNull(recordingTracer.observe(Fault.class));
            assertEquals(1, recordingTracer.faults());
            assertEquals(1, tracer.faults());

            long clockArm = pageCache.evictPages(1, 1, tracer.beginPageEvictions(1));
            assertThat(clockArm).isEqualTo(1L);
            assertNotNull(tracer.observe(Evict.class));
        }
    }

    @Test
    void mustFlushDirtyPagesOnEvictingFirstPage() throws Exception {
        writeInitialDataTo(file("a"), reservedBytes);
        RecordingPageCacheTracer tracer = new RecordingPageCacheTracer();
        RecordingPageCursorTracer recordingTracer =
                new RecordingPageCursorTracer(tracer, "mustFlushDirtyPagesOnEvictingFirstPage");
        var contextFactory = new CursorContextFactory(tracer, EMPTY_CONTEXT_SUPPLIER);

        try (MuninnPageCache pageCache = createPageCache(fs, 10, blockCacheFlush(tracer));
                PagedFile pagedFile = map(pageCache, file("a"), 8 + reservedBytes)) {

            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, contextFactory.create(recordingTracer))) {
                assertTrue(cursor.next());
                cursor.putLong(0L);
            }
            recordingTracer.reportEvents();
            assertNotNull(recordingTracer.observe(Fault.class));
            assertEquals(1, recordingTracer.faults());
            assertEquals(1, tracer.faults());

            long clockArm = pageCache.evictPages(1, 0, tracer.beginPageEvictions(1));
            assertThat(clockArm).isEqualTo(1L);
            assertNotNull(tracer.observe(Evict.class));

            checkFileWithTwoLongs("a", 0L, Y);
        }
    }

    @Test
    void mustFlushDirtyPagesOnEvictingLastPage() throws Exception {
        writeInitialDataTo(file("a"), reservedBytes);
        RecordingPageCacheTracer tracer = new RecordingPageCacheTracer();
        RecordingPageCursorTracer recordingTracer =
                new RecordingPageCursorTracer(tracer, "mustFlushDirtyPagesOnEvictingLastPage");
        var contextFactory = new CursorContextFactory(tracer, EMPTY_CONTEXT_SUPPLIER);

        try (MuninnPageCache pageCache = createPageCache(fs, 10, blockCacheFlush(tracer));
                PagedFile pagedFile = map(pageCache, file("a"), 8 + reservedBytes)) {
            try (PageCursor cursor = pagedFile.io(1, PF_SHARED_WRITE_LOCK, contextFactory.create(recordingTracer))) {
                assertTrue(cursor.next());
                cursor.putLong(0L);
            }
            recordingTracer.reportEvents();
            assertNotNull(recordingTracer.observe(Fault.class));
            assertEquals(1, recordingTracer.faults());
            assertEquals(1, tracer.faults());

            long clockArm = pageCache.evictPages(1, 0, tracer.beginPageEvictions(1));
            assertThat(clockArm).isEqualTo(1L);
            assertNotNull(tracer.observe(Evict.class));

            checkFileWithTwoLongs("a", X, 0L);
        }
    }

    @Test
    void finishPinEventWhenOpenedWithNoFaultOption() throws IOException {
        writeInitialDataTo(file("a"), reservedBytes);
        DefaultPageCacheTracer cacheTracer = new DefaultPageCacheTracer(true);
        PageCursorTracer pageCursorTracer =
                cacheTracer.createPageCursorTracer("finishPinEventWhenOpenedWithNoFaultOption");
        var contextFactory = new CursorContextFactory(cacheTracer, EMPTY_CONTEXT_SUPPLIER);

        try (MuninnPageCache pageCache = createPageCache(fs, 4, cacheTracer);
                PagedFile pagedFile = map(pageCache, file("a"), 8 + reservedBytes)) {
            CursorContext cursorContext = contextFactory.create(pageCursorTracer);
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK | PF_NO_FAULT, cursorContext)) {
                assertTrue(cursor.next());
                assertTrue(cursor.next());
                assertFalse(cursor.next());
            }
            assertEquals(2, cursorContext.getCursorTracer().pins());
            assertEquals(2, cursorContext.getCursorTracer().noFaults());
            assertEquals(0, cursorContext.getCursorTracer().unpins());
            assertEquals(2, pagedFile.pageFileCounters().pins());
            assertEquals(2, pagedFile.pageFileCounters().noFaults());
            assertEquals(0, pagedFile.pageFileCounters().unpins());
        }
    }

    @Test
    void finishPinEventWhenRetryOnEvictedPage() throws IOException {
        writeInitialDataTo(file("a"), reservedBytes);
        DefaultPageCacheTracer cacheTracer = new DefaultPageCacheTracer(true);
        PageCursorTracer pageCursorTracer =
                cacheTracer.createPageCursorTracer("finishPinEventWhenOpenedWithNoFaultOption");
        var contextFactory = new CursorContextFactory(cacheTracer, EMPTY_CONTEXT_SUPPLIER);

        try (MuninnPageCache pageCache = createPageCache(fs, 2, cacheTracer);
                PagedFile pagedFile = map(pageCache, file("a"), 8 + reservedBytes)) {
            CursorContext cursorContext = contextFactory.create(pageCursorTracer);
            try (var readCursor = pagedFile.io(0, PF_SHARED_READ_LOCK, cursorContext)) {
                assertTrue(readCursor.next()); // first pin

                var clockArm = pageCache.evictPages(1, 0, cacheTracer.beginPageEvictions(1));
                assertThat(clockArm).isEqualTo(1L);
                assertTrue(readCursor.shouldRetry()); // another pin
            }
            assertEquals(2, cursorContext.getCursorTracer().pins());
            assertEquals(2, cursorContext.getCursorTracer().unpins());
            assertEquals(2, pagedFile.pageFileCounters().pins());
            assertEquals(2, pagedFile.pageFileCounters().unpins());
        }
    }

    @Test
    void finishPinEventReportedPerFile() throws IOException {
        writeInitialDataTo(file("a"), reservedBytes);
        writeInitialDataTo(file("b"), reservedBytes);
        DefaultPageCacheTracer cacheTracer = new DefaultPageCacheTracer(true);
        PageCursorTracer pageCursorTracer = cacheTracer.createPageCursorTracer("finishPinEventReportedPerFile");
        var contextFactory = new CursorContextFactory(cacheTracer, EMPTY_CONTEXT_SUPPLIER);

        try (MuninnPageCache pageCache = createPageCache(fs, 4, cacheTracer);
                PagedFile pagedFileA = map(pageCache, file("a"), 8 + reservedBytes);
                PagedFile pagedFileB = map(pageCache, file("b"), 8 + reservedBytes)) {
            CursorContext cursorContext = contextFactory.create(pageCursorTracer);
            try (PageCursor cursor = pagedFileA.io(0, PF_SHARED_READ_LOCK, cursorContext)) {
                assertTrue(cursor.next());
                assertTrue(cursor.next());
                assertFalse(cursor.next());
            }
            try (PageCursor cursor = pagedFileB.io(0, PF_SHARED_READ_LOCK, cursorContext)) {
                assertTrue(cursor.next());
                assertTrue(cursor.next());
                assertFalse(cursor.next());
            }
            assertEquals(4, cursorContext.getCursorTracer().pins());
            assertEquals(4, cursorContext.getCursorTracer().unpins());

            assertEquals(2, pagedFileA.pageFileCounters().pins());
            assertEquals(2, pagedFileA.pageFileCounters().unpins());
            assertEquals(2, pagedFileB.pageFileCounters().pins());
            assertEquals(2, pagedFileB.pageFileCounters().unpins());
        }
    }

    @Test
    void finishPinEventReportedPerFileAsInTransaction() throws IOException {
        writeInitialDataTo(file("a"), reservedBytes);
        writeInitialDataTo(file("b"), reservedBytes);
        DefaultPageCacheTracer cacheTracer = new DefaultPageCacheTracer(true);
        PageCursorTracer pageCursorTracer = cacheTracer.createPageCursorTracer("finishPinEventReportedPerFile");
        var contextFactory = new CursorContextFactory(cacheTracer, EMPTY_CONTEXT_SUPPLIER);

        try (MuninnPageCache pageCache = createPageCache(fs, 4, cacheTracer);
                PagedFile pagedFileA = map(pageCache, file("a"), 8 + reservedBytes);
                PagedFile pagedFileB = map(pageCache, file("b"), 8 + reservedBytes)) {
            CursorContext cursorContext = contextFactory.create(pageCursorTracer);
            try (PageCursor cursorA = pagedFileA.io(0, PF_SHARED_READ_LOCK, cursorContext);
                    PageCursor cursorB = pagedFileB.io(0, PF_SHARED_READ_LOCK, cursorContext)) {
                assertTrue(cursorA.next());
                assertTrue(cursorB.next());
            }
            assertEquals(2, cursorContext.getCursorTracer().pins());
            assertEquals(2, cursorContext.getCursorTracer().unpins());

            assertEquals(1, pagedFileA.pageFileCounters().pins());
            assertEquals(1, pagedFileA.pageFileCounters().unpins());
            assertEquals(1, pagedFileB.pageFileCounters().pins());
            assertEquals(1, pagedFileB.pageFileCounters().unpins());
        }
    }

    @Test
    void mustFlushDirtyPagesOnEvictingAllPages() throws Exception {
        writeInitialDataTo(file("a"), reservedBytes);
        RecordingPageCacheTracer tracer = new RecordingPageCacheTracer();
        RecordingPageCursorTracer recordingTracer =
                new RecordingPageCursorTracer(tracer, "mustFlushDirtyPagesOnEvictingAllPages", Fault.class);
        var contextFactory = new CursorContextFactory(tracer, EMPTY_CONTEXT_SUPPLIER);

        try (MuninnPageCache pageCache = createPageCache(fs, 10, blockCacheFlush(tracer));
                PagedFile pagedFile = map(pageCache, file("a"), 8 + reservedBytes)) {
            try (PageCursor cursor =
                    pagedFile.io(0, PF_SHARED_WRITE_LOCK | PF_NO_GROW, contextFactory.create(recordingTracer))) {
                assertTrue(cursor.next());
                cursor.putLong(0L);
                assertTrue(cursor.next());
                cursor.putLong(0L);
                assertFalse(cursor.next());
            }
            recordingTracer.reportEvents();
            assertNotNull(recordingTracer.observe(Fault.class));
            assertNotNull(recordingTracer.observe(Fault.class));
            assertEquals(2, recordingTracer.faults());
            assertEquals(2, tracer.faults());

            long clockArm = pageCache.evictPages(2, 0, tracer.beginPageEvictions(2));
            assertThat(clockArm).isEqualTo(2L);
            assertNotNull(tracer.observe(Evict.class));
            assertNotNull(tracer.observe(Evict.class));

            checkFileWithTwoLongs("a", 0L, 0L);
        }
    }

    @Test
    void trackPageModificationTransactionId() throws Exception {
        assumeFalse(multiVersioned);

        TestVersionContext versionContext = new TestVersionContext(() -> 0);
        var contextFactory =
                new CursorContextFactory(PageCacheTracer.NULL, new SingleVersionContextSupplier(versionContext));
        try (MuninnPageCache pageCache = createPageCache(fs, 2, PageCacheTracer.NULL);
                PagedFile pagedFile = map(pageCache, file("a"), 8);
                CursorContext cursorContext = contextFactory.create("trackPageModificationTransactionId")) {
            versionContext.initWrite(7);
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, cursorContext)) {
                assertTrue(cursor.next());
                cursor.putLong(1);
            }

            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, cursorContext)) {
                assertTrue(cursor.next());
                MuninnPageCursor pageCursor = (MuninnPageCursor) cursor;
                assertEquals(7, PageList.getLastModifiedTxId(pageCursor.pinnedPageRef));
                assertEquals(1, cursor.getLong());
            }
        }
    }

    @Test
    void verificationOfCursorWithNoFaultFlag() throws IOException {
        var versionContext = new TestVersionContext(() -> 15);
        var contextFactory =
                new CursorContextFactory(PageCacheTracer.NULL, new SingleVersionContextSupplier(versionContext));
        try (var pageCache = createPageCache(fs, 2, PageCacheTracer.NULL)) {

            Path file = file("a");
            try (var pagedFile = map(pageCache, file, 8);
                    var cursorContext = contextFactory.create("verificationOfCursorWithNoFaultFlag")) {
                try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, cursorContext)) {
                    assertTrue(cursor.next());
                    cursor.putLong(1);
                }
            }

            try (var pagedFile = map(pageCache, file, 8);
                    var cursorContext = contextFactory.create("verificationOfCursorWithNoFaultFlag");
                    var cursor = pagedFile.io(0, PF_SHARED_READ_LOCK | PF_NO_FAULT, cursorContext)) {
                assertTrue(assertDoesNotThrow(() -> cursor.next()));
            }
        }
    }

    @Test
    void reportFreeListSizeToTracers() throws IOException {
        var pageCacheTracer = new InfoTracer();
        int maxPages = 40;
        var contextFactory = new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER);

        try (MuninnPageCache pageCache = createPageCache(fs, maxPages, pageCacheTracer);
                PagedFile pagedFile = map(pageCache, file("a"), (int) ByteUnit.kibiBytes(8))) {
            for (int pageId = 0; pageId < 5; pageId++) {
                CursorContext cursorContext = contextFactory.create("test");
                try (PageCursor cursor = pagedFile.io(pageId, PF_SHARED_WRITE_LOCK, cursorContext)) {
                    assertTrue(cursor.next());
                    cursor.putLong(1);
                }
                assertEquals(maxPages - (pageId + 1), pageCacheTracer.getFreeListSize());
            }
            pagedFile.flushAndForce(FileFlushEvent.NULL);
        }
    }

    @Test
    void countNotModifiedPagesPerChunkWithNoBuffers() throws IOException {
        assumeTrue(DISABLED_BUFFER_FACTORY.equals(fixture.getBufferFactory()));
        var pageCacheTracer = new InfoTracer();
        try (MuninnPageCache pageCache = createPageCache(fs, 40, pageCacheTracer);
                PagedFile pagedFile = map(pageCache, file("a"), (int) ByteUnit.kibiBytes(8))) {
            for (int pageId = 0; pageId < 4; pageId++) {
                try (PageCursor cursor = pagedFile.io(pageId, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                    assertTrue(cursor.next());
                    cursor.putLong(1);
                }
            }
            try (var flushEvent = pageCacheTracer.beginFileFlush()) {
                pagedFile.flushAndForce(flushEvent);
            }

            var observedChunks = pageCacheTracer.getObservedChunks();
            assertThat(observedChunks).hasSize(1);
            var chunkInfo = observedChunks.get(0);
            assertThat(chunkInfo.getNotModifiedPages()).isEqualTo(0);
            observedChunks.clear();

            try (PageCursor cursor = pagedFile.io(1, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putLong(1);
            }
            try (PageCursor cursor = pagedFile.io(2, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putLong(1);
            }
            try (var flushEvent = pageCacheTracer.beginFileFlush()) {
                pagedFile.flushAndForce(flushEvent);
            }

            var secondFlushChunks = pageCacheTracer.getObservedChunks();
            assertThat(secondFlushChunks).hasSize(1);
            var partialChunkInfo = secondFlushChunks.get(0);
            assertThat(partialChunkInfo.getNotModifiedPages()).isEqualTo(2);
        }
    }

    @Test
    void flushFileWithSeveralChunks() throws IOException {
        assumeFalse(DISABLED_BUFFER_FACTORY.equals(fixture.getBufferFactory()));
        var pageCacheTracer = new InfoTracer();
        int maxPages = 4096 /* chunk size */ + 250;
        int dirtyPages = 4096 + 10;
        PageSwapperFactory swapperFactory = new MultiChunkSwapperFilePageSwapperFactory(pageCacheTracer);
        try (MuninnPageCache pageCache = createPageCache(swapperFactory, maxPages, pageCacheTracer);
                PagedFile pagedFile = map(pageCache, file("a"), (int) ByteUnit.kibiBytes(8))) {
            for (int pageId = 0; pageId < dirtyPages; pageId++) {
                try (PageCursor cursor = pagedFile.io(pageId, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                    assertTrue(cursor.next());
                    cursor.putLong(1);
                }
            }
            try (var flushEvent = pageCacheTracer.beginFileFlush()) {
                pagedFile.flushAndForce(flushEvent);
            }

            var observedChunks = pageCacheTracer.getObservedChunks();
            assertThat(observedChunks).hasSize(2);
            var chunkInfo = observedChunks.get(0);
            assertThat(chunkInfo.getFlushPerChunk())
                    .isGreaterThanOrEqualTo(dirtyPages / pagecache_flush_buffer_size_in_pages.defaultValue());
            var chunkInfo2 = observedChunks.get(1);
            assertThat(chunkInfo2.getFlushPerChunk()).isGreaterThanOrEqualTo(1);
            observedChunks.clear();
        }
    }

    @Test
    void countNotModifiedPagesPerChunkWithBuffers() throws IOException {
        assumeFalse(DISABLED_BUFFER_FACTORY.equals(fixture.getBufferFactory()));
        var pageCacheTracer = new InfoTracer();
        try (MuninnPageCache pageCache = createPageCache(fs, 40, pageCacheTracer);
                PagedFile pagedFile = map(pageCache, file("a"), (int) ByteUnit.kibiBytes(8))) {
            for (int pageId = 0; pageId < 4; pageId++) {
                try (PageCursor cursor = pagedFile.io(pageId, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                    assertTrue(cursor.next());
                    cursor.putLong(1);
                }
            }
            try (var flushEvent = pageCacheTracer.beginFileFlush()) {
                pagedFile.flushAndForce(flushEvent);
            }

            var observedChunks = pageCacheTracer.getObservedChunks();
            assertThat(observedChunks).hasSize(1);
            var chunkInfo = observedChunks.get(0);
            assertThat(chunkInfo.getNotModifiedPages()).isEqualTo(0);
            observedChunks.clear();

            try (PageCursor cursor = pagedFile.io(1, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putLong(1);
            }
            try (PageCursor cursor = pagedFile.io(2, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putLong(1);
            }
            try (var flushEvent = pageCacheTracer.beginFileFlush()) {
                pagedFile.flushAndForce(flushEvent);
            }

            var secondFlushChunks = pageCacheTracer.getObservedChunks();
            assertThat(secondFlushChunks).hasSize(1);
            var partialChunkInfo = secondFlushChunks.get(0);
            // all the rest went to buffer as dirty so we do not count those as nnon modified
            assertThat(partialChunkInfo.getNotModifiedPages()).isEqualTo(1);
        }
    }

    @Test
    void countFlushesFromEvictor() throws IOException {
        DefaultPageCacheTracer pageCacheTracer = new DefaultPageCacheTracer();
        try (MuninnPageCache pageCache = createPageCache(fs, 40, pageCacheTracer);
                PagedFile pagedFile = map(pageCache, file("a"), (int) ByteUnit.kibiBytes(8))) {
            long evictionsBefore = pageCacheTracer.evictions();
            long evictionFlushesBefore = pageCacheTracer.evictionFlushes();
            long flushesBefore = pageCacheTracer.flushes();

            // generate non evictor flush
            try (PageCursor cursor = pagedFile.io(2, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putLong(1);
            }
            try (var flushEvent = pageCacheTracer.beginFileFlush()) {
                pagedFile.flushAndForce(flushEvent);
            }

            for (int pageId = 0; pageId < 10; pageId++) {
                try (PageCursor cursor = pagedFile.io(pageId, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                    assertTrue(cursor.next());
                    cursor.putLong(1);
                }
            }

            try (EvictionRunEvent evictionRunEvent = pageCacheTracer.beginEviction()) {
                pageCache.evictPages(10, 0, evictionRunEvent);
            }

            assertEquals(10, pageCacheTracer.evictions() - evictionsBefore);
            assertEquals(10, pageCacheTracer.evictionFlushes() - evictionFlushesBefore);
            assertEquals(11, pageCacheTracer.flushes() - flushesBefore);
        }
    }

    @Test
    void countFlushesPerChunkWithNoBuffers() throws IOException {
        assumeTrue(DISABLED_BUFFER_FACTORY.equals(fixture.getBufferFactory()));
        var pageCacheTracer = new InfoTracer();
        try (MuninnPageCache pageCache = createPageCache(fs, 40, pageCacheTracer);
                PagedFile pagedFile = map(pageCache, file("a"), (int) ByteUnit.kibiBytes(8))) {
            for (int pageId = 0; pageId < 4; pageId++) {
                try (PageCursor cursor = pagedFile.io(pageId, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                    assertTrue(cursor.next());
                    cursor.putLong(1);
                }
            }
            try (var flushEvent = pageCacheTracer.beginFileFlush()) {
                pagedFile.flushAndForce(flushEvent);
            }

            // we flushed one big region
            var observedChunks = pageCacheTracer.getObservedChunks();
            assertThat(observedChunks).hasSize(1);
            var chunkInfo = observedChunks.get(0);
            assertThat(chunkInfo.getFlushPerChunk()).isEqualTo(1);
            observedChunks.clear();

            // we flush one smaller region in the middle
            try (PageCursor cursor = pagedFile.io(1, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putLong(1);
            }
            try (PageCursor cursor = pagedFile.io(2, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putLong(1);
            }
            try (var flushEvent = pageCacheTracer.beginFileFlush()) {
                pagedFile.flushAndForce(flushEvent);
            }

            var secondFlushChunks = pageCacheTracer.getObservedChunks();
            assertThat(secondFlushChunks).hasSize(1);
            var partialChunkInfo = secondFlushChunks.get(0);
            assertThat(partialChunkInfo.getFlushPerChunk()).isEqualTo(1);
            observedChunks.clear();

            // we flush 2 regions in the middle
            try (PageCursor cursor = pagedFile.io(1, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putLong(1);
            }
            try (PageCursor cursor = pagedFile.io(3, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putLong(1);
            }
            try (var flushEvent = pageCacheTracer.beginFileFlush()) {
                pagedFile.flushAndForce(flushEvent);
            }
            var thirdFlushChunks = pageCacheTracer.getObservedChunks();
            assertThat(thirdFlushChunks).hasSize(1);
            var thirdChunkInfo = thirdFlushChunks.get(0);
            assertThat(thirdChunkInfo.getFlushPerChunk()).isEqualTo(2);
        }
    }

    @Test
    void countFlushesPerChunkWithBuffers() throws IOException {
        assumeFalse(DISABLED_BUFFER_FACTORY.equals(fixture.getBufferFactory()));
        var pageCacheTracer = new InfoTracer();
        try (MuninnPageCache pageCache = createPageCache(fs, 40, pageCacheTracer);
                PagedFile pagedFile = map(pageCache, file("a"), (int) ByteUnit.kibiBytes(8))) {
            for (int pageId = 0; pageId < 4; pageId++) {
                try (PageCursor cursor = pagedFile.io(pageId, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                    assertTrue(cursor.next());
                    cursor.putLong(1);
                }
            }
            try (var flushEvent = pageCacheTracer.beginFileFlush()) {
                pagedFile.flushAndForce(flushEvent);
            }

            // we flushed one big region
            var observedChunks = pageCacheTracer.getObservedChunks();
            assertThat(observedChunks).hasSize(1);
            var chunkInfo = observedChunks.get(0);
            assertThat(chunkInfo.getFlushPerChunk()).isEqualTo(1);
            observedChunks.clear();

            // we flush one smaller region in the middle
            try (PageCursor cursor = pagedFile.io(1, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putLong(1);
            }
            try (PageCursor cursor = pagedFile.io(2, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putLong(1);
            }
            try (var flushEvent = pageCacheTracer.beginFileFlush()) {
                pagedFile.flushAndForce(flushEvent);
            }

            var secondFlushChunks = pageCacheTracer.getObservedChunks();
            assertThat(secondFlushChunks).hasSize(1);
            var partialChunkInfo = secondFlushChunks.get(0);
            assertThat(partialChunkInfo.getFlushPerChunk()).isEqualTo(1);
            observedChunks.clear();

            // we flush 2 regions in the middle
            try (PageCursor cursor = pagedFile.io(1, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putLong(1);
            }
            try (PageCursor cursor = pagedFile.io(3, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putLong(1);
            }
            try (var flushEvent = pageCacheTracer.beginFileFlush()) {
                pagedFile.flushAndForce(flushEvent);
            }
            var thirdFlushChunks = pageCacheTracer.getObservedChunks();
            assertThat(thirdFlushChunks).hasSize(1);
            var thirdChunkInfo = thirdFlushChunks.get(0);
            assertThat(thirdChunkInfo.getFlushPerChunk()).isEqualTo(1);
        }
    }

    @Test
    void countMergesPerChunkWithNoBuffers() throws IOException {
        assumeTrue(DISABLED_BUFFER_FACTORY.equals(fixture.getBufferFactory()));
        var pageCacheTracer = new InfoTracer();
        try (MuninnPageCache pageCache = createPageCache(fs, 40, pageCacheTracer);
                PagedFile pagedFile = map(pageCache, file("a"), (int) ByteUnit.kibiBytes(8))) {
            for (int pageId = 0; pageId < 4; pageId++) {
                try (PageCursor cursor = pagedFile.io(pageId, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                    assertTrue(cursor.next());
                    cursor.putLong(1);
                }
            }
            try (var flushEvent = pageCacheTracer.beginFileFlush()) {
                pagedFile.flushAndForce(flushEvent);
            }

            // we flushed one big region
            var observedChunks = pageCacheTracer.getObservedChunks();
            assertThat(observedChunks).hasSize(1);
            var chunkInfo = observedChunks.get(0);
            assertThat(chunkInfo.getMergesPerChunk()).isEqualTo(3);
            observedChunks.clear();

            // we flush one smaller region in the middle
            try (PageCursor cursor = pagedFile.io(1, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putLong(1);
            }
            try (PageCursor cursor = pagedFile.io(2, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putLong(1);
            }
            try (var flushEvent = pageCacheTracer.beginFileFlush()) {
                pagedFile.flushAndForce(flushEvent);
            }

            var secondFlushChunks = pageCacheTracer.getObservedChunks();
            assertThat(secondFlushChunks).hasSize(1);
            var partialChunkInfo = secondFlushChunks.get(0);
            assertThat(partialChunkInfo.getMergesPerChunk()).isEqualTo(1);
            observedChunks.clear();

            // we flush 2 regions in the middle
            try (PageCursor cursor = pagedFile.io(1, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putLong(1);
            }
            try (PageCursor cursor = pagedFile.io(3, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putLong(1);
            }
            try (var flushEvent = pageCacheTracer.beginFileFlush()) {
                pagedFile.flushAndForce(flushEvent);
            }
            var thirdFlushChunks = pageCacheTracer.getObservedChunks();
            assertThat(thirdFlushChunks).hasSize(1);
            var thirdChunkInfo = thirdFlushChunks.get(0);
            assertThat(thirdChunkInfo.getMergesPerChunk()).isEqualTo(0);
        }
    }

    @Test
    void countMergesPerChunkWithBuffers() throws IOException {
        assumeFalse(DISABLED_BUFFER_FACTORY.equals(fixture.getBufferFactory()));
        var pageCacheTracer = new InfoTracer();
        try (MuninnPageCache pageCache = createPageCache(fs, 40, pageCacheTracer);
                PagedFile pagedFile = map(pageCache, file("a"), (int) ByteUnit.kibiBytes(8))) {
            for (int pageId = 0; pageId < 4; pageId++) {
                try (PageCursor cursor = pagedFile.io(pageId, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                    assertTrue(cursor.next());
                    cursor.putLong(1);
                }
            }
            try (var flushEvent = pageCacheTracer.beginFileFlush()) {
                pagedFile.flushAndForce(flushEvent);
            }

            // we flushed one big region
            var observedChunks = pageCacheTracer.getObservedChunks();
            assertThat(observedChunks).hasSize(1);
            var chunkInfo = observedChunks.get(0);
            assertThat(chunkInfo.getMergesPerChunk()).isEqualTo(0);
            observedChunks.clear();

            // we flush one smaller region in the middle
            try (PageCursor cursor = pagedFile.io(1, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putLong(1);
            }
            try (PageCursor cursor = pagedFile.io(2, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putLong(1);
            }
            try (var flushEvent = pageCacheTracer.beginFileFlush()) {
                pagedFile.flushAndForce(flushEvent);
            }

            var secondFlushChunks = pageCacheTracer.getObservedChunks();
            assertThat(secondFlushChunks).hasSize(1);
            var partialChunkInfo = secondFlushChunks.get(0);
            assertThat(partialChunkInfo.getMergesPerChunk()).isEqualTo(0);
            observedChunks.clear();

            // we flush 2 regions in the middle
            try (PageCursor cursor = pagedFile.io(1, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putLong(1);
            }
            try (PageCursor cursor = pagedFile.io(3, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putLong(1);
            }
            try (var flushEvent = pageCacheTracer.beginFileFlush()) {
                pagedFile.flushAndForce(flushEvent);
            }
            var thirdFlushChunks = pageCacheTracer.getObservedChunks();
            assertThat(thirdFlushChunks).hasSize(1);
            var thirdChunkInfo = thirdFlushChunks.get(0);
            assertThat(thirdChunkInfo.getMergesPerChunk()).isEqualTo(0);
        }
    }

    @Test
    void countUsedBuffersPerChunkWithNoBuffers() throws IOException {
        assumeTrue(DISABLED_BUFFER_FACTORY.equals(fixture.getBufferFactory()));
        var pageCacheTracer = new InfoTracer();
        try (MuninnPageCache pageCache = createPageCache(fs, 40, pageCacheTracer);
                PagedFile pagedFile = map(pageCache, file("a"), (int) ByteUnit.kibiBytes(8))) {
            for (int pageId = 0; pageId < 4; pageId++) {
                try (PageCursor cursor = pagedFile.io(pageId, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                    assertTrue(cursor.next());
                    cursor.putLong(1);
                }
            }
            try (var flushEvent = pageCacheTracer.beginFileFlush()) {
                pagedFile.flushAndForce(flushEvent);
            }

            // we flushed one big region
            var observedChunks = pageCacheTracer.getObservedChunks();
            assertThat(observedChunks).hasSize(1);
            var chunkInfo = observedChunks.get(0);
            assertThat(chunkInfo.getBuffersPerChunk()).isEqualTo(1);
            observedChunks.clear();

            // we flush one smaller region in the middle
            try (PageCursor cursor = pagedFile.io(1, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putLong(1);
            }
            try (PageCursor cursor = pagedFile.io(2, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putLong(1);
            }
            try (var flushEvent = pageCacheTracer.beginFileFlush()) {
                pagedFile.flushAndForce(flushEvent);
            }

            var secondFlushChunks = pageCacheTracer.getObservedChunks();
            assertThat(secondFlushChunks).hasSize(1);
            var partialChunkInfo = secondFlushChunks.get(0);
            assertThat(partialChunkInfo.getBuffersPerChunk()).isEqualTo(1);
            observedChunks.clear();

            // we flush 2 regions in the middle
            try (PageCursor cursor = pagedFile.io(1, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putLong(1);
            }
            try (PageCursor cursor = pagedFile.io(3, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putLong(1);
            }
            try (var flushEvent = pageCacheTracer.beginFileFlush()) {
                pagedFile.flushAndForce(flushEvent);
            }
            var thirdFlushChunks = pageCacheTracer.getObservedChunks();
            assertThat(thirdFlushChunks).hasSize(1);
            var thirdChunkInfo = thirdFlushChunks.get(0);
            assertThat(thirdChunkInfo.getBuffersPerChunk()).isEqualTo(2);
        }
    }

    @Test
    void usedBuffersPerChunkIsAlwaysOneWithBuffers() throws IOException {
        assumeFalse(DISABLED_BUFFER_FACTORY.equals(fixture.getBufferFactory()));
        var pageCacheTracer = new InfoTracer();
        try (MuninnPageCache pageCache = createPageCache(fs, 40, pageCacheTracer);
                PagedFile pagedFile = map(pageCache, file("a"), (int) ByteUnit.kibiBytes(8))) {
            for (int pageId = 0; pageId < 4; pageId++) {
                try (PageCursor cursor = pagedFile.io(pageId, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                    assertTrue(cursor.next());
                    cursor.putLong(1);
                }
            }
            try (var flushEvent = pageCacheTracer.beginFileFlush()) {
                pagedFile.flushAndForce(flushEvent);
            }

            // we flushed one big region
            var observedChunks = pageCacheTracer.getObservedChunks();
            assertThat(observedChunks).hasSize(1);
            var chunkInfo = observedChunks.get(0);
            assertThat(chunkInfo.getBuffersPerChunk()).isEqualTo(1);
            observedChunks.clear();

            // we flush one smaller region in the middle
            try (PageCursor cursor = pagedFile.io(1, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putLong(1);
            }
            try (PageCursor cursor = pagedFile.io(2, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putLong(1);
            }
            try (var flushEvent = pageCacheTracer.beginFileFlush()) {
                pagedFile.flushAndForce(flushEvent);
            }

            var secondFlushChunks = pageCacheTracer.getObservedChunks();
            assertThat(secondFlushChunks).hasSize(1);
            var partialChunkInfo = secondFlushChunks.get(0);
            assertThat(partialChunkInfo.getBuffersPerChunk()).isEqualTo(1);
            observedChunks.clear();

            // we flush 2 regions in the middle
            try (PageCursor cursor = pagedFile.io(1, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putLong(1);
            }
            try (PageCursor cursor = pagedFile.io(3, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putLong(1);
            }
            try (var flushEvent = pageCacheTracer.beginFileFlush()) {
                pagedFile.flushAndForce(flushEvent);
            }
            var thirdFlushChunks = pageCacheTracer.getObservedChunks();
            assertThat(thirdFlushChunks).hasSize(1);
            var thirdChunkInfo = thirdFlushChunks.get(0);
            assertThat(thirdChunkInfo.getBuffersPerChunk()).isEqualTo(1);
        }
    }

    @Test
    void flushSequentialPagesOnPageFileFlushWithNoBuffers() throws IOException {
        assumeTrue(DISABLED_BUFFER_FACTORY.equals(fixture.getBufferFactory()));
        var pageCacheTracer = new DefaultPageCacheTracer(true);
        try (MuninnPageCache pageCache = createPageCache(fs, 4, pageCacheTracer);
                PagedFile pagedFile = map(pageCache, file("a"), (int) ByteUnit.kibiBytes(8))) {
            try (PageCursor cursor = pagedFile.io(1, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putLong(1);
            }
            try (PageCursor cursor = pagedFile.io(2, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putLong(1);
            }
            try (var flushEvent = pageCacheTracer.beginFileFlush()) {
                pagedFile.flushAndForce(flushEvent);
            }

            assertEquals(2, pageCacheTracer.flushes());
            assertEquals(1, pageCacheTracer.merges());
            assertEquals(2, pagedFile.pageFileCounters().flushes());
            assertEquals(1, pagedFile.pageFileCounters().merges());
        }
    }

    @Test
    void flushSequentialPagesOnPageFileFlushWithNoBuffersWithMultipleFiles() throws IOException {
        assumeTrue(DISABLED_BUFFER_FACTORY.equals(fixture.getBufferFactory()));
        writeInitialDataTo(file("a"), reservedBytes);
        writeInitialDataTo(file("b"), reservedBytes);
        var pageCacheTracer = new DefaultPageCacheTracer(true);
        try (MuninnPageCache pageCache = createPageCache(fs, 4, pageCacheTracer);
                PagedFile pagedFileA = map(pageCache, file("a"), (int) ByteUnit.kibiBytes(8));
                PagedFile pagedFileB = map(pageCache, file("b"), (int) ByteUnit.kibiBytes(8))) {
            try (PageCursor cursor = pagedFileA.io(1, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putLong(1);
            }
            try (PageCursor cursor = pagedFileA.io(2, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putLong(1);
            }
            try (var flushEvent = pageCacheTracer.beginFileFlush()) {
                pagedFileA.flushAndForce(flushEvent);
            }

            try (PageCursor cursor = pagedFileB.io(1, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putLong(1);
            }
            try (var flushEvent = pageCacheTracer.beginFileFlush()) {
                pagedFileB.flushAndForce(flushEvent);
            }

            assertEquals(3, pageCacheTracer.flushes());
            assertEquals(1, pageCacheTracer.merges());
            assertEquals(2, pagedFileA.pageFileCounters().flushes());
            assertEquals(1, pagedFileA.pageFileCounters().merges());
            assertEquals(1, pagedFileB.pageFileCounters().flushes());
            assertEquals(0, pagedFileB.pageFileCounters().merges());
        }
    }

    @Test
    void flushSequentialPagesOnPageFileFlushWithBuffers() throws IOException {
        assumeFalse(DISABLED_BUFFER_FACTORY.equals(fixture.getBufferFactory()));
        var pageCacheTracer = new DefaultPageCacheTracer(true);
        try (MuninnPageCache pageCache = createPageCache(fs, 4, pageCacheTracer);
                PagedFile pagedFile = map(pageCache, file("a"), (int) ByteUnit.kibiBytes(8))) {
            try (PageCursor cursor = pagedFile.io(1, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putLong(1);
            }
            try (PageCursor cursor = pagedFile.io(2, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putLong(1);
            }
            try (var flushEvent = pageCacheTracer.beginFileFlush()) {
                pagedFile.flushAndForce(flushEvent);
            }

            assertEquals(2, pageCacheTracer.flushes());
            assertEquals(0, pageCacheTracer.merges());
            assertEquals(2, pagedFile.pageFileCounters().flushes());
            assertEquals(0, pagedFile.pageFileCounters().merges());
        }
    }

    @Test
    void doNotMergeNonSequentialPageBuffersOnPageFileFlush() throws IOException {
        var pageCacheTracer = new DefaultPageCacheTracer(true);
        try (MuninnPageCache pageCache = createPageCache(fs, 6, pageCacheTracer);
                PagedFile pagedFile = map(pageCache, file("a"), (int) ByteUnit.kibiBytes(8))) {
            try (PageCursor cursor = pagedFile.io(1, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putLong(1);
            }
            try (PageCursor cursor = pagedFile.io(3, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putLong(1);
            }
            try (var flushEvent = pageCacheTracer.beginFileFlush()) {
                pagedFile.flushAndForce(flushEvent);
            }

            assertEquals(2, pageCacheTracer.flushes());
            assertEquals(0, pageCacheTracer.merges());
            assertEquals(2, pagedFile.pageFileCounters().flushes());
            assertEquals(0, pagedFile.pageFileCounters().merges());
        }
    }

    @Test
    void pageModificationTrackingNoticeWriteFromAnotherThread() throws Exception {
        assumeFalse(multiVersioned);

        TestVersionContext versionContext = new TestVersionContext(() -> 0);
        var contextFactory =
                new CursorContextFactory(PageCacheTracer.NULL, new SingleVersionContextSupplier(versionContext));
        CursorContext cursorContext = contextFactory.create("pageModificationTrackingNoticeWriteFromAnotherThread");
        try (MuninnPageCache pageCache = createPageCache(fs, 2, PageCacheTracer.NULL);
                PagedFile pagedFile = map(pageCache, file("a"), 8)) {
            versionContext.initWrite(7);

            Future<?> future = executor.submit(() -> {
                try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, cursorContext)) {
                    assertTrue(cursor.next());
                    cursor.putLong(1);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            future.get();

            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, cursorContext)) {
                assertTrue(cursor.next());
                MuninnPageCursor pageCursor = (MuninnPageCursor) cursor;
                assertEquals(7, PageList.getLastModifiedTxId(pageCursor.pinnedPageRef));
                assertEquals(1, cursor.getLong());
            }
        }
    }

    @Test
    void pageModificationTracksHighestModifierTransactionId() throws IOException {
        assumeFalse(multiVersioned);

        TestVersionContext versionContext = new TestVersionContext(() -> 0);
        var contextFactory =
                new CursorContextFactory(PageCacheTracer.NULL, new SingleVersionContextSupplier(versionContext));
        try (MuninnPageCache pageCache = createPageCache(fs, 2, PageCacheTracer.NULL);
                PagedFile pagedFile = map(pageCache, file("a"), 8);
                var cursorContext = contextFactory.create("pageModificationTracksHighestModifierTransactionId")) {
            versionContext.initWrite(1);
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, cursorContext)) {
                assertTrue(cursor.next());
                cursor.putLong(1);
            }
            versionContext.initWrite(12);
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, cursorContext)) {
                assertTrue(cursor.next());
                cursor.putLong(2);
            }
            versionContext.initWrite(7);
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, cursorContext)) {
                assertTrue(cursor.next());
                cursor.putLong(3);
            }

            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, cursorContext)) {
                assertTrue(cursor.next());
                MuninnPageCursor pageCursor = (MuninnPageCursor) cursor;
                assertEquals(12, PageList.getLastModifiedTxId(pageCursor.pinnedPageRef));
                assertEquals(3, cursor.getLong());
            }
        }
    }

    @Test
    void markCursorContextDirtyWhenRepositionCursorOnItsCurrentPage() throws IOException {
        assumeFalse(multiVersioned);

        TestVersionContext versionContext = new TestVersionContext(() -> 3);
        var contextFactory =
                new CursorContextFactory(PageCacheTracer.NULL, new SingleVersionContextSupplier(versionContext));
        var cursorContext = contextFactory.create("markCursorContextDirtyWhenRepositionCursorOnItsCurrentPage");
        try (MuninnPageCache pageCache = createPageCache(fs, 2, PageCacheTracer.NULL);
                PagedFile pagedFile = map(pageCache, file("a"), 8)) {
            versionContext.initRead();
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, cursorContext)) {
                assertTrue(cursor.next(0));
                assertFalse(versionContext.isDirty());

                MuninnPageCursor pageCursor = (MuninnPageCursor) cursor;
                PageList.setLastModifiedTxId(pageCursor.pinnedPageRef, 17);

                assertTrue(cursor.next(0));
                assertTrue(versionContext.isDirty());
            }
        }
    }

    @Test
    void markCursorContextAsDirtyWhenReadingDataFromMoreRecentTransactions() throws IOException {
        assumeFalse(multiVersioned);

        TestVersionContext versionContext = new TestVersionContext(() -> 3);
        var contextFactory =
                new CursorContextFactory(PageCacheTracer.NULL, new SingleVersionContextSupplier(versionContext));
        try (MuninnPageCache pageCache = createPageCache(fs, 2, PageCacheTracer.NULL);
                PagedFile pagedFile = map(pageCache, file("a"), 8);
                var cursorContext =
                        contextFactory.create("markCursorContextAsDirtyWhenReadingDataFromMoreRecentTransactions")) {
            versionContext.initWrite(7);
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, cursorContext)) {
                assertTrue(cursor.next());
                cursor.putLong(3);
            }

            versionContext.initRead();
            assertFalse(versionContext.isDirty());
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, cursorContext)) {
                assertTrue(cursor.next());
                assertEquals(3, cursor.getLong());
                assertTrue(versionContext.isDirty());
            }
        }
    }

    @Test
    void failToSetHorizonOnReadOnlyCursor() throws IOException {
        try (MuninnPageCache pageCache = createPageCache(fs, 4, NULL);
                PagedFile pagedFile = map(pageCache, file("a"), (int) ByteUnit.kibiBytes(8))) {
            try (PageCursor cursor = pagedFile.io(1, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                assertThrows(IllegalStateException.class, () -> cursor.setPageHorizon(17));
            }
        }
    }

    @Test
    void setHorizonOnMultiversionWriteCursor() throws IOException {
        try (MuninnPageCache pageCache = createPageCache(fs, 4, NULL);
                PagedFile pagedFile =
                        map(pageCache, file("a"), (int) ByteUnit.kibiBytes(8), immutable.of(MULTI_VERSIONED))) {
            try (PageCursor cursor = pagedFile.io(1, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                assertEquals(0, getPageHorizon(((MuninnPageCursor) cursor).pinnedPageRef));
                cursor.setPageHorizon(42);
                assertEquals(42, getPageHorizon(((MuninnPageCursor) cursor).pinnedPageRef));
            }
        }
    }

    @Test
    void setHorizonOnPlainWriteCursorHasNoEffect() throws IOException {
        assumeFalse(getOpenOptions().contains(MULTI_VERSIONED));

        try (MuninnPageCache pageCache = createPageCache(fs, 4, NULL);
                PagedFile pagedFile = map(pageCache, file("a"), (int) ByteUnit.kibiBytes(8))) {
            try (PageCursor cursor = pagedFile.io(1, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                assertEquals(0, getPageHorizon(((MuninnPageCursor) cursor).pinnedPageRef));
                cursor.setPageHorizon(42);
                assertEquals(0, getPageHorizon(((MuninnPageCursor) cursor).pinnedPageRef));
            }
        }
    }

    @Test
    void doNotMarkCursorContextAsDirtyWhenReadingDataFromOlderTransactions() throws IOException {
        TestVersionContext versionContext = new TestVersionContext(() -> 23);
        var contextFactory =
                new CursorContextFactory(PageCacheTracer.NULL, new SingleVersionContextSupplier(versionContext));
        try (MuninnPageCache pageCache = createPageCache(fs, 2, PageCacheTracer.NULL);
                PagedFile pagedFile = map(pageCache, file("a"), 8 + reservedBytes);
                CursorContext cursorContext =
                        contextFactory.create("doNotMarkCursorContextAsDirtyWhenReadingDataFromOlderTransactions")) {
            versionContext.initWrite(17);
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, cursorContext)) {
                assertTrue(cursor.next());
                cursor.putLong(3);
            }

            versionContext.initRead();
            assertFalse(versionContext.isDirty());
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, cursorContext)) {
                assertTrue(cursor.next());
                assertEquals(3, cursor.getLong());
                assertFalse(versionContext.isDirty());
            }
        }
    }

    @Test
    void markContextAsDirtyWhenAnyEvictedPageHaveModificationTransactionHigherThenReader() throws IOException {
        assumeFalse(multiVersioned);

        TestVersionContext versionContext = new TestVersionContext(() -> 5);
        var contextFactory =
                new CursorContextFactory(PageCacheTracer.NULL, new SingleVersionContextSupplier(versionContext));
        try (MuninnPageCache pageCache = createPageCache(fs, 2, PageCacheTracer.NULL);
                PagedFile pagedFile = map(pageCache, file("a"), 8 + reservedBytes);
                CursorContext cursorContext = contextFactory.create(
                        "markContextAsDirtyWhenAnyEvictedPageHaveModificationTransactionHigherThenReader")) {
            versionContext.initWrite(3);
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, cursorContext)) {
                assertTrue(cursor.next());
                cursor.putLong(3);
            }

            versionContext.initWrite(13);
            try (PageCursor cursor = pagedFile.io(1, PF_SHARED_WRITE_LOCK, cursorContext)) {
                assertTrue(cursor.next());
                cursor.putLong(4);
            }

            evictAllPages(pageCache);

            versionContext.initRead();
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, cursorContext)) {
                assertTrue(cursor.next());
                assertEquals(3, cursor.getLong());
                assertTrue(versionContext.isDirty());
            }
        }
    }

    @Test
    void doNotMarkContextAsDirtyWhenAnyEvictedPageHaveModificationTransactionLowerThenReader() throws IOException {
        TestVersionContext versionContext = new TestVersionContext(() -> 15);
        var contextFactory =
                new CursorContextFactory(PageCacheTracer.NULL, new SingleVersionContextSupplier(versionContext));
        try (MuninnPageCache pageCache = createPageCache(fs, 2, PageCacheTracer.NULL);
                PagedFile pagedFile = map(pageCache, file("a"), 8 + reservedBytes);
                CursorContext cursorContext = contextFactory.create(
                        "doNotMarkContextAsDirtyWhenAnyEvictedPageHaveModificationTransactionLowerThenReader")) {
            versionContext.initWrite(3);
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, cursorContext)) {
                assertTrue(cursor.next());
                cursor.putLong(3);
            }

            versionContext.initWrite(13);
            try (PageCursor cursor = pagedFile.io(1, PF_SHARED_WRITE_LOCK, cursorContext)) {
                assertTrue(cursor.next());
                cursor.putLong(4);
            }

            evictAllPages(pageCache);

            versionContext.initRead();
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, cursorContext)) {
                assertTrue(cursor.next());
                assertEquals(3, cursor.getLong());
                assertFalse(versionContext.isDirty());
            }
        }
    }

    @Test
    void closingTheCursorMustUnlockModifiedPage() throws Exception {
        writeInitialDataTo(file("a"), reservedBytes);

        try (MuninnPageCache pageCache = createPageCache(fs, 10, PageCacheTracer.NULL);
                PagedFile pagedFile = map(pageCache, file("a"), 8 + reservedBytes)) {
            Future<?> task = executor.submit(() -> {
                try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                    assertTrue(cursor.next());
                    cursor.putLong(41);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            task.get();

            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                long value = cursor.getLong();
                cursor.setOffset(0);
                cursor.putLong(value + 1);
            }

            long clockArm = pageCache.evictPages(1, 0, EvictionRunEvent.NULL);
            assertThat(clockArm).isEqualTo(1L);

            checkFileWithTwoLongs("a", 42L, Y);
        }
    }

    @Test
    void mustUnblockPageFaultersWhenEvictionGetsException() {
        assertTimeoutPreemptively(ofMillis(SEMI_LONG_TIMEOUT_MILLIS), () -> {
            writeInitialDataTo(file("a"), reservedBytes);

            MutableBoolean throwException = new MutableBoolean(true);
            FileSystemAbstraction fs = new DelegatingFileSystemAbstraction(this.fs) {
                @Override
                public StoreChannel open(Path fileName, Set<OpenOption> options) throws IOException {
                    return new DelegatingStoreChannel(super.open(fileName, options)) {
                        @Override
                        public void writeAll(ByteBuffer src, long position) throws IOException {
                            if (throwException.booleanValue()) {
                                throw new IOException("uh-oh...");
                            } else {
                                super.writeAll(src, position);
                            }
                        }
                    };
                }
            };

            try (MuninnPageCache pageCache = createPageCache(fs, 2, PageCacheTracer.NULL);
                    PagedFile pagedFile = map(pageCache, file("a"), 8 + reservedBytes)) {
                // The basic idea is that this loop, which will encounter a lot of page faults, must not block forever
                // even
                // though the eviction thread is unable to flush any dirty pages because the file system throws
                // exceptions on all writes.
                try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                    for (int i = 0; i < 1000; i++) {
                        assertTrue(cursor.next());
                    }
                    fail("Expected an exception at this point");
                } catch (IOException ignore) {
                    // Good.
                }

                throwException.setFalse();
            }
        });
    }

    @RepeatedTest(50)
    void racePageFileCloseAndEviction() {
        assertTimeoutPreemptively(ofMillis(SEMI_LONG_TIMEOUT_MILLIS), () -> {
            assumeTrue(
                    fs.getClass() == EphemeralFileSystemAbstraction.class,
                    "This test is very slow on real file system");

            var pages = 10;
            try (MuninnPageCache pageCache = createPageCache(fs, 2, PageCacheTracer.NULL)) {
                var race = new Race();
                race.addContestant(Race.throwing(() -> {
                    try {
                        for (int i = 0; i < 1000; i++) {
                            try (PagedFile pagedFile = map(pageCache, file("a"), 8 + reservedBytes)) {
                                try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                                    for (int k = 0; k < pages; k++) {
                                        cursor.next();
                                        cursor.putLong(101010101);
                                    }
                                }
                            }
                        }
                    } catch (CacheLiveLockException ignore) {
                    } finally {
                        // to stop the other contestant
                        pageCache.close();
                    }
                }));
                race.addContestant(Race.throwing(() -> {
                    try (var evictionRunEvent = PageCacheTracer.NULL.beginPageEvictions(1000)) {
                        pageCache.evictPages(1000, 0, evictionRunEvent);
                    }
                }));
                race.go();
            }
        });
    }

    @Test
    void pageCacheFlushAndForceMustClearBackgroundEvictionException() {
        assertTimeoutPreemptively(ofMillis(SEMI_LONG_TIMEOUT_MILLIS), () -> {
            MutableBoolean throwException = new MutableBoolean(true);
            FileSystemAbstraction fs = new DelegatingFileSystemAbstraction(this.fs) {
                @Override
                public StoreChannel open(Path fileName, Set<OpenOption> options) throws IOException {
                    return new DelegatingStoreChannel(super.open(fileName, options)) {
                        @Override
                        public void writeAll(ByteBuffer src, long position) throws IOException {
                            if (throwException.booleanValue()) {
                                throw new IOException("uh-oh...");
                            } else {
                                super.writeAll(src, position);
                            }
                        }
                    };
                }
            };

            try (MuninnPageCache pageCache = createPageCache(fs, 10, PageCacheTracer.NULL);
                    PagedFile pagedFile = map(pageCache, file("a"), 8 + reservedBytes)) {
                try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                    assertTrue(cursor.next()); // Page 0 is now dirty, but flushing it will throw an exception.
                }

                // This will run into that exception, in background eviction:
                pageCache.evictPages(1, 0, EvictionRunEvent.NULL);

                // We now have a background eviction exception. A successful flushAndForce should clear it, though.
                throwException.setFalse();
                pageCache.flushAndForce(DatabaseFlushEvent.NULL);

                // And with a cleared exception, we should be able to work with the page cache without worry.
                try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                    for (int i = 0; i < maxPages * 20; i++) {
                        assertTrue(cursor.next());
                    }
                }
            }
        });
    }

    @Test
    void mustThrowIfMappingFileWouldOverflowReferenceCount() {
        assertTimeoutPreemptively(ofMillis(SEMI_LONG_TIMEOUT_MILLIS), () -> {
            Path file = file("a");
            writeInitialDataTo(file, reservedBytes);
            try (MuninnPageCache pageCache = createPageCache(fs, 30, PageCacheTracer.NULL)) {
                PagedFile pf = null;
                int i = 0;

                try {
                    for (; i < Integer.MAX_VALUE; i++) {
                        pf = map(pageCache, file, filePageSize);
                    }
                    fail("Failure was expected");
                } catch (IllegalStateException ile) {
                    // expected
                } finally {
                    for (int j = 0; j < i; j++) {
                        try {
                            pf.close();
                        } catch (Exception e) {
                            //noinspection ThrowFromFinallyBlock
                            throw new AssertionError("Did not expect pf.close() to throw", e);
                        }
                    }
                }
            }
        });
    }

    @Test
    void unlimitedShouldFlushInParallel() {
        assertTimeoutPreemptively(ofMillis(SEMI_LONG_TIMEOUT_MILLIS), () -> {
            List<Path> mappedFiles = new ArrayList<>();
            mappedFiles.add(existingFile("a"));
            mappedFiles.add(existingFile("b"));
            getPageCache(fs, maxPages, new FlushRendezvousTracer(mappedFiles.size()));

            List<PagedFile> mappedPagedFiles = new ArrayList<>();
            for (Path mappedFile : mappedFiles) {
                PagedFile pagedFile = map(pageCache, mappedFile, filePageSize);
                mappedPagedFiles.add(pagedFile);
                try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                    assertTrue(cursor.next());
                    cursor.putInt(1);
                }
            }

            pageCache.flushAndForce(DatabaseFlushEvent.NULL);

            IOUtils.closeAll(mappedPagedFiles);
        });
    }

    @Test
    void transientCursorShouldNotUpdateUsageCounter() throws IOException {
        try (MuninnPageCache pageCache = createPageCache(fs, 40, PageCacheTracer.NULL);
                PagedFile pagedFile = map(pageCache, file("a"), 8 + reservedBytes)) {
            PageList pages = pageCache.pages;
            long zeroPageRef = pages.deref(0);

            // Pretend to read some data
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                assertThat(PageList.getUsage(zeroPageRef)).isEqualTo(1);
            }
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                assertThat(PageList.getUsage(zeroPageRef)).isEqualTo(2);
            }

            // Using transient cursors should not update usage
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK | PF_TRANSIENT, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                assertThat(PageList.getUsage(zeroPageRef)).isEqualTo(2);
            }
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK | PF_TRANSIENT, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                assertThat(PageList.getUsage(zeroPageRef)).isEqualTo(2);
            }
        }
    }

    @Test
    void pageHorizonIsZeroAfterFlushOrEviction() throws IOException {
        int maxPages = 40;
        final AtomicReference<PageList> pagesReferenceHolder = new AtomicReference<>();
        var pageCacheTracer = new PageHorizonSettingPageCacheTracer(pagesReferenceHolder);
        var contextFactory = new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER);
        try (MuninnPageCache pageCache = createPageCache(fs, maxPages, pageCacheTracer);
                PagedFile pagedFile = map(pageCache, file("a"), 8 + reservedBytes, immutable.of(MULTI_VERSIONED))) {

            pagesReferenceHolder.set(pageCache.pages);

            try (PageCursor cursor = pagedFile.io(
                    0, PF_SHARED_WRITE_LOCK, contextFactory.create("pageHorizonIsZeroAfterFlushOrEviction"))) {
                for (int i = 0; i < maxPages * 3; i++) {
                    assertTrue(cursor.next());
                    cursor.putLong(1);
                }
            }
            pagedFile.flushAndForce(FileFlushEvent.NULL);

            checkAllPagesForZeroHorizon(maxPages, pageCache);

            try (PageCursor cursor = pagedFile.io(
                    0, PF_SHARED_READ_LOCK, contextFactory.create("pageHorizonIsZeroAfterFlushOrEviction"))) {
                for (int i = 0; i < maxPages * 3; i++) {
                    assertTrue(cursor.next());
                    assertEquals(1, cursor.getLong());
                }
            }

            checkAllPagesForZeroHorizon(maxPages, pageCache);
        }
    }

    @Test
    void pageHorizonIsZeroAfterFileTruncate() throws IOException {
        int maxPages = 40;
        final AtomicReference<PageList> pagesReferenceHolder = new AtomicReference<>();
        var pageCacheTracer = new PageHorizonSettingPageCacheTracer(pagesReferenceHolder);
        var contextFactory = new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER);
        try (MuninnPageCache pageCache = createPageCache(fs, maxPages, pageCacheTracer);
                PagedFile pagedFile = map(pageCache, file("a"), 8 + reservedBytes)) {

            pagesReferenceHolder.set(pageCache.pages);

            try (PageCursor cursor = pagedFile.io(
                    0, PF_SHARED_WRITE_LOCK, contextFactory.create("pageHorizonIsZeroAfterFileTruncate"))) {
                for (int i = 0; i < maxPages * 3; i++) {
                    assertTrue(cursor.next());
                    cursor.putLong(1);
                }
            }
            pagedFile.truncate(5, FileTruncateEvent.NULL);

            checkAllPagesForZeroHorizon(maxPages, pageCache);
        }
    }

    @Test
    void shouldDealWithOutOfBoundsWithRetries() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, new DefaultPageCacheTracer())) {
            Path file = existingFile("a");
            writeInitialDataTo(file, reservedBytes);
            try (PagedFile pagedFile = map(pageCache, file, 8 + reservedBytes)) {
                try (PageCursor readCursor = pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                    // Given
                    assertThat(readCursor.next(0)).isTrue();
                    readCursor.setOffset(-256); // Note negative page offset
                    // When (common pattern for reading)
                    boolean first = true;
                    readCursor.mark();
                    do {
                        readCursor.setOffsetToMark();
                        readCursor.getLong();
                        if (first) {
                            // Pretend some concurrent write, will trigger a retry for the read cursor
                            try (PageCursor writeCursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                                writeCursor.next(0);
                                writeCursor.putLong(1);
                            }
                            first = false;
                        }
                    } while (readCursor.shouldRetry());

                    // Then
                    assertThat(readCursor.checkAndClearBoundsFlag()).isTrue();
                }
            }
        }
    }

    @Test
    void pageCursorsHaveCorrectPayloadSize() throws IOException {
        try (var pageCache = getPageCache(fs, 1024, new DefaultPageCacheTracer());
                var pagedFile = map(file("a"), pageCache.pageSize())) {
            try (MuninnPageCursor writer = (MuninnPageCursor) pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(writer.next());
                assertEquals(pagedFile.payloadSize(), writer.getPayloadSize());
                assertEquals(pageCache.pageSize() - pagedFile.pageReservedBytes(), writer.getPayloadSize());
                assertEquals(pageCache.pageSize(), writer.getPageSize());
            }

            try (MuninnPageCursor reader = (MuninnPageCursor) pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                assertTrue(reader.next());
                assertEquals(pagedFile.payloadSize(), reader.getPayloadSize());
                assertEquals(pageCache.pageSize() - pagedFile.pageReservedBytes(), reader.getPayloadSize());
                assertEquals(pageCache.pageSize(), reader.getPageSize());
            }
        }
    }

    @Test
    void unboundPageCursorPayload() throws IOException {
        try (var pageCache = getPageCache(fs, 1024, new DefaultPageCacheTracer());
                var pagedFile = map(file("a"), pageCache.pageSize())) {
            try (var writer = (MuninnPageCursor) pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertEquals(writer.getPayloadSize(), 0);
            }

            try (var reader = (MuninnPageCursor) pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                assertEquals(reader.getPayloadSize(), 0);
            }
        }
    }

    @Test
    void fileGrowOnWritePageWithNoLoadOption() throws IOException {
        try (var pageCache = getPageCache(fs, 1024, new DefaultPageCacheTracer());
                var pagedFile = map(file("a"), pageCache.pageSize())) {
            int pagesToPin = 100;
            for (int i = 0; i <= pagesToPin; i++) {
                try (var writer = (MuninnPageCursor) pagedFile.io(i, PF_SHARED_WRITE_LOCK | PF_NO_LOAD, NULL_CONTEXT)) {
                    assertTrue(writer.next());
                }
            }

            assertEquals(pagesToPin, pagedFile.getLastPageId());
            assertEquals((pagesToPin + 1) * PAGE_SIZE, pagedFile.fileSize());
        }
    }

    @Test
    void writeDataWithWriteCursorWithNoLoadOption() throws IOException {
        try (var pageCache = getPageCache(fs, 10, new DefaultPageCacheTracer());
                var pagedFile = map(file("a"), pageCache.pageSize())) {
            int pagesToPin = 100;
            for (int i = 0; i <= pagesToPin; i++) {
                try (var writer = (MuninnPageCursor) pagedFile.io(i, PF_SHARED_WRITE_LOCK | PF_NO_LOAD, NULL_CONTEXT)) {
                    assertTrue(writer.next());
                    writer.putInt(i);
                }
            }

            for (int i = 0; i <= pagesToPin; i++) {
                try (var reader = (MuninnPageCursor) pagedFile.io(i, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                    assertTrue(reader.next());
                    int result;
                    do {
                        reader.setOffset(0);
                        result = reader.getInt();
                    } while (reader.shouldRetry());
                    assertEquals(i, result);
                }
            }
        }
    }

    @Test
    void eventsOnWriteCursorWithNoLoadOption() throws IOException {
        DefaultPageCacheTracer defaultPageCacheTracer = new DefaultPageCacheTracer();
        var contextFactory = new CursorContextFactory(defaultPageCacheTracer, EMPTY_CONTEXT_SUPPLIER);
        try (var pageCache = getPageCache(fs, 10, defaultPageCacheTracer);
                var pagedFile = map(file("a"), pageCache.pageSize())) {
            int pagesToPin = 100;
            for (int i = 0; i <= pagesToPin; i++) {
                try (var context = contextFactory.create("eventsOnWriteCursorWithNoLoadOption");
                        var writer = (MuninnPageCursor) pagedFile.io(i, PF_SHARED_WRITE_LOCK | PF_NO_LOAD, context)) {
                    assertTrue(writer.next());
                    var cursorTracer = context.getCursorTracer();
                    assertEquals(1, cursorTracer.pins());
                    assertEquals(0, cursorTracer.bytesRead());
                    assertEquals(1, cursorTracer.faults());
                }
            }
        }
    }

    @Test
    void linkedCursorsPreservePayloadSize() throws IOException {
        try (var pageCache = getPageCache(fs, 1024, new DefaultPageCacheTracer())) {
            var file = file("a");
            generateFileWithRecords(
                    file, recordsPerFilePage * 2, recordSize, recordsPerFilePage, reservedBytes, pageCachePageSize);

            try (var pagedFile = map(file("a"), pageCache.pageSize());
                    var reader = pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                assertTrue(reader.next());
                try (var linkedReader = (MuninnPageCursor) reader.openLinkedCursor(1)) {
                    assertTrue(linkedReader.next());
                    assertEquals(pageCache.pageSize() - pagedFile.pageReservedBytes(), linkedReader.getPayloadSize());
                }
            }
        }
    }

    private static class FlushRendezvousTracer extends DefaultPageCacheTracer {
        private final CountDownLatch latch;

        FlushRendezvousTracer(int fileCountToWaitFor) {
            latch = new CountDownLatch(fileCountToWaitFor);
        }

        @Override
        public FileFlushEvent beginFileFlush(PageSwapper swapper) {
            latch.countDown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return FileFlushEvent.NULL;
        }
    }

    private static void evictAllPages(MuninnPageCache pageCache) throws IOException {
        PageList pages = pageCache.pages;
        for (int pageId = 0; pageId < pages.getPageCount(); pageId++) {
            long pageReference = pages.deref(pageId);
            while (PageList.isLoaded(pageReference)) {
                pages.tryEvict(pageReference, EvictionRunEvent.NULL);
            }
        }
        for (int pageId = 0; pageId < pages.getPageCount(); pageId++) {
            long pageReference = pages.deref(pageId);
            pageCache.addFreePageToFreelist(pageReference, EvictionRunEvent.NULL);
        }
    }

    private void writeInitialDataTo(Path path, int reservedBytes) throws IOException {
        try (StoreChannel channel = fs.write(path)) {
            ByteBuffer buf = ByteBuffers.allocate(16 + 2 * reservedBytes, ByteOrder.LITTLE_ENDIAN, INSTANCE);
            buf.put(ByteBuffer.allocate(reservedBytes));
            buf.putLong(X);
            buf.put(ByteBuffer.allocate(reservedBytes));
            buf.putLong(Y);
            buf.flip();
            channel.writeAll(buf);
        }
    }

    private int pagedFileSwapperId(MuninnPageCache pageCache) throws IOException {
        try (MuninnPagedFile pagedFile = (MuninnPagedFile) map(pageCache, file("a"), 8)) {
            return pagedFile.swapperId;
        }
    }

    private CursorSwapperId pagedFileCursorSwapperId(MuninnPageCache pageCache) throws IOException {
        try (MuninnPagedFile pagedFile = (MuninnPagedFile) map(pageCache, file("a"), 8)) {
            PageCursor pageCursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
            pageCursor.next();
            return new CursorSwapperId(pageCursor, pagedFile.swapperId);
        }
    }

    private void checkFileWithTwoLongs(String fileName, long valueA, long valueB) throws IOException {
        ByteBuffer buffer = ByteBuffers.allocate(16 + 2 * reservedBytes, ByteOrder.LITTLE_ENDIAN, INSTANCE);
        try (StoreChannel channel = fs.read(file(fileName))) {
            channel.readAll(buffer);
        }
        buffer.flip();

        buffer.position(reservedBytes);
        assertThat(buffer.getLong()).isEqualTo(valueA);
        buffer.position(buffer.position() + reservedBytes);
        assertThat(buffer.getLong()).isEqualTo(valueB);
    }

    private SwapperSet extractSwapperSet(MuninnPageCache pageCache) throws IOException {
        try (var pagedFile = (MuninnPagedFile) map(pageCache, file("a"), 8)) {
            return pagedFile.getSwappers();
        }
    }

    private static class CursorSwapperId implements AutoCloseable {
        private final PageCursor pageCursor;
        private final int cursorId;

        CursorSwapperId(PageCursor pageCursor, int cursorId) {
            this.pageCursor = pageCursor;
            this.cursorId = cursorId;
        }

        @Override
        public void close() {
            pageCursor.close();
        }
    }

    private static class TestVersionContext implements VersionContext {

        private final IntSupplier closedTxIdSupplier;
        private boolean invisibleHead;
        private long committingTxId;
        private long lastClosedTxId;
        private long headVersion;
        private boolean dirty;

        TestVersionContext(IntSupplier closedTxIdSupplier) {
            this.closedTxIdSupplier = closedTxIdSupplier;
        }

        @Override
        public void initRead() {
            this.lastClosedTxId = closedTxIdSupplier.getAsInt();
        }

        @Override
        public void initWrite(long committingTxId) {
            this.committingTxId = committingTxId;
        }

        @Override
        public long committingTransactionId() {
            return committingTxId;
        }

        @Override
        public long lastClosedTransactionId() {
            return lastClosedTxId;
        }

        @Override
        public long highestClosed() {
            return lastClosedTxId;
        }

        @Override
        public void markAsDirty() {
            dirty = true;
        }

        @Override
        public boolean isDirty() {
            return dirty;
        }

        @Override
        public long[] notVisibleTransactionIds() {
            return EMPTY_LONG_ARRAY;
        }

        @Override
        public long oldestVisibleTransactionNumber() {
            return 0;
        }

        @Override
        public void refreshVisibilityBoundaries() {}

        @Override
        public void observedChainHead(long headVersion) {
            this.headVersion = headVersion;
        }

        @Override
        public boolean invisibleHeadObserved() {
            return invisibleHead;
        }

        @Override
        public void resetObsoleteHeadState() {
            headVersion = -1;
            invisibleHead = false;
        }

        @Override
        public void markHeadInvisible() {
            invisibleHead = true;
        }

        @Override
        public long chainHeadVersion() {
            return headVersion;
        }

        @Override
        public boolean initializedForWrite() {
            return committingTxId > 0;
        }
    }

    private static class InfoTracer extends DefaultPageCacheTracer {
        private final CopyOnWriteArrayList<ChunkInfo> observedChunks = new CopyOnWriteArrayList<>();
        private volatile int freeListSize;

        public CopyOnWriteArrayList<ChunkInfo> getObservedChunks() {
            return observedChunks;
        }

        public int getFreeListSize() {
            return freeListSize;
        }

        @Override
        public PageCursorTracer createPageCursorTracer(String tag) {
            return new InfoPageCursorTracer(this, tag);
        }

        @Override
        public FileFlushEvent beginFileFlush(PageSwapper swapper) {
            return new FlushInfoFileFlushEvent();
        }

        @Override
        public FileFlushEvent beginFileFlush() {
            return new FlushInfoFileFlushEvent();
        }

        private class InfoPageCursorTracer extends DefaultPageCursorTracer {
            InfoPageCursorTracer(PageCacheTracer pageCacheTracer, String tag) {
                super(pageCacheTracer, tag);
            }

            @Override
            public PinEvent beginPin(boolean writeLock, long filePageId, PageSwapper swapper) {
                return new PinEvent() {
                    @Override
                    public void setCachePageId(long cachePageId) {}

                    @Override
                    public PinPageFaultEvent beginPageFault(long filePageId, PageSwapper swapper) {
                        return new PinPageFaultEvent() {
                            @Override
                            public void addBytesRead(long bytes) {}

                            @Override
                            public void setCachePageId(long cachePageId) {}

                            @Override
                            public void setException(Throwable throwable) {}

                            @Override
                            public void freeListSize(int listSize) {
                                freeListSize = listSize;
                            }

                            @Override
                            public EvictionEvent beginEviction(long cachePageId) {
                                return null;
                            }

                            @Override
                            public void close() {}
                        };
                    }

                    @Override
                    public void hit() {}

                    @Override
                    public void noFault() {}

                    @Override
                    public void close() {}

                    @Override
                    public void snapshotsLoaded(int oldSnapshotsLoaded) {}
                };
            }
        }

        private class FlushInfoFileFlushEvent implements FileFlushEvent {

            @Override
            public void close() {
                // nothing
            }

            @Override
            public FlushEvent beginFlush(
                    long[] pageRefs,
                    PageSwapper swapper,
                    PageReferenceTranslator pageReferenceTranslator,
                    int pagesToFlush,
                    int mergedPages) {
                return FlushEvent.NULL;
            }

            @Override
            public FlushEvent beginFlush(
                    long pageRef, PageSwapper swapper, PageReferenceTranslator pageReferenceTranslator) {
                return FlushEvent.NULL;
            }

            @Override
            public void startFlush(int[][] translationTable) {}

            @Override
            public void reset() {}

            @Override
            public long ioPerformed() {
                return 0;
            }

            @Override
            public long limitedNumberOfTimes() {
                return 0;
            }

            @Override
            public long limitedMillis() {
                return 0;
            }

            @Override
            public long pagesFlushed() {
                return 0;
            }

            @Override
            public ChunkEvent startChunk(int[] chunk) {
                return new FlushInfoChunk();
            }

            @Override
            public void throttle(long recentlyCompletedIOs, long millis) {}

            @Override
            public void reportIO(int completedIOs) {}

            @Override
            public long localBytesWritten() {
                return 0;
            }
        }

        private class FlushInfoChunk extends FileFlushEvent.ChunkEvent {
            @Override
            public void chunkFlushed(
                    long notModifiedPages, long flushPerChunk, long buffersPerChunk, long mergesPerChunk) {
                var chunkInfo = new ChunkInfo(notModifiedPages, flushPerChunk, buffersPerChunk, mergesPerChunk);
                observedChunks.add(chunkInfo);
            }
        }

        private static class ChunkInfo {
            private final long notModifiedPages;
            private final long flushPerChunk;
            private final long buffersPerChunk;
            private final long mergesPerChunk;

            public long getNotModifiedPages() {
                return notModifiedPages;
            }

            public long getFlushPerChunk() {
                return flushPerChunk;
            }

            public long getBuffersPerChunk() {
                return buffersPerChunk;
            }

            public long getMergesPerChunk() {
                return mergesPerChunk;
            }

            ChunkInfo(long notModifiedPages, long flushPerChunk, long buffersPerChunk, long mergesPerChunk) {
                this.notModifiedPages = notModifiedPages;
                this.flushPerChunk = flushPerChunk;
                this.buffersPerChunk = buffersPerChunk;
                this.mergesPerChunk = mergesPerChunk;
            }
        }
    }

    private static class PageHorizonSettingPageCacheTracer extends DefaultPageCacheTracer {
        private final AtomicReference<PageList> pagesHolder;

        public PageHorizonSettingPageCacheTracer(AtomicReference<PageList> pagesHolder) {
            this.pagesHolder = pagesHolder;
        }

        @Override
        public PageCursorTracer createPageCursorTracer(String tag) {
            return new DefaultPageCursorTracer(this, tag) {
                @Override
                public PinEvent beginPin(boolean writeLock, long filePageId, PageSwapper swapper) {
                    return new HorizonPinEvent();
                }
            };
        }

        private class HorizonPinEvent implements PinEvent {
            @Override
            public void setCachePageId(long cachePageId) {
                PageList pageList = pagesHolder.get();
                long pageRef = pageList.deref((int) cachePageId);
                if (PageList.isWriteLocked(pageRef)) {
                    PageList.setPageHorizon(pageRef, 42);
                }
            }

            @Override
            public PinPageFaultEvent beginPageFault(long filePageId, PageSwapper pageSwapper) {
                return PinPageFaultEvent.NULL;
            }

            @Override
            public void hit() {}

            @Override
            public void noFault() {}

            @Override
            public void close() {}

            @Override
            public void snapshotsLoaded(int oldSnapshotsLoaded) {}
        }
    }

    private class MultiChunkSwapperFilePageSwapperFactory extends SingleFilePageSwapperFactory {
        MultiChunkSwapperFilePageSwapperFactory(PageCacheTracer pageCacheTracer) {
            super(MuninnPageCacheTest.this.fs, pageCacheTracer, EmptyMemoryTracker.INSTANCE);
        }

        @Override
        public PageSwapper createPageSwapper(
                Path file,
                int filePageSize,
                PageEvictionCallback onEviction,
                boolean createIfNotExist,
                boolean useDirectIO,
                IOController ioController,
                EvictionBouncer evictionBouncer,
                SwapperSet swappers)
                throws IOException {
            return new DelegatingPageSwapper(super.createPageSwapper(
                    file,
                    filePageSize,
                    onEviction,
                    createIfNotExist,
                    useDirectIO,
                    ioController,
                    evictionBouncer,
                    swappers)) {
                @Override
                public long write(
                        long startFilePageId,
                        long[] bufferAddresses,
                        int[] bufferLengths,
                        int length,
                        int totalAffectedPages)
                        throws IOException {
                    int flushedDataSize = 0;
                    for (int i = 0; i < length; i++) {
                        flushedDataSize += bufferLengths[i];
                    }
                    assertThat(totalAffectedPages * filePageSize)
                            .describedAs(
                                    "Number of affected pages multiplied by page size should be equal to size of buffers we want to flush")
                            .isEqualTo(flushedDataSize);
                    return super.write(startFilePageId, bufferAddresses, bufferLengths, length, totalAffectedPages);
                }
            };
        }
    }

    private static class SingleVersionContextSupplier implements VersionContextSupplier {
        private final TestVersionContext versionContext;

        SingleVersionContextSupplier(TestVersionContext versionContext) {
            this.versionContext = versionContext;
        }

        @Override
        public void init(
                TransactionIdSnapshotFactory transactionIdSnapshotFactory,
                OldestTransactionIdFactory oldestTransactionIdFactory) {}

        @Override
        public VersionContext createVersionContext() {
            return versionContext;
        }
    }

    @Test
    void touchShouldLoadPagesIntoPageCache() throws Exception {
        DefaultPageCacheTracer tracer = new DefaultPageCacheTracer(true);
        var contextFactory = new CursorContextFactory(tracer, EMPTY_CONTEXT_SUPPLIER);
        getPageCache(fs, 1000, tracer);
        Path file = file("a");
        int toTouch = 128;
        var fileSize = toTouch * 4;
        generateFile(file, fileSize);

        try (var pf = map(file, filePageSize)) {
            pf.touch(0, toTouch, NULL_CONTEXT);

            var faultsAfterTouch = tracer.faults();
            try (var context = contextFactory.create("testTouch");
                    var cursor = pf.io(0, PF_SHARED_READ_LOCK, context)) {
                for (int i = 0; i < toTouch; i++) {
                    cursor.next(i);
                    int valueInPage;
                    do {
                        valueInPage = cursor.getInt();
                    } while (cursor.shouldRetry());
                    assertThat(valueInPage).isEqualTo(i);
                }
            }
            assertThat(tracer.faults()).isEqualTo(faultsAfterTouch);
        }
    }

    @Test
    void touchMustNotGrowFile() throws Exception {
        DefaultPageCacheTracer tracer = new DefaultPageCacheTracer(true);
        getPageCache(fs, 1000, tracer);
        Path file = file("a");
        int toTouch = 128;
        var fileSize = toTouch * 4;
        generateFile(file, fileSize);

        var sizeBefore = fs.getFileSize(file);
        try (var pf = map(file, filePageSize)) {
            assertThat(pf.touch(fileSize - 1, toTouch, NULL_CONTEXT)).isEqualTo(1);

            try (var cursor = pf.io(fileSize, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                cursor.next();
            }
            pf.flushAndForce(FileFlushEvent.NULL);
        }
        assertThat(fs.getFileSize(file)).isEqualTo(sizeBefore + filePageSize);
    }

    @Test
    void touchShouldReportFaults() throws Exception {
        DefaultPageCacheTracer tracer = new DefaultPageCacheTracer(true);
        var contextFactory = new CursorContextFactory(tracer, EMPTY_CONTEXT_SUPPLIER);
        getPageCache(fs, 1000, tracer);
        Path file = file("a");
        int toTouch = 128;
        var fileSize = toTouch * 4;
        generateFile(file, fileSize);

        var initialFaults = tracer.faults();

        try (var pf = map(file, filePageSize)) {
            try (var context = contextFactory.create("testTouch")) {
                assertThat(pf.touch(0, toTouch, context)).isEqualTo(toTouch);
            }
            assertThat(tracer.faults()).isEqualTo(initialFaults + toTouch);
            assertThat(tracer.vectoredFaults()).isEqualTo(1);

            // touched the same pages, no more faults
            try (var context = contextFactory.create("testTouch")) {
                assertThat(pf.touch(0, toTouch, context)).isEqualTo(toTouch);
            }
            assertThat(tracer.faults()).isEqualTo(initialFaults + toTouch);
            assertThat(tracer.vectoredFaults()).isEqualTo(2);

            // touched new pages
            try (var context = contextFactory.create("testTouch")) {
                assertThat(pf.touch(toTouch, toTouch, context)).isEqualTo(toTouch);
            }
            assertThat(tracer.faults()).isEqualTo(initialFaults + toTouch * 2);
            assertThat(tracer.vectoredFaults()).isEqualTo(3);

            // touched pages at the end
            try (var context = contextFactory.create("testTouch")) {
                assertThat(pf.touch(fileSize - toTouch / 2, toTouch, context)).isEqualTo(toTouch / 2);
            }
            assertThat(tracer.faults()).isEqualTo(initialFaults + toTouch * 2 + toTouch / 2);
            assertThat(tracer.vectoredFaults()).isEqualTo(4);
        }
    }

    @Test
    void touchWhenSomePagesAlreadyLoaded() throws Exception {
        DefaultPageCacheTracer tracer = new DefaultPageCacheTracer(true);
        var contextFactory = new CursorContextFactory(tracer, EMPTY_CONTEXT_SUPPLIER);
        getPageCache(fs, 1000, tracer);
        Path file = file("a");
        int toTouch = 128;
        var fileSize = toTouch * 4;
        generateFile(file, fileSize);

        var initialFaults = tracer.faults();

        try (var pf = map(file, filePageSize)) {

            try (var cursor = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                cursor.next(10);
                cursor.next(16);
                cursor.next(37);
            }

            try (var context = contextFactory.create("testTouch")) {
                assertThat(pf.touch(0, toTouch, context)).isEqualTo(toTouch);
            }
            assertThat(tracer.faults()).isEqualTo(initialFaults + toTouch - 3);
        }
    }

    @Test
    void touchMoreThenLockStriping() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            DefaultPageCacheTracer tracer = new DefaultPageCacheTracer(true);
            var contextFactory = new CursorContextFactory(tracer, EMPTY_CONTEXT_SUPPLIER);
            getPageCache(fs, LatchMap.faultLockStriping * 2, tracer);
            Path file = file("a");

            int toTouch = LatchMap.faultLockStriping + 27;
            var fileSize = toTouch * 4;
            generateFile(file, fileSize);

            var initialFaults = tracer.faults();

            try (var pf = map(file, filePageSize)) {
                try (var context = contextFactory.create("testTouch")) {
                    assertThat(pf.touch(0, toTouch, context)).isEqualTo(toTouch);
                }
                assertThat(tracer.faults()).isEqualTo(initialFaults + toTouch);
            }
        });
    }

    @Test
    void touchMoreThenPageCacheCanFit() {
        assertTimeoutPreemptively(ofMillis(SHORT_TIMEOUT_MILLIS), () -> {
            getPageCache(fs, 200, NULL);
            Path file = file("a");

            int toTouch = 256;
            var fileSize = toTouch * 4;
            generateFile(file, fileSize);
            try (var pf = map(file, filePageSize)) {
                assertThatThrownBy(() -> pf.touch(0, toTouch, NULL_CONTEXT)).isInstanceOf(CacheLiveLockException.class);
                // after that we should be able to pin pages, i.e. locks and latches are released
                try (var cursor = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                    for (int i = fileSize; i > 0; i--) {
                        assertThat(cursor.next()).isTrue();
                    }
                }
            }
        });
    }

    @RepeatedTest(50)
    void racePageFileTouchAndEviction() throws IOException {
        assumeTrue(fs.getClass() == EphemeralFileSystemAbstraction.class, "This test is very slow on real file system");
        var stopFlag = new AtomicBoolean(false);
        var pageSize = 8 + reservedBytes;
        var file = file("a");
        try (var tempPageCache = createPageCache(fs, 8, PageCacheTracer.NULL)) {
            generateFile(tempPageCache, file, 10, pageSize);
        }
        // use new instance of page cache so it's pages are allocated as part of the concurrent test
        try (var localPageCache = createPageCache(fs, 8, PageCacheTracer.NULL)) {
            assertTimeoutPreemptively(
                    ofMillis(SEMI_LONG_TIMEOUT_MILLIS),
                    () -> {
                        var race = new Race();
                        race.addContestant(Race.throwing(() -> {
                            while (!stopFlag.get()) {
                                try {
                                    try (var pagedFile = map(localPageCache, file, pageSize)) {
                                        pagedFile.touch(0, 8, NULL_CONTEXT);
                                    }
                                } catch (CacheLiveLockException ignore) {
                                }
                            }
                        }));
                        race.addContestant(Race.throwing(() -> {
                            try (var evictionRunEvent = PageCacheTracer.NULL.beginPageEvictions(1000)) {
                                localPageCache.evictPages(1000, 0, evictionRunEvent);
                            } finally {
                                stopFlag.set(true);
                            }
                        }));
                        race.go();
                        assertAllPagesEvicted(localPageCache);
                    },
                    () -> "PageCache: " + localPageCache.toString()
                            + " pages to evict to have all free: "
                            + localPageCache.tryGetNumberOfPagesToEvict((int) localPageCache.maxCachedPages())
                            + "\nObserved exception:\n"
                            + localPageCache.describePages());
        }
    }

    @RepeatedTest(50)
    void racePageFileTouchAndClose() throws IOException {
        assumeTrue(fs.getClass() == EphemeralFileSystemAbstraction.class, "This test is very slow on real file system");
        var pageSize = 8 + reservedBytes;
        var file = file("a");
        try (var tempPageCache = createPageCache(fs, 8, PageCacheTracer.NULL)) {
            generateFile(tempPageCache, file, 16, pageSize);
        }
        // use new instance of page cache so it's pages are allocated as part of the concurrent test
        try (var localPageCache = createPageCache(fs, 8, PageCacheTracer.NULL)) {
            var exceptionRef = new AtomicReference<Exception>();
            assertTimeoutPreemptively(
                    ofMillis(SEMI_LONG_TIMEOUT_MILLIS),
                    () -> {
                        var pagedFile = map(localPageCache, file, pageSize);
                        var race = new Race();
                        race.addContestant(Race.throwing(() -> {
                            try {
                                boolean firstPart = true;
                                for (int i = 0; i < 10000; i++) {
                                    pagedFile.touch(firstPart ? 0 : 8, 8, NULL_CONTEXT);
                                    firstPart = !firstPart;
                                }
                            } catch (CacheLiveLockException
                                    | FileIsNotMappedException
                                    | ClosedChannelException exception) {
                                exceptionRef.set(exception);
                            }
                        }));
                        race.addContestant(Race.throwing(pagedFile::close));
                        race.go();
                        assertAllPagesEvicted(localPageCache);
                    },
                    () -> "PageCache: " + localPageCache.toString()
                            + " pages to evict to have all free: "
                            + localPageCache.tryGetNumberOfPagesToEvict((int) localPageCache.maxCachedPages())
                            + "\nObserved exception:\n"
                            + (exceptionRef.get() != null ? Exceptions.stringify(exceptionRef.get()) : "none") + "\n"
                            + localPageCache.describePages());
        }
    }

    @RepeatedTest(50)
    void racePageFilePinAndClose() throws IOException {
        assumeTrue(fs.getClass() == EphemeralFileSystemAbstraction.class, "This test is very slow on real file system");
        var pageSize = 8 + reservedBytes;
        var file = file("a");
        try (var tempPageCache = createPageCache(fs, 8, PageCacheTracer.NULL)) {
            generateFile(tempPageCache, file, 16, pageSize);
        }
        // use new instance of page cache so it's pages are allocated as part of the concurrent test
        try (var localPageCache = createPageCache(fs, 8, PageCacheTracer.NULL)) {
            var exceptionRef = new AtomicReference<Exception>();
            assertTimeoutPreemptively(
                    ofMillis(SEMI_LONG_TIMEOUT_MILLIS),
                    () -> {
                        var pagedFile = map(localPageCache, file, pageSize);
                        var race = new Race();
                        race.addContestant(Race.throwing(() -> {
                            try {
                                for (int i = 0; i < 10000; i++) {
                                    try (var cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                                        while (cursor.next()) {}
                                    }
                                }
                            } catch (CacheLiveLockException
                                    | FileIsNotMappedException
                                    | ClosedChannelException exception) {
                                exceptionRef.set(exception);
                            }
                        }));
                        race.addContestant(Race.throwing(pagedFile::close));
                        race.go();
                        assertAllPagesEvicted(localPageCache);
                    },
                    () -> "PageCache: " + localPageCache.toString()
                            + " pages to evict to have all free: "
                            + localPageCache.tryGetNumberOfPagesToEvict((int) localPageCache.maxCachedPages())
                            + "\nObserved exception:\n"
                            + (exceptionRef.get() != null ? Exceptions.stringify(exceptionRef.get()) : "none") + "\n"
                            + localPageCache.describePages());
        }
    }

    private void assertAllPagesEvicted(MuninnPageCache pageCache) {
        // first wait for the background evictor to do its job, then evict the rest
        assertEventually(
                () -> pageCache.tryGetNumberOfPagesToEvict(pageCache.getKeepFree()),
                i -> i == -1,
                SHORT_TIMEOUT_MILLIS,
                TimeUnit.MILLISECONDS);
        var maxCachedPages = (int) pageCache.maxCachedPages();
        pageCache.evictPages(pageCache.tryGetNumberOfPagesToEvict(maxCachedPages), 0, EvictionRunEvent.NULL);
        assertThat(pageCache.tryGetNumberOfPagesToEvict(maxCachedPages)).isEqualTo(-1);
    }

    private static void checkAllPagesForZeroHorizon(int maxPages, MuninnPageCache pageCache) throws IOException {
        var pageRefs = LongLists.mutable.withInitialCapacity(maxPages);
        for (int i = 0; i < maxPages; i++) {
            lockAndCheckPage(pageCache, pageRefs);
        }
        pageRefs.forEach(pageRef -> pageCache.addFreePageToFreelist(pageRef, EvictionRunEvent.NULL));
    }

    private static void lockAndCheckPage(MuninnPageCache pageCache, MutableLongList pageRefs) throws IOException {
        while (true) {
            try {
                long pageRef = pageCache.grabFreeAndExclusivelyLockedPage(PinPageFaultEvent.NULL);
                assertEquals(0, getPageHorizon(pageRef));
                pageRefs.add(pageRef);
                return;
            }
            // we can be racing with evictor and throwing live lock exception in some rare cases, retry should be good
            // enough
            catch (CacheLiveLockException ignored) {
            }
        }
    }

    private void generateFile(Path file, int numberOfPages) throws IOException {
        generateFile(pageCache, file, numberOfPages, filePageSize);
    }

    private void generateFile(PageCache pageCache, Path file, int numberOfPages, int pageSize) throws IOException {
        try (var pf = map(pageCache, file, pageSize);
                var writer = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            for (int i = 0; i < numberOfPages; i++) {
                assertThat(writer.next()).isTrue();
                writer.putInt(i);
            }
        }
    }
}
