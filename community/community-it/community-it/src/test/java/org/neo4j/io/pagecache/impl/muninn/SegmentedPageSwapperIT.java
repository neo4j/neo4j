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

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.io.async.AsyncBlockAccessor.EMPTY_ASYNC_BLOCK_ACCESSOR;
import static org.neo4j.io.pagecache.PageCache.PAGE_SIZE;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_WRITE_LOCK;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.tracing.FileFlushEvent.NULL;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.eclipse.collections.api.factory.Sets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOController;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCacheOpenOptions.SegmentedOpenOption;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageEvictionCallback;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.muninn.swapper.PageSwapper;
import org.neo4j.io.pagecache.impl.muninn.swapper.PageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.swapper.SegmentedPageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.swapper.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.DummyPageSwapper;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.SegmentEvent;
import org.neo4j.io.pagecache.tracing.version.FileTruncateEvent;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class SegmentedPageSwapperIT {
    private static final int PAGES_PER_SEGMENT = 4;

    @Inject
    private TestDirectory directory;

    @Inject
    private FileSystemAbstraction fs;

    private JobScheduler jobScheduler;
    private final LifeSupport life = new LifeSupport();
    private final SwapperSet swapperSet = new SwapperSet();

    @BeforeEach
    void start() {
        jobScheduler = JobSchedulerFactory.createScheduler();
        life.add(jobScheduler);
        life.start();
    }

    @AfterEach
    void stop() {
        life.shutdown();
    }

    @Test
    void singleSegmentPageFile() throws IOException {
        Path baseFile = directory.file("a");
        try (PageCache pageCache = newPageCache()) {
            try (PagedFile pagedFile = mapSegmented(pageCache, baseFile, CREATE)) {
                try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                    for (int p = 0; p < PAGES_PER_SEGMENT; p++) {
                        assertTrue(cursor.next(p));
                        cursor.putLong(0xC0FFEEL + p);
                    }
                }
                assertThat(pagedFile.getLastPageId()).isEqualTo(PAGES_PER_SEGMENT - 1);
                pagedFile.flushAndForce(NULL, EMPTY_ASYNC_BLOCK_ACCESSOR);
            }

            assertThat(fs.fileExists(baseFile)).isTrue();
            assertThat(fs.getFileSize(baseFile)).isEqualTo((long) PAGES_PER_SEGMENT * pageCache.pageSize());
            assertThat(fs.fileExists(segment(baseFile, 1))).isFalse();

            try (PagedFile pagedFile = mapSegmented(pageCache, baseFile);
                    PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                assertThat(pagedFile.getLastPageId()).isEqualTo(PAGES_PER_SEGMENT - 1);
                for (int p = 0; p < PAGES_PER_SEGMENT - 1; p++) {
                    cursor.next(p);
                    long value;
                    do {
                        value = cursor.getLong();
                    } while (cursor.shouldRetry());
                    assertThat(value).as("page %d", p).isEqualTo(0xC0FFEEL + p);
                }
            }
        }
    }

    @Test
    void multiSegmentPageFile() throws IOException {
        Path baseFile = directory.file("data");
        int totalPages = 3 * PAGES_PER_SEGMENT;
        try (PageCache pageCache = newPageCache()) {
            try (PagedFile pagedFile = mapSegmented(pageCache, baseFile, CREATE)) {
                try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                    for (int p = 0; p < totalPages; p++) {
                        assertTrue(cursor.next(p));
                        cursor.putLong(0xC0FFEEL + p);
                    }
                }
                assertThat(pagedFile.getLastPageId()).isEqualTo(totalPages - 1L);
                pagedFile.flushAndForce(NULL, EMPTY_ASYNC_BLOCK_ACCESSOR);
            }

            // Segment files on disk: data, data.1, data.2
            assertThat(fs.fileExists(baseFile)).isTrue();
            assertThat(fs.getFileSize(baseFile)).isEqualTo((long) PAGES_PER_SEGMENT * pageCache.pageSize());

            assertThat(fs.fileExists(segment(baseFile, 1))).isTrue();
            assertThat(fs.getFileSize(segment(baseFile, 1))).isEqualTo((long) PAGES_PER_SEGMENT * pageCache.pageSize());

            assertThat(fs.fileExists(segment(baseFile, 2))).isTrue();
            assertThat(fs.getFileSize(segment(baseFile, 2))).isEqualTo((long) PAGES_PER_SEGMENT * pageCache.pageSize());

            assertThat(fs.fileExists(segment(baseFile, 3))).isFalse();

            // Re-map and read everything back, including pages across segment boundaries.
            try (PagedFile pagedFile = mapSegmented(pageCache, baseFile);
                    PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                assertThat(pagedFile.getLastPageId()).isEqualTo(totalPages - 1L);
                for (int p = 0; p < totalPages; p++) {
                    cursor.next(p);
                    long value;
                    do {
                        value = cursor.getLong();
                    } while (cursor.shouldRetry());
                    assertThat(value).as("page %d", p).isEqualTo(0xC0FFEEL + p);
                }
            }
        }
    }

    @Test
    void segmentedFileConsumesSingleSwapperId() throws IOException {
        Path baseFile = directory.file("data");
        try (MuninnPageCache pageCache = newPageCache();
                PagedFile pagedFile = mapSegmented(pageCache, baseFile, CREATE)) {
            MuninnPagedFile muninnPagedFile = (MuninnPagedFile) pagedFile;
            SwapperSet swapperSet = pageCache.swapperSet();
            assertThat(swapperSet.getAllocation(muninnPagedFile.swapperId).swapper)
                    .isSameAs(muninnPagedFile.swapper);

            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                for (int p = 0; p < 3 * PAGES_PER_SEGMENT; p++) {
                    assertTrue(cursor.next(p));
                    cursor.putLong(p);
                }
            }
            pagedFile.flushAndForce(NULL, EMPTY_ASYNC_BLOCK_ACCESSOR);
            assertThat(fs.fileExists(segment(baseFile, 2))).isTrue();

            assertThat(swapperSet.allocate(new DummyPageSwapper("dummy", PAGE_SIZE)))
                    .isEqualTo(muninnPagedFile.swapperId + 1);
        }
    }

    @Test
    void singleFileWhenOptionAbsentBehavesUnchanged() throws IOException {
        Path file = directory.file("legacy");
        try (PageCache pageCache = newPageCache()) {
            try (PagedFile pagedFile = pageCache.map(
                            new StoreFile(file), PAGE_SIZE, DEFAULT_DATABASE_NAME, Sets.immutable.of(CREATE));
                    PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                cursor.next(7);
                cursor.putLong(42L);
            }

            assertThat(fs.fileExists(file.resolveSibling("legacy"))).isTrue();
            assertThat(fs.fileExists(file.resolveSibling("legacy.1"))).isFalse();
        }
    }

    @Test
    void writingHighPageCreatesIntermediateSegmentFiles() throws IOException {
        Path baseFile = directory.file("high");
        int targetPage = 2 * PAGES_PER_SEGMENT + 1; // lands in segment 2

        try (PageCache pageCache = newPageCache();
                PagedFile pagedFile = mapSegmented(pageCache, baseFile, CREATE)) {
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                cursor.next(targetPage);
                cursor.putLong(0xABCD);
            }
            pagedFile.flushAndForce(NULL, EMPTY_ASYNC_BLOCK_ACCESSOR);

            assertThat(fs.fileExists(baseFile)).isTrue();
            assertThat(fs.fileExists(segment(baseFile, 1))).isTrue();
            assertThat(fs.fileExists(segment(baseFile, 2))).isTrue();
            assertThat(fs.fileExists(segment(baseFile, 3))).isFalse();
            assertThat(pagedFile.getLastPageId()).isEqualTo(targetPage);
        }
    }

    @Test
    void getLastPageIdReflectsHighestWrittenPageAcrossSegments() throws IOException {
        Path baseFile = directory.file("highpage");
        int targetPage = 3 * PAGES_PER_SEGMENT + 2;

        try (PageCache pageCache = newPageCache();
                PagedFile pagedFile = mapSegmented(pageCache, baseFile, CREATE)) {
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                cursor.next(targetPage);
                cursor.putLong(1L);
            }
            pagedFile.flushAndForce(NULL, EMPTY_ASYNC_BLOCK_ACCESSOR);
            assertThat(pagedFile.getLastPageId()).isEqualTo(targetPage);
        }

        try (PageCache pageCache = newPageCache();
                PagedFile pagedFile = mapSegmented(pageCache, baseFile)) {
            assertThat(pagedFile.getLastPageId()).isEqualTo(targetPage);
        }
    }

    @Test
    void truncateExistingRemovesTrailingSegmentFiles() throws IOException {
        Path baseFile = directory.file("truncate");
        int totalPages = 3 * PAGES_PER_SEGMENT;

        try (PageCache pageCache = newPageCache();
                PagedFile pagedFile = mapSegmented(pageCache, baseFile, CREATE)) {
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                for (int p = 0; p < totalPages; p++) {
                    cursor.next(p);
                    cursor.putLong(p);
                }
            }
            pagedFile.flushAndForce(NULL, EMPTY_ASYNC_BLOCK_ACCESSOR);
        }
        assertThat(fs.fileExists(segment(baseFile, 1))).isTrue();
        assertThat(fs.fileExists(segment(baseFile, 2))).isTrue();

        try (PageCache pageCache = newPageCache();
                PagedFile pagedFile = mapSegmented(pageCache, baseFile, CREATE, TRUNCATE_EXISTING)) {
            assertThat(fs.fileExists(baseFile)).isTrue();
            assertThat(fs.fileExists(segment(baseFile, 1))).isFalse();
            assertThat(fs.fileExists(segment(baseFile, 2))).isFalse();
            assertThat(pagedFile.getLastPageId()).isNegative();
        }
    }

    @Test
    void reopenDiscoversExistingSegmentFiles() throws IOException {
        Path baseFile = directory.file("reopen");
        int totalPages = 2 * PAGES_PER_SEGMENT + 1;

        try (PageCache pageCache = newPageCache();
                PagedFile pagedFile = mapSegmented(pageCache, baseFile, CREATE);
                PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            for (int p = 0; p < totalPages; p++) {
                cursor.next(p);
                cursor.putLong(p * 13L);
            }
            pagedFile.flushAndForce(NULL, EMPTY_ASYNC_BLOCK_ACCESSOR);
        }

        try (PageCache pageCache = newPageCache();
                PagedFile pagedFile = mapSegmented(pageCache, baseFile);
                PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
            assertThat(pagedFile.getLastPageId()).isEqualTo(totalPages - 1L);
            for (int p = 0; p < totalPages; p++) {
                cursor.next(p);
                long value;
                do {
                    value = cursor.getLong();
                } while (cursor.shouldRetry());
                assertThat(value).as("page %d after re-open", p).isEqualTo(p * 13L);
            }
        }
    }

    @Test
    void flushAndForceFlushesAllSegmentFiles() throws IOException {
        Path baseFile = directory.file("flushall");
        int totalPages = 3 * PAGES_PER_SEGMENT;

        try (PageCache pageCache = newPageCache();
                PagedFile pagedFile = mapSegmented(pageCache, baseFile, CREATE)) {
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                for (int p = 0; p < totalPages; p++) {
                    cursor.next(p);
                    cursor.putLong(0xF0F0L + p);
                }
            }
            pagedFile.flushAndForce(NULL, EMPTY_ASYNC_BLOCK_ACCESSOR);

            for (int idx = 0; idx < 3; idx++) {
                Path seg = segment(baseFile, idx);
                long size = fs.getFileSize(seg);
                assertThat(size)
                        .as("segment %d (%s) size", idx, seg)
                        .isGreaterThanOrEqualTo((long) PAGES_PER_SEGMENT * PAGE_SIZE);
            }
        }
    }

    @Test
    void concurrentWritersAcrossSegmentsDoNotCorruptData() throws Exception {
        Path baseFile = directory.file("concurrent");
        int writers = 4;
        int pagesPerWriter = 3 * PAGES_PER_SEGMENT;
        int totalPages = writers * pagesPerWriter;

        try (var executor = Executors.newFixedThreadPool(writers);
                PageCache pageCache = newPageCache();
                PagedFile pagedFile = mapSegmented(pageCache, baseFile, CREATE)) {

            // Ensure the file has enough pages so concurrent next(pageId) calls don't all race
            // to grow the same segment from the same starting point.
            try (PageCursor seedCursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                seedCursor.next(totalPages - 1);
            }

            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();
            for (int w = 0; w < writers; w++) {
                final int writerId = w;
                futures.add(executor.submit(() -> {
                    try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                        start.await();
                        for (int p = 0; p < pagesPerWriter; p++) {
                            int page = writerId * pagesPerWriter + p;
                            cursor.next(page);
                            cursor.putLong(0, ((long) writerId << 32) | p);
                        }
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }

            pagedFile.flushAndForce(NULL, EMPTY_ASYNC_BLOCK_ACCESSOR);

            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                for (int w = 0; w < writers; w++) {
                    for (int p = 0; p < pagesPerWriter; p++) {
                        int page = w * pagesPerWriter + p;
                        cursor.next(page);
                        assertThat(getLong(cursor))
                                .as("writer=%d page=%d", w, page)
                                .isEqualTo(((long) w << 32) | p);
                    }
                }
            }
        }
    }

    @Test
    void getLastPageIdLazilyOpensLastSegment() throws IOException {
        Path baseFile = directory.file("lazy-lastpage");
        int targetPage = 2 * PAGES_PER_SEGMENT + 3; // segment 2

        try (PageCache pageCache = newPageCache();
                PagedFile pagedFile = mapSegmented(pageCache, baseFile, CREATE);
                PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            cursor.next(targetPage);
            cursor.putLong(0x42);
            pagedFile.flushAndForce(NULL, EMPTY_ASYNC_BLOCK_ACCESSOR);
        }

        try (PageCache pageCache = newPageCache();
                PagedFile pagedFile = mapSegmented(pageCache, baseFile)) {
            assertThat(pagedFile.getLastPageId()).isEqualTo(targetPage);
        }
    }

    @Test
    void dataInExpectedSegmentPages() throws IOException {
        Path baseFile = directory.file("placement");

        int pageInSegment0 = 0;
        int pageInSegment1 = PAGES_PER_SEGMENT;
        int pageInSegment2 = 2 * PAGES_PER_SEGMENT;
        int pageInSegment3 = 3 * PAGES_PER_SEGMENT;
        long data0 = 0xAAAA_0000_0000_0001L;
        long data1 = 0xBBBB_0000_0000_0002L;
        long data2 = 0xCCCC_0000_0000_0003L;
        long data3 = 0xDDDD_0000_0000_0004L;

        try (PageCache pageCache = newPageCache();
                PagedFile pagedFile = mapSegmented(pageCache, baseFile, CREATE);
                PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            cursor.next(pageInSegment0);
            cursor.putLong(data0);
            cursor.next(pageInSegment1);
            cursor.putLong(data1);
            cursor.next(pageInSegment2);
            cursor.putLong(data2);
            cursor.next(pageInSegment3);
            cursor.putLong(data3);
            pagedFile.flushAndForce(NULL, EMPTY_ASYNC_BLOCK_ACCESSOR);
        }

        assertSegmentContainsMarkerAtPage(segment(baseFile, 0), 0, data0);
        assertSegmentContainsMarkerAtPage(segment(baseFile, 1), 0, data1);
        assertSegmentContainsMarkerAtPage(segment(baseFile, 2), 0, data2);
        assertSegmentContainsMarkerAtPage(segment(baseFile, 3), 0, data3);
    }

    @Test
    void writeToMiddleSegmentDoesNotLeakIntoOtherSegments() throws IOException {
        Path baseFile = directory.file("middle-only");
        int pageInSegment2 = 2 * PAGES_PER_SEGMENT + 1;
        long marker = 0x42L;

        try (PageCache pageCache = newPageCache();
                PagedFile pagedFile = mapSegmented(pageCache, baseFile, CREATE);
                PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            cursor.next(pageInSegment2);
            cursor.putLong(marker);
            pagedFile.flushAndForce(NULL, EMPTY_ASYNC_BLOCK_ACCESSOR);
        }

        assertSegmentContainsMarkerAtPage(segment(baseFile, 2), 1, marker);

        assertSegmentIsAllZeros(segment(baseFile, 0));
        assertSegmentIsAllZeros(segment(baseFile, 1));
    }

    @Test
    void growingAcrossSegmentsCreatesSegmentsLazilyAndIncrementally() throws IOException {
        Path baseFile = directory.file("incremental-grow");

        try (PageCache pageCache = newPageCache();
                PagedFile pagedFile = mapSegmented(pageCache, baseFile, CREATE)) {
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                cursor.next(1);
                cursor.putLong(1L);
            }
            pagedFile.flushAndForce(NULL, EMPTY_ASYNC_BLOCK_ACCESSOR);
            assertThat(fs.fileExists(segment(baseFile, 0))).isTrue();
            assertThat(fs.fileExists(segment(baseFile, 1)))
                    .as("segment 1 not yet needed")
                    .isFalse();

            // Extend into segment 1.
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                cursor.next(PAGES_PER_SEGMENT);
                cursor.putLong(2L);
            }
            pagedFile.flushAndForce(NULL, EMPTY_ASYNC_BLOCK_ACCESSOR);
            assertThat(fs.fileExists(segment(baseFile, 1))).isTrue();
            assertThat(fs.fileExists(segment(baseFile, 2)))
                    .as("segment 2 not yet needed")
                    .isFalse();

            // Extend into segment 2.
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                cursor.next(2 * PAGES_PER_SEGMENT);
                cursor.putLong(3L);
            }
            pagedFile.flushAndForce(NULL, EMPTY_ASYNC_BLOCK_ACCESSOR);
            assertThat(fs.fileExists(segment(baseFile, 2))).isTrue();
            assertThat(fs.fileExists(segment(baseFile, 3)))
                    .as("segment 3 not yet needed")
                    .isFalse();
        }

        assertSegmentContainsMarkerAtPage(segment(baseFile, 0), 1, 1L);
        assertSegmentContainsMarkerAtPage(segment(baseFile, 1), 0, 2L);
        assertSegmentContainsMarkerAtPage(segment(baseFile, 2), 0, 3L);
    }

    @Test
    void truncateLastSegment() throws IOException {
        Path baseFile = directory.file("trunc-last");

        try (PageCache pageCache = newPageCache();
                PagedFile pagedFile = mapSegmented(pageCache, baseFile, CREATE)) {

            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                for (int p = 0; p < 3 * PAGES_PER_SEGMENT; p++) {
                    cursor.next(p);
                    cursor.putLong(0xAAAA + p);
                }
            }
            pagedFile.flushAndForce(NULL, EMPTY_ASYNC_BLOCK_ACCESSOR);

            assertThat(fs.fileExists(segment(baseFile, 1))).isTrue();
            assertThat(fs.fileExists(segment(baseFile, 2))).isTrue();
            assertThat(fs.getFileSize(segment(baseFile, 2))).isEqualTo(PAGES_PER_SEGMENT * pageCache.pageSize());

            pagedFile.truncate(3 * PAGES_PER_SEGMENT - 1, FileTruncateEvent.NULL);

            assertThat(fs.fileExists(segment(baseFile, 2))).isTrue();
            assertThat(fs.getFileSize(segment(baseFile, 2))).isEqualTo((PAGES_PER_SEGMENT - 1) * pageCache.pageSize());
        }
    }

    @Test
    void truncateExistingThenRegrowReusesSegmentsCorrectly() throws IOException {
        Path baseFile = directory.file("trunc-regrow");

        try (PageCache pageCache = newPageCache();
                PagedFile pagedFile = mapSegmented(pageCache, baseFile, CREATE);
                PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            for (int p = 0; p < 3 * PAGES_PER_SEGMENT; p++) {
                cursor.next(p);
                cursor.putLong(0xAAAA + p);
            }
            pagedFile.flushAndForce(NULL, EMPTY_ASYNC_BLOCK_ACCESSOR);
        }
        assertThat(fs.fileExists(segment(baseFile, 1))).isTrue();
        assertThat(fs.fileExists(segment(baseFile, 2))).isTrue();

        long freshMarker = 0xDEADBEEFL;
        int pageInSegment2 = 2 * PAGES_PER_SEGMENT + 3;
        try (PageCache pageCache = newPageCache();
                PagedFile pagedFile = mapSegmented(pageCache, baseFile, CREATE, TRUNCATE_EXISTING)) {
            assertThat(fs.fileExists(segment(baseFile, 1))).isFalse();
            assertThat(fs.fileExists(segment(baseFile, 2))).isFalse();

            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                cursor.next(pageInSegment2);
                cursor.putLong(freshMarker);
            }
            pagedFile.flushAndForce(NULL, EMPTY_ASYNC_BLOCK_ACCESSOR);
        }

        assertThat(fs.fileExists(segment(baseFile, 1))).isTrue();
        assertThat(fs.fileExists(segment(baseFile, 2))).isTrue();
        assertSegmentContainsMarkerAtPage(segment(baseFile, 2), pageInSegment2 % PAGES_PER_SEGMENT, freshMarker);
        assertSegmentIsAllZeros(segment(baseFile, 0));
        assertSegmentIsAllZeros(segment(baseFile, 1));
    }

    @Test
    void truncateKeepingPagesWithinFirstSegmentDeletesTrailingSegments() throws IOException {
        Path baseFile = directory.file("trunc-partial-first");
        int totalPages = 3 * PAGES_PER_SEGMENT;
        int pagesToKeep = 3;

        try (PageCache pageCache = newPageCache();
                PagedFile pagedFile = mapSegmented(pageCache, baseFile, CREATE)) {
            writePages(pagedFile, totalPages);
            pagedFile.flushAndForce(NULL, EMPTY_ASYNC_BLOCK_ACCESSOR);
            assertThat(fs.fileExists(segment(baseFile, 1))).isTrue();
            assertThat(fs.fileExists(segment(baseFile, 2))).isTrue();

            pagedFile.truncate(pagesToKeep, FileTruncateEvent.NULL);
            pagedFile.flushAndForce(NULL, EMPTY_ASYNC_BLOCK_ACCESSOR);

            assertThat(pagedFile.getLastPageId()).isEqualTo(pagesToKeep - 1L);
            assertThat(fs.fileExists(segment(baseFile, 1)))
                    .as("trailing segment 1 must be removed")
                    .isFalse();
            assertThat(fs.fileExists(segment(baseFile, 2)))
                    .as("trailing segment 2 must be removed")
                    .isFalse();
            assertThat(fs.getFileSize(baseFile)).isEqualTo((long) pagesToKeep * pageCache.pageSize());
            assertPagesContainMarkers(pagedFile, pagesToKeep);
        }
    }

    @Test
    void truncateAtSegmentBoundaryKeepsFullSegmentsAndDropsTrailing() throws IOException {
        Path baseFile = directory.file("trunc-boundary");
        int totalPages = 3 * PAGES_PER_SEGMENT;
        int pagesToKeep = 2 * PAGES_PER_SEGMENT;

        try (PageCache pageCache = newPageCache();
                PagedFile pagedFile = mapSegmented(pageCache, baseFile, CREATE)) {
            writePages(pagedFile, totalPages);
            pagedFile.flushAndForce(NULL, EMPTY_ASYNC_BLOCK_ACCESSOR);

            pagedFile.truncate(pagesToKeep, FileTruncateEvent.NULL);
            pagedFile.flushAndForce(NULL, EMPTY_ASYNC_BLOCK_ACCESSOR);

            assertThat(pagedFile.getLastPageId()).isEqualTo(pagesToKeep - 1L);
            assertThat(fs.fileExists(segment(baseFile, 0))).isTrue();
            assertThat(fs.fileExists(segment(baseFile, 1))).isTrue();
            assertThat(fs.fileExists(segment(baseFile, 2)))
                    .as("segment 2 sits entirely past the kept range")
                    .isFalse();
            assertThat(fs.getFileSize(segment(baseFile, 0))).isEqualTo((long) PAGES_PER_SEGMENT * pageCache.pageSize());
            assertThat(fs.getFileSize(segment(baseFile, 1))).isEqualTo((long) PAGES_PER_SEGMENT * pageCache.pageSize());
            assertPagesContainMarkers(pagedFile, pagesToKeep);
        }
    }

    @Test
    void truncateInsideMiddleSegmentTruncatesThatSegmentAndDropsTrailing() throws IOException {
        Path baseFile = directory.file("trunc-middle");
        int totalPages = 3 * PAGES_PER_SEGMENT;
        int pagesToKeep = PAGES_PER_SEGMENT + 2;
        int keptPagesInLastSegment = pagesToKeep - PAGES_PER_SEGMENT;

        try (PageCache pageCache = newPageCache();
                PagedFile pagedFile = mapSegmented(pageCache, baseFile, CREATE)) {
            writePages(pagedFile, totalPages);
            pagedFile.flushAndForce(NULL, EMPTY_ASYNC_BLOCK_ACCESSOR);

            pagedFile.truncate(pagesToKeep, FileTruncateEvent.NULL);
            pagedFile.flushAndForce(NULL, EMPTY_ASYNC_BLOCK_ACCESSOR);

            assertThat(pagedFile.getLastPageId()).isEqualTo(pagesToKeep - 1L);
            assertThat(fs.fileExists(segment(baseFile, 0))).isTrue();
            assertThat(fs.fileExists(segment(baseFile, 1))).isTrue();
            assertThat(fs.fileExists(segment(baseFile, 2)))
                    .as("trailing segment 2 must be removed")
                    .isFalse();
            assertThat(fs.getFileSize(segment(baseFile, 0))).isEqualTo((long) PAGES_PER_SEGMENT * pageCache.pageSize());
            assertThat(fs.getFileSize(segment(baseFile, 1)))
                    .as("partially kept segment is truncated to the kept page count")
                    .isEqualTo((long) keptPagesInLastSegment * pageCache.pageSize());
            assertPagesContainMarkers(pagedFile, pagesToKeep);
        }
    }

    @Test
    void truncateToZeroPagesEmptiesFile() throws IOException {
        Path baseFile = directory.file("trunc-zero");
        int totalPages = 3 * PAGES_PER_SEGMENT;

        try (PageCache pageCache = newPageCache();
                PagedFile pagedFile = mapSegmented(pageCache, baseFile, CREATE)) {
            writePages(pagedFile, totalPages);
            pagedFile.flushAndForce(NULL, EMPTY_ASYNC_BLOCK_ACCESSOR);
            assertThat(fs.fileExists(segment(baseFile, 0))).isTrue();
            assertThat(fs.fileExists(segment(baseFile, 1))).isTrue();
            assertThat(fs.fileExists(segment(baseFile, 2))).isTrue();
            assertThat(fs.getFileSize(segment(baseFile, 2))).isPositive();

            pagedFile.truncate(0, FileTruncateEvent.NULL);
            pagedFile.flushAndForce(NULL, EMPTY_ASYNC_BLOCK_ACCESSOR);

            assertThat(pagedFile.getLastPageId()).isNegative();
            assertThat(fs.fileExists(segment(baseFile, 0))).isTrue();
            assertThat(fs.getFileSize(segment(baseFile, 0))).isZero();
            assertThat(fs.fileExists(segment(baseFile, 1))).isFalse();
            assertThat(fs.fileExists(segment(baseFile, 2))).isFalse();
        }
    }

    @Test
    void truncateKeepingMorePagesThanExistIsNoOp() throws IOException {
        Path baseFile = directory.file("trunc-noop");
        int totalPages = 2 * PAGES_PER_SEGMENT - 1;

        try (PageCache pageCache = newPageCache();
                PagedFile pagedFile = mapSegmented(pageCache, baseFile, CREATE)) {
            writePages(pagedFile, totalPages);
            pagedFile.flushAndForce(NULL, EMPTY_ASYNC_BLOCK_ACCESSOR);
            assertThat(fs.fileExists(segment(baseFile, 0))).isTrue();
            assertThat(fs.fileExists(segment(baseFile, 1))).isTrue();
            assertThat(fs.fileExists(segment(baseFile, 2))).isFalse();

            pagedFile.truncate(10L * PAGES_PER_SEGMENT, FileTruncateEvent.NULL);

            assertThat(pagedFile.getLastPageId()).isEqualTo(totalPages - 1L);
            assertThat(fs.fileExists(segment(baseFile, 0))).isTrue();
            assertThat(fs.fileExists(segment(baseFile, 1))).isTrue();
            assertThat(fs.fileExists(segment(baseFile, 2))).isFalse();
            assertPagesContainMarkers(pagedFile, totalPages);
        }
    }

    @Test
    void truncateThenRegrowRecreatesSegmentsWithFreshData() throws IOException {
        Path baseFile = directory.file("trunc-regrow-live");
        int totalPages = 3 * PAGES_PER_SEGMENT;
        int pagesToKeep = PAGES_PER_SEGMENT + 1;
        int regrowPage = 2 * PAGES_PER_SEGMENT + 2;
        long freshMarker = 0xFEEDL;

        try (PageCache pageCache = newPageCache();
                PagedFile pagedFile = mapSegmented(pageCache, baseFile, CREATE)) {
            writePages(pagedFile, totalPages);
            pagedFile.flushAndForce(NULL, EMPTY_ASYNC_BLOCK_ACCESSOR);
            assertThat(fs.fileExists(segment(baseFile, 0))).isTrue();
            assertThat(fs.fileExists(segment(baseFile, 1))).isTrue();
            assertThat(fs.fileExists(segment(baseFile, 2))).isTrue();

            pagedFile.truncate(pagesToKeep, FileTruncateEvent.NULL);
            assertThat(fs.fileExists(segment(baseFile, 2))).isFalse();

            try (PageCursor cursor = pagedFile.io(regrowPage, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                cursor.putLong(freshMarker);
            }
            pagedFile.flushAndForce(NULL, EMPTY_ASYNC_BLOCK_ACCESSOR);

            assertThat(pagedFile.getLastPageId()).isEqualTo(regrowPage);
            assertThat(fs.fileExists(segment(baseFile, 1))).isTrue();
            assertThat(fs.fileExists(segment(baseFile, 2))).isTrue();

            assertPagesContainMarkers(pagedFile, pagesToKeep);
            try (PageCursor cursor = pagedFile.io(pagesToKeep, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next());
                assertThat(getLong(cursor)).isZero();

                assertTrue(cursor.next(regrowPage));
                assertThat(getLong(cursor)).isEqualTo(freshMarker);
            }
        }
    }

    private static long getLong(PageCursor cursor) throws IOException {
        long value;
        do {
            value = cursor.getLong(0);
        } while (cursor.shouldRetry());
        return value;
    }

    @Test
    void truncateExistingEmptiesSingleSegmentFile() throws IOException {
        Path baseFile = directory.file("trunc-existing-single");

        try (PageCache pageCache = newPageCache();
                PagedFile pagedFile = mapSegmented(pageCache, baseFile, CREATE)) {
            writePages(pagedFile, PAGES_PER_SEGMENT);
            pagedFile.flushAndForce(NULL, EMPTY_ASYNC_BLOCK_ACCESSOR);
        }
        assertThat(fs.fileExists(baseFile)).isTrue();
        assertThat(fs.fileExists(segment(baseFile, 1))).isFalse();

        try (PageCache pageCache = newPageCache();
                PagedFile pagedFile = mapSegmented(pageCache, baseFile, CREATE, TRUNCATE_EXISTING)) {
            assertThat(fs.fileExists(baseFile)).isTrue();
            assertThat(pagedFile.getLastPageId()).isNegative();
        }
    }

    @Test
    void truncateExistingRemovesAllTrailingSegments() throws IOException {
        Path baseFile = directory.file("trunc-existing-many");
        int totalPages = 5 * PAGES_PER_SEGMENT;

        try (PageCache pageCache = newPageCache();
                PagedFile pagedFile = mapSegmented(pageCache, baseFile, CREATE)) {
            writePages(pagedFile, totalPages);
            pagedFile.flushAndForce(NULL, EMPTY_ASYNC_BLOCK_ACCESSOR);
        }
        for (int idx = 1; idx <= 4; idx++) {
            assertThat(fs.fileExists(segment(baseFile, idx)))
                    .as("segment %d present before truncate", idx)
                    .isTrue();
        }

        try (PageCache pageCache = newPageCache();
                PagedFile pagedFile = mapSegmented(pageCache, baseFile, CREATE, TRUNCATE_EXISTING)) {
            assertThat(fs.fileExists(baseFile)).isTrue();
            for (int idx = 1; idx <= 4; idx++) {
                assertThat(fs.fileExists(segment(baseFile, idx)))
                        .as("trailing segment %d removed by truncate", idx)
                        .isFalse();
            }
            assertThat(pagedFile.getLastPageId()).isNegative();
        }
    }

    @Test
    void truncateExistingResetsSegmentZeroContent() throws IOException {
        Path baseFile = directory.file("trunc-existing-reset");

        try (PageCache pageCache = newPageCache();
                PagedFile pagedFile = mapSegmented(pageCache, baseFile, CREATE)) {
            writePages(pagedFile, PAGES_PER_SEGMENT);
            pagedFile.flushAndForce(NULL, EMPTY_ASYNC_BLOCK_ACCESSOR);
        }

        long freshMarker = 0xDEADBEEFL;
        try (PageCache pageCache = newPageCache();
                PagedFile pagedFile = mapSegmented(pageCache, baseFile, CREATE, TRUNCATE_EXISTING)) {
            // Old content is gone: page 0 reads as zero after truncate.
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next(0));
                assertThat(getLong(cursor))
                        .as("segment 0 must be reset by truncate")
                        .isZero();
            }

            // Re-grow within segment 0 produces fresh, isolated data.
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next(1));
                cursor.putLong(freshMarker);
            }
            pagedFile.flushAndForce(NULL, EMPTY_ASYNC_BLOCK_ACCESSOR);
            assertThat(pagedFile.getLastPageId()).isEqualTo(1L);
        }

        assertSegmentContainsMarkerAtPage(segment(baseFile, 0), 1, freshMarker);
        assertThat(fs.fileExists(segment(baseFile, 1))).isFalse();
    }

    @Test
    void truncateExistingRegrowsAcrossSegmentsWithFreshData() throws IOException {
        Path baseFile = directory.file("trunc-existing-regrow");
        int regrowPage = 2 * PAGES_PER_SEGMENT + 2; // back into segment 2
        long freshMarker = 0xFEEDL;

        try (PageCache pageCache = newPageCache();
                PagedFile pagedFile = mapSegmented(pageCache, baseFile, CREATE)) {
            writePages(pagedFile, 3 * PAGES_PER_SEGMENT);
            pagedFile.flushAndForce(NULL, EMPTY_ASYNC_BLOCK_ACCESSOR);
        }

        try (PageCache pageCache = newPageCache();
                PagedFile pagedFile = mapSegmented(pageCache, baseFile, CREATE, TRUNCATE_EXISTING)) {
            assertThat(fs.fileExists(segment(baseFile, 1))).isFalse();
            assertThat(fs.fileExists(segment(baseFile, 2))).isFalse();

            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                assertTrue(cursor.next(regrowPage));
                cursor.putLong(freshMarker);
            }
            pagedFile.flushAndForce(NULL, EMPTY_ASYNC_BLOCK_ACCESSOR);

            assertThat(pagedFile.getLastPageId()).isEqualTo(regrowPage);
            assertThat(fs.fileExists(segment(baseFile, 1))).isTrue();
            assertThat(fs.fileExists(segment(baseFile, 2))).isTrue();
        }

        assertSegmentContainsMarkerAtPage(segment(baseFile, 2), regrowPage % PAGES_PER_SEGMENT, freshMarker);
        // Segments below the re-grown page that the recreated file fills in are zeroed, not stale.
        assertSegmentIsAllZeros(segment(baseFile, 0));
        assertSegmentIsAllZeros(segment(baseFile, 1));
    }

    @Test
    void segmentCreateAndUnloadEventsFireOnGrowthAndClose() throws IOException {
        Path baseFile = directory.file("trace-grow");
        RecordingSegmentTracer tracer = new RecordingSegmentTracer();

        try (PageCache pageCache = newPageCache(tracer);
                PagedFile pagedFile = mapSegmented(pageCache, baseFile, CREATE)) {
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                for (int p = 0; p < 3 * PAGES_PER_SEGMENT; p++) {
                    cursor.next(p);
                    cursor.putLong(p);
                }
            }
            pagedFile.flushAndForce(NULL, EMPTY_ASYNC_BLOCK_ACCESSOR);
        }

        assertThat(tracer.events(baseFile, RecordingSegmentTracer.Kind.CREATE))
                .as("segment indices observed at create")
                .containsExactlyInAnyOrder(0, 1, 2);
        assertThat(tracer.events(baseFile, RecordingSegmentTracer.Kind.UNLOAD))
                .as("segment indices observed at unload")
                .containsExactlyInAnyOrder(0, 1, 2);
        assertThat(tracer.events(baseFile, RecordingSegmentTracer.Kind.LOAD)).isEmpty();
        assertThat(tracer.events(baseFile, RecordingSegmentTracer.Kind.DELETE)).isEmpty();
        assertThat(tracer.segmentsCreated()).isEqualTo(3);
        assertThat(tracer.segmentsUnloaded()).isEqualTo(3);
        assertThat(tracer.segmentsLoaded()).isZero();
        assertThat(tracer.segmentsDeleted()).isZero();
    }

    @Test
    void segmentLoadEventFiresWhenReopeningExistingSegment() throws IOException {
        Path baseFile = directory.file("trace-reopen");

        try (PageCache pageCache = newPageCache();
                PagedFile pagedFile = mapSegmented(pageCache, baseFile, CREATE)) {
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                for (int p = 0; p < 2 * PAGES_PER_SEGMENT + 1; p++) {
                    cursor.next(p);
                    cursor.putLong(p);
                }
            }
            pagedFile.flushAndForce(NULL, EMPTY_ASYNC_BLOCK_ACCESSOR);
        }

        RecordingSegmentTracer tracer = new RecordingSegmentTracer();
        try (PageCache pageCache = newPageCache(tracer);
                PagedFile pagedFile = mapSegmented(pageCache, baseFile);
                PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
            for (int p = 0; p < 2 * PAGES_PER_SEGMENT + 1; p++) {
                cursor.next(p);
                assertThat(getLong(cursor)).isEqualTo(p);
            }
        }

        assertThat(tracer.events(baseFile, RecordingSegmentTracer.Kind.LOAD))
                .as("segments loaded during re-open")
                .containsExactlyInAnyOrder(0, 1, 2);
        assertThat(tracer.events(baseFile, RecordingSegmentTracer.Kind.CREATE)).isEmpty();
        assertThat(tracer.segmentsLoaded()).isEqualTo(3);
        assertThat(tracer.segmentsCreated()).isZero();
    }

    @Test
    void segmentDeleteEventFiresOnTruncate() throws IOException {
        Path baseFile = directory.file("trace-truncate");

        try (PageCache pageCache = newPageCache();
                PagedFile pagedFile = mapSegmented(pageCache, baseFile, CREATE)) {
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                for (int p = 0; p < 3 * PAGES_PER_SEGMENT; p++) {
                    cursor.next(p);
                    cursor.putLong(p);
                }
            }
            pagedFile.flushAndForce(NULL, EMPTY_ASYNC_BLOCK_ACCESSOR);
        }

        RecordingSegmentTracer tracer = new RecordingSegmentTracer();
        try (PageCache pageCache = newPageCache(tracer);
                PagedFile pagedFile = mapSegmented(pageCache, baseFile, CREATE, TRUNCATE_EXISTING)) {
            assertThat(pagedFile.getLastPageId()).isNegative();
        }

        assertThat(tracer.events(baseFile, RecordingSegmentTracer.Kind.DELETE))
                .as("segments deleted by truncate")
                .containsExactlyInAnyOrder(1, 2);
        assertThat(tracer.segmentsDeleted()).isEqualTo(2);
    }

    @Test
    void closingMultiSegmentFileEvictsAllPagesAndFreesMemory() throws IOException {
        Path baseFile = directory.file("evict-on-close");
        int totalPages = 3 * PAGES_PER_SEGMENT;

        try (var pageCache = newPageCache()) {
            PagedFile pagedFile = mapSegmented(pageCache, baseFile, CREATE);
            writePages(pagedFile, totalPages);
            pagedFile.flushAndForce(NULL, EMPTY_ASYNC_BLOCK_ACCESSOR);

            assertThat(fs.fileExists(segment(baseFile, 0))).isTrue();
            assertThat(fs.fileExists(segment(baseFile, 1))).isTrue();
            assertThat(fs.fileExists(segment(baseFile, 2))).isTrue();

            assertThat(pageCache.freePages())
                    .as("pages from every opened segment are resident while mapped")
                    .isEqualTo(pageCache.maxCachedPages() - totalPages);

            pagedFile.close();

            assertThat(pageCache.freePages())
                    .as("all pages of all opened segments are evicted on close")
                    .isEqualTo(pageCache.maxCachedPages());
        }
    }

    @Test
    void closingFileEvictsPagesFaultedWhenReadingBackAllSegments() throws IOException {
        Path baseFile = directory.file("evict-after-read");
        int totalPages = 3 * PAGES_PER_SEGMENT;

        try (var pageCache = newPageCache();
                PagedFile pagedFile = mapSegmented(pageCache, baseFile, CREATE)) {
            writePages(pagedFile, totalPages);
            pagedFile.flushAndForce(NULL, EMPTY_ASYNC_BLOCK_ACCESSOR);
        }

        try (var pageCache = newPageCache()) {
            PagedFile pagedFile = mapSegmented(pageCache, baseFile);
            assertPagesContainMarkers(pagedFile, totalPages);

            assertThat(pageCache.freePages())
                    .as("re-read pages from every segment are resident while mapped")
                    .isEqualTo(pageCache.maxCachedPages() - totalPages);

            pagedFile.close();

            assertThat(pageCache.freePages())
                    .as("all re-read segment pages are evicted on close")
                    .isEqualTo(pageCache.maxCachedPages());
        }
    }

    @Test
    @EnabledOnOs(OS.LINUX)
    void allocateCreatesAsManySegmentsAsNeededToCoverRequestedSize() throws IOException {
        Path baseFile = directory.file("allocate-spanning");
        long maxSegmentBytes = (long) PAGES_PER_SEGMENT * PAGE_SIZE;
        long requestedSize = 2 * maxSegmentBytes + 2L * PAGE_SIZE;

        try (PageSwapper swapper = createSegmentedSwapper(baseFile)) {
            swapper.allocate(requestedSize);
            swapper.force();
        }

        assertThat(fs.fileExists(segment(baseFile, 0))).isTrue();
        assertThat(fs.getFileSize(segment(baseFile, 0))).isEqualTo(maxSegmentBytes);
        assertThat(fs.fileExists(segment(baseFile, 1))).isTrue();
        assertThat(fs.getFileSize(segment(baseFile, 1))).isEqualTo(maxSegmentBytes);
        assertThat(fs.fileExists(segment(baseFile, 2))).isTrue();
        assertThat(fs.getFileSize(segment(baseFile, 2))).isEqualTo(2L * PAGE_SIZE);
        assertThat(fs.fileExists(segment(baseFile, 3)))
                .as("no segment is created beyond the requested size")
                .isFalse();
    }

    @Test
    @EnabledOnOs(OS.LINUX)
    void allocateAtExactSegmentMultipleDoesNotCreateTrailingSegment() throws IOException {
        Path baseFile = directory.file("allocate-exact");
        long maxSegmentBytes = (long) PAGES_PER_SEGMENT * PAGE_SIZE;
        long requestedSize = 2 * maxSegmentBytes;

        try (PageSwapper swapper = createSegmentedSwapper(baseFile)) {
            swapper.allocate(requestedSize);
            swapper.force();
        }

        assertThat(fs.fileExists(segment(baseFile, 0))).isTrue();
        assertThat(fs.getFileSize(segment(baseFile, 0))).isEqualTo(maxSegmentBytes);
        assertThat(fs.fileExists(segment(baseFile, 1))).isTrue();
        assertThat(fs.getFileSize(segment(baseFile, 1))).isEqualTo(maxSegmentBytes);
        assertThat(fs.fileExists(segment(baseFile, 2)))
                .as("an exact multiple of the segment size must not allocate an empty trailing segment")
                .isFalse();
    }

    @Test
    void readRejectsNegativeFilePageId() throws IOException {
        Path baseFile = directory.file("read-negative");
        try (PageSwapper swapper = createSegmentedSwapper(baseFile)) {
            long buffer = allocateFilled(PAGE_SIZE, (byte) 0);
            try {
                assertThrows(IOException.class, () -> swapper.read(-1, buffer));
            } finally {
                freeBuffer(buffer, PAGE_SIZE);
            }
        }
    }

    @Test
    void readWithinSingleSegment() throws IOException {
        Path baseFile = directory.file("read-single-segment");
        int pages = PAGES_PER_SEGMENT - 1;
        try (PageSwapper swapper = createSegmentedSwapper(baseFile)) {
            writeRangeViaSwapper(swapper, 0, pages);

            long buffer = allocateFilled(PAGE_SIZE, (byte) 0xFF);
            try {
                assertThat(swapper.read(0, buffer)).isEqualTo(PAGE_SIZE);
                assertPageMarkers(buffer, 0, 1);
            } finally {
                freeBuffer(buffer, PAGE_SIZE);
            }
        }
    }

    @Test
    void readStartingInLaterSegment() throws IOException {
        Path baseFile = directory.file("read-later-segment");
        int totalPages = 3 * PAGES_PER_SEGMENT;
        int startPage = 2 * PAGES_PER_SEGMENT + 1;
        try (PageSwapper swapper = createSegmentedSwapper(baseFile)) {
            writeRangeViaSwapper(swapper, 0, totalPages);

            long buffer = allocateFilled(PAGE_SIZE, (byte) 0xFF);
            try {
                assertThat(swapper.read(startPage, buffer)).isEqualTo(PAGE_SIZE);
                assertPageMarkers(buffer, startPage, 1);
            } finally {
                freeBuffer(buffer, PAGE_SIZE);
            }
        }
    }

    @Test
    void readEntirelyPastEndOfFileReturnsZeroAndZeroFillsBuffer() throws IOException {
        Path baseFile = directory.file("read-past-eof");
        int pageBeyondData = 2;
        try (PageSwapper swapper = createSegmentedSwapper(baseFile)) {
            writeRangeViaSwapper(swapper, 0, 1);

            long buffer = allocateFilled(PAGE_SIZE * 2, (byte) 0xFF);
            try {
                assertThat(swapper.read(pageBeyondData, buffer)).isZero();
                assertPageIsZero(buffer, 0);
            } finally {
                freeBuffer(buffer, PAGE_SIZE);
            }
        }
    }

    @Test
    void readFromNotYetExistingSegmentReturnsZeroWithoutGrowingNewSegments() throws IOException {
        Path baseFile = directory.file("read-missing-segment");
        int pageInSegment2 = 2 * PAGES_PER_SEGMENT;
        try (PageSwapper swapper = createSegmentedSwapper(baseFile)) {
            writeRangeViaSwapper(swapper, 0, 1);
            assertThat(fs.getFileSize(baseFile)).isEqualTo(PAGE_SIZE);

            long buffer = allocateFilled(PAGE_SIZE, (byte) 0xFF);
            try {
                assertThat(swapper.read(pageInSegment2, buffer)).isZero();
                assertPageIsZero(buffer, 0);

                assertThat(fs.getFileSize(baseFile)).isEqualTo(PAGE_SIZE);
                assertThat(fs.fileExists(segment(baseFile, 1)))
                        .as("reading a missing segment must not create it")
                        .isFalse();
                assertThat(fs.fileExists(segment(baseFile, 2)))
                        .as("reading a missing segment must not create it")
                        .isFalse();
            } finally {
                freeBuffer(buffer, PAGE_SIZE);
            }
        }
    }

    @Test
    void readFromNotYetExistingPagesInExistingSegmentReturnsZeroWithoutGrowingSegment() throws IOException {
        Path baseFile = directory.file("read-resize-segment");
        int pageInSegment = PAGES_PER_SEGMENT - 1;
        try (PageSwapper swapper = createSegmentedSwapper(baseFile)) {
            writeRangeViaSwapper(swapper, 0, 1);
            assertThat(fs.getFileSize(baseFile)).isEqualTo(PAGE_SIZE);

            long buffer = allocateFilled(PAGE_SIZE, (byte) 0xFF);
            try {
                assertThat(swapper.read(pageInSegment, buffer)).isZero();
                assertPageIsZero(buffer, 0);
                assertThat(fs.fileExists(baseFile)).isTrue();
                assertThat(fs.getFileSize(baseFile)).isEqualTo(PAGE_SIZE);
            } finally {
                freeBuffer(buffer, PAGE_SIZE);
            }
        }
    }

    @Test
    void writeWithinSingleSegment() throws IOException {
        Path baseFile = directory.file("write-single-segment");
        int pages = PAGES_PER_SEGMENT - 1;
        try (PageSwapper swapper = createSegmentedSwapper(baseFile)) {
            writeRangeViaSwapper(swapper, 0, pages);
            swapper.force();
        }
        assertThat(fs.fileExists(segment(baseFile, 0))).isTrue();
        assertThat(fs.fileExists(segment(baseFile, 1))).isFalse();
        assertSegmentFilesContainMarkers(baseFile, 0, pages);
    }

    @Test
    void writeOverSegmentBoundary() throws IOException {
        Path baseFile = directory.file("write-over-boundary");
        int startPage = PAGES_PER_SEGMENT - 2;
        int pages = PAGES_PER_SEGMENT;
        try (PageSwapper swapper = createSegmentedSwapper(baseFile)) {
            writeRangeViaSwapper(swapper, startPage, pages);
            swapper.force();
        }
        assertThat(fs.fileExists(segment(baseFile, 0))).isTrue();
        assertThat(fs.fileExists(segment(baseFile, 1))).isTrue();
        assertSegmentFilesContainMarkers(baseFile, startPage, pages);
    }

    @Test
    void writeSpanningMultipleSegments() throws IOException {
        Path baseFile = directory.file("write-span-many");
        int pages = 3 * PAGES_PER_SEGMENT;
        try (PageSwapper swapper = createSegmentedSwapper(baseFile)) {
            writeRangeViaSwapper(swapper, 0, pages);
            swapper.force();
        }
        assertThat(fs.fileExists(segment(baseFile, 0))).isTrue();
        assertThat(fs.fileExists(segment(baseFile, 1))).isTrue();
        assertThat(fs.fileExists(segment(baseFile, 2))).isTrue();
        assertThat(fs.fileExists(segment(baseFile, 3))).isFalse();
        assertSegmentFilesContainMarkers(baseFile, 0, pages);
    }

    @Test
    void writeStartingInLaterSegmentCreatesIntermediateSegments() throws IOException {
        Path baseFile = directory.file("write-later-segment");
        int startPage = 2 * PAGES_PER_SEGMENT + 1;
        int pages = 2;
        try (PageSwapper swapper = createSegmentedSwapper(baseFile)) {
            writeRangeViaSwapper(swapper, startPage, pages);
            swapper.force();
        }
        assertThat(fs.fileExists(segment(baseFile, 0))).isTrue();
        assertThat(fs.fileExists(segment(baseFile, 1))).isTrue();
        assertThat(fs.fileExists(segment(baseFile, 2))).isTrue();
        assertSegmentFilesContainMarkers(baseFile, startPage, pages);
    }

    @Test
    void readVectoredBuffersWithinSingleSegment() throws IOException {
        Path baseFile = directory.file("read-vec-single-segment");
        int pages = PAGES_PER_SEGMENT - 1;
        try (PageSwapper swapper = createSegmentedSwapper(baseFile)) {
            writeRangeViaSwapper(swapper, 0, pages);
            assertVectoredReadMarkers(swapper, 0, new int[] {1, 1, 1});
        }
    }

    @Test
    void readVectoredBuffersAlignedToSegmentBoundary() throws IOException {
        Path baseFile = directory.file("read-vec-aligned-boundary");
        int startPage = PAGES_PER_SEGMENT - 2;
        try (PageSwapper swapper = createSegmentedSwapper(baseFile)) {
            writeRangeViaSwapper(swapper, startPage, PAGES_PER_SEGMENT);
            assertVectoredReadMarkers(swapper, startPage, new int[] {2, 2});
        }
        assertThat(fs.fileExists(segment(baseFile, 0))).isTrue();
        assertThat(fs.fileExists(segment(baseFile, 1))).isTrue();
    }

    @Test
    void readVectoredSingleBufferStraddlesSegmentBoundary() throws IOException {
        Path baseFile = directory.file("read-vec-straddle");
        int startPage = PAGES_PER_SEGMENT - 1;
        int straddlingPages = 3;
        try (PageSwapper swapper = createSegmentedSwapper(baseFile)) {
            writeRangeViaSwapper(swapper, startPage, straddlingPages);
            assertVectoredReadMarkers(swapper, startPage, new int[] {straddlingPages});
        }
    }

    @Test
    void readVectoredSpanningMultipleSegmentsWithMixedBufferSizes() throws IOException {
        Path baseFile = directory.file("read-vec-span-many");
        int totalPages = 3 * PAGES_PER_SEGMENT;
        try (PageSwapper swapper = createSegmentedSwapper(baseFile)) {
            writeRangeViaSwapper(swapper, 0, totalPages);
            assertVectoredReadMarkers(swapper, 0, new int[] {3, 4, 5});
        }
    }

    @Test
    void readVectoredStartingInLaterSegment() throws IOException {
        Path baseFile = directory.file("read-vec-later-segment");
        int totalPages = 3 * PAGES_PER_SEGMENT;
        int startPage = 2 * PAGES_PER_SEGMENT;
        try (PageSwapper swapper = createSegmentedSwapper(baseFile)) {
            writeRangeViaSwapper(swapper, 0, totalPages);
            assertVectoredReadMarkers(swapper, startPage, new int[] {2, 2});
        }
    }

    @Test
    void readVectoredPartiallyPastEndOfFileZeroFillsTail() throws IOException {
        Path baseFile = directory.file("read-vec-partial-eof");
        int writtenPages = 1;
        try (PageSwapper swapper = createSegmentedSwapper(baseFile)) {
            writeRangeViaSwapper(swapper, 0, writtenPages);

            int[] bufferPages = {1, 1, 1};
            long[] addresses = new long[bufferPages.length];
            int[] lengths = new int[bufferPages.length];
            for (int i = 0; i < bufferPages.length; i++) {
                lengths[i] = bufferPages[i] * PAGE_SIZE;
                addresses[i] = allocateFilled(lengths[i], (byte) 0xFF);
            }
            long[] baseAddresses = addresses.clone();
            int[] baseLengths = lengths.clone();
            try {
                assertThat(swapper.read(0, addresses, lengths, bufferPages.length))
                        .isEqualTo((long) writtenPages * PAGE_SIZE);
                assertPageMarkers(baseAddresses[0], 0, 1);
                assertPageIsZero(baseAddresses[1], 0);
                assertPageIsZero(baseAddresses[2], 0);
            } finally {
                for (int i = 0; i < baseAddresses.length; i++) {
                    freeBuffer(baseAddresses[i], baseLengths[i]);
                }
            }
        }
    }

    @Test
    void readVectoredFromNotYetExistingSegmentReturnsZeroWithoutGrowingStore() throws IOException {
        Path baseFile = directory.file("read-vec-missing-segment");
        int startPage = 2 * PAGES_PER_SEGMENT;
        try (PageSwapper swapper = createSegmentedSwapper(baseFile)) {
            writeRangeViaSwapper(swapper, 0, 1);

            int[] bufferPages = {1, 1};
            long[] addresses = new long[bufferPages.length];
            int[] lengths = new int[bufferPages.length];
            for (int i = 0; i < bufferPages.length; i++) {
                lengths[i] = bufferPages[i] * PAGE_SIZE;
                addresses[i] = allocateFilled(lengths[i], (byte) 0xFF);
            }
            long[] baseAddresses = addresses.clone();
            int[] baseLengths = lengths.clone();
            try {
                assertThat(swapper.read(startPage, addresses, lengths, bufferPages.length))
                        .isZero();
                assertPageIsZero(baseAddresses[0], 0);
                assertPageIsZero(baseAddresses[1], 0);

                assertThat(fs.fileExists(segment(baseFile, 1))).isFalse();
                assertThat(fs.fileExists(segment(baseFile, 2))).isFalse();
            } finally {
                for (int i = 0; i < baseAddresses.length; i++) {
                    freeBuffer(baseAddresses[i], baseLengths[i]);
                }
            }
        }
    }

    @Test
    void readVectoredReadPartialSegmentAndFailToGrow() throws IOException {
        Path baseFile = directory.file("read-vec-partial-missing");
        int startPage = PAGES_PER_SEGMENT - 3;
        try (PageSwapper swapper = createSegmentedSwapper(baseFile)) {
            writeRangeViaSwapper(swapper, 0, PAGES_PER_SEGMENT - 1);
            assertThat(fs.getFileSize(baseFile)).isEqualTo(PAGE_SIZE * (PAGES_PER_SEGMENT - 1));

            int[] bufferPages = {1, 1, 1, 1};
            long[] addresses = new long[bufferPages.length];
            int[] lengths = new int[bufferPages.length];
            for (int i = 0; i < bufferPages.length; i++) {
                lengths[i] = bufferPages[i] * PAGE_SIZE;
                addresses[i] = allocateFilled(lengths[i], (byte) 0xFF);
            }
            long[] baseAddresses = addresses.clone();
            int[] baseLengths = lengths.clone();
            try {
                assertThat(swapper.read(startPage, addresses, lengths, bufferPages.length))
                        .as("only the two pages that exist in segment 0 are read")
                        .isEqualTo(2L * PAGE_SIZE);
                assertPageMarkers(baseAddresses[0], startPage, 1);
                assertPageMarkers(baseAddresses[1], startPage + 1, 1);
                assertPageIsZero(baseAddresses[2], 0);
                assertPageIsZero(baseAddresses[3], 0);

                assertThat(fs.getFileSize(baseFile)).isEqualTo(PAGE_SIZE * (PAGES_PER_SEGMENT - 1));
                assertThat(fs.fileExists(segment(baseFile, 1)))
                        .as("read must not create the next segment")
                        .isFalse();
            } finally {
                for (int i = 0; i < baseAddresses.length; i++) {
                    freeBuffer(baseAddresses[i], baseLengths[i]);
                }
            }
        }
    }

    /**
     * Performs a vectored read into freshly allocated buffers whose page counts are given by {@code bufferPages}, and
     * asserts that every page holds the marker written for its logical page id. The buffer arrays are cloned before the
     * call because the swapper rewrites them in place when a buffer straddles a segment boundary.
     */
    private static void assertVectoredReadMarkers(PageSwapper swapper, int startPage, int[] bufferPages)
            throws IOException {
        long[] addresses = new long[bufferPages.length];
        int[] lengths = new int[bufferPages.length];
        long expectedBytes = 0;
        for (int i = 0; i < bufferPages.length; i++) {
            lengths[i] = bufferPages[i] * PAGE_SIZE;
            addresses[i] = allocateFilled(lengths[i], (byte) 0xFF);
            expectedBytes += lengths[i];
        }
        long[] baseAddresses = addresses.clone();
        int[] baseLengths = lengths.clone();
        try {
            assertThat(swapper.read(startPage, addresses, lengths, bufferPages.length))
                    .isEqualTo(expectedBytes);
            int page = startPage;
            for (int i = 0; i < bufferPages.length; i++) {
                assertPageMarkers(baseAddresses[i], page, bufferPages[i]);
                page += bufferPages[i];
            }
        } finally {
            for (int i = 0; i < baseAddresses.length; i++) {
                freeBuffer(baseAddresses[i], baseLengths[i]);
            }
        }
    }

    /**
     * Verifies that a contiguous range of logical pages written through the segmented swapper landed in the expected
     * segment file at the expected physical page. Each segment file is opened directly with a plain single-file swapper
     * so the assertion is independent of the segmented read path.
     */
    private void assertSegmentFilesContainMarkers(Path baseFile, int startPage, int pages) throws IOException {
        for (int p = 0; p < pages; p++) {
            int logicalPage = startPage + p;
            int segmentIndex = logicalPage / PAGES_PER_SEGMENT;
            int physicalPage = logicalPage % PAGES_PER_SEGMENT;
            assertSegmentFilePageMarker(baseFile, segmentIndex, physicalPage, marker(logicalPage));
        }
    }

    private void assertSegmentFilePageMarker(Path baseFile, int segmentIndex, int physicalPage, long expectedMarker)
            throws IOException {
        PageSwapperFactory factory =
                new SingleFilePageSwapperFactory(fs, PageCacheTracer.NULL, EmptyMemoryTracker.INSTANCE);
        try (PageSwapper swapper = factory.createPageSwapper(
                segment(baseFile, segmentIndex),
                PAGE_SIZE,
                NO_CALLBACK,
                false,
                false,
                0,
                IOController.DISABLED,
                EvictionBouncer.ALWAYS_ALLOW,
                swapperSet::allocate)) {
            long buffer = allocateFilled(PAGE_SIZE, (byte) 0xFF);
            try {
                swapper.read(physicalPage, buffer);
                assertThat(UnsafeUtil.getLong(buffer))
                        .as("segment %d physical page %d marker", segmentIndex, physicalPage)
                        .isEqualTo(expectedMarker);
            } finally {
                freeBuffer(buffer, PAGE_SIZE);
            }
        }
    }

    private static void writeRangeViaSwapper(PageSwapper swapper, int startPage, int pages) throws IOException {
        int length = pages * PAGE_SIZE;
        long buffer = allocateFilled(length, (byte) 0);
        try {
            for (int p = 0; p < pages; p++) {
                UnsafeUtil.putLong(buffer + (long) p * PAGE_SIZE, marker(startPage + p));
            }
            assertThat(swapper.write(startPage, buffer, length)).isEqualTo(length);
        } finally {
            freeBuffer(buffer, length);
        }
    }

    private static void assertPageMarkers(long buffer, int startPage, int pages) {
        for (int p = 0; p < pages; p++) {
            long actual = UnsafeUtil.getLong(buffer + (long) p * PAGE_SIZE);
            assertThat(actual).as("page %d marker", startPage + p).isEqualTo(marker(startPage + p));
        }
    }

    private static void assertPageIsZero(long buffer, int pageIndex) {
        long base = buffer + (long) pageIndex * PAGE_SIZE;
        for (int offset = 0; offset < PAGE_SIZE; offset += Long.BYTES) {
            assertThat(UnsafeUtil.getLong(base + offset))
                    .as("page %d offset %d must be zero-filled", pageIndex, offset)
                    .isZero();
        }
    }

    private static long allocateFilled(int bytes, byte fill) {
        long address = UnsafeUtil.allocateMemory(bytes, EmptyMemoryTracker.INSTANCE);
        UnsafeUtil.setMemory(address, bytes, fill);
        return address;
    }

    private static void freeBuffer(long address, int bytes) {
        UnsafeUtil.free(address, bytes, EmptyMemoryTracker.INSTANCE);
    }

    private PageSwapper createSegmentedSwapper(Path baseFile) throws IOException {
        PageSwapperFactory factory = new SegmentedPageSwapperFactory(
                new SingleFilePageSwapperFactory(fs, PageCacheTracer.NULL, EmptyMemoryTracker.INSTANCE),
                fs,
                PageCacheTracer.NULL);
        return factory.createPageSwapper(
                baseFile,
                PAGE_SIZE,
                NO_CALLBACK,
                true,
                false,
                PAGES_PER_SEGMENT,
                IOController.DISABLED,
                EvictionBouncer.ALWAYS_ALLOW,
                swapperSet::allocate);
    }

    private static final PageEvictionCallback NO_CALLBACK = (pageRef, filePageId) -> {};

    private static final class RecordingSegmentTracer extends DefaultPageCacheTracer {
        enum Kind {
            CREATE,
            LOAD,
            UNLOAD,
            DELETE
        }

        record Event(Path basePath, int segmentIndex, Kind kind) {}

        private final List<Event> events = new CopyOnWriteArrayList<>();

        @Override
        public SegmentEvent createSegment(Path basePath, int segmentIndex) {
            return chain(super.createSegment(basePath, segmentIndex), basePath, segmentIndex, Kind.CREATE);
        }

        @Override
        public SegmentEvent loadSegment(Path basePath, int segmentIndex) {
            return chain(super.loadSegment(basePath, segmentIndex), basePath, segmentIndex, Kind.LOAD);
        }

        @Override
        public SegmentEvent unloadSegment(Path basePath, int segmentIndex) {
            return chain(super.unloadSegment(basePath, segmentIndex), basePath, segmentIndex, Kind.UNLOAD);
        }

        @Override
        public SegmentEvent deleteSegment(Path basePath, int segmentIndex) {
            return chain(super.deleteSegment(basePath, segmentIndex), basePath, segmentIndex, Kind.DELETE);
        }

        private SegmentEvent chain(SegmentEvent delegate, Path basePath, int segmentIndex, Kind kind) {
            return () -> {
                delegate.close();
                events.add(new Event(basePath, segmentIndex, kind));
            };
        }

        List<Integer> events(Path basePath, Kind kind) {
            synchronized (events) {
                return events.stream()
                        .filter(e -> e.kind == kind && e.basePath.equals(basePath))
                        .map(Event::segmentIndex)
                        .toList();
            }
        }
    }

    private static long marker(int page) {
        return 0xC0FFEEL + page;
    }

    private static void writePages(PagedFile pagedFile, int pages) throws IOException {
        try (PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            for (int p = 0; p < pages; p++) {
                assertTrue(cursor.next(p));
                cursor.putLong(0, marker(p));
            }
        }
    }

    private static void assertPagesContainMarkers(PagedFile pagedFile, int pages) throws IOException {
        try (PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
            for (int p = 0; p < pages; p++) {
                assertTrue(cursor.next(p));
                assertThat(getLong(cursor)).as("page %d marker", p).isEqualTo(marker(p));
            }
        }
    }

    private void assertSegmentContainsMarkerAtPage(Path segmentFile, int page, long expectedMarker) throws IOException {
        try (PageCache pageCache = newPageCache();
                PagedFile pagedFile = pageCache.map(new StoreFile(segmentFile), DEFAULT_DATABASE_NAME);
                PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
            assertThat(cursor.next(page))
                    .as("segment file %s missing physical page %d", segmentFile, page)
                    .isTrue();
            assertThat(getLong(cursor))
                    .as("segment file %s user page %d marker", segmentFile, page)
                    .isEqualTo(expectedMarker);
        }
    }

    private void assertSegmentIsAllZeros(Path segmentFile) throws IOException {
        assertThat(fs.fileExists(segmentFile))
                .as("segment file %s must exist", segmentFile)
                .isTrue();
        try (PageCache pageCache = newPageCache();
                PagedFile pagedFile = pageCache.map(new StoreFile(segmentFile), DEFAULT_DATABASE_NAME);
                PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
            while (cursor.next()) {
                if (cursor.getCurrentPageId() == 0) {
                    continue;
                }
                int payloadSize = pagedFile.payloadSize();
                for (int offset = 0; offset < payloadSize; offset += Long.BYTES) {
                    long value;
                    do {
                        value = cursor.getLong(offset);
                    } while (cursor.shouldRetry());
                    assertThat(value)
                            .as("segment file %s, page %d, offset %d", segmentFile, cursor.getCurrentPageId(), offset)
                            .isEqualTo(0L);
                }
            }
        }
    }

    private PagedFile mapSegmented(PageCache pageCache, Path baseFile, StandardOpenOption... extra) throws IOException {
        var options = Sets.mutable.<OpenOption>of(new SegmentedOpenOption(PAGES_PER_SEGMENT));
        Collections.addAll(options, extra);
        return pageCache.map(new StoreFile(baseFile), PAGE_SIZE, DEFAULT_DATABASE_NAME, options.toImmutable());
    }

    private static Path segment(Path baseFile, int index) {
        if (index == 0) {
            return baseFile;
        }
        return baseFile.resolveSibling(baseFile.getFileName() + "." + index);
    }

    private MuninnPageCache newPageCache() {
        return new MuninnPageCache(fs, jobScheduler, MuninnPageCache.config(1_000));
    }

    private MuninnPageCache newPageCache(PageCacheTracer tracer) {
        return new MuninnPageCache(
                fs, jobScheduler, MuninnPageCache.config(1_000).pageCacheTracer(tracer));
    }
}
