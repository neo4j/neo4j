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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicBoolean;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.ImmutableSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.DelegatingPageSwapper;
import org.neo4j.io.pagecache.IOController;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCacheOpenOptions;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageEvictionCallback;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class MuninnPageCursorTest {
    @Inject
    private TestDirectory directory;

    @Inject
    private FileSystemAbstraction fs;

    private JobScheduler jobScheduler;
    private final LifeSupport life = new LifeSupport();

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
    void shouldUnlockLatchOnPageFaultingWhenConcurrentlyCursorClosed() throws IOException {
        // given
        Path file = directory.file("dude");
        createSomeData(file);

        // when
        var enableException = new AtomicBoolean(false);
        Runnable onReadAction = () -> {
            if (enableException.get()) {
                throw new RuntimeException();
            }
        };
        try (PageCache pageCache = startPageCache(customSwapper(defaultPageSwapperFactory(), onReadAction));
                PagedFile pagedFile = pageCache.map(
                        file,
                        PageCache.PAGE_SIZE,
                        DEFAULT_DATABASE_NAME,
                        Sets.immutable.of(StandardOpenOption.CREATE))) {
            try (PageCursor cursor = pagedFile.io(0, PagedFile.PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                enableException.set(true);
                assertThatThrownBy(cursor::next).isInstanceOf(RuntimeException.class);
            } finally {
                enableException.set(false);
            }

            // then hopefully the latch is not jammed. Assert that we can read normally from this new cursor.
            try (PageCursor cursor = pagedFile.io(0, PagedFile.PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                for (int i = 0; i < 100; i++) {
                    cursor.next(i);
                    for (int j = 0; j < 100; j++) {
                        assertEquals(j, cursor.getLong());
                    }
                }
            }
        }
    }

    @Test
    void byteOrder() throws IOException {
        testByteOrder(ByteOrder.LITTLE_ENDIAN);
        testByteOrder(ByteOrder.BIG_ENDIAN);
    }

    private void testByteOrder(ByteOrder byteOrder) throws IOException {
        Path file = directory.file("file" + byteOrder);
        try (PageCache pageCache = startPageCache(customSwapper(defaultPageSwapperFactory(), () -> {}))) {
            try (PagedFile pagedFile =
                    pageCache.map(file, PageCache.PAGE_SIZE, DEFAULT_DATABASE_NAME, getOpenOptions(byteOrder))) {
                // Write cursor
                try (PageCursor cursor = pagedFile.io(0, PagedFile.PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                    assertThat(cursor.getByteOrder()).isEqualTo(byteOrder);
                }
                // Read cursor
                try (PageCursor cursor = pagedFile.io(0, PagedFile.PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                    assertThat(cursor.getByteOrder()).isEqualTo(byteOrder);
                }
            }
        }
    }

    private static ImmutableSet<OpenOption> getOpenOptions(ByteOrder byteOrder) {
        if (byteOrder == ByteOrder.LITTLE_ENDIAN) {
            return Sets.immutable.of(StandardOpenOption.CREATE);
        }
        return Sets.immutable.of(StandardOpenOption.CREATE, PageCacheOpenOptions.BIG_ENDIAN);
    }

    private PageCache startPageCache(PageSwapperFactory pageSwapperFactory) {
        return new MuninnPageCache(pageSwapperFactory, jobScheduler, MuninnPageCache.config(1_000));
    }

    private void createSomeData(Path file) throws IOException {
        try (PageCache pageCache = startPageCache(defaultPageSwapperFactory());
                PagedFile pagedFile = pageCache.map(
                        file,
                        PageCache.PAGE_SIZE,
                        DEFAULT_DATABASE_NAME,
                        Sets.immutable.of(StandardOpenOption.CREATE));
                PageCursor cursor = pagedFile.io(0, PagedFile.PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            for (int i = 0; i < 100; i++) {
                cursor.next(i);
                for (int j = 0; j < 100; j++) {
                    cursor.putLong(j);
                }
            }
        }
    }

    private PageSwapperFactory customSwapper(PageSwapperFactory actual, Runnable onRead) {
        return new PageSwapperFactory() {
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
                PageSwapper actualSwapper = actual.createPageSwapper(
                        path,
                        filePageSize,
                        onEviction,
                        createIfNotExist,
                        useDirectIO,
                        ioController,
                        evictionBouncer,
                        swappers);
                return new DelegatingPageSwapper(actualSwapper) {
                    @Override
                    public long read(long filePageId, long bufferAddress) throws IOException {
                        onRead.run();
                        return super.read(filePageId, bufferAddress);
                    }
                };
            }
        };
    }

    private PageSwapperFactory defaultPageSwapperFactory() {
        return new SingleFilePageSwapperFactory(fs, new DefaultPageCacheTracer(), EmptyMemoryTracker.INSTANCE);
    }
}
