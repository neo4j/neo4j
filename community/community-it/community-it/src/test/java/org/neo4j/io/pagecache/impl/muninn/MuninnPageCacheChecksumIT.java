/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.io.pagecache.impl.muninn;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.collections.api.factory.Sets.immutable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_WRITE_LOCK;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import org.eclipse.collections.api.set.ImmutableSet;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.ChecksumMismatchException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCacheTestSupport;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.context.EmptyVersionContextSupplier;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;

public class MuninnPageCacheChecksumIT extends PageCacheTestSupport<MuninnPageCache> {

    private final DefaultPageCacheTracer tracer = new DefaultPageCacheTracer();
    private final CursorContextFactory contextFactory =
            new CursorContextFactory(tracer, EmptyVersionContextSupplier.EMPTY);
    private final AtomicBoolean checksumPagesFlag = new AtomicBoolean(true);

    @Override
    protected Fixture<MuninnPageCache> createFixture() {
        return new MuninnPageCacheFixture().withReservedBytes(Long.BYTES * 3);
    }

    @Test
    void checksumMultiVersionedPages() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, tracer)) {
            try (var pageFile = map(pageCache, file("a"), pageCache.pageSize());
                    var context = contextFactory.create("checksumMultiVersionedPages");
                    var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, context)) {

                assertTrue(mutator.next());
                mutator.putInt(10);
            }

            try (var pageFile = map(pageCache, file("a"), pageCache.pageSize());
                    var context = contextFactory.create("checksumMultiVersionedPages");
                    var reader = (MuninnPageCursor) pageFile.io(0, PF_SHARED_READ_LOCK, context)) {

                assertTrue(reader.next());
                assertEquals(10, reader.getInt());
                long pageChecksum = reader.getPageChecksum();
                assertThat(pageChecksum).isNotEqualTo(0);
            }
        }
    }

    @Test
    void anyByteChangeInfluencesChecksum() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, tracer)) {
            long previousChecksum = 0;
            for (int offset = 0; offset < pageCache.payloadSize(); offset++) {
                try (var pageFile = map(pageCache, file("a"), pageCache.pageSize());
                        var context = contextFactory.create("anyByteChangeInfluencesChecksum");
                        var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, context)) {

                    assertTrue(mutator.next());
                    mutator.putByte(offset, (byte) 1);
                }

                try (var pageFile = map(pageCache, file("a"), pageCache.pageSize());
                        var context = contextFactory.create("anyByteChangeInfluencesChecksum");
                        var reader = (MuninnPageCursor) pageFile.io(0, PF_SHARED_READ_LOCK, context)) {

                    assertTrue(reader.next());
                    assertEquals(1, reader.getByte(offset));
                    long pageChecksum = reader.getPageChecksum();
                    assertThat(pageChecksum).isNotEqualTo(0).isNotEqualTo(previousChecksum);
                    previousChecksum = pageChecksum;
                }
            }
        }
    }

    @Test
    void pageChecksumIsNotChangedByReaders() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, tracer)) {
            try (var pageFile = map(pageCache, file("a"), pageCache.pageSize());
                    var context = contextFactory.create("pageChecksumIsNotChangedByReaders");
                    var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, context)) {

                assertTrue(mutator.next());
                mutator.putInt(10);
            }

            long checksum = 0;
            for (int i = 0; i < 1024; i++) {
                try (var pageFile = map(pageCache, file("a"), pageCache.pageSize());
                        var context = contextFactory.create("pageChecksumIsNotChangedByReaders");
                        var reader = (MuninnPageCursor) pageFile.io(0, PF_SHARED_READ_LOCK, context)) {

                    assertTrue(reader.next());
                    assertEquals(10, reader.getInt());
                    long pageChecksum = reader.getPageChecksum();
                    if (checksum == 0) {
                        checksum = pageChecksum;
                    }
                    assertThat(pageChecksum).isNotEqualTo(0).isEqualTo(checksum);
                }
            }
        }
    }

    @Test
    void failToReadPageWithBrokenChecksum() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, tracer)) {
            try (var pageFile = map(pageCache, file("a"), pageCache.pageSize());
                    var context = contextFactory.create("failToReadPageWithBrokenChecksum");
                    var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, context)) {

                assertTrue(mutator.next());
                mutator.putInt(10);
            }

            // checksum is there and everything is "fine"
            try (var pageFile = map(pageCache, file("a"), pageCache.pageSize());
                    var context = contextFactory.create("failToReadPageWithBrokenChecksum");
                    var reader = (MuninnPageCursor) pageFile.io(0, PF_SHARED_READ_LOCK, context)) {

                assertTrue(reader.next());
                assertEquals(10, reader.getInt());
                long pageChecksum = reader.getPageChecksum();
                assertThat(pageChecksum).isNotEqualTo(0);
            }

            // write new content to the page while keeping same checksum
            checksumPagesFlag.set(false);
            try (var pageFile = map(pageCache, file("a"), pageCache.pageSize());
                    var context = contextFactory.create("failToReadPageWithBrokenChecksum");
                    var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, context)) {

                assertTrue(mutator.next());
                mutator.putInt(20);
            }

            // enable checksums again to have reader that will fail reading that page
            checksumPagesFlag.set(true);
            try (var pageFile = map(pageCache, file("a"), pageCache.pageSize());
                    var context = contextFactory.create("failToReadPageWithBrokenChecksum");
                    var reader = (MuninnPageCursor) pageFile.io(0, PF_SHARED_READ_LOCK, context)) {

                var e = assertThrows(Exception.class, () -> assertTrue(reader.next()));
                assertThat(e).isInstanceOf(ChecksumMismatchException.class);
            }
        }
    }

    @Test
    void neverWrittenPagesHaveZeroChecksum() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, tracer)) {
            try (var pageFile = map(pageCache, file("a"), pageCache.pageSize());
                    var context = contextFactory.create("neverWrittenPagesHaveZeroChecksum");
                    var mutator = pageFile.io(10, PF_SHARED_WRITE_LOCK, context)) {

                assertTrue(mutator.next());
                mutator.putInt(10);
            }

            try (var pageFile = map(pageCache, file("a"), pageCache.pageSize());
                    var context = contextFactory.create("neverWrittenPagesHaveZeroChecksum");
                    var reader = (MuninnPageCursor) pageFile.io(0, PF_SHARED_READ_LOCK, context)) {

                assertTrue(reader.next());
                assertEquals(0, reader.getInt());
                long pageChecksum = reader.getPageChecksum();
                assertThat(pageChecksum).isEqualTo(0);
            }

            try (var pageFile = map(pageCache, file("a"), pageCache.pageSize());
                    var context = contextFactory.create("neverWrittenPagesHaveZeroChecksum");
                    var reader = (MuninnPageCursor) pageFile.io(10, PF_SHARED_READ_LOCK, context)) {

                assertTrue(reader.next());
                assertEquals(10, reader.getInt());
                long pageChecksum = reader.getPageChecksum();
                assertThat(pageChecksum).isNotEqualTo(0);
            }
        }
    }

    @Test
    void fileFlushUpdatingPageChecksums() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, tracer)) {

            try (var pageFile = map(pageCache, file("a"), pageCache.pageSize());
                    var context = contextFactory.create("fileFlushUpdatingPageChecksums")) {

                for (int i = 0; i < 1000; i++) {
                    try (var mutator = pageFile.io(i, PF_SHARED_WRITE_LOCK, context)) {
                        assertTrue(mutator.next());
                        mutator.putInt(i);
                    }
                }
                pageFile.flushAndForce();
            }

            try (var pageFile = map(pageCache, file("a"), pageCache.pageSize());
                    var context = contextFactory.create("fileFlushUpdatingPageChecksums")) {
                for (int i = 0; i < 1000; i++) {
                    try (var reader = (MuninnPageCursor) pageFile.io(i, PF_SHARED_READ_LOCK, context)) {
                        assertTrue(reader.next());
                        assertEquals(i, reader.getInt());
                        assertThat(reader.getPageChecksum()).isNotEqualTo(0);
                    }
                }
            }
        }
    }

    @Test
    void fileSwapInOutUpdatingPageChecksums() throws IOException {
        try (var pageCache = createPageCache(fs, 10, tracer)) {

            try (var pageFile = map(pageCache, file("a"), pageCache.pageSize());
                    var context = contextFactory.create("fileSwapInOutUpdatingPageChecksums")) {

                for (int i = 0; i < 1000; i++) {
                    try (var mutator = pageFile.io(i, PF_SHARED_WRITE_LOCK, context)) {
                        assertTrue(mutator.next());
                        mutator.putInt(i);
                    }
                }
            }

            try (var pageFile = map(pageCache, file("a"), pageCache.pageSize());
                    var context = contextFactory.create("fileSwapInOutUpdatingPageChecksums")) {
                for (int i = 0; i < 1000; i++) {
                    try (var reader = (MuninnPageCursor) pageFile.io(i, PF_SHARED_READ_LOCK, context)) {
                        assertTrue(reader.next());
                        assertEquals(i, reader.getInt());
                        assertThat(reader.getPageChecksum()).isNotEqualTo(0);
                    }
                }
            }
        }
    }

    @Override
    protected MuninnPageCache createPageCache(FileSystemAbstraction fs, int maxPages, PageCacheTracer tracer) {
        SingleFilePageSwapperFactory swapperFactory = createDefaultPageSwapperFactory(fs, tracer);
        PageSwapperFactory pageSwapperFactory =
                (path,
                        filePageSize,
                        reservedPageBytes,
                        onEviction,
                        createIfNotExist,
                        useDirectIO,
                        preallocateStoreFiles,
                        checksumPages,
                        ioController,
                        swappers) -> swapperFactory.createPageSwapper(
                        path,
                        filePageSize,
                        reservedPageBytes,
                        onEviction,
                        createIfNotExist,
                        useDirectIO,
                        preallocateStoreFiles,
                        checksumPagesFlag.get(),
                        ioController,
                        swappers);
        return super.createPageCache(pageSwapperFactory, maxPages, tracer);
    }

    protected PagedFile map(PageCache pageCache, Path file, int filePageSize) throws IOException {
        return map(pageCache, file, filePageSize, immutable.empty());
    }

    protected PagedFile map(PageCache pageCache, Path file, int filePageSize, ImmutableSet<OpenOption> options)
            throws IOException {
        return pageCache.map(file, filePageSize, DEFAULT_DATABASE_NAME, options);
    }

    protected PagedFile map(Path file, int filePageSize) throws IOException {
        return map(pageCache, file, filePageSize, immutable.empty());
    }

    protected PagedFile map(Path file, int filePageSize, ImmutableSet<OpenOption> options) throws IOException {
        return map(pageCache, file, filePageSize, options);
    }
}
