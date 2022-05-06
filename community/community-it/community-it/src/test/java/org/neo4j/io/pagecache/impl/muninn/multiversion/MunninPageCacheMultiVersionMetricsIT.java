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
package org.neo4j.io.pagecache.impl.muninn.multiversion;

import static org.eclipse.collections.api.factory.Sets.immutable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_WRITE_LOCK;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import org.eclipse.collections.api.set.ImmutableSet;
import org.junit.jupiter.api.Test;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCacheTestSupport;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCacheFixture;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;

public class MunninPageCacheMultiVersionMetricsIT extends PageCacheTestSupport<MuninnPageCache> {

    private final DefaultPageCacheTracer tracer = new DefaultPageCacheTracer();
    private final SingleThreadedTestContextFactory contextFactory = new SingleThreadedTestContextFactory(tracer);

    @Override
    protected Fixture<MuninnPageCache> createFixture() {
        return new MuninnPageCacheFixture().withReservedBytes(Long.BYTES * 3);
    }

    @Test
    void noReportedLoadedSnapshotsOnEmptyChain() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, tracer);
                var pageFile = map(pageCache, file("a"), pageCache.pageSize());
                var context = contextFactory.create("failToReadEmptyNonUpdatedPage");
                var pageCursor = pageFile.io(0, PF_SHARED_READ_LOCK, context)) {
            context.setWriteAndReadVersion(BASE_TX_ID);

            assertFalse(pageCursor.next());
        }

        assertEquals(0, tracer.snapshotsLoaded());
    }

    @Test
    void reportLoadOfEmptyPageSnapshot() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, tracer);
                var pageFile = map(pageCache, file("a"), pageCache.pageSize())) {
            try (var context = contextFactory.create("reportLoadOfEmptyPageSnapshot");
                    var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, context)) {

                context.setWriteAndReadVersion(10);
                assertTrue(mutator.next());
                mutator.putInt(10);
            }

            try (var context = contextFactory.create("reportLoadOfEmptyPageSnapshot");
                    var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, context)) {

                context.setWriteAndReadVersion(3);
                assertTrue(mutator.next());
                assertEquals(0, mutator.getInt());
            }
            assertEquals(1, tracer.snapshotsLoaded());
        }
    }

    @Test
    void reportLoadedSnapshotsOnChainLoad() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, tracer);
                var pageFile = map(pageCache, file("a"), pageCache.pageSize())) {
            try (var context = contextFactory.create("reportLoadedSnapshotsOnChainLoad");
                    var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, context)) {

                context.setWriteAndReadVersion(2);
                assertTrue(mutator.next());
                mutator.putInt(2);
            }
            try (var context = contextFactory.create("reportLoadedSnapshotsOnChainLoad");
                    var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, context)) {

                context.setWriteAndReadVersion(3);
                assertTrue(mutator.next());
                mutator.putInt(3);
            }
            try (var context = contextFactory.create("reportLoadedSnapshotsOnChainLoad");
                    var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, context)) {

                context.setWriteAndReadVersion(4);
                assertTrue(mutator.next());
                mutator.putInt(4);
            }

            long initialLoaded = tracer.snapshotsLoaded();
            try (var context = contextFactory.create("reportLoadedSnapshotsOnChainLoad");
                    var pageCursor = pageFile.io(0, PF_SHARED_READ_LOCK, context)) {
                context.setWriteAndReadVersion(2);

                assertTrue(pageCursor.next());
                assertEquals(2, pageCursor.getInt());
            }
            assertEquals(2 + initialLoaded, tracer.snapshotsLoaded());
        }
    }

    @Test
    void reportCopiedOnWritePages() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, tracer);
                var pageFile = map(pageCache, file("a"), pageCache.pageSize())) {
            try (var context = contextFactory.create("reportCopiedOnWritePages");
                    var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, context)) {

                context.setWriteAndReadVersion(10);
                assertTrue(mutator.next());
                mutator.putInt(10);
            }

            assertEquals(1, tracer.copiedPages());
        }
    }

    @Test
    void reportCopiedOnWriteMultiplePages() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, tracer);
                var pageFile = map(pageCache, file("a"), pageCache.pageSize())) {
            try (var context = contextFactory.create("reportCopiedOnWriteMultiplePages");
                    var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, context)) {

                context.setWriteAndReadVersion(10);
                assertTrue(mutator.next());
                mutator.putInt(10);
            }

            try (var context = contextFactory.create("reportCopiedOnWriteMultiplePages");
                    var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, context)) {

                context.setWriteAndReadVersion(20);
                assertTrue(mutator.next());
                mutator.putInt(20);
            }

            try (var context = contextFactory.create("reportCopiedOnWriteMultiplePages");
                    var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, context)) {

                context.setWriteAndReadVersion(30);
                assertTrue(mutator.next());
                mutator.putInt(30);
            }

            assertEquals(3, tracer.copiedPages());
        }
    }

    @Test
    void noCopiesWhenWriterHasTheSameVersion() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, tracer);
                var pageFile = map(pageCache, file("a"), pageCache.pageSize())) {
            try (var context = contextFactory.create("noCopiesWhenWriterHasTheSameVersion");
                    var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, context)) {

                context.setWriteAndReadVersion(10);
                assertTrue(mutator.next());
                mutator.putInt(10);
            }
            assertEquals(1, tracer.copiedPages());

            try (var context = contextFactory.create("noCopiesWhenWriterHasTheSameVersion");
                    var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, context)) {

                context.setWriteAndReadVersion(10);
                assertTrue(mutator.next());
                mutator.putInt(20);
            }
            try (var context = contextFactory.create("noCopiesWhenWriterHasTheSameVersion");
                    var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, context)) {

                context.setWriteAndReadVersion(10);
                assertTrue(mutator.next());
                mutator.putInt(30);
            }
            assertEquals(1, tracer.copiedPages());
        }
    }

    @Test
    void readersDoNotDoAnyCopies() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, tracer);
                var pageFile = map(pageCache, file("a"), pageCache.pageSize())) {
            try (var context = contextFactory.create("readersDoNotDoAnyCopies");
                    var reader = pageFile.io(0, PF_SHARED_READ_LOCK, context)) {
                context.setWriteAndReadVersion(10);
                assertFalse(reader.next());
            }
            try (var context = contextFactory.create("readersDoNotDoAnyCopies");
                    var reader = pageFile.io(0, PF_SHARED_READ_LOCK, context)) {
                context.setWriteAndReadVersion(20);
                assertFalse(reader.next());
            }
            try (var context = contextFactory.create("readersDoNotDoAnyCopies");
                    var reader = pageFile.io(0, PF_SHARED_READ_LOCK, context)) {
                context.setWriteAndReadVersion(30);
                assertFalse(reader.next());
            }
            assertEquals(0, tracer.copiedPages());

            try (var context = contextFactory.create("readersDoNotDoAnyCopies");
                    var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, context)) {

                context.setWriteAndReadVersion(10);
                assertTrue(mutator.next());
                mutator.putInt(30);
            }
            assertEquals(1, tracer.copiedPages());

            try (var context = contextFactory.create("readersDoNotDoAnyCopies");
                    var reader = pageFile.io(0, PF_SHARED_READ_LOCK, context)) {
                context.setWriteAndReadVersion(5);
                assertTrue(reader.next());
                assertEquals(0, reader.getInt());
            }
            try (var context = contextFactory.create("readersDoNotDoAnyCopies");
                    var reader = pageFile.io(0, PF_SHARED_READ_LOCK, context)) {
                context.setWriteAndReadVersion(20);
                assertTrue(reader.next());
                assertEquals(30, reader.getInt());
            }
            assertEquals(1, tracer.copiedPages());
        }
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
