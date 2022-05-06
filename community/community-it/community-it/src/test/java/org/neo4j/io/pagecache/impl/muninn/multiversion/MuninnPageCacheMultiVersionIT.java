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

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.collections.api.factory.Sets.immutable;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
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
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.eclipse.collections.api.set.ImmutableSet;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCacheTestSupport;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.context.VersionContext;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCacheFixture;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.util.concurrent.Futures;

public class MuninnPageCacheMultiVersionIT extends PageCacheTestSupport<MuninnPageCache> {
    private final DefaultPageCacheTracer tracer = new DefaultPageCacheTracer();
    private final SingleThreadedTestContextFactory contextFactory = new SingleThreadedTestContextFactory(tracer);

    // TODO: test page pins??

    @Override
    protected Fixture<MuninnPageCache> createFixture() {
        return new MuninnPageCacheFixture().withReservedBytes(Long.BYTES * 3);
    }

    @Test
    void readLatestPageWithoutWritesAndShouldRetry() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, tracer);
                var pageFile = map(pageCache, file("a"), pageCache.pageSize())) {
            try (var mutatorContext = contextFactory.create("readLatestPageWithoutWritesAndShouldRetry");
                    var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, mutatorContext)) {
                mutatorContext.setWriteAndReadVersion(BASE_TX_ID);

                assertTrue(mutator.next());
                mutator.putInt(42);
            }

            try (var readContext = contextFactory.create("readLatestPageWithoutWritesAndShouldRetry");
                    var reader = pageFile.io(0, PF_SHARED_READ_LOCK, readContext)) {
                readContext.setWriteAndReadVersion(BASE_TX_ID + 1);
                assertTrue(reader.next());
                assertEquals(42, reader.getInt());
                assertFalse(reader.shouldRetry());
            }
        }
    }

    @Test
    void failToReadEmptyNonUpdatedPage() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, tracer);
                var pageFile = map(pageCache, file("a"), pageCache.pageSize());
                var context = contextFactory.create("failToReadEmptyNonUpdatedPage");
                var pageCursor = pageFile.io(0, PF_SHARED_READ_LOCK, context)) {
            context.setWriteAndReadVersion(BASE_TX_ID);

            assertFalse(pageCursor.next());
        }
    }

    @Test
    void failToReadNonExistingPageVersionOnEmptyChain() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, tracer);
                var pageFile = map(pageCache, file("a"), pageCache.pageSize());
                var context = contextFactory.create("failToReadNonExistingPageVersionOnEmptyChain");
                var pageCursor = pageFile.io(0, PF_SHARED_READ_LOCK, context)) {
            context.setWriteAndReadVersion(100);

            assertFalse(pageCursor.next());
        }
    }

    @Test
    void readNonExistingPageVersionOnNonEmptyChain() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, tracer);
                var pageFile = map(pageCache, file("a"), pageCache.pageSize())) {
            try (var context = contextFactory.create("readNonExistingPageVersionOnNonEmptyChain");
                    var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, context)) {
                context.setWriteAndReadVersion(10);

                assertTrue(mutator.next());
                mutator.putInt(12);
            }

            try (var context = contextFactory.create("readNonExistingPageVersionOnNonEmptyChain");
                    var pageCursor = pageFile.io(0, PF_SHARED_READ_LOCK, context)) {
                context.setWriteAndReadVersion(4);

                assertTrue(pageCursor.next());
                for (int i = 0; i < pageCache.payloadSize(); i++) {
                    assertEquals(0, pageCursor.getByte());
                }
            }
        }
    }

    @Test
    void readCanObserveWholeVersionedPageContent() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, tracer);
                var pageFile = map(pageCache, file("a"), pageCache.pageSize())) {
            try (var context = contextFactory.create("readCanObserveWholeVersionedPageContent");
                    var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, context)) {
                context.setWriteAndReadVersion(17);

                assertTrue(mutator.next());

                for (int i = 0; i < pageCache.payloadSize(); i += Integer.BYTES) {
                    mutator.putInt(i);
                }
            }

            // and now reader of old version should observe whole page of zero's and new reader of the latest version
            // should be able to see the whole page of numbers
            try (var context = contextFactory.create("readCanObserveWholeVersionedPageContent");
                    var pageCursor = pageFile.io(0, PF_SHARED_READ_LOCK, context)) {
                context.setWriteAndReadVersion(4);

                assertTrue(pageCursor.next());
                for (int i = 0; i < pageCache.payloadSize(); i += Integer.BYTES) {
                    assertEquals(0, pageCursor.getInt());
                }
            }
            try (var context = contextFactory.create("readCanObserveWholeVersionedPageContent");
                    var pageCursor = pageFile.io(0, PF_SHARED_READ_LOCK, context)) {
                context.setWriteAndReadVersion(17);

                assertTrue(pageCursor.next());
                for (int i = 0; i < pageCache.payloadSize(); i += Integer.BYTES) {
                    assertEquals(i, pageCursor.getInt());
                }
            }
        }
    }

    @Test
    void readCanObserveWrittenDataFromTheSameTransaction() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, tracer);
                var pageFile = map(pageCache, file("a"), pageCache.pageSize())) {
            try (var context = contextFactory.create("readCanObserveWrittenDataFromTheSameTransaction");
                    var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, context)) {
                context.setWriteAndReadVersion(10, 2);

                assertTrue(mutator.next());

                for (int i = 0; i < pageCache.payloadSize(); i += Integer.BYTES) {
                    mutator.putInt(i);
                }
            }

            // and now reader of old version should observe whole page of zero's and new reader of the latest version
            // should be able to see the whole page of numbers
            try (var context = contextFactory.create("readCanObserveWholeVersionedPageContent");
                    var pageCursor = pageFile.io(0, PF_SHARED_READ_LOCK, context)) {
                context.setWriteAndReadVersion(2);

                assertTrue(pageCursor.next());
                for (int i = 0; i < pageCache.payloadSize(); i += Integer.BYTES) {
                    assertEquals(0, pageCursor.getInt());
                }
            }
            try (var context = contextFactory.create("readCanObserveWholeVersionedPageContent");
                    var pageCursor = pageFile.io(0, PF_SHARED_READ_LOCK, context)) {
                context.setWriteAndReadVersion(10, 2);

                assertTrue(pageCursor.next());
                for (int i = 0; i < pageCache.payloadSize(); i += Integer.BYTES) {
                    assertEquals(i, pageCursor.getInt());
                }
            }
        }
    }

    @Test
    void failToReadEmptyNonUpdatedPageAfterWriteLaterInSameFile() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, tracer);
                var pageFile = map(pageCache, file("a"), pageCache.pageSize())) {
            try (var mutator = pageFile.io(
                    10,
                    PF_SHARED_WRITE_LOCK,
                    contextFactory.create("failToReadEmptyNonUpdatedPageAfterWriteLaterInSameFile"))) {
                assertTrue(mutator.next());
            }

            try (var reader = pageFile.io(
                    0,
                    PF_SHARED_READ_LOCK,
                    contextFactory.create("failToReadEmptyNonUpdatedPageAfterWriteLaterInSameFile"))) {
                assertTrue(reader.next());
            }
        }
    }

    @Test
    void updateEmptyPageWithLatestVersion() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, tracer);
                var pageFile = map(pageCache, file("a"), pageCache.pageSize())) {
            try (var context = contextFactory.create("updateEmptyPageWithLatestVersion");
                    var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, context)) {
                context.setWriteAndReadVersion(10);
                assertTrue(mutator.next());
                mutator.putInt(8);
                assertFalse(mutator.checkAndClearBoundsFlag());
            }

            try (var context = contextFactory.create("updateEmptyPageWithLatestVersion");
                    var reader = pageFile.io(0, PF_SHARED_READ_LOCK, context)) {
                context.setWriteAndReadVersion(11);
                assertTrue(reader.next());
                assertEquals(8, reader.getInt());
            }
        }
    }

    @Test
    void contextIsNotMarkedAsDirtyInMultiVersionedCase() throws IOException {
        try (MuninnPageCache pageCache = createPageCache(fs, 2, PageCacheTracer.NULL);
                PagedFile pagedFile = map(pageCache, file("a"), 8);
                var cursorContext = contextFactory.create("contextIsNotMarkedAsDirtyInMultiVersionedCase")) {
            VersionContext versionContext = cursorContext.getVersionContext();
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
                assertFalse(versionContext.isDirty());
            }
        }
    }

    @Test
    void readLatestPageVersionMultipleUpdates() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, tracer);
                var pageFile = map(pageCache, file("a"), pageCache.pageSize())) {
            int maxIteration = 100;
            for (int i = 1; i <= maxIteration; i++) {
                try (var context = contextFactory.create("readLatestPageVersionMultipleUpdates");
                        var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, context)) {
                    context.setWriteAndReadVersion(i);
                    assertTrue(mutator.next());
                    mutator.putInt(i);
                    assertFalse(mutator.checkAndClearBoundsFlag());
                }
            }

            try (var context = contextFactory.create("readLatestPageVersionMultipleUpdates");
                    var reader = pageFile.io(0, PF_SHARED_READ_LOCK, context)) {
                context.setWriteAndReadVersion(maxIteration);
                assertTrue(reader.next());
                assertEquals(maxIteration, reader.getInt());
                assertFalse(reader.checkAndClearBoundsFlag());
            }
        }
    }

    @Test
    void readLatestPageVersionMultipleUpdatesWithHigherClosedTxId() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, tracer);
                var pageFile = map(pageCache, file("a"), pageCache.pageSize())) {
            int maxIteration = 100;
            for (int i = 1; i <= maxIteration; i++) {
                try (var context = contextFactory.create("readLatestPageVersionMultipleUpdatesWithHigherClosedTxId");
                        var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, context)) {
                    context.setWriteAndReadVersion(i);
                    assertTrue(mutator.next());
                    mutator.putInt(i);
                    assertFalse(mutator.checkAndClearBoundsFlag());
                }
            }

            try (var context = contextFactory.create("readLatestPageVersionMultipleUpdatesWithHigherClosedTxId");
                    var reader = pageFile.io(0, PF_SHARED_READ_LOCK, context)) {
                context.setWriteAndReadVersion(10_000);
                assertTrue(reader.next());
                assertEquals(maxIteration, reader.getInt());
                assertFalse(reader.checkAndClearBoundsFlag());
            }
        }
    }

    @Test
    void readPreviousPageVersionAfterUpdate() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, tracer);
                var pageFile = map(pageCache, file("a"), pageCache.pageSize())) {
            var write1Context = contextFactory.create("readPreviousPageVersionAfterUpdate");
            write1Context.setWriteAndReadVersion(1);
            try (var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, write1Context)) {
                assertTrue(mutator.next());
                mutator.putInt(1);
                assertFalse(mutator.checkAndClearBoundsFlag());
            }

            var write2Context = contextFactory.create("readPreviousPageVersionAfterUpdate");
            write2Context.setWriteAndReadVersion(2);
            try (var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, write2Context)) {
                assertTrue(mutator.next());
                mutator.putInt(2);
                assertFalse(mutator.checkAndClearBoundsFlag());
            }

            var read1Context = contextFactory.create("readPreviousPageVersionAfterUpdate");
            read1Context.setWriteAndReadVersion(BASE_TX_ID);
            try (var reader = pageFile.io(0, PF_SHARED_READ_LOCK, read1Context)) {
                assertTrue(reader.next());
                assertEquals(1, reader.getInt());
                assertFalse(reader.checkAndClearBoundsFlag());
            }
        }
    }

    @Test
    void readAllPageVersionsWithMultipleUpdates() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, tracer);
                var pageFile = map(pageCache, file("a"), pageCache.pageSize())) {
            int maxIteration = 100;
            for (int iteration = 1; iteration <= maxIteration; iteration++) {
                try (var context = contextFactory.create("readAllPageVersionsWithMultipleUpdates");
                        var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, context)) {
                    context.setWriteAndReadVersion(iteration);

                    assertTrue(mutator.next());
                    mutator.putInt(iteration);
                    assertFalse(mutator.checkAndClearBoundsFlag());
                }
            }

            for (int iteration = 1; iteration <= maxIteration; iteration++) {
                try (var context = contextFactory.create("readAllPageVersionsWithMultipleUpdates");
                        var reader = pageFile.io(0, PF_SHARED_READ_LOCK, context)) {
                    context.setWriteAndReadVersion(iteration);

                    assertTrue(reader.next());
                    assertEquals(iteration, reader.getInt());
                    assertFalse(reader.checkAndClearBoundsFlag());
                }
            }
        }
    }

    @Test
    void readMultiplePageVersionsInATime() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, tracer);
                var pageFile = map(pageCache, file("a"), pageCache.pageSize())) {
            int maxIteration = 100;
            for (int iteration = 1; iteration <= maxIteration; iteration++) {
                try (var context = contextFactory.create("readMultiplePageVersionsInATime");
                        var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, context)) {
                    context.setWriteAndReadVersion(iteration);

                    assertTrue(mutator.next());
                    mutator.putInt(iteration);
                    assertFalse(mutator.checkAndClearBoundsFlag());
                }
            }

            for (int iteration = maxIteration; iteration > 2; iteration--) {
                try (var context1 = contextFactory.create("readMultiplePageVersionsInATime");
                        var reader1 = pageFile.io(0, PF_SHARED_READ_LOCK, context1)) {
                    context1.setWriteAndReadVersion(iteration);
                    assertTrue(reader1.next());
                    assertEquals(iteration, reader1.getInt());

                    try (var context2 = contextFactory.create("readMultiplePageVersionsInATime");
                            var reader2 = pageFile.io(0, PF_SHARED_READ_LOCK, context2)) {
                        context2.setWriteAndReadVersion(iteration - 1);
                        assertTrue(reader2.next());
                        assertEquals(iteration - 1, reader2.getInt());

                        try (var context3 = contextFactory.create("readMultiplePageVersionsInATime");
                                var reader3 = pageFile.io(0, PF_SHARED_READ_LOCK, context3)) {
                            context3.setWriteAndReadVersion(iteration - 2);
                            assertTrue(reader3.next());
                            assertEquals(iteration - 2, reader3.getInt());
                        }
                    }
                }
            }
        }
    }

    @Test
    void writeOldVersionWithPreviousVersionExistence() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, tracer);
                var pageFile = map(pageCache, file("a"), pageCache.pageSize())) {

            try (var context = contextFactory.create("writeOldVersionWithPreviousVersionExistence");
                    var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, context)) {
                context.setWriteAndReadVersion(BASE_TX_ID);

                assertTrue(mutator.next());
                mutator.putInt(17);
                assertFalse(mutator.checkAndClearBoundsFlag());
            }
            try (var context = contextFactory.create("writeOldVersionWithPreviousVersionExistence");
                    var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, context)) {
                context.setWriteAndReadVersion(10);

                assertTrue(mutator.next());
                mutator.putInt(42);
                assertFalse(mutator.checkAndClearBoundsFlag());
            }
            try (var context = contextFactory.create("writeOldVersionWithPreviousVersionExistence");
                    var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, context)) {
                context.setWriteAndReadVersion(8);
                assertTrue(mutator.next());
                mutator.putInt(128, 7);
                assertFalse(mutator.checkAndClearBoundsFlag());
            }

            try (var context = contextFactory.create("writeOldVersionWithPreviousVersionExistence");
                    var mutator = pageFile.io(0, PF_SHARED_READ_LOCK, context)) {
                context.setWriteAndReadVersion(11);
                assertTrue(mutator.next());
                assertEquals(42, mutator.getInt());
                assertEquals(7, mutator.getInt(128));
            }
        }
    }

    @Test
    void readPatchedDataFromOlderWriter() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, tracer);
                var pageFile = map(pageCache, file("a"), pageCache.pageSize())) {
            try (var context = contextFactory.create("readPatchedDataFromOlderWriter");
                    var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, context)) {
                context.setWriteAndReadVersion(BASE_TX_ID);

                assertTrue(mutator.next());
                mutator.putInt(17);
                assertFalse(mutator.checkAndClearBoundsFlag());
            }
            try (var context = contextFactory.create("readPatchedDataFromOlderWriter");
                    var reader = pageFile.io(0, PF_SHARED_READ_LOCK, context)) {
                context.setWriteAndReadVersion(2);

                assertTrue(reader.next());
                assertEquals(17, reader.getInt());
                assertFalse(reader.checkAndClearBoundsFlag());
            }

            try (var context = contextFactory.create("readPatchedDataFromOlderWriter");
                    var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, context)) {
                context.setWriteAndReadVersion(10);

                assertTrue(mutator.next());
                mutator.putInt(42);
                assertFalse(mutator.checkAndClearBoundsFlag());
            }

            try (var context = contextFactory.create("readPatchedDataFromOlderWriter");
                    var reader = pageFile.io(0, PF_SHARED_READ_LOCK, context)) {
                context.setWriteAndReadVersion(2);

                assertTrue(reader.next());
                assertEquals(17, reader.getInt());
                assertFalse(reader.checkAndClearBoundsFlag());
            }
            try (var context = contextFactory.create("readPatchedDataFromOlderWriter");
                    var reader = pageFile.io(0, PF_SHARED_READ_LOCK, context)) {
                context.setWriteAndReadVersion(11);

                assertTrue(reader.next());
                assertEquals(42, reader.getInt());
                assertEquals(0, reader.getInt(128));
                assertFalse(reader.checkAndClearBoundsFlag());
            }

            try (var context = contextFactory.create("readPatchedDataFromOlderWriter");
                    var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, context)) {
                context.setWriteAndReadVersion(8);
                assertTrue(mutator.next());
                mutator.putInt(128, 7);
            }
            try (var context = contextFactory.create("readPatchedDataFromOlderWriter");
                    var reader = pageFile.io(0, PF_SHARED_READ_LOCK, context)) {
                context.setWriteAndReadVersion(2);

                assertTrue(reader.next());
                assertEquals(17, reader.getInt());
                assertFalse(reader.checkAndClearBoundsFlag());
            }
            try (var context = contextFactory.create("readPatchedDataFromOlderWriter");
                    var reader = pageFile.io(0, PF_SHARED_READ_LOCK, context)) {
                context.setWriteAndReadVersion(9);

                assertTrue(reader.next());
                assertEquals(17, reader.getInt());
                assertEquals(7, reader.getInt(128));
                assertFalse(reader.checkAndClearBoundsFlag());
            }
            try (var context = contextFactory.create("readPatchedDataFromOlderWriter");
                    var reader = pageFile.io(0, PF_SHARED_READ_LOCK, context)) {
                context.setWriteAndReadVersion(12);

                assertTrue(reader.next());
                assertEquals(42, reader.getInt());
                assertEquals(7, reader.getInt(128));
                assertFalse(reader.checkAndClearBoundsFlag());
            }
        }
    }

    @Test
    void writeOldVersionWithoutPreviousVersionExistence() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, tracer);
                var pageFile = map(pageCache, file("a"), pageCache.pageSize())) {
            try (var context = contextFactory.create("writeOldVersionWithoutPreviousVersionExistence");
                    var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, context)) {
                context.setWriteAndReadVersion(10);

                assertTrue(mutator.next());
                mutator.putInt(42);
                assertFalse(mutator.checkAndClearBoundsFlag());
            }
            try (var context = contextFactory.create("writeOldVersionWithoutPreviousVersionExistence");
                    var writer = pageFile.io(0, PF_SHARED_WRITE_LOCK, context)) {
                context.setWriteAndReadVersion(8);
                assertTrue(writer.next());
                writer.putInt(64, Integer.MAX_VALUE);
                assertFalse(writer.checkAndClearBoundsFlag());
            }
        }
    }

    @Test
    void readNonExistentVersionWithPagesUpdated() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, tracer);
                var pageFile = map(pageCache, file("a"), pageCache.pageSize())) {

            try (var context = contextFactory.create("readNonExistentVersionWithPagesUpdated");
                    var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, context)) {
                context.setWriteAndReadVersion(5);

                assertTrue(mutator.next());
                mutator.putInt(5);
                assertFalse(mutator.checkAndClearBoundsFlag());
            }
            try (var context = contextFactory.create("readNonExistentVersionWithPagesUpdated");
                    var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, context)) {
                context.setWriteAndReadVersion(6);

                assertTrue(mutator.next());
                mutator.putInt(42);
                assertFalse(mutator.checkAndClearBoundsFlag());
            }
            try (var context = contextFactory.create("readNonExistentVersionWithPagesUpdated");
                    var reader = pageFile.io(0, PF_SHARED_READ_LOCK, context)) {
                context.setWriteAndReadVersion(3);
                assertTrue(reader.next());
                assertEquals(0, reader.getInt());
            }
        }
    }

    @Test
    void readLatestVersionedDataFromDifferentPages() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, tracer);
                var pageFile = map(pageCache, file("a"), pageCache.pageSize())) {
            try (var page1Context = contextFactory.create("readLatestVersionedDataFromDifferentPages");
                    var writer1 = pageFile.io(0, PF_SHARED_WRITE_LOCK, page1Context)) {
                page1Context.setWriteAndReadVersion(7);

                assertTrue(writer1.next());
                writer1.putInt(7);
            }
            try (var page2Context = contextFactory.create("readLatestVersionedDataFromDifferentPages");
                    var writer2 = pageFile.io(1, PF_SHARED_WRITE_LOCK, page2Context)) {
                page2Context.setWriteAndReadVersion(14);

                assertTrue(writer2.next());
                writer2.putInt(14);
            }
            try (var context = contextFactory.create("readLatestVersionedDataFromDifferentPages");
                    var reader = pageFile.io(0, PF_SHARED_READ_LOCK, context)) {
                context.setWriteAndReadVersion(15);

                assertTrue(reader.next());
                assertEquals(7, reader.getInt());

                assertTrue(reader.next());
                assertEquals(14, reader.getInt());
            }
        }
    }

    @Test
    void readOldVersionedDataFromDifferentPages() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, tracer);
                var pageFile = map(pageCache, file("a"), pageCache.pageSize())) {
            // page 1 mutations
            try (var page11Context = contextFactory.create("readOldVersionedDataFromDifferentPages");
                    var writer11 = pageFile.io(0, PF_SHARED_WRITE_LOCK, page11Context)) {
                page11Context.setWriteAndReadVersion(BASE_TX_ID);

                assertTrue(writer11.next());
                writer11.putInt(1);
            }
            try (var page12Context = contextFactory.create("readOldVersionedDataFromDifferentPages");
                    var writer12 = pageFile.io(0, PF_SHARED_WRITE_LOCK, page12Context)) {
                page12Context.setWriteAndReadVersion(20);

                assertTrue(writer12.next());
                writer12.putInt(20);
            }
            try (var page13Context = contextFactory.create("readOldVersionedDataFromDifferentPages");
                    var writer13 = pageFile.io(0, PF_SHARED_WRITE_LOCK, page13Context)) {
                page13Context.setWriteAndReadVersion(30);

                assertTrue(writer13.next());
                writer13.putInt(30);
            }

            // page 2 mutations
            try (var page21Context = contextFactory.create("readOldVersionedDataFromDifferentPages");
                    var writer21 = pageFile.io(1, PF_SHARED_WRITE_LOCK, page21Context)) {
                page21Context.setWriteAndReadVersion(21);

                assertTrue(writer21.next());
                writer21.putInt(21);
            }
            try (var page22Context = contextFactory.create("readOldVersionedDataFromDifferentPages");
                    var writer22 = pageFile.io(1, PF_SHARED_WRITE_LOCK, page22Context)) {
                page22Context.setWriteAndReadVersion(27);

                assertTrue(writer22.next());
                writer22.putInt(27);
            }
            try (var page23Context = contextFactory.create("readOldVersionedDataFromDifferentPages");
                    var writer23 = pageFile.io(1, PF_SHARED_WRITE_LOCK, page23Context)) {
                page23Context.setWriteAndReadVersion(42);

                assertTrue(writer23.next());
                writer23.putInt(42);
            }

            // readers
            try (var context = contextFactory.create("readOldVersionedDataFromDifferentPages");
                    var reader = pageFile.io(0, PF_SHARED_READ_LOCK, context)) {
                context.setWriteAndReadVersion(22);

                assertTrue(reader.next());
                assertEquals(20, reader.getInt());

                assertTrue(reader.next());
                assertEquals(21, reader.getInt());
            }
            try (var context = contextFactory.create("readOldVersionedDataFromDifferentPages");
                    var reader = pageFile.io(0, PF_SHARED_READ_LOCK, context)) {
                context.setWriteAndReadVersion(28);

                assertTrue(reader.next());
                assertEquals(20, reader.getInt());

                assertTrue(reader.next());
                assertEquals(27, reader.getInt());
            }
        }
    }

    @Test
    void readerSeeSamePageVersionWhileWriterDoUpdates() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, tracer);
                var pageFile = map(pageCache, file("a"), pageCache.pageSize())) {
            try (var pageContext = contextFactory.create("readerSeeSamePageVersionWhileWriterDoUpdates");
                    var writer = pageFile.io(0, PF_SHARED_WRITE_LOCK, pageContext)) {
                pageContext.setWriteAndReadVersion(1);
                assertTrue(writer.next());

                while (writer.getOffset() < pageFile.payloadSize()) {
                    writer.putByte((byte) 1);
                }
            }

            try (var readContext = contextFactory.create("readerSeeSamePageVersionWhileWriterDoUpdates");
                    var writeContext = contextFactory.create("readerSeeSamePageVersionWhileWriterDoUpdates");
                    var reader = pageFile.io(0, PF_SHARED_READ_LOCK, readContext);
                    var writer = pageFile.io(0, PF_SHARED_WRITE_LOCK, writeContext)) {
                readContext.setWriteAndReadVersion(2);
                writeContext.setWriteAndReadVersion(4);

                assertTrue(reader.next());
                assertTrue(writer.next());

                int readerOffset = 0;
                boolean firstRead = true;
                while (writer.getOffset() < pageFile.payloadSize()) {
                    reader.setOffset(readerOffset);
                    reader.getByte();
                    readerOffset = reader.getOffset();
                    writer.putByte((byte) 2);
                    writer.putByte((byte) 3);
                    if (firstRead) {
                        assertTrue(reader.shouldRetry());
                        firstRead = false;
                    } else {
                        assertFalse(reader.shouldRetry());
                    }
                }

                // reader after writer completion can do reads
                reader.setOffset(0);
                while (reader.getOffset() < pageFile.payloadSize()) {
                    assertEquals(1, reader.getByte());
                }
            }
        }
    }

    @RepeatedTest(20)
    void readConsistentPageWithConcurrentWriters() throws IOException, ExecutionException {
        try (var pageCache = createPageCache(fs, 1024, tracer);
                var pageFile = map(pageCache, file("a"), pageCache.pageSize())) {
            int baseVersion = 2;
            try (var pageContext = contextFactory.create("readConsistentPageWithConcurrentWriters");
                    var writer = pageFile.io(0, PF_SHARED_WRITE_LOCK, pageContext)) {
                pageContext.setWriteAndReadVersion(baseVersion);
                assertTrue(writer.next());

                while (writer.getOffset() < pageFile.payloadSize()) {
                    writer.putByte((byte) 1);
                }
            }

            int numberOfExecutors = 20;
            var cursorExecutors = Executors.newFixedThreadPool(numberOfExecutors);
            try {
                int numberOfReaders = 19;
                int numberOfWriters = numberOfExecutors - numberOfReaders;
                var readers = new ArrayList<Future<?>>(numberOfReaders);
                var writers = new ArrayList<Future<?>>(numberOfWriters);
                CountDownLatch startLatch = new CountDownLatch(1);
                for (int i = 0; i < numberOfReaders; i++) {
                    readers.add(cursorExecutors.submit(() -> {
                        try (var pageContext = contextFactory.create("readConsistentPageWithConcurrentWriters");
                                var reader = pageFile.io(0, PF_SHARED_READ_LOCK, pageContext)) {
                            pageContext.setWriteAndReadVersion(baseVersion);
                            byte[] bytes = new byte[pageFile.payloadSize()];
                            assertTrue(reader.next());
                            startLatch.await();

                            do {
                                reader.setOffset(0);
                                int index = 0;
                                while (reader.getOffset() < pageFile.payloadSize()) {
                                    bytes[index++] = reader.getByte();
                                }
                            } while (reader.shouldRetry());
                            assertThat(bytes).containsOnly(1);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }));
                }
                for (int i = 0; i < numberOfWriters; i++) {
                    writers.add(cursorExecutors.submit(() -> {
                        try (var pageContext = contextFactory.create("readConsistentPageWithConcurrentWriters");
                                var writer = pageFile.io(0, PF_SHARED_WRITE_LOCK, pageContext)) {
                            pageContext.setWriteAndReadVersion(baseVersion + 10);
                            assertTrue(writer.next());
                            startLatch.await();

                            while (writer.getOffset() < pageFile.payloadSize()) {
                                writer.putByte((byte) 5);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }));
                }
                startLatch.countDown();
                Futures.getAll(writers);
                Futures.getAll(readers);
            } finally {
                cursorExecutors.shutdown();
            }
        }
    }

    @RepeatedTest(20)
    void sameVersionConcurrentWriters() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, tracer);
                var pageFile = map(pageCache, file("a"), pageCache.pageSize())) {
            int baseVersion = 2;
            try (var pageContext = contextFactory.create("sameVersionConcurrentWriters");
                    var writer = pageFile.io(0, PF_SHARED_WRITE_LOCK, pageContext)) {
                pageContext.setWriteAndReadVersion(baseVersion);
                assertTrue(writer.next());

                while (writer.getOffset() < pageFile.payloadSize()) {
                    writer.putByte((byte) 1);
                }
            }

            int numberOfExecutors = 20;
            var cursorExecutors = Executors.newFixedThreadPool(numberOfExecutors);
            try {
                var writers = new ArrayList<Future<?>>(numberOfExecutors);
                CountDownLatch startLatch = new CountDownLatch(1);
                for (int i = 0; i < numberOfExecutors; i++) {
                    int writerData = i + 2;
                    writers.add(cursorExecutors.submit(() -> {
                        try (var pageContext = contextFactory.create("sameVersionConcurrentWriters");
                                var writer = pageFile.io(0, PF_SHARED_WRITE_LOCK, pageContext)) {
                            pageContext.setWriteAndReadVersion(baseVersion + 1);
                            assertTrue(writer.next());
                            startLatch.await();

                            while (writer.getOffset() < pageFile.payloadSize()) {
                                writer.putInt(writerData);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }));
                }
                startLatch.countDown();
                assertDoesNotThrow(() -> Futures.getAll(writers));
            } finally {
                cursorExecutors.shutdown();
            }
        }
    }

    @RepeatedTest(20)
    void differentVersionConcurrentWriters() throws IOException, ExecutionException {
        try (var pageCache = createPageCache(fs, 1024, tracer);
                var pageFile = map(pageCache, file("a"), pageCache.pageSize())) {
            int baseVersion = 2;
            var contextFactory = new SingleThreadedTestContextFactory(tracer);
            try (var pageContext = contextFactory.create("differentVersionConcurrentWriters");
                    var writer = pageFile.io(0, PF_SHARED_WRITE_LOCK, pageContext)) {
                pageContext.setWriteAndReadVersion(baseVersion);
                assertTrue(writer.next());

                while (writer.getOffset() < pageFile.payloadSize()) {
                    writer.putByte((byte) 1);
                }
            }

            int numberOfExecutors = 20;
            var cursorExecutors = Executors.newFixedThreadPool(numberOfExecutors);
            try {
                var writers = new ArrayList<Future<?>>(numberOfExecutors);
                CountDownLatch startLatch = new CountDownLatch(1);
                for (int i = 0; i < numberOfExecutors; i++) {
                    int writerData = i + 2;
                    writers.add(cursorExecutors.submit(() -> {
                        try (var pageContext = contextFactory.create("differentVersionConcurrentWriters");
                                var writer = pageFile.io(0, PF_SHARED_WRITE_LOCK, pageContext)) {
                            pageContext.setWriteAndReadVersion(baseVersion + writerData);
                            assertTrue(writer.next());
                            startLatch.await();

                            while (writer.getOffset() < pageFile.payloadSize()) {
                                writer.putInt(writerData);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }));
                }
                startLatch.countDown();
                Futures.getAll(writers);
            } finally {
                cursorExecutors.shutdown();
            }
        }
    }

    @Test
    void readOldPageVersionWithShouldRetryWhileNewWriterCreatedThePage() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, tracer);
                var pageFile = map(pageCache, file("a"), pageCache.pageSize())) {
            try (var mutatorContext =
                            contextFactory.create("readOldPageVersionWithShouldRetryWhileNewWriterCreatedThePage");
                    var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, mutatorContext)) {
                mutatorContext.setWriteAndReadVersion(BASE_TX_ID);

                assertTrue(mutator.next());
                mutator.putInt(1);
            }

            try (var readContext =
                            contextFactory.create("readOldPageVersionWithShouldRetryWhileNewWriterCreatedThePage");
                    var reader = pageFile.io(0, PF_SHARED_READ_LOCK, readContext)) {
                readContext.setWriteAndReadVersion(BASE_TX_ID + 1);
                assertTrue(reader.next());

                try (var mutatorContext =
                                contextFactory.create("readOldPageVersionWithShouldRetryWhileNewWriterCreatedThePage");
                        var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, mutatorContext)) {
                    mutatorContext.setWriteAndReadVersion(BASE_TX_ID + 1);
                    assertTrue(mutator.next());
                    mutator.putInt(17);
                }

                reader.getInt(1);
                assertTrue(reader.shouldRetry());
            }
        }
    }

    @Test
    void readOlderPageWithoutShouldRetry() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, tracer);
                var pageFile = map(pageCache, file("a"), pageCache.pageSize())) {
            try (var mutatorContext = contextFactory.create("readOlderPageWithoutShouldRetry");
                    var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, mutatorContext)) {
                mutatorContext.setWriteAndReadVersion(BASE_TX_ID);

                assertTrue(mutator.next());
                mutator.putInt(1);
            }

            try (var mutatorContext = contextFactory.create("readOlderPageWithoutShouldRetry");
                    var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, mutatorContext)) {
                mutatorContext.setWriteAndReadVersion(BASE_TX_ID + 5);

                assertTrue(mutator.next());
                mutator.putInt(5);
            }

            try (var mutatorContext = contextFactory.create("readOlderPageWithoutShouldRetry");
                    var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, mutatorContext)) {
                mutatorContext.setWriteAndReadVersion(BASE_TX_ID + 10);

                assertTrue(mutator.next());
                mutator.putInt(10);
            }

            try (var readContext = contextFactory.create("readOlderPageWithoutShouldRetry");
                    var reader = pageFile.io(0, PF_SHARED_READ_LOCK, readContext)) {
                readContext.setWriteAndReadVersion(BASE_TX_ID + 1);
                assertTrue(reader.next());

                try (var mutatorContext = contextFactory.create("readOlderPageWithoutShouldRetry");
                        var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, mutatorContext)) {
                    mutatorContext.setWriteAndReadVersion(BASE_TX_ID + 20);
                    assertTrue(mutator.next());
                    mutator.putInt(17);
                }

                assertEquals(1, reader.getInt());
                assertFalse(reader.shouldRetry());
            }
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
