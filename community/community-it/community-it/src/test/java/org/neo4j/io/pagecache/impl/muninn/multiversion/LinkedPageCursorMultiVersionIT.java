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
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCacheFixture;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;

public class LinkedPageCursorMultiVersionIT extends PageCacheTestSupport<MuninnPageCache> {
    private final DefaultPageCacheTracer tracer = new DefaultPageCacheTracer();
    private final SingleThreadedTestContextFactory contextFactory = new SingleThreadedTestContextFactory(tracer);

    @Override
    protected PageCacheTestSupport.Fixture<MuninnPageCache> createFixture() {
        return new MuninnPageCacheFixture().withReservedBytes(Long.BYTES * 3);
    }

    @Test
    void readStableVersionWithCompositeCursor() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, tracer);
                var pageFile = map(pageCache, file("a"), pageCache.pageSize())) {

            try (var mutatorContext = contextFactory.create("readStableVersionWithCompositeCursor");
                    var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, mutatorContext)) {
                mutatorContext.setWriteAndReadVersion(BASE_TX_ID);

                assertTrue(mutator.next());
                mutator.putInt(1);
            }
            try (var mutatorContext = contextFactory.create("readStableVersionWithCompositeCursor");
                    var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, mutatorContext)) {
                mutatorContext.setWriteAndReadVersion(BASE_TX_ID + 5);

                assertTrue(mutator.next());
                mutator.putInt(5);
            }
            try (var mutatorContext = contextFactory.create("readStableVersionWithCompositeCursor");
                    var mutator = pageFile.io(1, PF_SHARED_WRITE_LOCK, mutatorContext)) {
                mutatorContext.setWriteAndReadVersion(BASE_TX_ID + 7);

                assertTrue(mutator.next());
                mutator.putInt(5);
            }

            try (var readContext = contextFactory.create("readStableVersionWithCompositeCursor");
                    var readerVersion10 = pageFile.io(0, PF_SHARED_READ_LOCK, readContext)) {
                readContext.setWriteAndReadVersion(BASE_TX_ID + 10);
                assertTrue(readerVersion10.next());
                assertEquals(5, readerVersion10.getInt());

                PageCursor linkedReader = readerVersion10.openLinkedCursor(1);
                assertTrue(linkedReader.next());
                assertEquals(5, linkedReader.getInt());
            }

            try (var readContext = contextFactory.create("readStableVersionWithCompositeCursor");
                    var readerVersion6 = pageFile.io(0, PF_SHARED_READ_LOCK, readContext)) {
                readContext.setWriteAndReadVersion(BASE_TX_ID + 6);
                assertTrue(readerVersion6.next());
                assertEquals(5, readerVersion6.getInt());

                PageCursor linkedReader = readerVersion6.openLinkedCursor(1);
                assertTrue(linkedReader.next());
                assertEquals(0, linkedReader.getInt());
            }

            try (var readContext = contextFactory.create("readStableVersionWithCompositeCursor");
                    var readerVersion4 = pageFile.io(0, PF_SHARED_READ_LOCK, readContext)) {
                readContext.setWriteAndReadVersion(BASE_TX_ID + 4);
                assertTrue(readerVersion4.next());
                assertEquals(1, readerVersion4.getInt());

                PageCursor linkedReader = readerVersion4.openLinkedCursor(1);
                assertTrue(linkedReader.next());
                assertEquals(0, linkedReader.getInt());
            }
        }
    }

    @Test
    void linkedWriterPreservesProperVersionContext() throws IOException {
        try (var pageCache = createPageCache(fs, 1024, tracer);
                var pageFile = map(pageCache, file("a"), pageCache.pageSize())) {
            try (var mutatorContext = contextFactory.create("linkedWriterPreservesProperVersionContext");
                    var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, mutatorContext)) {
                mutatorContext.setWriteAndReadVersion(BASE_TX_ID);

                assertTrue(mutator.next());
                mutator.putInt(1);

                var linkedMutator = mutator.openLinkedCursor(1);
                assertTrue(linkedMutator.next());
                linkedMutator.putInt(1);
            }
            try (var mutatorContext = contextFactory.create("linkedWriterPreservesProperVersionContext");
                    var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, mutatorContext)) {
                mutatorContext.setWriteAndReadVersion(BASE_TX_ID + 7);

                assertTrue(mutator.next());
                mutator.putInt(7);

                var linkedMutator = mutator.openLinkedCursor(1);
                assertTrue(linkedMutator.next());
                linkedMutator.putInt(7);
            }
            try (var mutatorContext = contextFactory.create("linkedWriterPreservesProperVersionContext");
                    var mutator = pageFile.io(0, PF_SHARED_WRITE_LOCK, mutatorContext)) {
                mutatorContext.setWriteAndReadVersion(BASE_TX_ID + 12);

                assertTrue(mutator.next());
                mutator.putInt(12);

                var linkedMutator = mutator.openLinkedCursor(1);
                assertTrue(linkedMutator.next());
                linkedMutator.putInt(12);
            }

            try (var readContext = contextFactory.create("linkedWriterPreservesProperVersionContext");
                    var readerVersion15 = pageFile.io(0, PF_SHARED_READ_LOCK, readContext)) {
                readContext.setWriteAndReadVersion(BASE_TX_ID + 15);
                assertTrue(readerVersion15.next());
                assertEquals(12, readerVersion15.getInt());

                PageCursor linkedReader = readerVersion15.openLinkedCursor(1);
                assertTrue(linkedReader.next());
                assertEquals(12, linkedReader.getInt());
            }
            try (var readContext = contextFactory.create("linkedWriterPreservesProperVersionContext");
                    var readerVersion8 = pageFile.io(0, PF_SHARED_READ_LOCK, readContext)) {
                readContext.setWriteAndReadVersion(BASE_TX_ID + 8);
                assertTrue(readerVersion8.next());
                assertEquals(7, readerVersion8.getInt());

                PageCursor linkedReader = readerVersion8.openLinkedCursor(1);
                assertTrue(linkedReader.next());
                assertEquals(7, linkedReader.getInt());
            }
            try (var readContext = contextFactory.create("linkedWriterPreservesProperVersionContext");
                    var readerVersion5 = pageFile.io(0, PF_SHARED_READ_LOCK, readContext)) {
                readContext.setWriteAndReadVersion(BASE_TX_ID + 5);
                assertTrue(readerVersion5.next());
                assertEquals(1, readerVersion5.getInt());

                PageCursor linkedReader = readerVersion5.openLinkedCursor(1);
                assertTrue(linkedReader.next());
                assertEquals(1, linkedReader.getInt());
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
}
