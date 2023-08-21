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
package org.neo4j.internal.batchimport.cache;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.DELETE_ON_CLOSE;
import static org.eclipse.collections.impl.factory.Sets.immutable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.batchimport.cache.NumberArrayFactories.NO_MONITOR;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.logging.NullLog;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.utils.TestDirectory;

@PageCacheExtension
@ExtendWith(RandomExtension.class)
class PageCacheLongArrayTest {
    private static final int COUNT = 1_000_000;

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private PageCache pageCache;

    @Inject
    private RandomSupport random;

    @Test
    void verifyPageCacheLongArray() throws Exception {
        PagedFile file = pageCache.map(
                testDirectory.file("file"),
                pageCache.pageSize(),
                DEFAULT_DATABASE_NAME,
                immutable.of(CREATE, DELETE_ON_CLOSE));

        try (LongArray array = new PageCacheLongArray(
                file, new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER), COUNT, 0, 0)) {
            verifyBehaviour(array);
        }
    }

    @Test
    void verifyChunkingArrayWithPageCacheLongArray() {
        Path directory = testDirectory.homePath();
        var contextFactory = new CursorContextFactory(NULL, EMPTY_CONTEXT_SUPPLIER);
        NumberArrayFactory numberArrayFactory = NumberArrayFactories.auto(
                pageCache, contextFactory, directory, false, NO_MONITOR, NullLog.getInstance(), DEFAULT_DATABASE_NAME);
        try (LongArray array = numberArrayFactory.newDynamicLongArray(COUNT / 1_000, 0, INSTANCE)) {
            verifyBehaviour(array);
        }
    }

    private void verifyBehaviour(LongArray array) {
        // insert
        for (int i = 0; i < COUNT; i++) {
            array.set(i, i);
        }

        // verify inserted data
        for (int i = 0; i < COUNT; i++) {
            assertEquals(i, array.get(i));
        }

        // verify inserted data with random access patterns
        int stride = 12_345_678;
        int next = random.nextInt(COUNT);
        for (int i = 0; i < COUNT; i++) {
            assertEquals(next, array.get(next));
            next = (next + stride) % COUNT;
        }
    }
}
