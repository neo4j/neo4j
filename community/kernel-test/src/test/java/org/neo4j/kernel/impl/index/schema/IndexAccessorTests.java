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
package org.neo4j.kernel.impl.index.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.impl.api.index.IndexUpdateMode.ONLINE;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;
import org.neo4j.test.utils.PageCacheConfig;

abstract class IndexAccessorTests<KEY, VALUE, LAYOUT extends Layout<KEY, VALUE>>
        extends IndexTestUtil<KEY, VALUE, LAYOUT> {
    IndexAccessor accessor;

    @BeforeEach
    void setupAccessor() throws IOException {
        accessor = createAccessor(pageCache);
    }

    @AfterEach
    void closeAccessor() {
        accessor.close();
    }

    abstract IndexAccessor createAccessor(PageCache pageCache) throws IOException;

    @Test
    void shouldHandleCloseWithoutCallsToProcess() throws Exception {
        // given
        IndexUpdater updater = accessor.newUpdater(ONLINE, NULL_CONTEXT, false);

        // when
        updater.close();

        // then
        // ... should be fine
    }

    @Test
    void requestForSecondUpdaterMustThrow() throws Exception {
        // given
        try (IndexUpdater ignored = accessor.newUpdater(ONLINE, NULL_CONTEXT, false)) {
            assertThrows(IllegalStateException.class, () -> accessor.newUpdater(ONLINE, NULL_CONTEXT, false));
        }
    }

    @Test
    void dropShouldDeleteAndCloseIndex() {
        // given
        assertFilePresent();

        // when
        accessor.drop();

        // then
        assertFileNotPresent();
    }

    @Test
    void dropShouldNotFlushContent() throws IOException {
        // given
        accessor.close();
        try (PageCache pageCache = PageCacheSupportExtension.getPageCache(fs, PageCacheConfig.config())) {
            accessor = createAccessor(pageCache);
            long baseline = pageCacheTracer.flushes();
            try (var flushEvent = pageCacheTracer.beginFileFlush()) {
                accessor.force(flushEvent, NULL_CONTEXT);
            }
            long preDrop = pageCacheTracer.flushes();
            assertThat(preDrop).isGreaterThan(baseline);

            // when
            accessor.drop();

            // then
            long postDrop = pageCacheTracer.flushes();
            assertEquals(preDrop, postDrop);
        }
    }

    @Test
    void snapshotFilesShouldReturnIndexFile() throws IOException {
        // when
        ResourceIterator<Path> files = accessor.snapshotFiles();

        // then
        assertTrue(files.hasNext());
        assertEquals(indexFiles.getStoreFile(), files.next());
        assertFalse(files.hasNext());
    }

    @Test
    void writingAfterDropShouldThrow() {
        // given
        accessor.drop();

        assertThrows(IllegalStateException.class, () -> accessor.newUpdater(ONLINE, NULL_CONTEXT, false));
    }

    @Test
    void writingAfterCloseShouldThrow() {
        // given
        accessor.close();

        assertThrows(IllegalStateException.class, () -> accessor.newUpdater(ONLINE, NULL_CONTEXT, false));
    }
}
