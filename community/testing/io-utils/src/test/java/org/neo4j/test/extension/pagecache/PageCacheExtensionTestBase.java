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
package org.neo4j.test.extension.pagecache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_WRITE_LOCK;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.adversaries.pagecache.AdversarialPageCache;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.utils.TestDirectory;

abstract class PageCacheExtensionTestBase {
    @Inject
    private PageCache pageCache;

    @Inject
    protected TestDirectory testDirectory;

    @Inject
    private RandomSupport random;

    @Test
    void pageCacheInjected() {
        assertNotNull(pageCache);
    }

    @Test
    void testDirectoryInjected() {
        assertNotNull(testDirectory);
    }

    @Test
    void pageCacheCanFindFileCreatedByTestDirectory() throws IOException {
        Path testFile = testDirectory.createFile("testFile");
        try (PagedFile map = pageCache.map(
                testFile, 4096, testDirectory.homePath().getFileName().toString())) {
            assertNotNull(map);
        }
    }

    @Test
    @RandomSupport.Seed(1) // Intentionally committed
    void shareSeedWithRandomExtension() throws IOException {
        assertThat(random.random())
                .isSameAs(((AdversarialPageCache) pageCache).adversary().random());

        Path testFile = testDirectory.createFile("testFile");
        try (PagedFile map = pageCache.map(
                testFile,
                PageCache.PAGE_SIZE,
                testDirectory.homePath().getFileName().toString())) {
            try (PageCursor cursor = map.io(0, PF_SHARED_WRITE_LOCK, CursorContext.NULL_CONTEXT)) {
                cursor.next(0);
                cursor.setOffset(0);
                for (int i = 0; i < pageCache.pageSize() / Long.BYTES; i++) {
                    cursor.putLong(i);
                }
                assertThat(cursor.shouldRetry()).isFalse();
            }
            try (PageCursor cursor = map.io(0, PF_SHARED_READ_LOCK, CursorContext.NULL_CONTEXT)) {
                cursor.next(0);
                readUntil(cursor, pageCache.pageSize() / Long.BYTES, true);
                readUntil(cursor, 37, true);
                readUntil(cursor, pageCache.pageSize() / Long.BYTES, false);
            }
        }
    }

    private void readUntil(PageCursor cursor, int end, boolean expectRetry) throws IOException {
        cursor.setOffset(0);
        for (int i = 0; i < end; i++) {
            assertThat(cursor.getLong()).isEqualTo(i);
        }
        if (end != pageCache.pageSize() / Long.BYTES) {
            assertThat(cursor.getLong()).isNotEqualTo(end);
        }
        assertThat(cursor.shouldRetry()).isEqualTo(expectRetry);
    }

    @Nested
    class NestedPageCacheTest {
        @Inject
        private PageCache nestedPageCache;

        @Test
        void nestedPageCacheInjection() {
            assertNotNull(nestedPageCache);
        }

        @Test
        void nestedAndRootPageCacheAreTheSame() {
            assertSame(pageCache, nestedPageCache);
        }
    }
}
