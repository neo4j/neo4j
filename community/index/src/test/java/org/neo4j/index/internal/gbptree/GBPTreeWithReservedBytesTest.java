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
package org.neo4j.index.internal.gbptree;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.OpenOption;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.ImmutableSet;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.ChecksumMismatchException;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCacheOpenOptions;

public class GBPTreeWithReservedBytesTest extends GBPTreeTest {
    @Override
    protected ImmutableSet<OpenOption> getOpenOptions() {
        return Sets.immutable.of(PageCacheOpenOptions.MULTI_VERSIONED);
    }

    // mvcc version of this behavior fails earlier on page checksum mismatch due to different page sizes
    @Test
    void shouldFailOnOpenWithSmallerPageSize() throws Exception {
        // GIVEN
        int pageSize = 2 * defaultPageSize;
        try (PageCache pageCache = createPageCache(pageSize)) {
            index(pageCache).build().close();
        }

        // WHEN
        int smallerPageSize = pageSize / 2;
        try (PageCache pageCache = createPageCache(smallerPageSize)) {
            assertThatThrownBy(() -> index(pageCache).build())
                    .isInstanceOf(ChecksumMismatchException.class)
                    .hasMessageContaining("Page checksum mismatch");
        }
    }

    @Test
    void shouldFailOnOpenWithLargerPageSize() throws Exception {
        // GIVEN
        int pageSize = 2 * defaultPageSize;
        try (PageCache pageCache = createPageCache(pageSize)) {
            index(pageCache).build().close();
        }

        // WHEN
        int largerPageSize = 2 * pageSize;
        try (PageCache pageCache = createPageCache(largerPageSize)) {
            assertThatThrownBy(() -> index(pageCache).build())
                    .isInstanceOf(ChecksumMismatchException.class)
                    .hasMessageContaining("Page checksum mismatch");
        }
    }
}
