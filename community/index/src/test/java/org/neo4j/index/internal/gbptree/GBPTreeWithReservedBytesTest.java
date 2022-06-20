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
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.index.internal.gbptree.DataTree.W_BATCHED_SINGLE_THREADED;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_WRITE_LOCK;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.nio.file.OpenOption;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.ImmutableSet;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.ChecksumMismatchException;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCacheOpenOptions;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

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

    @Test
    void shouldThrowIfTreeStatePointToRootWithValidSuccessor() throws Exception {
        // GIVEN
        try (PageCache specificPageCache = createPageCache(defaultPageSize)) {
            index(specificPageCache).build().close();

            // a tree state pointing to root with valid successor
            try (PagedFile pagedFile = specificPageCache.map(
                            indexFile, specificPageCache.pageSize(), DEFAULT_DATABASE_NAME, getOpenOptions());
                    PageCursor cursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                Pair<TreeState, TreeState> treeStates =
                        TreeStatePair.readStatePages(cursor, IdSpace.STATE_PAGE_A, IdSpace.STATE_PAGE_B);
                TreeState newestState = TreeStatePair.selectNewestValidState(treeStates);
                long rootId = newestState.rootId();
                long stableGeneration = newestState.stableGeneration();
                long unstableGeneration = newestState.unstableGeneration();

                TreeNode.goTo(cursor, "root", rootId);
                TreeNode.setSuccessor(cursor, 42, stableGeneration + 1, unstableGeneration + 1);
            }

            // WHEN
            try (GBPTree<MutableLong, MutableLong> index =
                    index(specificPageCache).build()) {
                assertThatThrownBy(
                                () -> {
                                    try (var unused = index.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT)) {
                                        unused.put(new MutableLong(11), new MutableLong(13));
                                    }
                                },
                                "Expected to throw because root pointed to by tree state should have a valid successor.")
                        .isInstanceOf(TreeInconsistencyException.class)
                        .hasMessageContaining(PointerChecking.WRITER_TRAVERSE_OLD_STATE_MESSAGE);
            }
        }
    }
}
