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
package org.neo4j.index.internal.gbptree;

import static org.neo4j.index.internal.gbptree.TreeNodeUtil.goTo;
import static org.neo4j.index.internal.gbptree.TreeStatePair.readStatePages;
import static org.neo4j.index.internal.gbptree.TreeStatePair.selectNewestValidState;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

/**
 * Test utility for {@link org.neo4j.internal.id.indexed.IndexedIdGenerator}, which uses the "unsafe" functionality
 * for {@link GBPTree} to alter persisted state of the tree arbitrarily.
 */
public class IndexedIdGeneratorUnsafe {
    private static <KEY, VALUE> void unsafe(
            PageCache pageCache,
            Path file,
            Layout<KEY, VALUE> layout,
            ImmutableSet<OpenOption> openOptions,
            GBPTreeUnsafe<KEY, VALUE> unsafe)
            throws IOException {
        try (var pagedFile = pageCache.map(file, pageCache.pageSize(), "db", openOptions)) {
            TreeState treeState;
            try (var cursor = pagedFile.io(0, PagedFile.PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                treeState = selectNewestValidState(readStatePages(cursor, IdSpace.STATE_PAGE_A, IdSpace.STATE_PAGE_B));
            }
            // Currently there's no need for the lead/internal node behaviours, so skip them
            unsafe.access(pagedFile, layout, null, null, treeState);
        }
    }

    public static <KEY, VALUE> void changeHeaderDataLength(
            PageCache pageCache, Path file, Layout<KEY, VALUE> layout, ImmutableSet<OpenOption> openOptions, int diff)
            throws IOException {
        unsafe(pageCache, file, layout, openOptions, (pagedFile, layout1, leafNode, internalNode, treeState) -> {
            try (PageCursor cursor = pagedFile.io(0, PagedFile.PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                goTo(cursor, "", treeState.pageId());
                cursor.setOffset(TreeState.SIZE);
                int length = cursor.getInt(cursor.getOffset());
                int newLength = length + diff;
                cursor.putInt(newLength);
            }
        });
    }
}
