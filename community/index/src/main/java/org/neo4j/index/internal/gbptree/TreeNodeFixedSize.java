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

import java.io.IOException;
import java.io.UncheckedIOException;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;

/**
 *  TreeNode with fixed-sized keys and values.
 *  See {@link LeafNodeFixedSize} and {@link InternalNodeFixedSize} for details
 *
 * @param <KEY> type of key
 * @param <VALUE> type of value
 */
class TreeNodeFixedSize<KEY, VALUE> extends TreeNode<KEY, VALUE> {
    static final byte FORMAT_IDENTIFIER = 2;
    static final byte FORMAT_VERSION = 0;

    TreeNodeFixedSize(int pageSize, Layout<KEY, VALUE> layout) {
        super(layout, new LeafNodeFixedSize<>(pageSize, layout), new InternalNodeFixedSize<>(pageSize, layout));
    }

    @Override
    void printNode(
            PageCursor cursor,
            boolean includeValue,
            boolean includeAllocSpace,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext) {
        try {
            boolean isDataNode = TreeNodeUtil.layerType(cursor) == TreeNodeUtil.DATA_LAYER_FLAG;
            if (isDataNode) {
                new GBPTreeStructure<>(null, null, this, layout, stableGeneration, unstableGeneration)
                        .visitTreeNode(cursor, new PrintingGBPTreeVisitor<>(PrintConfig.defaults()), cursorContext);
            } else {
                new GBPTreeStructure(this, layout, null, null, stableGeneration, unstableGeneration)
                        .visitTreeNode(cursor, new PrintingGBPTreeVisitor<>(PrintConfig.defaults()), cursorContext);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String toString() {
        return "TreeNodeFixedSize[internal:" + internal + ", leaf:" + leaf + "]";
    }
}
