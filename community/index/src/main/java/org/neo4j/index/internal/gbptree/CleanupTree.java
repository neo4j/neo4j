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
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.isLeaf;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.isNode;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.setKeyCount;

import java.io.IOException;
import java.util.function.LongConsumer;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;

public class CleanupTree<K, V> implements TreeWriteOperation<K, V> {
    @Override
    public boolean run(
            Layout<K, V> layout,
            InternalAccess<K, V> internalAccess,
            PageCursor cursor,
            StructurePropagation<K> structurePropagation,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext,
            LongConsumer rootSetter,
            IdProvider freeList)
            throws IOException {
        if (!internalAccess.coordination().pessimistic()) {
            return false;
        }
        // cursor is at root
        if (!isLeaf(cursor)) {
            long rootPageId = cursor.getCurrentPageId();
            long child = internalAccess.internalNode().childAt(cursor, 0, stableGeneration, unstableGeneration);
            while (child >= 0) {
                child = cleanLevel(cursor, child, internalAccess, stableGeneration, unstableGeneration, freeList);
            }
            cursor.next(rootPageId);
        }
        // reinit root as leaf and reset key count to zero
        internalAccess
                .leafNode()
                .initialize(cursor, TreeNodeUtil.DATA_LAYER_FLAG, stableGeneration, unstableGeneration);
        setKeyCount(cursor, 0);
        rootSetter.accept(cursor.getCurrentPageId());
        return true;
    }

    private long cleanLevel(
            PageCursor cursor,
            long child,
            InternalAccess<K, V> internalAccess,
            long stableGeneration,
            long unstableGeneration,
            IdProvider freeList)
            throws IOException {
        goTo(cursor, "child", child);
        long leftMostChild = isLeaf(cursor)
                ? -1
                : internalAccess.internalNode().childAt(cursor, 0, stableGeneration, unstableGeneration);

        long rightSibling;
        while (true) {
            freeList.releaseId(
                    stableGeneration, unstableGeneration, cursor.getCurrentPageId(), CursorCreator.bind(cursor));
            rightSibling = TreeNodeUtil.rightSibling(cursor, stableGeneration, unstableGeneration)
                    .pointer();
            if (!isNode(rightSibling)) {
                break;
            }
            goTo(cursor, "right sibling", rightSibling);
        }

        return leftMostChild;
    }
}
