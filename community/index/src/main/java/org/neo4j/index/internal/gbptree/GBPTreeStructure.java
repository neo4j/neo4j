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

import static java.util.Arrays.asList;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.pointer;
import static org.neo4j.index.internal.gbptree.TreeNode.Type.INTERNAL;
import static org.neo4j.index.internal.gbptree.TreeNode.Type.LEAF;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.index.internal.gbptree.RootMappingLayout.RootMappingValue;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.util.Preconditions;

/**
 * Utility class for printing a {@link GBPTree}, either whole or sub-tree.
 *
 * @param <ROOT_KEY> type of root mapping key.
 * @param <DATA_KEY>> type of keys in the data trees.
 * @param <DATA_VALUE>> type of values in the data trees.
 */
public class GBPTreeStructure<ROOT_KEY, DATA_KEY, DATA_VALUE> {
    private final TreeNode<ROOT_KEY, RootMappingValue> rootTreeNode;
    private final Layout<ROOT_KEY, RootMappingValue> rootLayout;
    private final TreeNode<DATA_KEY, DATA_VALUE> dataTreeNode;
    private final Layout<DATA_KEY, DATA_VALUE> dataLayout;
    private final long stableGeneration;
    private final long unstableGeneration;

    GBPTreeStructure(
            TreeNode<ROOT_KEY, RootMappingValue> rootTreeNode,
            Layout<ROOT_KEY, RootMappingValue> rootLayout,
            TreeNode<DATA_KEY, DATA_VALUE> dataTreeNode,
            Layout<DATA_KEY, DATA_VALUE> dataLayout,
            long stableGeneration,
            long unstableGeneration) {
        this.rootTreeNode = rootTreeNode;
        this.rootLayout = rootLayout;
        this.dataTreeNode = dataTreeNode;
        this.dataLayout = dataLayout;
        this.stableGeneration = stableGeneration;
        this.unstableGeneration = unstableGeneration;
    }

    /**
     * Visit the meta page of the tree present in the given {@code file}.
     *
     * @param pageCache {@link PageCache} able to map tree contained in {@code file}.
     * @param file {@link Path} containing the tree to visit meta page for.
     * @param visitor {@link GBPTreeVisitor} that shall visit meta.
     * @param databaseName name of the database file belongs to
     * @param openOptions
     * @throws IOException on I/O error.
     */
    public static void visitMeta(
            PageCache pageCache,
            Path file,
            GBPTreeVisitor visitor,
            String databaseName,
            CursorContext cursorContext,
            ImmutableSet<OpenOption> openOptions)
            throws IOException {
        var options =
                openOptions.newWithoutAll(asList(GBPTreeOpenOptions.values())).newWith(StandardOpenOption.READ);
        try (var pagedFile = pageCache.map(file, pageCache.pageSize(), databaseName, options)) {
            try (var cursor = pagedFile.io(IdSpace.META_PAGE_ID, PagedFile.PF_SHARED_READ_LOCK, cursorContext)) {
                visitMeta(cursor, visitor);
            }
        }
    }

    /**
     * Visit the state of the tree present in the given {@code file}.
     *
     * @param openOptions
     * @param pageCache {@link PageCache} able to map tree contained in {@code file}.
     * @param file {@link Path} containing the tree to visit state for.
     * @param visitor {@link GBPTreeVisitor} that shall visit state.
     * @param databaseName name of the database file belongs to
     * @throws IOException on I/O error.
     */
    public static void visitState(
            PageCache pageCache,
            Path file,
            GBPTreeVisitor visitor,
            String databaseName,
            CursorContext cursorContext,
            ImmutableSet<OpenOption> openOptions)
            throws IOException {
        var options =
                openOptions.newWithoutAll(asList(GBPTreeOpenOptions.values())).newWith(StandardOpenOption.READ);
        try (var pagedFile = pageCache.map(file, pageCache.pageSize(), databaseName, options)) {
            try (var cursor = pagedFile.io(IdSpace.STATE_PAGE_A, PagedFile.PF_SHARED_READ_LOCK, cursorContext)) {
                visitTreeState(cursor, visitor);
            }
        }
    }

    private static void visitMeta(PageCursor cursor, GBPTreeVisitor visitor) throws IOException {
        PageCursorUtil.goTo(cursor, "meta page", IdSpace.META_PAGE_ID);
        Meta meta = Meta.read(cursor);
        visitor.meta(meta);
    }

    static void visitTreeState(PageCursor cursor, GBPTreeVisitor visitor) throws IOException {
        Pair<TreeState, TreeState> statePair =
                TreeStatePair.readStatePages(cursor, IdSpace.STATE_PAGE_A, IdSpace.STATE_PAGE_B);
        visitor.treeState(statePair);
    }

    /**
     * Let the passed in {@code cursor} point to the root or sub-tree (internal node) of what to visit.
     *
     * @param cursor {@link PageCursor} placed at root of tree or sub-tree.
     * @param visitor {@link GBPTreeVisitor} that should visit the tree.
     * @param cursorContext underlying page cursor context.
     * @throws IOException on page cache access error.
     */
    void visitTree(
            PageCursor cursor, GBPTreeVisitor<ROOT_KEY, DATA_KEY, DATA_VALUE> visitor, CursorContext cursorContext)
            throws IOException {
        // TreeState
        long currentPage = cursor.getCurrentPageId();
        Pair<TreeState, TreeState> statePair =
                TreeStatePair.readStatePages(cursor, IdSpace.STATE_PAGE_A, IdSpace.STATE_PAGE_B);
        visitor.treeState(statePair);
        TreeNode.goTo(cursor, "back to tree node from reading state", currentPage);

        assertOnTreeNode(cursor);

        boolean isDataTree;
        do {
            isDataTree = TreeNode.layerType(cursor) == TreeNode.DATA_LAYER_FLAG;
        } while (cursor.shouldRetry());
        visitor.beginTree(isDataTree);

        // Traverse the tree
        int level = 0;
        do {
            // One level at the time
            visitor.beginLevel(level);
            long leftmostSibling = cursor.getCurrentPageId();

            // Go right through all siblings
            visitLevel(cursor, visitor, cursorContext);

            visitor.endLevel(level);
            level++;

            // Then go back to the left-most node on this level
            TreeNode.goTo(cursor, "back", leftmostSibling);
        }
        // And continue down to next level if this level was an internal level
        while (goToLeftmostChild(cursor));

        visitor.endTree(isDataTree);
    }

    private static void assertOnTreeNode(PageCursor cursor) throws IOException {
        byte nodeType;
        boolean isInternal;
        boolean isLeaf;
        do {
            nodeType = TreeNode.nodeType(cursor);
            isInternal = TreeNode.isInternal(cursor);
            isLeaf = TreeNode.isLeaf(cursor);
        } while (cursor.shouldRetry());

        if (nodeType != TreeNode.NODE_TYPE_TREE_NODE) {
            throw new IllegalArgumentException(
                    "Cursor is not pinned to a tree node page. pageId:" + cursor.getCurrentPageId());
        }
        if (!isInternal && !isLeaf) {
            throw new IllegalArgumentException(
                    "Cursor is not pinned to a page containing a tree node. pageId:" + cursor.getCurrentPageId());
        }
    }

    void visitTreeNode(
            PageCursor cursor, GBPTreeVisitor<ROOT_KEY, DATA_KEY, DATA_VALUE> visitor, CursorContext cursorContext)
            throws IOException {
        // [TYPE][GEN][KEYCOUNT] ([RIGHTSIBLING][LEFTSIBLING][SUCCESSOR]))
        boolean isLeaf;
        int keyCount;
        long generation = -1;
        boolean isDataNode;
        do {
            isLeaf = TreeNode.isLeaf(cursor);
            keyCount = TreeNode.keyCount(cursor);
            isDataNode = TreeNode.layerType(cursor) == TreeNode.DATA_LAYER_FLAG;
            generation = TreeNode.generation(cursor);
        } while (cursor.shouldRetry());
        Preconditions.checkState(treeNode(isDataNode).reasonableKeyCount(keyCount), "Unexpected keyCount %d", keyCount);
        visitor.beginNode(cursor.getCurrentPageId(), isLeaf, generation, keyCount);

        for (int i = 0; i < keyCount; i++) {
            if (isDataNode) {
                visitDataEntry(cursor, visitor, cursorContext, isLeaf, i);
            } else {
                visitRootEntry(cursor, visitor, cursorContext, isLeaf, i);
            }
        }
        if (!isLeaf) {
            long child;
            do {
                child = pointer(treeNode(isDataNode).childAt(cursor, keyCount, stableGeneration, unstableGeneration));
            } while (cursor.shouldRetry());
            visitor.position(keyCount);
            visitor.child(child);
        }
        visitor.endNode(cursor.getCurrentPageId());
    }

    private TreeNode<?, ?> treeNode(boolean isDataNode) {
        return isDataNode ? dataTreeNode : rootTreeNode;
    }

    private void visitDataEntry(
            PageCursor cursor,
            GBPTreeVisitor<ROOT_KEY, DATA_KEY, DATA_VALUE> visitor,
            CursorContext cursorContext,
            boolean isLeaf,
            int i)
            throws IOException {
        DATA_KEY key = dataLayout.newKey();
        DATA_VALUE value = dataLayout.newValue();
        long offloadId;
        long child = -1;
        do {
            TreeNode.Type type = isLeaf ? LEAF : INTERNAL;
            offloadId = dataTreeNode.offloadIdAt(cursor, i, type);
            dataTreeNode.keyAt(cursor, key, i, type, cursorContext);
            if (isLeaf) {
                dataTreeNode.valueAt(cursor, value, i, cursorContext);
            } else {
                child = pointer(dataTreeNode.childAt(cursor, i, stableGeneration, unstableGeneration));
            }
        } while (cursor.shouldRetry());

        visitor.position(i);
        if (isLeaf) {
            visitor.key(key, isLeaf, offloadId);
            visitor.value(value);
        } else {
            visitor.child(child);
            visitor.key(key, isLeaf, offloadId);
        }
    }

    private void visitRootEntry(
            PageCursor cursor,
            GBPTreeVisitor<ROOT_KEY, DATA_KEY, DATA_VALUE> visitor,
            CursorContext cursorContext,
            boolean isLeaf,
            int i)
            throws IOException {
        ROOT_KEY key = rootLayout.newKey();
        RootMappingValue value = rootLayout.newValue();
        long offloadId;
        long child = -1;
        do {
            TreeNode.Type type = isLeaf ? LEAF : INTERNAL;
            offloadId = rootTreeNode.offloadIdAt(cursor, i, type);
            if (isLeaf) {
                rootTreeNode.keyValueAt(cursor, key, value, i, cursorContext);
            } else {
                rootTreeNode.keyAt(cursor, key, i, type, cursorContext);
                child = pointer(rootTreeNode.childAt(cursor, i, stableGeneration, unstableGeneration));
            }
        } while (cursor.shouldRetry());

        visitor.position(i);
        if (isLeaf) {
            visitor.rootKey(key, isLeaf, offloadId);
            visitor.rootMapping(value.rootId, value.rootGeneration);
        } else {
            visitor.child(child);
            visitor.rootKey(key, isLeaf, offloadId);
        }
    }

    private boolean goToLeftmostChild(PageCursor cursor) throws IOException {
        boolean isInternal;
        long leftmostSibling = -1;
        boolean isDataNode;
        do {
            isInternal = TreeNode.isInternal(cursor);
            isDataNode = TreeNode.layerType(cursor) == TreeNode.DATA_LAYER_FLAG;
        } while (cursor.shouldRetry());

        if (isInternal) {
            do {
                leftmostSibling = treeNode(isDataNode).childAt(cursor, 0, stableGeneration, unstableGeneration);
            } while (cursor.shouldRetry());
            TreeNode.goTo(cursor, "child", leftmostSibling);
        }
        return isInternal;
    }

    private void visitLevel(
            PageCursor cursor, GBPTreeVisitor<ROOT_KEY, DATA_KEY, DATA_VALUE> visitor, CursorContext cursorContext)
            throws IOException {
        long rightSibling;
        do {
            visitTreeNode(cursor, visitor, cursorContext);

            do {
                rightSibling = TreeNode.rightSibling(cursor, stableGeneration, unstableGeneration);
            } while (cursor.shouldRetry());

            if (TreeNode.isNode(rightSibling)) {
                TreeNode.goTo(cursor, "right sibling", rightSibling);
            }
        } while (TreeNode.isNode(rightSibling));
    }
}
