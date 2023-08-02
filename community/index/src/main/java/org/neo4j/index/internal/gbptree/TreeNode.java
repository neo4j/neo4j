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

import static org.neo4j.index.internal.gbptree.TreeNode.Type.INTERNAL;
import static org.neo4j.index.internal.gbptree.TreeNode.Type.LEAF;

import java.io.IOException;
import java.util.Comparator;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.util.VisibleForTesting;

/**
 * Methods to manipulate single tree node such as set and get header fields,
 * insert and fetch keys, values and children.
 */
abstract class TreeNode<KEY, VALUE> {
    enum Type {
        LEAF,
        INTERNAL
    }

    enum Overflow {
        YES,
        NO,
        NO_NEED_DEFRAG
    }

    /**
     * Holds VALUE and `defined` flag. See {@link #valueAt(PageCursor, ValueHolder, int, CursorContext)}
     * and {@link #keyValueAt(PageCursor, Object, ValueHolder, int, CursorContext)}
     * @param <VALUE>
     */
    static final class ValueHolder<VALUE> {
        VALUE value;
        boolean defined;

        public ValueHolder(VALUE value) {
            this(value, false);
        }

        public ValueHolder(VALUE value, boolean defined) {
            this.value = value;
            this.defined = defined;
        }
    }

    protected final Layout<KEY, VALUE> layout;
    protected final LeafNodeBehaviour<KEY, VALUE> leaf;
    protected final InternalNodeBehaviour<KEY> internal;
    protected final int maxKeyCount;

    /**
     * @param layout - layout
     * @param leaf - leaf node behaviour
     * @param internal - internal node behaviour
     */
    TreeNode(Layout<KEY, VALUE> layout, LeafNodeBehaviour<KEY, VALUE> leaf, InternalNodeBehaviour<KEY> internal) {
        this.layout = layout;
        this.leaf = leaf;
        this.internal = internal;
        this.maxKeyCount = Math.max(internal.maxKeyCount(), leaf.maxKeyCount());
    }

    final void initializeLeaf(PageCursor cursor, byte layerType, long stableGeneration, long unstableGeneration) {
        TreeNodeUtil.writeBaseHeader(cursor, TreeNodeUtil.LEAF_FLAG, layerType, stableGeneration, unstableGeneration);
        writeAdditionalHeader(cursor, Type.LEAF);
    }

    final void initializeInternal(PageCursor cursor, byte layerType, long stableGeneration, long unstableGeneration) {
        TreeNodeUtil.writeBaseHeader(
                cursor, TreeNodeUtil.INTERNAL_FLAG, layerType, stableGeneration, unstableGeneration);
        writeAdditionalHeader(cursor, Type.INTERNAL);
    }

    final void writeAdditionalHeader(PageCursor cursor, Type type) {
        switch (type) {
            case LEAF -> leaf.writeAdditionalHeader(cursor);
            case INTERNAL -> internal.writeAdditionalHeader(cursor);
        }
    }

    final long offloadIdAt(PageCursor cursor, int pos, Type type) {
        return switch (type) {
            case LEAF -> leaf.offloadIdAt(cursor, pos);
            case INTERNAL -> internal.offloadIdAt(cursor, pos);
        };
    }

    final KEY keyAtLeaf(PageCursor cursor, KEY into, int pos, CursorContext cursorContext) {
        return leaf.keyAt(cursor, into, pos, cursorContext);
    }

    final KEY keyAtInternal(PageCursor cursor, KEY into, int pos, CursorContext cursorContext) {
        return internal.keyAt(cursor, into, pos, cursorContext);
    }

    final KEY keyAt(PageCursor cursor, KEY into, int pos, Type type, CursorContext cursorContext) {
        return switch (type) {
            case LEAF -> leaf.keyAt(cursor, into, pos, cursorContext);
            case INTERNAL -> internal.keyAt(cursor, into, pos, cursorContext);
        };
    }

    void keyValueAt(PageCursor cursor, KEY intoKey, ValueHolder<VALUE> intoValue, int pos, CursorContext cursorContext)
            throws IOException {
        leaf.keyValueAt(cursor, intoKey, intoValue, pos, cursorContext);
    }

    final <ROOT_KEY> void deepVisitValue(PageCursor cursor, int pos, GBPTreeVisitor<ROOT_KEY, KEY, VALUE> visitor)
            throws IOException {
        leaf.deepVisitValue(cursor, pos, visitor);
    }

    final void insertKeyAndRightChildAt(
            PageCursor cursor,
            KEY key,
            long child,
            int pos,
            int keyCount,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext)
            throws IOException {
        internal.insertKeyAndRightChildAt(
                cursor, key, child, pos, keyCount, stableGeneration, unstableGeneration, cursorContext);
    }

    final void insertKeyValueAt(
            PageCursor cursor,
            KEY key,
            VALUE value,
            int pos,
            int keyCount,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext)
            throws IOException {
        leaf.insertKeyValueAt(cursor, key, value, pos, keyCount, stableGeneration, unstableGeneration, cursorContext);
    }

    final int removeKeyValueAt(
            PageCursor cursor,
            int pos,
            int keyCount,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext)
            throws IOException {
        return leaf.removeKeyValueAt(cursor, pos, keyCount, stableGeneration, unstableGeneration, cursorContext);
    }

    final int removeKeyValues(
            PageCursor cursor,
            int fromPosInclusive,
            int toPosExclusive,
            int keyCount,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext)
            throws IOException {
        return leaf.removeKeyValues(
                cursor,
                fromPosInclusive,
                toPosExclusive,
                keyCount,
                stableGeneration,
                unstableGeneration,
                cursorContext);
    }

    final void removeKeyAndRightChildAt(
            PageCursor cursor,
            int keyPos,
            int keyCount,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext)
            throws IOException {
        internal.removeKeyAndRightChildAt(
                cursor, keyPos, keyCount, stableGeneration, unstableGeneration, cursorContext);
    }

    final void removeKeyAndLeftChildAt(
            PageCursor cursor,
            int keyPos,
            int keyCount,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext)
            throws IOException {
        internal.removeKeyAndLeftChildAt(cursor, keyPos, keyCount, stableGeneration, unstableGeneration, cursorContext);
    }

    final boolean setKeyAtInternal(PageCursor cursor, KEY key, int pos) {
        return internal.setKeyAt(cursor, key, pos);
    }

    ValueHolder<VALUE> valueAt(PageCursor cursor, ValueHolder<VALUE> value, int pos, CursorContext cursorContext)
            throws IOException {
        return leaf.valueAt(cursor, value, pos, cursorContext);
    }

    final boolean setValueAt(
            PageCursor cursor,
            VALUE value,
            int pos,
            CursorContext cursorContext,
            long stableGeneration,
            long unstableGeneration)
            throws IOException {
        return leaf.setValueAt(cursor, value, pos, cursorContext, stableGeneration, unstableGeneration);
    }

    final long childAt(PageCursor cursor, int pos, long stableGeneration, long unstableGeneration) {
        return internal.childAt(cursor, pos, stableGeneration, unstableGeneration);
    }

    final long childAt(
            PageCursor cursor,
            int pos,
            long stableGeneration,
            long unstableGeneration,
            GBPTreeGenerationTarget generationTarget) {
        return internal.childAt(cursor, pos, stableGeneration, unstableGeneration, generationTarget);
    }

    final void setChildAt(PageCursor cursor, long child, int pos, long stableGeneration, long unstableGeneration) {
        internal.setChildAt(cursor, child, pos, stableGeneration, unstableGeneration);
    }

    final int keyValueSizeCap() {
        return leaf.keyValueSizeCap();
    }

    final int inlineKeyValueSizeCap() {
        return leaf.inlineKeyValueSizeCap();
    }

    final void validateKeyValueSize(KEY key, VALUE value) {
        leaf.validateKeyValueSize(key, value);
    }

    final boolean reasonableKeyCount(int keyCount) {
        return keyCount >= 0 && keyCount <= maxKeyCount;
    }

    final boolean reasonableChildCount(int childCount) {
        return internal.reasonableChildCount(childCount);
    }

    final int childOffset(int pos) {
        return internal.childOffset(pos);
    }

    final Comparator<KEY> keyComparator() {
        return layout;
    }

    final Overflow internalOverflow(PageCursor cursor, int currentKeyCount, KEY newKey) {
        return internal.overflow(cursor, currentKeyCount, newKey);
    }

    final Overflow leafOverflow(PageCursor cursor, int currentKeyCount, KEY newKey, VALUE newValue) {
        return leaf.overflow(cursor, currentKeyCount, newKey, newValue);
    }

    final int availableSpace(PageCursor cursor, int currentKeyCount, boolean isInternal) {
        return isInternal
                ? internal.availableSpace(cursor, currentKeyCount)
                : leaf.availableSpace(cursor, currentKeyCount);
    }

    final int totalSpaceOfKeyValue(KEY key, VALUE value) {
        return leaf.totalSpaceOfKeyValue(key, value);
    }

    final int totalSpaceOfKeyChild(KEY key) {
        return internal.totalSpaceOfKeyChild(key);
    }

    final int leafUnderflowThreshold() {
        return leaf.underflowThreshold();
    }

    final void defragmentLeaf(PageCursor cursor) {
        leaf.defragment(cursor);
    }

    final void defragmentInternal(PageCursor cursor) {
        internal.defragment(cursor);
    }

    final boolean leafUnderflow(PageCursor cursor, int keyCount) {
        return leaf.underflow(cursor, keyCount);
    }

    final int canRebalanceLeaves(PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount) {
        return leaf.canRebalance(leftCursor, leftKeyCount, rightCursor, rightKeyCount);
    }

    final boolean canMergeLeaves(PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount) {
        return leaf.canMerge(leftCursor, leftKeyCount, rightCursor, rightKeyCount);
    }

    final int findSplitter(
            PageCursor cursor,
            int keyCount,
            KEY newKey,
            VALUE newValue,
            int insertPos,
            KEY newSplitter,
            double ratioToKeepInLeftOnSplit,
            CursorContext cursorContext) {
        return leaf.findSplitter(
                cursor, keyCount, newKey, newValue, insertPos, newSplitter, ratioToKeepInLeftOnSplit, cursorContext);
    }

    final void doSplitLeaf(
            PageCursor leftCursor,
            int leftKeyCount,
            PageCursor rightCursor,
            int insertPos,
            KEY newKey,
            VALUE newValue,
            KEY newSplitter,
            int splitPos,
            double ratioToKeepInLeftOnSplit,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext)
            throws IOException {
        leaf.doSplit(
                leftCursor,
                leftKeyCount,
                rightCursor,
                insertPos,
                newKey,
                newValue,
                newSplitter,
                splitPos,
                ratioToKeepInLeftOnSplit,
                stableGeneration,
                unstableGeneration,
                cursorContext);
    }

    final void doSplitInternal(
            PageCursor leftCursor,
            int leftKeyCount,
            PageCursor rightCursor,
            int insertPos,
            KEY newKey,
            long newRightChild,
            long stableGeneration,
            long unstableGeneration,
            KEY newSplitter,
            double ratioToKeepInLeftOnSplit,
            CursorContext cursorContext)
            throws IOException {
        internal.doSplit(
                leftCursor,
                leftKeyCount,
                rightCursor,
                insertPos,
                newKey,
                newRightChild,
                stableGeneration,
                unstableGeneration,
                newSplitter,
                ratioToKeepInLeftOnSplit,
                cursorContext);
    }

    final void moveKeyValuesFromLeftToRight(
            PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount, int fromPosInLeftNode) {
        leaf.moveKeyValuesFromLeftToRight(leftCursor, leftKeyCount, rightCursor, rightKeyCount, fromPosInLeftNode);
    }

    final void copyKeyValuesFromLeftToRight(
            PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount) {
        leaf.copyKeyValuesFromLeftToRight(leftCursor, leftKeyCount, rightCursor, rightKeyCount);
    }

    void printNode(
            PageCursor cursor,
            boolean includeValue,
            boolean includeAllocSpace,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext) {
        Type type = TreeNodeUtil.isInternal(cursor) ? INTERNAL : LEAF;
        switch (type) {
            case LEAF -> leaf.printNode(
                    cursor, includeValue, includeAllocSpace, stableGeneration, unstableGeneration, cursorContext);
            case INTERNAL -> internal.printNode(
                    cursor, includeAllocSpace, stableGeneration, unstableGeneration, cursorContext);
        }
    }

    String checkMetaConsistency(PageCursor cursor, int keyCount, Type type, GBPTreeConsistencyCheckVisitor visitor) {
        return switch (type) {
            case LEAF -> leaf.checkMetaConsistency(cursor, keyCount, visitor);
            case INTERNAL -> internal.checkMetaConsistency(cursor, keyCount, visitor);
        };
    }

    @VisibleForTesting
    final int internalMaxKeyCount() {
        return internal.maxKeyCount();
    }
}
