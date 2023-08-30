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

import static org.neo4j.index.internal.gbptree.CursorCreator.bind;
import static org.neo4j.index.internal.gbptree.GBPTreeGenerationTarget.NO_GENERATION_TARGET;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.pointer;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.BYTE_POS_KEYCOUNT;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.BYTE_POS_LEFTSIBLING;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.BYTE_POS_RIGHTSIBLING;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.BYTE_POS_SUCCESSOR;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.goTo;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.io.IOException;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.context.CursorContext;

/**
 * Use together with {@link GBPTree#unsafe(GBPTreeUnsafe, CursorContext)}
 */
public final class GBPTreeCorruption {
    private GBPTreeCorruption() {}

    /* PageCorruption */
    public static <KEY, VALUE> PageCorruption<KEY, VALUE> crashed(GBPTreePointerType gbpTreePointerType) {
        return (pageCursor, layout, leafNode, internalNode, treeState) -> {
            int offset = gbpTreePointerType.offset(internalNode);
            long stableGeneration = treeState.stableGeneration();
            long unstableGeneration = treeState.unstableGeneration();
            long crashGeneration = crashGeneration(treeState);
            pageCursor.setOffset(offset);
            long pointer = pointer(GenerationSafePointerPair.read(
                    pageCursor, stableGeneration, unstableGeneration, NO_GENERATION_TARGET));
            overwriteGSPP(pageCursor, offset, crashGeneration, pointer);
        };
    }

    public static <KEY, VALUE> PageCorruption<KEY, VALUE> broken(GBPTreePointerType gbpTreePointerType) {
        return (pageCursor, layout, leafNode, internalNode, treeState) -> {
            int offset = gbpTreePointerType.offset(internalNode);
            pageCursor.setOffset(offset);
            pageCursor.putInt(Integer.MAX_VALUE);
        };
    }

    public static <KEY, VALUE> PageCorruption<KEY, VALUE> setPointer(GBPTreePointerType pointerType, long pointer) {
        return (cursor, layout, leafNode, internalNode, treeState) -> {
            overwriteGSPP(cursor, pointerType.offset(internalNode), treeState.stableGeneration(), pointer);
        };
    }

    public static <KEY, VALUE> PageCorruption<KEY, VALUE> notATreeNode() {
        return (cursor, layout, leafNode, internalNode, treeState) ->
                cursor.putByte(TreeNodeUtil.BYTE_POS_NODE_TYPE, Byte.MAX_VALUE);
    }

    public static <KEY, VALUE> PageCorruption<KEY, VALUE> notAnOffloadNode() {
        return (cursor, layout, leafNode, internalNode, treeState) ->
                cursor.putByte(TreeNodeUtil.BYTE_POS_NODE_TYPE, Byte.MAX_VALUE);
    }

    public static <KEY, VALUE> PageCorruption<KEY, VALUE> unknownTreeNodeType() {
        return (cursor, layout, leafNode, internalNode, treeState) ->
                cursor.putByte(TreeNodeUtil.BYTE_POS_TYPE, Byte.MAX_VALUE);
    }

    public static <KEY, VALUE> PageCorruption<KEY, VALUE> rightSiblingPointToNonExisting() {
        return (cursor, layout, leafNode, internalNode, treeState) -> overwriteGSPP(
                cursor,
                GBPTreePointerType.rightSibling().offset(internalNode),
                treeState.stableGeneration(),
                GenerationSafePointer.MAX_POINTER);
    }

    public static <KEY, VALUE> PageCorruption<KEY, VALUE> leftSiblingPointToNonExisting() {
        return (cursor, layout, leafNode, internalNode, treeState) -> overwriteGSPP(
                cursor,
                GBPTreePointerType.leftSibling().offset(internalNode),
                treeState.stableGeneration(),
                GenerationSafePointer.MAX_POINTER);
    }

    public static <KEY, VALUE> PageCorruption<KEY, VALUE> rightSiblingPointerHasTooLowGeneration() {
        return (cursor, layout, leafNode, internalNode, treeState) -> {
            long rightSibling = pointer(
                    TreeNodeUtil.rightSibling(cursor, treeState.stableGeneration(), treeState.unstableGeneration()));
            overwriteGSPP(cursor, BYTE_POS_RIGHTSIBLING, GenerationSafePointer.MIN_GENERATION, rightSibling);
        };
    }

    public static <KEY, VALUE> PageCorruption<KEY, VALUE> leftSiblingPointerHasTooLowGeneration() {
        return (cursor, layout, leafNode, internalNode, treeState) -> {
            long leftSibling = pointer(
                    TreeNodeUtil.leftSibling(cursor, treeState.stableGeneration(), treeState.unstableGeneration()));
            overwriteGSPP(cursor, BYTE_POS_LEFTSIBLING, GenerationSafePointer.MIN_GENERATION, leftSibling);
        };
    }

    public static <KEY, VALUE> PageCorruption<KEY, VALUE> childPointerHasTooLowGeneration(int childPos) {
        return (cursor, layout, leafNode, internalNode, treeState) -> {
            long stableGeneration = treeState.stableGeneration();
            long unstableGeneration = treeState.unstableGeneration();
            long child = pointer(internalNode.childAt(cursor, childPos, stableGeneration, unstableGeneration));
            overwriteGSPP(cursor, internalNode.childOffset(childPos), GenerationSafePointer.MIN_GENERATION, child);
        };
    }

    public static <KEY, VALUE> PageCorruption<KEY, VALUE> setChild(int childPos, long childPointer) {
        return (cursor, layout, leafNode, internalNode, treeState) -> {
            GenerationKeeper childGeneration = new GenerationKeeper();
            long stableGeneration = treeState.stableGeneration();
            long unstableGeneration = treeState.unstableGeneration();
            internalNode.childAt(cursor, childPos, stableGeneration, unstableGeneration, childGeneration);
            overwriteGSPP(
                    cursor,
                    GBPTreePointerType.child(childPos).offset(internalNode),
                    childGeneration.generation,
                    childPointer);
        };
    }

    public static <KEY, VALUE> PageCorruption<KEY, VALUE> hasSuccessor() {
        return (cursor, layout, leafNode, internalNode, treeState) -> overwriteGSPP(
                cursor, BYTE_POS_SUCCESSOR, treeState.unstableGeneration(), GenerationSafePointer.MAX_POINTER);
    }

    public static <KEY, VALUE> PageCorruption<KEY, VALUE> swapKeyOrderLeaf(
            int firstKeyPos, int secondKeyPos, int keyCount) {
        return (cursor, layout, leafNode, internalNode, treeState) -> {
            // Remove key from higher position and insert into lower position
            int lowerKeyPos = Math.min(firstKeyPos, secondKeyPos);
            int higherKeyPos = firstKeyPos == lowerKeyPos ? secondKeyPos : firstKeyPos;

            // Record key and value on higher position
            KEY key = layout.newKey();
            VALUE value = layout.newValue();
            leafNode.keyAt(cursor, key, higherKeyPos, NULL_CONTEXT);
            leafNode.valueAt(cursor, new ValueHolder<>(value), higherKeyPos, NULL_CONTEXT);

            // Remove key and value, may need to defragment node to make sure we have room for insert later
            long stableGeneration1 = treeState.stableGeneration();
            long unstableGeneration1 = treeState.unstableGeneration();
            var newCount = leafNode.removeKeyValueAt(
                    cursor, higherKeyPos, keyCount, stableGeneration1, unstableGeneration1, NULL_CONTEXT);
            TreeNodeUtil.setKeyCount(cursor, newCount);
            leafNode.defragment(cursor);

            // Insert key and value in lower position
            long stableGeneration = treeState.stableGeneration();
            long unstableGeneration = treeState.unstableGeneration();
            leafNode.insertKeyValueAt(
                    cursor, key, value, lowerKeyPos, newCount, stableGeneration, unstableGeneration, NULL_CONTEXT);
            TreeNodeUtil.setKeyCount(cursor, keyCount);
        };
    }

    public static <KEY, VALUE> PageCorruption<KEY, VALUE> swapKeyOrderInternal(
            int firstKeyPos, int secondKeyPos, int keyCount) {
        return (cursor, layout, leafNode, internalNode, treeState) -> {
            // Remove key from higher position and insert into lower position
            int lowerKeyPos = Math.min(firstKeyPos, secondKeyPos);
            int higherKeyPos = firstKeyPos == lowerKeyPos ? secondKeyPos : firstKeyPos;

            // Record key and right child on higher position together with generation of child pointer
            KEY key = layout.newKey();
            internalNode.keyAt(cursor, key, higherKeyPos, NULL_CONTEXT);
            final GenerationKeeper childPointerGeneration = new GenerationKeeper();
            long stableGeneration2 = treeState.stableGeneration();
            long unstableGeneration2 = treeState.unstableGeneration();
            long rightChild = internalNode.childAt(
                    cursor, higherKeyPos + 1, stableGeneration2, unstableGeneration2, childPointerGeneration);

            // Remove key and right child, may need to defragment node to make sure we have room for insert later
            long stableGeneration1 = treeState.stableGeneration();
            long unstableGeneration1 = treeState.unstableGeneration();
            internalNode.removeKeyAndRightChildAt(
                    cursor, higherKeyPos, keyCount, stableGeneration1, unstableGeneration1, NULL_CONTEXT);
            TreeNodeUtil.setKeyCount(cursor, keyCount - 1);
            internalNode.defragment(cursor);

            // Insert key and right child in lower position
            long stableGeneration = treeState.stableGeneration();
            long unstableGeneration = treeState.unstableGeneration();
            internalNode.insertKeyAndRightChildAt(
                    cursor,
                    key,
                    rightChild,
                    lowerKeyPos,
                    keyCount - 1,
                    stableGeneration,
                    unstableGeneration,
                    NULL_CONTEXT);
            TreeNodeUtil.setKeyCount(cursor, keyCount);

            // Overwrite the newly inserted child to reset the generation
            final int childOffset = internalNode.childOffset(lowerKeyPos + 1);
            overwriteGSPP(cursor, childOffset, childPointerGeneration.generation, rightChild);
        };
    }

    public static <KEY, VALUE> PageCorruption<KEY, VALUE> swapChildOrder(
            int firstChildPos, int secondChildPos, int keyCount) {
        return (cursor, layout, leafNode, internalNode, treeState) -> {
            // Read first and second child together with generation
            final GenerationKeeper firstChildGeneration = new GenerationKeeper();
            long stableGeneration1 = treeState.stableGeneration();
            long unstableGeneration1 = treeState.unstableGeneration();
            long firstChild = internalNode.childAt(
                    cursor, firstChildPos, stableGeneration1, unstableGeneration1, firstChildGeneration);
            final GenerationKeeper secondChildGeneration = new GenerationKeeper();
            long stableGeneration = treeState.stableGeneration();
            long unstableGeneration = treeState.unstableGeneration();
            long secondChild = internalNode.childAt(
                    cursor, secondChildPos, stableGeneration, unstableGeneration, secondChildGeneration);

            // Overwrite respective child with the other
            overwriteGSPP(
                    cursor,
                    GBPTreePointerType.child(firstChildPos).offset(internalNode),
                    secondChildGeneration.generation,
                    secondChild);
            overwriteGSPP(
                    cursor,
                    GBPTreePointerType.child(secondChildPos).offset(internalNode),
                    firstChildGeneration.generation,
                    firstChild);
        };
    }

    public static <KEY, VALUE> PageCorruption<KEY, VALUE> overwriteKeyAtPosLeaf(KEY key, int keyPos, int keyCount) {
        return (cursor, layout, leafNode, internalNode, treeState) -> {
            // Record value so that we can reinsert it together with key later
            VALUE value = layout.newValue();
            leafNode.valueAt(cursor, new ValueHolder<>(value), keyPos, NULL_CONTEXT);

            // Remove key and value, may need to defragment node to make sure we have room for insert later
            long stableGeneration1 = treeState.stableGeneration();
            long unstableGeneration1 = treeState.unstableGeneration();
            leafNode.removeKeyValueAt(cursor, keyPos, keyCount, stableGeneration1, unstableGeneration1, NULL_CONTEXT);
            TreeNodeUtil.setKeyCount(cursor, keyCount - 1);
            leafNode.defragment(cursor);

            // Insert new key and value
            long stableGeneration = treeState.stableGeneration();
            long unstableGeneration = treeState.unstableGeneration();
            leafNode.insertKeyValueAt(
                    cursor, key, value, keyPos, keyCount - 1, stableGeneration, unstableGeneration, NULL_CONTEXT);
            TreeNodeUtil.setKeyCount(cursor, keyCount);
        };
    }

    public static <KEY, VALUE> PageCorruption<KEY, VALUE> overwriteKeyAtPosInternal(KEY key, int keyPos, int keyCount) {
        return (cursor, layout, leafNode, internalNode, treeState) -> {
            // Record rightChild so that we can reinsert it together with key later
            long stableGeneration2 = treeState.stableGeneration();
            long unstableGeneration2 = treeState.unstableGeneration();
            long rightChild = internalNode.childAt(cursor, keyPos + 1, stableGeneration2, unstableGeneration2);

            // Remove key and right child, may need to defragment node to make sure we have room for insert later
            long stableGeneration1 = treeState.stableGeneration();
            long unstableGeneration1 = treeState.unstableGeneration();
            internalNode.removeKeyAndRightChildAt(
                    cursor, keyPos, keyCount, stableGeneration1, unstableGeneration1, NULL_CONTEXT);
            TreeNodeUtil.setKeyCount(cursor, keyCount - 1);
            internalNode.defragment(cursor);

            // Insert key and right child
            long stableGeneration = treeState.stableGeneration();
            long unstableGeneration = treeState.unstableGeneration();
            internalNode.insertKeyAndRightChildAt(
                    cursor, key, rightChild, keyPos, keyCount - 1, stableGeneration, unstableGeneration, NULL_CONTEXT);
            TreeNodeUtil.setKeyCount(cursor, keyCount);
        };
    }

    public static <KEY, VALUE> PageCorruption<KEY, VALUE> maximizeAllocOffsetInDynamicNode() {
        return (cursor, layout, leafNode, internalNode, treeState) -> {
            assertDynamicNode(internalNode);
            DynamicSizeUtil.setAllocOffset(cursor, cursor.getPagedFile().payloadSize()); // Clear alloc space
        };
    }

    public static <KEY, VALUE> PageCorruption<KEY, VALUE> minimizeAllocOffsetInDynamicNode() {
        return (cursor, layout, leafNode, internalNode, treeState) -> {
            assertDynamicNode(internalNode);
            DynamicSizeUtil.setAllocOffset(cursor, DynamicSizeUtil.HEADER_LENGTH_DYNAMIC);
        };
    }

    public static <KEY, VALUE> PageCorruption<KEY, VALUE> decrementAllocOffsetInDynamicNode() {
        return (cursor, layout, leafNode, internalNode, treeState) -> {
            assertDynamicNode(internalNode);
            int allocOffset = DynamicSizeUtil.getAllocOffset(cursor);
            DynamicSizeUtil.setAllocOffset(cursor, allocOffset - 1);
        };
    }

    public static <KEY, VALUE> PageCorruption<KEY, VALUE> incrementDeadSpaceInDynamicNode() {
        return (cursor, layout, leafNode, internalNode, treeState) -> {
            assertDynamicNode(internalNode);
            int deadSpace = DynamicSizeUtil.getDeadSpace(cursor);
            DynamicSizeUtil.setDeadSpace(cursor, deadSpace + 1);
        };
    }

    public static <KEY, VALUE> IndexCorruption<KEY, VALUE> decrementFreelistWritePos() {
        return (pagedFile, layout, leafNode, internalNode, treeState) -> {
            try (PageCursor cursor = pagedFile.io(0, PagedFile.PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                goTo(cursor, "", treeState.pageId());
                int decrementedWritePos = treeState.freeListWritePos() - 1;
                TreeState.write(
                        cursor,
                        treeState.stableGeneration(),
                        treeState.unstableGeneration(),
                        treeState.rootId(),
                        treeState.rootGeneration(),
                        treeState.lastId(),
                        treeState.freeListWritePageId(),
                        treeState.freeListReadPageId(),
                        decrementedWritePos,
                        treeState.freeListReadPos(),
                        treeState.isClean());
            }
        };
    }

    public static <KEY, VALUE> IndexCorruption<KEY, VALUE> addFreelistEntry(long releasedId) {
        return (pagedFile, layout, leafNode, internalNode, treeState) -> {
            FreeListIdProvider freelist = getFreelist(pagedFile, treeState);
            var cursorCreator = bind(pagedFile, PagedFile.PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
            freelist.releaseId(treeState.stableGeneration(), treeState.unstableGeneration(), releasedId, cursorCreator);
            freelist.flush(treeState.stableGeneration(), treeState.unstableGeneration(), cursorCreator);
            try (PageCursor cursor = pagedFile.io(0, PagedFile.PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                goTo(cursor, "", treeState.pageId());
                FreeListIdProvider.FreelistMetaData freelistMetaData = freelist.metaData();
                TreeState.write(
                        cursor,
                        treeState.stableGeneration(),
                        treeState.unstableGeneration(),
                        treeState.rootId(),
                        treeState.rootGeneration(),
                        freelistMetaData.lastId(),
                        freelistMetaData.writePageId(),
                        freelistMetaData.readPageId(),
                        freelistMetaData.writePos(),
                        freelistMetaData.readPos(),
                        treeState.isClean());
            }
        };
    }

    public static <KEY, VALUE> IndexCorruption<KEY, VALUE> setTreeState(TreeState target) {
        return (pagedFile, layout, leafNode, internalNode, treeState) -> {
            try (PageCursor cursor = pagedFile.io(0, PagedFile.PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                goTo(cursor, "", treeState.pageId()); // Write new tree state to current tree states page
                TreeState.write(
                        cursor,
                        target.stableGeneration(),
                        target.unstableGeneration(),
                        target.rootId(),
                        target.rootGeneration(),
                        target.lastId(),
                        target.freeListWritePageId(),
                        target.freeListReadPageId(),
                        target.freeListWritePos(),
                        target.freeListReadPos(),
                        target.isClean());
            }
        };
    }

    public static <VALUE, KEY> IndexCorruption<KEY, VALUE> copyChildPointerFromOther(
            long targetInternalNode, long otherInternalNode, int targetChildPos, int otherChildPos) {
        return (pagedFile, layout, leafNode, internalNode, treeState) -> {
            try (PageCursor cursor = pagedFile.io(0, PagedFile.PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                goTo(cursor, "", otherInternalNode);
                final GenerationKeeper generationKeeper = new GenerationKeeper();
                long stableGeneration = treeState.stableGeneration();
                long unstableGeneration = treeState.unstableGeneration();
                final long child = internalNode.childAt(
                        cursor, otherChildPos, stableGeneration, unstableGeneration, generationKeeper);

                goTo(cursor, "", targetInternalNode);
                overwriteGSPP(
                        cursor,
                        GBPTreePointerType.child(targetChildPos).offset(internalNode),
                        generationKeeper.generation,
                        child);
            }
        };
    }

    public static <KEY, VALUE> PageCorruption<KEY, VALUE> setKeyCount(int keyCount) {
        return (cursor, layout, leafNode, internalNode, treeState) -> {
            cursor.putInt(BYTE_POS_KEYCOUNT, keyCount);
        };
    }

    public static <KEY, VALUE> PageCorruption<KEY, VALUE> setHighestReasonableKeyCount() {
        return (cursor, layout, leafNode, internalNode, treeState) -> {
            int keyCount = 0;
            while (keyCount + 1 >= 0 && keyCount + 1 <= Math.max(internalNode.maxKeyCount(), leafNode.maxKeyCount())) {
                keyCount++;
            }
            cursor.putInt(BYTE_POS_KEYCOUNT, keyCount);
        };
    }

    public static <KEY, VALUE> IndexCorruption<KEY, VALUE> pageSpecificCorruption(
            long targetPage, PageCorruption<KEY, VALUE> corruption) {
        return (pagedFile, layout, leafNode, internalNode, treeState) -> {
            try (PageCursor cursor = pagedFile.io(0, PagedFile.PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                goTo(cursor, "", targetPage);
                corruption.corrupt(cursor, layout, leafNode, internalNode, treeState);
            }
        };
    }

    public static <KEY, VALUE> IndexCorruption<KEY, VALUE> makeDirty() {
        return (pagedFile, layout, leafNode, internalNode, treeState) -> {
            try (PageCursor cursor = pagedFile.io(0, PagedFile.PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                goTo(cursor, "", treeState.pageId());
                // Need to also bump generations here otherwise both tree states will be identical which is an illegal
                // state to be in.
                TreeState.write(
                        cursor,
                        treeState.stableGeneration() + 1,
                        treeState.unstableGeneration() + 1,
                        treeState.rootId(),
                        treeState.rootGeneration(),
                        treeState.lastId(),
                        treeState.freeListWritePageId(),
                        treeState.freeListReadPageId(),
                        treeState.freeListWritePos(),
                        treeState.freeListReadPos(),
                        false);
            }
        };
    }

    private static FreeListIdProvider getFreelist(PagedFile pagedFile, TreeState treeState) {
        FreeListIdProvider freelist = new FreeListIdProvider(pagedFile.payloadSize());
        freelist.initialize(
                treeState.lastId(),
                treeState.freeListWritePageId(),
                treeState.freeListReadPageId(),
                treeState.freeListWritePos(),
                0);
        return freelist;
    }

    private static void assertDynamicNode(InternalNodeBehaviour<?> node) {
        if (!(node instanceof InternalNodeDynamicSize)) {
            throw new RuntimeException("Can not use this corruption if node is not of dynamic type");
        }
    }

    private static void overwriteGSPP(PageCursor cursor, int gsppOffset, long generation, long pointer) {
        cursor.setOffset(gsppOffset);
        GenerationSafePointer.write(cursor, generation, pointer);
        GenerationSafePointer.clean(cursor);
    }

    private static long crashGeneration(TreeState treeState) {
        if (treeState.unstableGeneration() - treeState.stableGeneration() < 2) {
            throw new IllegalStateException(
                    "Need stable and unstable generation to have a crash gap but was stableGeneration="
                            + treeState.stableGeneration() + " and unstableGeneration="
                            + treeState.unstableGeneration());
        }
        return treeState.unstableGeneration() - 1;
    }

    interface PageCorruption<KEY, VALUE> {
        void corrupt(
                PageCursor pageCursor,
                Layout<KEY, VALUE> layout,
                LeafNodeBehaviour<KEY, VALUE> leafNode,
                InternalNodeBehaviour<KEY> internalNode,
                TreeState treeState)
                throws IOException;
    }

    interface IndexCorruption<KEY, VALUE> extends GBPTreeUnsafe<KEY, VALUE> {}
}
