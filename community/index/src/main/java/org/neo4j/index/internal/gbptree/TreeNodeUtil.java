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

import static java.lang.String.format;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.extractKeySize;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.extractOffload;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.extractTombstone;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.extractValueSize;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.readKeyValueSize;
import static org.neo4j.index.internal.gbptree.GBPTreeGenerationTarget.NO_GENERATION_TARGET;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.read;

import java.io.IOException;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageCursorUtil;
import org.neo4j.io.pagecache.context.CursorContext;

public final class TreeNodeUtil {
    // Shared between all node types: TreeNode and FreelistNode
    static final int BYTE_POS_NODE_TYPE = 0;

    static final int SIZE_PAGE_REFERENCE = GenerationSafePointerPair.SIZE;
    // [___r,___t]
    // t: leaf/internal
    // r: data node/root node
    static final int BYTE_POS_TYPE = BYTE_POS_NODE_TYPE + Byte.BYTES;
    static final int BYTE_POS_GENERATION = BYTE_POS_TYPE + Byte.BYTES;
    static final int BYTE_POS_KEYCOUNT = BYTE_POS_GENERATION + Integer.BYTES;
    static final int BYTE_POS_RIGHTSIBLING = BYTE_POS_KEYCOUNT + Integer.BYTES;
    static final int BYTE_POS_LEFTSIBLING = BYTE_POS_RIGHTSIBLING + SIZE_PAGE_REFERENCE;
    static final int BYTE_POS_SUCCESSOR = BYTE_POS_LEFTSIBLING + SIZE_PAGE_REFERENCE;
    static final int BASE_HEADER_LENGTH = BYTE_POS_SUCCESSOR + SIZE_PAGE_REFERENCE;

    static final byte NODE_TYPE_TREE_NODE = 1;
    static final byte NODE_TYPE_FREE_LIST_NODE = 2;
    static final byte NODE_TYPE_OFFLOAD = 3;
    static final byte LEAF_FLAG = 1;
    static final byte INTERNAL_FLAG = 0;
    public static final byte DATA_LAYER_FLAG = 0;
    static final byte ROOT_LAYER_FLAG = 1;
    static final long NO_NODE_FLAG = 0;
    static final long NO_OFFLOAD_ID = -1;
    static final int NO_KEY_VALUE_SIZE_CAP = -1;

    private static final int LAYER_TYPE_SHIFT = 4;
    private static final int TREE_NODE_MASK = (1 << (Byte.SIZE - LAYER_TYPE_SHIFT)) - 1;
    private static final int LAYER_TYPE_MASK = (1 << LAYER_TYPE_SHIFT) - 1;

    /**
     * Given a range with keyCount number of fixed size keys,
     * then splitPos point to the first key that should be moved to right node.
     * Everything before splitPos will be kept in left node.
     *
     * Middle split
     *       0,1,2,3,4
     * split     ^
     * left  0,1
     * right 2,3,4
     *
     * Min split
     *       0,1,2,3,4
     * split   ^
     * left  0
     * right 1,2,3,4
     *
     * Max split
     *       0,1,2,3,4
     * split         ^
     * left  0,1,2,3
     * right 4
     *
     * Note that splitPos can not point past last position (keyCount - 1) or before pos 1.
     * This is because we need to split the range somewhere.
     *
     * @param keyCount number of keys in range.
     * @param ratioToKeepInLeftOnSplit How large ratio of key range to try and keep in left node.
     * @return position of first key to move to right node.
     */
    static int splitPos(int keyCount, double ratioToKeepInLeftOnSplit) {
        // Key
        int minSplitPos = 1;
        int maxSplitPos = keyCount - 1;
        return Math.max(minSplitPos, Math.min(maxSplitPos, (int) (ratioToKeepInLeftOnSplit * keyCount)));
    }

    static byte nodeType(PageCursor cursor) {
        return cursor.getByte(BYTE_POS_NODE_TYPE);
    }

    static void writeBaseHeader(
            PageCursor cursor, byte type, byte layerType, long stableGeneration, long unstableGeneration) {
        cursor.putByte(BYTE_POS_NODE_TYPE, NODE_TYPE_TREE_NODE);
        cursor.putByte(BYTE_POS_TYPE, buildTypeByte(type, layerType));
        setGeneration(cursor, unstableGeneration);
        setKeyCount(cursor, 0);
        setRightSibling(cursor, NO_NODE_FLAG, stableGeneration, unstableGeneration);
        setLeftSibling(cursor, NO_NODE_FLAG, stableGeneration, unstableGeneration);
        setSuccessor(cursor, NO_NODE_FLAG, stableGeneration, unstableGeneration);
    }

    private static byte buildTypeByte(byte type, byte layerType) {
        return (byte) (type | (layerType << LAYER_TYPE_SHIFT));
    }

    static byte treeNodeType(PageCursor cursor) {
        return (byte) (cursor.getByte(BYTE_POS_TYPE) & TREE_NODE_MASK);
    }

    static byte layerType(PageCursor cursor) {
        return (byte) ((cursor.getByte(BYTE_POS_TYPE) >>> LAYER_TYPE_SHIFT) & LAYER_TYPE_MASK);
    }

    public static boolean isLeaf(PageCursor cursor) {
        return treeNodeType(cursor) == LEAF_FLAG;
    }

    static boolean isInternal(PageCursor cursor) {
        return treeNodeType(cursor) == INTERNAL_FLAG;
    }

    static long generation(PageCursor cursor) {
        return cursor.getInt(BYTE_POS_GENERATION) & GenerationSafePointer.GENERATION_MASK;
    }

    public static int keyCount(PageCursor cursor) {
        return cursor.getInt(BYTE_POS_KEYCOUNT);
    }

    public static long rightSibling(PageCursor cursor, long stableGeneration, long unstableGeneration) {
        return rightSibling(cursor, stableGeneration, unstableGeneration, NO_GENERATION_TARGET);
    }

    static long rightSibling(
            PageCursor cursor,
            long stableGeneration,
            long unstableGeneration,
            GBPTreeGenerationTarget generationTarget) {
        cursor.setOffset(BYTE_POS_RIGHTSIBLING);
        return read(cursor, stableGeneration, unstableGeneration, generationTarget);
    }

    static long leftSibling(PageCursor cursor, long stableGeneration, long unstableGeneration) {
        return leftSibling(cursor, stableGeneration, unstableGeneration, NO_GENERATION_TARGET);
    }

    static long leftSibling(
            PageCursor cursor,
            long stableGeneration,
            long unstableGeneration,
            GBPTreeGenerationTarget generationTarget) {
        cursor.setOffset(BYTE_POS_LEFTSIBLING);
        return read(cursor, stableGeneration, unstableGeneration, generationTarget);
    }

    static long successor(PageCursor cursor, long stableGeneration, long unstableGeneration) {
        return successor(cursor, stableGeneration, unstableGeneration, NO_GENERATION_TARGET);
    }

    static long successor(
            PageCursor cursor,
            long stableGeneration,
            long unstableGeneration,
            GBPTreeGenerationTarget generationTarget) {
        cursor.setOffset(BYTE_POS_SUCCESSOR);
        return read(cursor, stableGeneration, unstableGeneration, generationTarget);
    }

    static void setGeneration(PageCursor cursor, long generation) {
        GenerationSafePointer.assertGenerationOnWrite(generation);
        cursor.putInt(BYTE_POS_GENERATION, (int) generation);
    }

    public static void setKeyCount(PageCursor cursor, int count) {
        if (count < 0) {
            throw new IllegalArgumentException(
                    "Invalid key count, " + count + ". On tree node " + cursor.getCurrentPageId() + ".");
        }
        cursor.putInt(BYTE_POS_KEYCOUNT, count);
    }

    static void setRightSibling(
            PageCursor cursor, long rightSiblingId, long stableGeneration, long unstableGeneration) {
        cursor.setOffset(BYTE_POS_RIGHTSIBLING);
        long result = GenerationSafePointerPair.write(cursor, rightSiblingId, stableGeneration, unstableGeneration);
        GenerationSafePointerPair.assertSuccess(
                result,
                cursor.getCurrentPageId(),
                GBPPointerType.RIGHT_SIBLING,
                stableGeneration,
                unstableGeneration,
                cursor,
                BYTE_POS_RIGHTSIBLING);
    }

    static void setLeftSibling(PageCursor cursor, long leftSiblingId, long stableGeneration, long unstableGeneration) {
        cursor.setOffset(BYTE_POS_LEFTSIBLING);
        long result = GenerationSafePointerPair.write(cursor, leftSiblingId, stableGeneration, unstableGeneration);
        GenerationSafePointerPair.assertSuccess(
                result,
                cursor.getCurrentPageId(),
                GBPPointerType.LEFT_SIBLING,
                stableGeneration,
                unstableGeneration,
                cursor,
                BYTE_POS_LEFTSIBLING);
    }

    static void setSuccessor(PageCursor cursor, long successorId, long stableGeneration, long unstableGeneration) {
        cursor.setOffset(BYTE_POS_SUCCESSOR);
        long result = GenerationSafePointerPair.write(cursor, successorId, stableGeneration, unstableGeneration);
        GenerationSafePointerPair.assertSuccess(
                result,
                cursor.getCurrentPageId(),
                GBPPointerType.SUCCESSOR,
                stableGeneration,
                unstableGeneration,
                cursor,
                BYTE_POS_SUCCESSOR);
    }

    /**
     * Moves data from left to right to open up a gap where data can later be written without overwriting anything.
     * Key count is NOT updated!
     *
     * @param cursor Write cursor on relevant page
     * @param pos Logical position where slots should be inserted, pos is based on baseOffset and slotSize.
     * @param numberOfSlots How many slots to be inserted.
     * @param totalSlotCount How many slots there are in total. (Usually keyCount for keys and values or keyCount+1 for children).
     * @param baseOffset Offset to slot in logical position 0.
     * @param slotSize Size of one single slot.
     */
    static void insertSlotsAt(
            PageCursor cursor, int pos, int numberOfSlots, int totalSlotCount, int baseOffset, int slotSize) {
        cursor.shiftBytes(baseOffset + pos * slotSize, (totalSlotCount - pos) * slotSize, numberOfSlots * slotSize);
    }

    /**
     * Moves data from right to left to remove a slot where data that should be deleted currently sits.
     * Key count is NOT updated!
     *
     * @param cursor Write cursor on relevant page
     * @param pos Logical position where slots should be removed, pos is based on baseOffset and slotSize.
     * @param totalSlotCount How many slots there are in total. (Usually keyCount for keys and values or keyCount+1 for children).
     * @param baseOffset Offset to slot in logical position 0.
     * @param slotSize Size of one single slot.
     */
    static void removeSlotAt(PageCursor cursor, int pos, int totalSlotCount, int baseOffset, int slotSize) {
        cursor.shiftBytes(baseOffset + (pos + 1) * slotSize, (totalSlotCount - (pos + 1)) * slotSize, -slotSize);
    }

    /**
     * Moves data from right to left to remove a slots where data that should be deleted currently sits.
     * Key count is NOT updated!
     *
     * @param cursor           Write cursor on relevant page
     * @param fromPosInclusive Logical position of the first slot to be removed, fromPosInclusive is based on baseOffset and slotSize.
     * @param toPosExclusive   Logical position of the first slot to be kept, toPosExclusive is based on baseOffset and slotSize.
     * @param totalSlotCount   How many slots there are in total. (Usually keyCount for keys and values or keyCount+1 for children).
     * @param baseOffset       Offset to slot in logical position 0.
     * @param slotSize         Size of one single slot.
     */
    static void removeSlotsAt(
            PageCursor cursor,
            int fromPosInclusive,
            int toPosExclusive,
            int totalSlotCount,
            int baseOffset,
            int slotSize) {
        cursor.shiftBytes(
                baseOffset + toPosExclusive * slotSize,
                (totalSlotCount - toPosExclusive) * slotSize,
                (fromPosInclusive - toPosExclusive) * slotSize);
    }

    static void writeChild(
            PageCursor cursor,
            long child,
            long stableGeneration,
            long unstableGeneration,
            int childPos,
            int childOffset) {
        long write = GenerationSafePointerPair.write(cursor, child, stableGeneration, unstableGeneration);
        GenerationSafePointerPair.assertSuccess(
                write,
                cursor.getCurrentPageId(),
                GBPPointerType.CHILD,
                stableGeneration,
                unstableGeneration,
                cursor,
                childOffset);
    }

    public static boolean isNode(long node) {
        return GenerationSafePointerPair.pointer(node) != NO_NODE_FLAG;
    }

    public static void goTo(PageCursor cursor, String messageOnError, long nodeId) throws IOException {
        PageCursorUtil.goTo(cursor, messageOnError, GenerationSafePointerPair.pointer(nodeId));
    }

    static void readUnreliableKeyValueSize(
            PageCursor cursor, int keySize, int valueSize, long keyValueSize, int pos, int valueSizeCap) {
        cursor.setCursorException(format(
                "Read unreliable key, id=%d, keySize=%d, valueSize=%d, keyValueSizeCap=%d, keyHasTombstone=%b, pos=%d",
                cursor.getCurrentPageId(), keySize, valueSize, valueSizeCap, extractTombstone(keyValueSize), pos));
    }

    static boolean isUnreliableKeyValueSize(int keySize, int valueSize, int valueSizeCap) {
        return keySize + valueSize > valueSizeCap || keySize < 0 || valueSize < 0;
    }

    static <KEY> KEY readDynamicKey(
            Layout<KEY, ?> layout,
            OffloadStore<KEY, ?> offloadStore,
            PageCursor cursor,
            KEY into,
            int pos,
            CursorContext cursorContext,
            int keyValueSizeCap) {
        long keyValueSize = readKeyValueSize(cursor);
        boolean offload = extractOffload(keyValueSize);
        if (offload) {
            long offloadId = DynamicSizeUtil.readOffloadId(cursor);
            try {
                offloadStore.readKey(offloadId, into, cursorContext);
            } catch (IOException e) {
                cursor.setCursorException("Failed to read key from offload, cause: " + e.getMessage());
            }
        } else {

            int keySize = extractKeySize(keyValueSize);
            int valueSize = extractValueSize(keyValueSize);
            if (isUnreliableKeyValueSize(keySize, valueSize, keyValueSizeCap)) {
                readUnreliableKeyValueSize(cursor, keySize, valueSize, keyValueSize, pos, keyValueSizeCap);
                return into;
            }
            layout.readKey(cursor, into, keySize);
        }
        return into;
    }
}
