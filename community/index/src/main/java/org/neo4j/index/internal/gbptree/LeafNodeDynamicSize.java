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
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.MIN_SIZE_KEY_VALUE_SIZE;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.SUPPORTED_PAGE_SIZE_LIMIT;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.compactToRight;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.extractKeySize;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.extractOffload;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.extractTombstone;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.extractValueSize;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.getAllocSpace;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.getDeadSpace;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.getOverhead;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.keyValueSizeCapFromPageSize;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.putKeyValueSize;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.putOffloadMarker;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.putTombstone;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.readKeyValueSize;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.readOffloadId;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.setAllocOffset;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.setDeadSpace;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.validateInlineCap;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.isUnreliableKeyValueSize;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.readDynamicKey;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.readUnreliableKeyValueSize;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.removeSlotsAt;
import static org.neo4j.io.pagecache.PageCursorUtil.getUnsignedShort;
import static org.neo4j.io.pagecache.PageCursorUtil.putUnsignedShort;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.StringJoiner;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;

/**
 * # = empty space
 * K* = offset to key or key and value
 *
 * LEAF
 * [                                   HEADER   86B                                                   ]|[KEY_OFFSETS]##########[KEYS_VALUES]
 * [NODETYPE][TYPE][GENERATION][KEYCOUNT][RIGHTSIBLING][LEFTSIBLING][SUCCESSOR][ALLOCOFFSET][DEADSPACE]|[K0*,K1*,K2*]->      <-[KV0,KV2,KV1]
 *  0         1     2           6         10            34           58         82           84          86
 * ---
 *
 * Concepts describing the part of a node containing the data
 * Total space - The space available for data (pageSize - headerSize)
 * Active space - Space currently occupied by active data (not including dead keys)
 * Dead space - Space currently occupied by dead data that could be reclaimed by defragment
 * Alloc offset - Exact offset to leftmost key and thus the end of alloc space
 * Alloc space - The available space between offset array and data space
 *
 * TotalSpace  |----------------------------------------|
 * ActiveSpace |-----------|   +    |---------|  + |----|
 * DeadSpace                                  |----|
 * AllocSpace              |--------|
 * AllocOffset                      v
 *     [Header][OffsetArray]........[_________,XXXX,____] (_ = alive key, X = dead key)
 *
 * ---
 *
 * See {@link DynamicSizeUtil} for more detailed layout for individual offset array entries and key / key_value entries.
 */
class LeafNodeDynamicSize<KEY, VALUE> implements LeafNodeBehaviour<KEY, VALUE> {

    private final int inlineKeyValueSizeCap;
    private final int keyValueSizeCap;

    private final int totalSpace;
    private final int halfSpace;
    final OffloadStore<KEY, VALUE> offloadStore;
    private final int maxKeyCount;

    final Layout<KEY, VALUE> layout;
    final int payloadSize;

    LeafNodeDynamicSize(int payloadSize, Layout<KEY, VALUE> layout, OffloadStore<KEY, VALUE> offloadStore) {
        this.payloadSize = payloadSize;
        this.layout = layout;

        assert payloadSize < SUPPORTED_PAGE_SIZE_LIMIT
                : "Only payload size less then " + SUPPORTED_PAGE_SIZE_LIMIT + " bytes supported";
        this.totalSpace = payloadSize - DynamicSizeUtil.HEADER_LENGTH_DYNAMIC;
        this.maxKeyCount = totalSpace / (DynamicSizeUtil.OFFSET_SIZE + MIN_SIZE_KEY_VALUE_SIZE);
        this.offloadStore = offloadStore;
        this.halfSpace = totalSpace >> 1;

        this.inlineKeyValueSizeCap = DynamicSizeUtil.inlineKeyValueSizeCap(payloadSize);
        this.keyValueSizeCap = keyValueSizeCapFromPageSize(payloadSize);

        validateInlineCap(inlineKeyValueSizeCap, payloadSize);
    }

    @Override
    public void initialize(PageCursor cursor, byte layerType, long stableGeneration, long unstableGeneration) {
        TreeNodeUtil.writeBaseHeader(cursor, TreeNodeUtil.LEAF_FLAG, layerType, stableGeneration, unstableGeneration);
        setAllocOffset(cursor, payloadSize);
        setDeadSpace(cursor, 0);
    }

    @Override
    public long offloadIdAt(PageCursor cursor, int pos) {
        placeCursorAtActualKey(cursor, pos);
        return DynamicSizeUtil.offloadIdAt(cursor);
    }

    @Override
    public KEY keyAt(PageCursor cursor, KEY into, int pos, CursorContext cursorContext) {
        placeCursorAtActualKey(cursor, pos);
        return readDynamicKey(layout, offloadStore, cursor, into, pos, cursorContext, keyValueSizeCap());
    }

    @Override
    public Comparator<KEY> keyComparator() {
        return layout;
    }

    @Override
    public void keyValueAt(
            PageCursor cursor, KEY intoKey, ValueHolder<VALUE> intoValue, int pos, CursorContext cursorContext)
            throws IOException {
        placeCursorAtActualKey(cursor, pos);

        intoValue.defined = true;
        long keyValueSize = readKeyValueSize(cursor);
        int keySize = extractKeySize(keyValueSize);
        int valueSize = extractValueSize(keyValueSize);
        boolean offload = extractOffload(keyValueSize);
        if (offload) {
            long offloadId = DynamicSizeUtil.readOffloadId(cursor);
            try {
                offloadStore.readKeyValue(offloadId, intoKey, intoValue.value, cursorContext);
            } catch (IOException e) {
                cursor.setCursorException("Failed to read keyValue from offload, cause: " + e.getMessage());
            }
        } else {
            if (isUnreliableKeyValueSize(keySize, valueSize, keyValueSizeCap())) {
                readUnreliableKeyValueSize(cursor, keySize, valueSize, keyValueSize, pos, keyValueSizeCap());
                return;
            }
            layout.readKey(cursor, intoKey, keySize);
            layout.readValue(cursor, intoValue.value, valueSize);
        }
    }

    @Override
    public void insertKeyValueAt(
            PageCursor cursor,
            KEY key,
            VALUE value,
            int pos,
            int keyCount,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext)
            throws IOException {
        // Where to write key?
        int currentKeyValueOffset = DynamicSizeUtil.getAllocOffset(cursor);
        int keySize = layout.keySize(key);
        int valueSize = layout.valueSize(value);
        int newKeyValueOffset;
        if (canInline(keySize + valueSize)) {
            newKeyValueOffset = currentKeyValueOffset - keySize - valueSize - getOverhead(keySize, valueSize, false);

            // Write key and value
            cursor.setOffset(newKeyValueOffset);
            putKeyValueSize(cursor, keySize, valueSize);

            layout.writeKey(cursor, key);
            layout.writeValue(cursor, value);
        } else {
            newKeyValueOffset = currentKeyValueOffset - getOverhead(keySize, valueSize, true);

            // Write
            cursor.setOffset(newKeyValueOffset);
            putOffloadMarker(cursor);

            long offloadId =
                    offloadStore.writeKeyValue(key, value, stableGeneration, unstableGeneration, cursorContext);
            DynamicSizeUtil.putOffloadId(cursor, offloadId);
        }

        // Update alloc space
        setAllocOffset(cursor, newKeyValueOffset);

        // Write to offset array
        TreeNodeUtil.insertSlotsAt(cursor, pos, 1, keyCount, keyPosOffsetLeaf(0), DynamicSizeUtil.OFFSET_SIZE);
        cursor.setOffset(keyPosOffsetLeaf(pos));
        putUnsignedShort(cursor, newKeyValueOffset);
    }

    @Override
    public int removeKeyValueAt(
            PageCursor cursor,
            int pos,
            int keyCount,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext)
            throws IOException {
        placeCursorAtActualKey(cursor, pos);
        int keyOffset = cursor.getOffset();
        long keyValueSize = readKeyValueSize(cursor);
        boolean offload = DynamicSizeUtil.extractOffload(keyValueSize);
        int keySize = extractKeySize(keyValueSize);
        int valueSize = extractValueSize(keyValueSize);

        // Free from offload
        if (offload) {
            long offloadId = readOffloadId(cursor);
            offloadStore.free(offloadId, stableGeneration, unstableGeneration, cursorContext);
        }

        // Kill actual key
        cursor.setOffset(keyOffset);
        putTombstone(cursor);

        // Update dead space
        int deadSpace = getDeadSpace(cursor);
        setDeadSpace(cursor, deadSpace + keySize + valueSize + getOverhead(keySize, valueSize, offload));

        // Remove from offset array
        TreeNodeUtil.removeSlotAt(cursor, pos, keyCount, keyPosOffsetLeaf(0), DynamicSizeUtil.OFFSET_SIZE);
        return keyCount - 1;
    }

    @Override
    public int removeKeyValues(
            PageCursor cursor,
            int fromPosInclusive,
            int toPosExclusive,
            int keyCount,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext)
            throws IOException {
        int addedDeadSpace = 0;
        for (int i = fromPosInclusive; i < toPosExclusive; i++) {
            placeCursorAtActualKey(cursor, i);
            int keyOffset = cursor.getOffset();
            long keyValueSize = readKeyValueSize(cursor);
            boolean offload = DynamicSizeUtil.extractOffload(keyValueSize);
            // Free from offload
            if (offload) {
                long offloadId = readOffloadId(cursor);
                offloadStore.free(offloadId, stableGeneration, unstableGeneration, cursorContext);
            }

            // Kill actual key
            cursor.setOffset(keyOffset);
            putTombstone(cursor);

            int keySize = extractKeySize(keyValueSize);
            int valueSize = extractValueSize(keyValueSize);
            addedDeadSpace += keySize + valueSize + getOverhead(keySize, valueSize, offload);
        }
        // Update dead space
        setDeadSpace(cursor, getDeadSpace(cursor) + addedDeadSpace);
        // Remove from offset array
        removeSlotsAt(
                cursor, fromPosInclusive, toPosExclusive, keyCount, keyPosOffsetLeaf(0), DynamicSizeUtil.OFFSET_SIZE);
        return keyCount - toPosExclusive + fromPosInclusive;
    }

    @Override
    public ValueHolder<VALUE> valueAt(PageCursor cursor, ValueHolder<VALUE> into, int pos, CursorContext cursorContext)
            throws IOException {
        placeCursorAtActualKey(cursor, pos);

        // Read value
        into.defined = true;
        long keyValueSize = readKeyValueSize(cursor);
        int keySize = extractKeySize(keyValueSize);
        int valueSize = extractValueSize(keyValueSize);
        boolean offload = extractOffload(keyValueSize);
        if (offload) {
            long offloadId = readOffloadId(cursor);
            try {
                offloadStore.readValue(offloadId, into.value, cursorContext);
            } catch (IOException e) {
                cursor.setCursorException("Failed to read value from offload, cause: " + e.getMessage());
            }
        } else {
            if (isUnreliableKeyValueSize(keySize, valueSize, keyValueSizeCap())) {
                readUnreliableKeyValueSize(cursor, keySize, valueSize, keyValueSize, pos, keyValueSizeCap());
                return into;
            }
            progressCursor(cursor, keySize);
            layout.readValue(cursor, into.value, valueSize);
        }
        return into;
    }

    @Override
    public boolean setValueAt(
            PageCursor cursor,
            VALUE value,
            int pos,
            CursorContext cursorContext,
            long stableGeneration,
            long unstableGeneration)
            throws IOException {
        placeCursorAtActualKey(cursor, pos);

        long keyValueSize = readKeyValueSize(cursor);
        int keySize = extractKeySize(keyValueSize);
        int oldValueSize = extractValueSize(keyValueSize);
        int newValueSize = layout.valueSize(value);
        if (oldValueSize == newValueSize) {
            // Fine we can just overwrite
            progressCursor(cursor, keySize);
            layout.writeValue(cursor, value);
            return true;
        }
        return false;
    }

    static void progressCursor(PageCursor cursor, int delta) {
        cursor.setOffset(cursor.getOffset() + delta);
    }

    @Override
    public int keyValueSizeCap() {
        return keyValueSizeCap;
    }

    @Override
    public int inlineKeyValueSizeCap() {
        return inlineKeyValueSizeCap;
    }

    @Override
    public void validateKeyValueSize(KEY key, VALUE value) {
        int keySize = layout.keySize(key);
        int valueSize = layout.valueSize(value);
        if (keyValueSizeTooLarge(keySize, valueSize)) {
            throw new IllegalArgumentException(
                    "Index key-value size it too large. Please see index documentation for limitations.");
        }
    }

    @Override
    public int maxKeyCount() {
        return maxKeyCount;
    }

    @Override
    public boolean reasonableKeyCount(int keyCount) {
        return keyCount >= 0 && keyCount <= maxKeyCount;
    }

    @Override
    public Overflow overflow(PageCursor cursor, int currentKeyCount, KEY newKey, VALUE newValue) {
        int neededSpace = totalSpaceOfKeyValue(newKey, newValue);
        int deadSpace = getDeadSpace(cursor);
        int allocSpace = getAllocSpace(cursor, keyPosOffsetLeaf(currentKeyCount));

        return DynamicSizeUtil.calculateOverflow(neededSpace, deadSpace, allocSpace);
    }

    @Override
    public int availableSpace(PageCursor cursor, int currentKeyCount) {
        int deadSpace = getDeadSpace(cursor);
        int allocSpace = getAllocSpace(cursor, keyPosOffsetLeaf(currentKeyCount));
        return allocSpace + deadSpace;
    }

    @Override
    public int underflowThreshold() {
        return halfSpace;
    }

    @Override
    public void defragment(PageCursor cursor) {
        doDefragment(cursor, TreeNodeUtil.keyCount(cursor));
    }

    private void doDefragment(PageCursor cursor, int keyCount) {
        var offsets = new int[keyCount];
        var sizes = new int[keyCount];
        // collect alive offsets and sizes
        recordAliveBlocks(cursor, keyCount, offsets, sizes, payloadSize, true);

        compactToRight(cursor, keyCount, offsets, sizes, payloadSize, LeafNodeDynamicSize::keyPosOffsetLeaf);
        // Update dead space
        setDeadSpace(cursor, 0);
    }

    protected void recordAliveBlocks(
            PageCursor cursor, int keyCount, int[] offsets, int[] sizes, int payloadSize, boolean assertKeyCount) {
        DynamicSizeUtil.recordAliveBlocks(cursor, keyCount, offsets, sizes, payloadSize);
    }

    @Override
    public boolean underflow(PageCursor cursor, int keyCount) {
        int allocSpace = getAllocSpace(cursor, keyPosOffsetLeaf(keyCount));
        int deadSpace = getDeadSpace(cursor);
        int availableSpace = allocSpace + deadSpace;

        return availableSpace > halfSpace;
    }

    @Override
    public int canRebalance(PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount) {
        int leftActiveSpace = totalActiveSpace(leftCursor, leftKeyCount);
        int rightActiveSpace = totalActiveSpace(rightCursor, rightKeyCount);

        if (leftActiveSpace + rightActiveSpace <= totalSpace) {
            // We can merge
            return -1;
        }
        if (leftActiveSpace < rightActiveSpace) {
            // Moving keys to the right will only create more imbalance
            return 0;
        }

        int prevDelta;
        int currentDelta = Math.abs(leftActiveSpace - rightActiveSpace);
        int keysToMove = 0;
        int lastChunkSize;
        do {
            keysToMove++;
            lastChunkSize = totalSpaceOfKeyValue(leftCursor, leftKeyCount - keysToMove);
            leftActiveSpace -= lastChunkSize;
            rightActiveSpace += lastChunkSize;

            prevDelta = currentDelta;
            currentDelta = Math.abs(leftActiveSpace - rightActiveSpace);
        } while (currentDelta < prevDelta);
        keysToMove--; // Move back to optimal split
        leftActiveSpace += lastChunkSize;
        rightActiveSpace -= lastChunkSize;

        boolean canRebalance = leftActiveSpace > halfSpace && rightActiveSpace > halfSpace;
        return canRebalance ? keysToMove : 0;
    }

    @Override
    public boolean canMerge(PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount) {
        int leftActiveSpace = totalActiveSpace(leftCursor, leftKeyCount);
        int rightActiveSpace = totalActiveSpace(rightCursor, rightKeyCount);
        int totalSpace = this.totalSpace;
        return totalSpace >= leftActiveSpace + rightActiveSpace;
    }

    @Override
    public int findSplitter(
            PageCursor cursor,
            int keyCount,
            KEY newKey,
            VALUE newValue,
            int insertPos,
            KEY newSplitter,
            double ratioToKeepInLeftOnSplit,
            CursorContext cursorContext) {
        // Find middle
        int keyCountAfterInsert = keyCount + 1;
        int splitPos =
                splitPosInLeaf(cursor, insertPos, newKey, newValue, keyCountAfterInsert, ratioToKeepInLeftOnSplit);

        KEY leftInSplit;
        KEY rightInSplit;
        if (splitPos == insertPos) {
            leftInSplit = keyAt(cursor, layout.newKey(), splitPos - 1, cursorContext);
            rightInSplit = newKey;
        } else {
            int rightPos = insertPos < splitPos ? splitPos - 1 : splitPos;
            rightInSplit = keyAt(cursor, layout.newKey(), rightPos, cursorContext);

            if (rightPos == insertPos) {
                leftInSplit = newKey;
            } else {
                int leftPos = rightPos - 1;
                leftInSplit = keyAt(cursor, layout.newKey(), leftPos, cursorContext);
            }
        }
        layout.minimalSplitter(leftInSplit, rightInSplit, newSplitter);
        return splitPos;
    }

    @Override
    public void doSplit(
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
        // Find middle
        int keyCountAfterInsert = leftKeyCount + 1;
        int rightKeyCount = keyCountAfterInsert - splitPos;

        if (insertPos < splitPos) {
            //                v---------v       copy
            // before _,_,_,_,_,_,_,_,_,_
            // insert _,_,_,X,_,_,_,_,_,_,_
            // split            ^
            moveKeysAndValues(leftCursor, splitPos - 1, rightCursor, 0, rightKeyCount);
            doDefragment(leftCursor, splitPos - 1);
            insertKeyValueAt(
                    leftCursor,
                    newKey,
                    newValue,
                    insertPos,
                    splitPos - 1,
                    stableGeneration,
                    unstableGeneration,
                    cursorContext);
        } else {
            //                  v---v           first copy
            //                        v-v       second copy
            // before _,_,_,_,_,_,_,_,_,_
            // insert _,_,_,_,_,_,_,_,X,_,_
            // split            ^

            // Copy everything in one go
            int newInsertPos = insertPos - splitPos;
            int keysToMove = leftKeyCount - splitPos;
            moveKeysAndValues(leftCursor, splitPos, rightCursor, 0, keysToMove);
            doDefragment(leftCursor, splitPos);
            insertKeyValueAt(
                    rightCursor,
                    newKey,
                    newValue,
                    newInsertPos,
                    keysToMove,
                    stableGeneration,
                    unstableGeneration,
                    cursorContext);
        }
        TreeNodeUtil.setKeyCount(leftCursor, splitPos);
        TreeNodeUtil.setKeyCount(rightCursor, rightKeyCount);
    }

    @Override
    public void moveKeyValuesFromLeftToRight(
            PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount, int fromPosInLeftNode) {
        doDefragment(rightCursor, rightKeyCount);
        int numberOfKeysToMove = leftKeyCount - fromPosInLeftNode;

        // Push keys and values in right sibling to the right
        TreeNodeUtil.insertSlotsAt(
                rightCursor, 0, numberOfKeysToMove, rightKeyCount, keyPosOffsetLeaf(0), DynamicSizeUtil.OFFSET_SIZE);

        // Move (also updates keyCount of left)
        moveKeysAndValues(leftCursor, fromPosInLeftNode, rightCursor, 0, numberOfKeysToMove);

        // Right keyCount
        TreeNodeUtil.setKeyCount(rightCursor, rightKeyCount + numberOfKeysToMove);
    }

    // NOTE: Does update keyCount
    private void moveKeysAndValues(PageCursor fromCursor, int fromPos, PageCursor toCursor, int toPos, int count) {
        int firstAllocOffset = DynamicSizeUtil.getAllocOffset(toCursor);
        int toAllocOffset = firstAllocOffset;
        for (int i = 0; i < count; i++, toPos++) {
            toAllocOffset = copyRawKeyValue(fromCursor, fromPos + i, toCursor, toAllocOffset, true);
            toCursor.setOffset(keyPosOffsetLeaf(toPos));
            putUnsignedShort(toCursor, toAllocOffset);
        }
        setAllocOffset(toCursor, toAllocOffset);

        // Update deadSpace
        int deadSpace = getDeadSpace(fromCursor);
        int totalMovedBytes = firstAllocOffset - toAllocOffset;
        setDeadSpace(fromCursor, deadSpace + totalMovedBytes);

        // Key count
        TreeNodeUtil.setKeyCount(fromCursor, fromPos);
    }

    /**
     * Transfer key and value from logical position in 'from' to physical position next to current alloc offset in 'to'.
     * Mark transferred key as dead.
     * @return new alloc offset in 'to'
     */
    protected int copyRawKeyValue(
            PageCursor fromCursor, int fromPos, PageCursor toCursor, int toAllocOffset, boolean markDead) {
        // What to copy?
        placeCursorAtActualKey(fromCursor, fromPos);
        int fromKeyOffset = fromCursor.getOffset();
        long keyValueSize = readKeyValueSize(fromCursor);
        int keySize = extractKeySize(keyValueSize);
        int valueSize = extractValueSize(keyValueSize);
        boolean offload = extractOffload(keyValueSize);

        // Copy
        int toCopy = getOverhead(keySize, valueSize, offload) + keySize + valueSize;
        int newRightAllocSpace = toAllocOffset - toCopy;
        fromCursor.copyTo(fromKeyOffset, toCursor, newRightAllocSpace, toCopy);

        if (markDead) {
            // Put tombstone
            fromCursor.setOffset(fromKeyOffset);
            putTombstone(fromCursor);
        }
        return newRightAllocSpace;
    }

    @Override
    public void copyKeyValuesFromLeftToRight(
            PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount) {
        doDefragment(rightCursor, rightKeyCount);

        // Push keys and values in right sibling to the right
        TreeNodeUtil.insertSlotsAt(
                rightCursor, 0, leftKeyCount, rightKeyCount, keyPosOffsetLeaf(0), DynamicSizeUtil.OFFSET_SIZE);

        // Copy
        copyKeysAndValues(leftCursor, 0, rightCursor, 0, leftKeyCount);

        // KeyCount
        TreeNodeUtil.setKeyCount(rightCursor, rightKeyCount + leftKeyCount);
    }

    private void copyKeysAndValues(PageCursor fromCursor, int fromPos, PageCursor toCursor, int toPos, int count) {
        int toAllocOffset = DynamicSizeUtil.getAllocOffset(toCursor);
        for (int i = 0; i < count; i++, toPos++) {
            toAllocOffset = copyRawKeyValue(fromCursor, fromPos + i, toCursor, toAllocOffset, false);
            toCursor.setOffset(keyPosOffsetLeaf(toPos));
            putUnsignedShort(toCursor, toAllocOffset);
        }
        setAllocOffset(toCursor, toAllocOffset);
    }

    /**
     * Calculates a valid and as optimal as possible position where to split a leaf if inserting a key overflows, trying to come as close as possible to
     * ratioToKeepInLeftOnSplit. There are a couple of goals/conditions which drives the search for it:
     * <ul>
     *     <li>The returned position will be one where the keys ending up in the left and right leaves respectively are guaranteed to fit.</li>
     *     <li>Out of those possible positions the one will be selected which leaves left node filled with with space closest to "targetLeftSpace".</li>
     * </ul>
     *
     * We loop over an imaginary range of keys where newKey has already been inserted at insertPos in the current node. splitPos point to position in the
     * imaginary range while currentPos point to the node. In the loop we "move" splitPos from left to right, accumulating space for left node as we go and
     * calculate delta towards targetLeftSpace. We want to continue loop as long as:
     * <ul>
     *     <li>We are still moving closer to optimal divide (currentDelta < prevDelta) and</li>
     *     <li>We are still inside end of range (splitPost < keyCountAfterInsert) and</li>
     *     <li>We have not accumulated to much space to fit in left node (accumulatedLeftSpace <= totalSpace).</li>
     * </ul>
     * But we also have to force loop to continue if the current position does not give a possible divide because right node will be given to much data to
     * fit (!thisPosPossible). Exiting loop means we've gone too far and thus we move one step back after loop, but only if the previous position gave us a
     * possible divide.
     *
     * @param cursor {@link PageCursor} to use for reading sizes of existing entries.
     * @param insertPos the pos which the new key will be inserted at.
     * @param newKey key to be inserted.
     * @param newValue value to be inserted.
     * @param keyCountAfterInsert key count including the new key.
     * @param ratioToKeepInLeftOnSplit What ratio of keys to try and keep in left node, 1=keep as much as possible, 0=move as much as possible to right
     * @return the pos where to split.
     */
    private int splitPosInLeaf(
            PageCursor cursor,
            int insertPos,
            KEY newKey,
            VALUE newValue,
            int keyCountAfterInsert,
            double ratioToKeepInLeftOnSplit) {
        int targetLeftSpace = (int) (this.totalSpace * ratioToKeepInLeftOnSplit);
        int splitPos = 0;
        int currentPos = 0;
        int accumulatedLeftSpace = 0;
        int currentDelta = targetLeftSpace;
        int prevDelta;
        int spaceOfNewKey = totalSpaceOfKeyValue(newKey, newValue);
        int totalSpaceIncludingNewKey = totalActiveSpace(cursor, keyCountAfterInsert - 1) + spaceOfNewKey;
        boolean includedNew = false;
        boolean prevPosPossible;
        boolean thisPosPossible = false;

        if (totalSpaceIncludingNewKey > totalSpace * 2) {
            throw new IllegalStateException(format(
                    "There's not enough space to insert new key, even when splitting the leaf. Space needed:%d, max space allowed:%d",
                    totalSpaceIncludingNewKey, totalSpace * 2));
        }

        do {
            prevPosPossible = thisPosPossible;

            // We may come closer to split by keeping one more in left
            int currentSpace;
            if (currentPos == insertPos && !includedNew) {
                currentSpace = spaceOfNewKey;
                includedNew = true;
                currentPos--;
            } else {
                currentSpace = totalSpaceOfKeyValue(cursor, currentPos);
            }
            accumulatedLeftSpace += currentSpace;
            prevDelta = currentDelta;
            currentDelta = Math.abs(accumulatedLeftSpace - targetLeftSpace);
            currentPos++;
            splitPos++;
            thisPosPossible = totalSpaceIncludingNewKey - accumulatedLeftSpace <= totalSpace;
        } while ((currentDelta < prevDelta && splitPos < keyCountAfterInsert && accumulatedLeftSpace <= totalSpace)
                || !thisPosPossible);
        // If previous position is possible then step back one pos since it divides the space most equally
        if (prevPosPossible) {
            splitPos--;
        }
        return splitPos;
    }

    private int totalActiveSpace(PageCursor cursor, int keyCount) {
        int deadSpace = getDeadSpace(cursor);
        int allocSpace = getAllocSpace(cursor, keyPosOffsetLeaf(keyCount));
        return totalSpace - deadSpace - allocSpace;
    }

    @Override
    public int totalSpaceOfKeyValue(KEY key, VALUE value) {
        int keySize = layout.keySize(key);
        int valueSize = layout.valueSize(value);
        boolean canInline = canInline(keySize + valueSize);
        if (canInline) {
            return DynamicSizeUtil.OFFSET_SIZE + getOverhead(keySize, valueSize, false) + keySize + valueSize;
        } else {
            return DynamicSizeUtil.OFFSET_SIZE + getOverhead(keySize, valueSize, true);
        }
    }

    protected int totalSpaceOfKeyValue(PageCursor cursor, int pos) {
        placeCursorAtActualKey(cursor, pos);
        long keyValueSize = readKeyValueSize(cursor);
        int keySize = extractKeySize(keyValueSize);
        int valueSize = extractValueSize(keyValueSize);
        boolean offload = extractOffload(keyValueSize);
        return DynamicSizeUtil.OFFSET_SIZE + getOverhead(keySize, valueSize, offload) + keySize + valueSize;
    }

    void placeCursorAtActualKey(PageCursor cursor, int pos) {
        // Set cursor to correct place in offset array
        int keyPosOffset = keyPosOffsetLeaf(pos);
        DynamicSizeUtil.redirectCursor(cursor, keyPosOffset, DynamicSizeUtil.HEADER_LENGTH_DYNAMIC, payloadSize);
    }

    boolean keyValueSizeTooLarge(int keySize, int valueSize) {
        return keySize + valueSize > keyValueSizeCap;
    }

    protected static int keyPosOffsetLeaf(int pos) {
        return DynamicSizeUtil.HEADER_LENGTH_DYNAMIC + pos * DynamicSizeUtil.OFFSET_SIZE;
    }

    @Override
    public String toString() {
        return "TreeNodeDynamicSize[pageSize:" + payloadSize + ", keyValueSizeCap:" + keyValueSizeCap
                + ", inlineKeyValueSizeCap:" + inlineKeyValueSizeCap + "]";
    }

    private String asString(PageCursor cursor, boolean includeValue, boolean includeAllocSpace) {
        int currentOffset = cursor.getOffset();
        // [header] <- dont care
        // LEAF:     [allocOffset=][child0,key0*,child1,...][keySize|key][keySize|key]
        // INTERNAL: [allocOffset=][key0*,key1*,...][offset|keySize|valueSize|key][keySize|valueSize|key]

        // HEADER
        int allocOffset = DynamicSizeUtil.getAllocOffset(cursor);
        int deadSpace = getDeadSpace(cursor);
        String additionalHeader =
                "{" + cursor.getCurrentPageId() + "} [allocOffset=" + allocOffset + " deadSpace=" + deadSpace + "] ";

        // OFFSET ARRAY
        String offsetArray = readOffsetArray(cursor);

        // ALLOC SPACE
        String allocSpace = "";
        if (includeAllocSpace) {
            allocSpace = readAllocSpace(cursor, allocOffset);
        }

        // KEYS
        KEY readKey = layout.newKey();
        VALUE readValue = layout.newValue();
        StringJoiner keys = new StringJoiner(" ");
        cursor.setOffset(allocOffset);
        while (cursor.getOffset() < cursor.getPagedFile().payloadSize()) {
            StringJoiner singleKey = new StringJoiner("|");
            singleKey.add(Integer.toString(cursor.getOffset()));
            long keyValueSize = readKeyValueSize(cursor);
            int keySize = extractKeySize(keyValueSize);
            boolean offload = extractOffload(keyValueSize);
            int valueSize;
            valueSize = extractValueSize(keyValueSize);
            if (DynamicSizeUtil.extractTombstone(keyValueSize)) {
                singleKey.add("T");
            } else {
                singleKey.add("_");
            }
            if (offload) {
                singleKey.add("O");
            } else {
                singleKey.add("_");
            }
            if (offload) {
                long offloadId = readOffloadId(cursor);
                singleKey.add(Long.toString(offloadId));
            } else {
                layout.readKey(cursor, readKey, keySize);
                layout.readValue(cursor, readValue, valueSize);
                singleKey.add(Integer.toString(keySize));
                if (includeValue) {
                    singleKey.add(Integer.toString(valueSize));
                }
                singleKey.add(readKey.toString());
                if (includeValue) {
                    singleKey.add(readValue.toString());
                }
            }
            keys.add(singleKey.toString());
        }

        cursor.setOffset(currentOffset);
        return additionalHeader + offsetArray + " " + allocSpace + " " + keys;
    }

    @Override
    public void printNode(
            PageCursor cursor,
            boolean includeValue,
            boolean includeAllocSpace,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext) {
        System.out.println(asString(cursor, includeValue, includeAllocSpace));
    }

    @Override
    public String checkMetaConsistency(PageCursor cursor) {
        // Reminder: Header layout
        // TotalSpace  |----------------------------------------|
        // ActiveSpace |-----------|   +    |---------|  + |----|
        // DeadSpace                                  |----|
        // AllocSpace              |--------|
        // AllocOffset                      v
        //     [Header][OffsetArray]........[_________,XXXX,____] (_ = alive key, X = dead key)

        long nodeId = cursor.getCurrentPageId();
        StringJoiner joiner =
                new StringJoiner(", ", "Meta data for tree node is inconsistent, id=" + nodeId + ": ", "");
        boolean hasInconsistency = false;

        // Verify allocOffset >= offsetArray
        int allocOffset = DynamicSizeUtil.getAllocOffset(cursor);
        int keyCount = TreeNodeUtil.keyCount(cursor);
        int offsetArray = keyPosOffsetLeaf(keyCount);
        if (allocOffset < offsetArray) {
            joiner.add(format(
                    "Overlap between offsetArray and allocSpace, offsetArray=%d, allocOffset=%d",
                    offsetArray, allocOffset));
            return joiner.toString();
        }

        // If keyCount is unreasonable we will likely go out of bounds in those checks
        if (reasonableKeyCount(keyCount)) {
            // Verify activeSpace + deadSpace + allocSpace == totalSpace
            int activeSpace = totalActiveSpaceRaw(cursor, keyCount);
            int deadSpace = getDeadSpace(cursor);
            int allocSpace = getAllocSpace(cursor, keyPosOffsetLeaf(keyCount));
            if (activeSpace + deadSpace + allocSpace != totalSpace) {
                hasInconsistency = true;
                joiner.add(format(
                        "Space areas did not sum to total space; activeSpace=%d, deadSpace=%d, allocSpace=%d, totalSpace=%d",
                        activeSpace, deadSpace, allocSpace, totalSpace));
            }

            // Verify no overlap between alloc space and active keys
            int lowestActiveKeyOffset = lowestActiveKeyOffset(cursor, keyCount, payloadSize);
            if (lowestActiveKeyOffset < allocOffset) {
                hasInconsistency = true;
                joiner.add(format(
                        "Overlap between allocSpace and active keys, allocOffset=%d, lowestActiveKeyOffset=%d",
                        allocOffset, lowestActiveKeyOffset));
            }
        }

        if (allocOffset < payloadSize && allocOffset >= 0) {
            // Verify allocOffset point at start of key
            cursor.setOffset(allocOffset);
            long keyValueAtAllocOffset = readKeyValueSize(cursor);
            if (keyValueAtAllocOffset == 0) {
                hasInconsistency = true;
                joiner.add(format(
                        "Pointer to allocSpace is misplaced, it should point to start of key, allocOffset=%d",
                        allocOffset));
            }
        }

        // Report inconsistencies as cursor exception
        if (hasInconsistency) {
            return joiner.toString();
        }
        return "";
    }

    @Override
    public <ROOT_KEY> void deepVisitValue(PageCursor cursor, int pos, GBPTreeVisitor<ROOT_KEY, KEY, VALUE> visitor) {}

    protected static int lowestActiveKeyOffset(PageCursor cursor, int keyCount, int payloadSize) {
        int lowestOffsetSoFar = payloadSize;
        for (int pos = 0; pos < keyCount; pos++) {
            // Set cursor to correct place in offset array
            int keyPosOffset = keyPosOffsetLeaf(pos);
            cursor.setOffset(keyPosOffset);

            // Read actual offset to key
            int keyOffset = getUnsignedShort(cursor);
            lowestOffsetSoFar = Math.min(lowestOffsetSoFar, keyOffset);
        }
        return lowestOffsetSoFar;
    }

    // Calculated by reading data instead of extrapolate from allocSpace and deadSpace
    private int totalActiveSpaceRaw(PageCursor cursor, int keyCount) {
        // Offset array
        int offsetArrayStart = DynamicSizeUtil.HEADER_LENGTH_DYNAMIC;
        int offsetArrayEnd = keyPosOffsetLeaf(keyCount);
        int offsetArraySize = offsetArrayEnd - offsetArrayStart;

        // Alive keys
        int aliveKeySize = 0;
        int nextKeyOffset = DynamicSizeUtil.getAllocOffset(cursor);
        while (nextKeyOffset < payloadSize) {
            cursor.setOffset(nextKeyOffset);
            long keyValueSize = readKeyValueSize(cursor);
            int keySize = extractKeySize(keyValueSize);
            int valueSize = extractValueSize(keyValueSize);
            boolean offload = extractOffload(keyValueSize);
            boolean tombstone = extractTombstone(keyValueSize);
            if (!tombstone) {
                aliveKeySize += getOverhead(keySize, valueSize, offload) + keySize + valueSize;
            }
            nextKeyOffset = cursor.getOffset() + (offload ? DynamicSizeUtil.SIZE_OFFLOAD_ID : keySize + valueSize);
        }
        return offsetArraySize + aliveKeySize;
    }

    private String readAllocSpace(PageCursor cursor, int allocOffset) {
        int keyCount = TreeNodeUtil.keyCount(cursor);
        int endOfOffsetArray = keyPosOffsetLeaf(keyCount);
        cursor.setOffset(endOfOffsetArray);
        int bytesToRead = allocOffset - endOfOffsetArray;
        byte[] allocSpace = new byte[bytesToRead];
        cursor.getBytes(allocSpace);
        for (byte b : allocSpace) {
            if (b != 0) {
                return "v" + endOfOffsetArray + ">" + bytesToRead + "|" + Arrays.toString(allocSpace);
            }
        }
        return "v" + endOfOffsetArray + ">" + bytesToRead + "|[0...]";
    }

    private String readOffsetArray(PageCursor cursor) {
        int keyCount = TreeNodeUtil.keyCount(cursor);
        StringJoiner offsetArray = new StringJoiner(" ");
        for (int i = 0; i < keyCount; i++) {
            cursor.setOffset(keyPosOffsetLeaf(i));
            offsetArray.add(Integer.toString(getUnsignedShort(cursor)));
        }
        return offsetArray.toString();
    }

    private boolean canInline(int entrySize) {
        return entrySize <= inlineKeyValueSizeCap;
    }
}
