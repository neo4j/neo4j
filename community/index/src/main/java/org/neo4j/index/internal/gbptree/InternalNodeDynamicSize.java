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
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.compactToRight;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.extractKeySize;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.extractOffload;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.extractTombstone;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.extractValueSize;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.getAllocSpace;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.getDeadSpace;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.getOverhead;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.inlineKeyValueSizeCap;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.keyValueSizeCapFromPageSize;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.putKeyValueSize;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.putOffloadMarker;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.putTombstone;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.readKeyValueSize;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.readOffloadId;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.recordAliveBlocks;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.setAllocOffset;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.setDeadSpace;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.validateInlineCap;
import static org.neo4j.index.internal.gbptree.GBPTreeGenerationTarget.NO_GENERATION_TARGET;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.read;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.SIZE_PAGE_REFERENCE;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.insertSlotsAt;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.readDynamicKey;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.removeSlotAt;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.writeChild;
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
 *  INTERNAL
 * [                                   HEADER   86B                                                   ]|[  KEY_OFFSET_CHILDREN  ]######[  KEYS  ]
 * [NODETYPE][TYPE][GENERATION][KEYCOUNT][RIGHTSIBLING][LEFTSIBLING][SUCCESSOR][ALLOCOFFSET][DEADSPACE]|[C0,K0*,C1,K1*,C2,K2*,C3]->  <-[K2,K0,K1]
 *  0         1     2           6         10            34           58         82           84          86
 *
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
public final class InternalNodeDynamicSize<KEY> implements InternalNodeBehaviour<KEY> {

    private final int inlineKeySizeCap;
    private final int keySizeCap;

    private final int totalSpace;
    final OffloadStore<KEY, ?> offloadStore;
    private final int maxKeyCount;

    final Layout<KEY, ?> layout;
    final int payloadSize;

    InternalNodeDynamicSize(int payloadSize, Layout<KEY, ?> layout, OffloadStore<KEY, ?> offloadStore) {
        this.payloadSize = payloadSize;
        this.layout = layout;

        assert payloadSize < DynamicSizeUtil.SUPPORTED_PAGE_SIZE_LIMIT
                : "Only payload size less then " + DynamicSizeUtil.SUPPORTED_PAGE_SIZE_LIMIT + " bytes supported";
        this.totalSpace = payloadSize - DynamicSizeUtil.HEADER_LENGTH_DYNAMIC;
        this.maxKeyCount = totalSpace / (DynamicSizeUtil.OFFSET_SIZE + MIN_SIZE_KEY_VALUE_SIZE);
        this.offloadStore = offloadStore;

        this.inlineKeySizeCap = inlineKeyValueSizeCap(payloadSize);
        this.keySizeCap = keyValueSizeCapFromPageSize(payloadSize);

        validateInlineCap(inlineKeySizeCap, payloadSize);
    }

    @Override
    public void initialize(PageCursor cursor, byte layerType, long stableGeneration, long unstableGeneration) {
        TreeNodeUtil.writeBaseHeader(
                cursor, TreeNodeUtil.INTERNAL_FLAG, layerType, stableGeneration, unstableGeneration);
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
        return readDynamicKey(layout, offloadStore, cursor, into, pos, cursorContext, keySizeCap);
    }

    @Override
    public Comparator<KEY> keyComparator() {
        return layout;
    }

    @Override
    public void insertKeyAndRightChildAt(
            PageCursor cursor,
            KEY key,
            long child,
            int pos,
            int keyCount,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext)
            throws IOException {
        // Where to write key?
        int currentKeyOffset = DynamicSizeUtil.getAllocOffset(cursor);
        int keySize = layout.keySize(key);
        int newKeyOffset;
        if (canInline(keySize)) {
            newKeyOffset = currentKeyOffset - keySize - getOverhead(keySize, 0, false);

            // Write key
            cursor.setOffset(newKeyOffset);
            putKeyValueSize(cursor, keySize, 0);

            layout.writeKey(cursor, key);
        } else {
            newKeyOffset = currentKeyOffset - getOverhead(keySize, 0, true);

            cursor.setOffset(newKeyOffset);
            putOffloadMarker(cursor);
            long offloadId = offloadStore.writeKey(key, stableGeneration, unstableGeneration, cursorContext);
            DynamicSizeUtil.putOffloadId(cursor, offloadId);
        }

        // Update alloc space
        setAllocOffset(cursor, newKeyOffset);

        // Write to offset array
        int childPos = pos + 1;
        int childOffset = childOffset(childPos);
        insertSlotsAt(cursor, pos, 1, keyCount, keyPosOffsetInternal(0), DynamicSizeUtil.KEY_OFFSET_AND_CHILD_SIZE);
        cursor.setOffset(keyPosOffsetInternal(pos));
        putUnsignedShort(cursor, newKeyOffset);
        writeChild(cursor, child, stableGeneration, unstableGeneration, childPos, childOffset);
    }

    @Override
    public void removeKeyAndRightChildAt(
            PageCursor cursor,
            int keyPos,
            int keyCount,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext)
            throws IOException {
        placeCursorAtActualKey(cursor, keyPos);
        int keyOffset = cursor.getOffset();
        long keyValueSize = readKeyValueSize(cursor);
        int keySize = extractKeySize(keyValueSize);
        boolean offload = extractOffload(keyValueSize);

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
        setDeadSpace(cursor, deadSpace + keySize + getOverhead(keySize, 0, offload));

        // Remove for offsetArray
        removeSlotAt(cursor, keyPos, keyCount, keyPosOffsetInternal(0), DynamicSizeUtil.KEY_OFFSET_AND_CHILD_SIZE);

        // Zero pad empty area
        zeroPad(cursor, keyPosOffsetInternal(keyCount - 1), DynamicSizeUtil.KEY_OFFSET_AND_CHILD_SIZE);
    }

    @Override
    public void removeKeyAndLeftChildAt(
            PageCursor cursor,
            int keyPos,
            int keyCount,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext)
            throws IOException {
        placeCursorAtActualKey(cursor, keyPos);
        int keyOffset = cursor.getOffset();
        long keyValueSize = readKeyValueSize(cursor);
        int keySize = extractKeySize(keyValueSize);
        boolean offload = extractOffload(keyValueSize);

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
        setDeadSpace(cursor, deadSpace + keySize + getOverhead(keySize, 0, offload));

        // Remove for offsetArray
        removeSlotAt(
                cursor,
                keyPos,
                keyCount,
                keyPosOffsetInternal(0) - SIZE_PAGE_REFERENCE,
                DynamicSizeUtil.KEY_OFFSET_AND_CHILD_SIZE);

        // Move last child
        cursor.copyTo(childOffset(keyCount), cursor, childOffset(keyCount - 1), SIZE_PAGE_REFERENCE);

        // Zero pad empty area
        zeroPad(cursor, keyPosOffsetInternal(keyCount - 1), DynamicSizeUtil.KEY_OFFSET_AND_CHILD_SIZE);
    }

    @Override
    public boolean setKeyAt(PageCursor cursor, KEY key, int pos) {
        placeCursorAtActualKey(cursor, pos);

        long keyValueSize = readKeyValueSize(cursor);
        int oldKeySize = extractKeySize(keyValueSize);
        int oldValueSize = extractValueSize(keyValueSize);
        if (keyValueSizeTooLarge(oldKeySize, oldValueSize)) {
            readUnreliableKeyValueSize(cursor, oldKeySize, oldValueSize, keyValueSize, pos);
        }
        int newKeySize = layout.keySize(key);
        if (newKeySize == oldKeySize) {
            // Fine, we can just overwrite
            layout.writeKey(cursor, key);
            return true;
        }
        return false;
    }

    @Override
    public void setChildAt(PageCursor cursor, long child, int pos, long stableGeneration, long unstableGeneration) {
        int childOffset = childOffset(pos);
        cursor.setOffset(childOffset);
        writeChild(cursor, child, stableGeneration, unstableGeneration, pos, childOffset);
    }

    @Override
    public boolean reasonableKeyCount(int keyCount) {
        return keyCount >= 0 && keyCount <= maxKeyCount;
    }

    @Override
    public int childOffset(int pos) {
        // Child pointer to the left of key at pos
        return keyPosOffsetInternal(pos) - SIZE_PAGE_REFERENCE;
    }

    @Override
    public int maxKeyCount() {
        return maxKeyCount;
    }

    @Override
    public Overflow overflow(PageCursor cursor, int currentKeyCount, KEY newKey) {
        int neededSpace = totalSpaceOfKeyChild(newKey);
        int deadSpace = getDeadSpace(cursor);
        int allocSpace = getAllocSpace(cursor, keyPosOffsetInternal(currentKeyCount));

        return DynamicSizeUtil.calculateOverflow(neededSpace, deadSpace, allocSpace);
    }

    @Override
    public int availableSpace(PageCursor cursor, int currentKeyCount) {
        int deadSpace = getDeadSpace(cursor);
        int allocSpace = getAllocSpace(cursor, keyPosOffsetInternal(currentKeyCount));
        return allocSpace + deadSpace;
    }

    @Override
    public void defragment(PageCursor cursor) {
        doDefragment(cursor, TreeNodeUtil.keyCount(cursor));
    }

    private void doDefragment(PageCursor cursor, int keyCount) {
        var offsets = new int[keyCount];
        var sizes = new int[keyCount];
        // collect alive offsets and sizes
        recordAliveBlocks(cursor, keyCount, offsets, sizes, payloadSize);

        compactToRight(cursor, keyCount, offsets, sizes, payloadSize, InternalNodeDynamicSize::keyPosOffsetInternal);
        // Update dead space
        setDeadSpace(cursor, 0);
    }

    @Override
    public void doSplit(
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
        int keyCountAfterInsert = leftKeyCount + 1;
        int splitPos = splitPosInternal(leftCursor, insertPos, newKey, keyCountAfterInsert, ratioToKeepInLeftOnSplit);

        if (splitPos == insertPos) {
            layout.copyKey(newKey, newSplitter);
        } else {
            keyAt(leftCursor, newSplitter, insertPos < splitPos ? splitPos - 1 : splitPos, cursorContext);
        }
        int rightKeyCount = keyCountAfterInsert - splitPos - 1; // -1 because don't keep prim key in internal

        if (insertPos < splitPos) {
            //                         v-------v       copy
            // before key    _,_,_,_,_,_,_,_,_,_
            // before child -,-,-,-,-,-,-,-,-,-,-
            // insert key    _,_,X,_,_,_,_,_,_,_,_
            // insert child -,-,-,x,-,-,-,-,-,-,-,-
            // split key               ^

            moveKeysAndChildren(leftCursor, splitPos, rightCursor, 0, rightKeyCount, true);
            // Rightmost key in left is the one we send up to parent, remove it from here.
            removeKeyAndRightChildAt(
                    leftCursor, splitPos - 1, splitPos, stableGeneration, unstableGeneration, cursorContext);
            doDefragment(leftCursor, splitPos - 1);
            insertKeyAndRightChildAt(
                    leftCursor,
                    newKey,
                    newRightChild,
                    insertPos,
                    splitPos - 1,
                    stableGeneration,
                    unstableGeneration,
                    cursorContext);
        } else {
            // pos > splitPos
            //                         v-v          first copy
            //                             v-v-v    second copy
            // before key    _,_,_,_,_,_,_,_,_,_
            // before child -,-,-,-,-,-,-,-,-,-,-
            // insert key    _,_,_,_,_,_,_,X,_,_,_
            // insert child -,-,-,-,-,-,-,-,x,-,-,-
            // split key               ^

            // pos == splitPos
            //                                      first copy
            //                         v-v-v-v-v    second copy
            // before key    _,_,_,_,_,_,_,_,_,_
            // before child -,-,-,-,-,-,-,-,-,-,-
            // insert key    _,_,_,_,_,X,_,_,_,_,_
            // insert child -,-,-,-,-,-,x,-,-,-,-,-
            // split key               ^

            // Keys
            if (insertPos == splitPos) {
                int copyFrom = splitPos;
                int copyCount = leftKeyCount - copyFrom;
                moveKeysAndChildren(leftCursor, copyFrom, rightCursor, 0, copyCount, false);
                doDefragment(leftCursor, splitPos);
                setChildAt(rightCursor, newRightChild, 0, stableGeneration, unstableGeneration);
            } else {
                int copyFrom = splitPos + 1;
                int copyCount = leftKeyCount - copyFrom;
                moveKeysAndChildren(leftCursor, copyFrom, rightCursor, 0, copyCount, true);
                // Rightmost key in left is the one we send up to parent, remove it from here.
                removeKeyAndRightChildAt(
                        leftCursor, splitPos, splitPos + 1, stableGeneration, unstableGeneration, cursorContext);
                doDefragment(leftCursor, splitPos);
                insertKeyAndRightChildAt(
                        rightCursor,
                        newKey,
                        newRightChild,
                        insertPos - copyFrom,
                        copyCount,
                        stableGeneration,
                        unstableGeneration,
                        cursorContext);
            }
        }
        TreeNodeUtil.setKeyCount(leftCursor, splitPos);
        TreeNodeUtil.setKeyCount(rightCursor, rightKeyCount);
    }

    // NOTE: Does NOT update keyCount
    private void moveKeysAndChildren(
            PageCursor fromCursor,
            int fromPos,
            PageCursor toCursor,
            int toPos,
            int count,
            boolean includeLeftMostChild) {
        if (count == 0 && !includeLeftMostChild) {
            // Nothing to move
            return;
        }

        // All children
        // This will also copy key offsets but those will be overwritten below.
        int childFromOffset = includeLeftMostChild ? childOffset(fromPos) : childOffset(fromPos + 1);
        int childToOffset = childOffset(fromPos + count) + SIZE_PAGE_REFERENCE;
        int lengthInBytes = childToOffset - childFromOffset;
        int targetOffset = includeLeftMostChild ? childOffset(0) : childOffset(1);
        fromCursor.copyTo(childFromOffset, toCursor, targetOffset, lengthInBytes);

        // Move actual keys and update pointers
        int toAllocOffset = DynamicSizeUtil.getAllocOffset(toCursor);
        int firstAllocOffset = toAllocOffset;
        for (int i = 0; i < count; i++, toPos++) {
            // Key
            toAllocOffset = transferRawKey(fromCursor, fromPos + i, toCursor, toAllocOffset);
            toCursor.setOffset(keyPosOffsetInternal(toPos));
            putUnsignedShort(toCursor, toAllocOffset);
        }
        setAllocOffset(toCursor, toAllocOffset);

        // Update deadSpace
        int deadSpace = getDeadSpace(fromCursor);
        int totalMovedBytes = firstAllocOffset - toAllocOffset;
        setDeadSpace(fromCursor, deadSpace + totalMovedBytes);

        // Zero pad empty area
        zeroPad(fromCursor, childFromOffset, lengthInBytes);
    }

    private static void zeroPad(PageCursor fromCursor, int fromOffset, int lengthInBytes) {
        fromCursor.setOffset(fromOffset);
        fromCursor.putBytes(lengthInBytes, (byte) 0);
    }

    private int transferRawKey(PageCursor fromCursor, int fromPos, PageCursor toCursor, int toAllocOffset) {
        // What to copy?
        placeCursorAtActualKey(fromCursor, fromPos);
        int fromKeyOffset = fromCursor.getOffset();
        long keyValueSize = readKeyValueSize(fromCursor);
        int keySize = extractKeySize(keyValueSize);
        boolean offload = extractOffload(keyValueSize);

        // Copy
        int toCopy = getOverhead(keySize, 0, offload) + keySize;
        toAllocOffset -= toCopy;
        fromCursor.copyTo(fromKeyOffset, toCursor, toAllocOffset, toCopy);

        // Put tombstone
        fromCursor.setOffset(fromKeyOffset);
        putTombstone(fromCursor);
        return toAllocOffset;
    }

    /**
     */
    private int splitPosInternal(
            PageCursor cursor, int insertPos, KEY newKey, int keyCountAfterInsert, double ratioToKeepInLeftOnSplit) {
        int targetLeftSpace = (int) (this.totalSpace * ratioToKeepInLeftOnSplit);
        int splitPos = 0;
        int currentPos = 0;
        int accumulatedLeftSpace = SIZE_PAGE_REFERENCE; // Leftmost child will always be included in left side
        int currentDelta = Math.abs(accumulatedLeftSpace - targetLeftSpace);
        int prevDelta;
        int spaceOfNewKeyAndChild = totalSpaceOfKeyChild(newKey);
        int totalSpaceIncludingNewKeyAndChild =
                totalActiveSpace(cursor, keyCountAfterInsert - 1) + spaceOfNewKeyAndChild;
        boolean includedNew = false;
        boolean prevPosPossible;
        boolean thisPosPossible = false;

        do {
            prevPosPossible = thisPosPossible;

            // We may come closer to split by keeping one more in left
            int space;
            if (currentPos == insertPos && !includedNew) {
                space = totalSpaceOfKeyChild(newKey);
                includedNew = true;
                currentPos--;
            } else {
                space = totalSpaceOfKeyChild(cursor, currentPos);
            }
            accumulatedLeftSpace += space;
            prevDelta = currentDelta;
            currentDelta = Math.abs(accumulatedLeftSpace - targetLeftSpace);
            splitPos++;
            currentPos++;
            thisPosPossible = totalSpaceIncludingNewKeyAndChild - accumulatedLeftSpace < totalSpace;
        } while ((currentDelta < prevDelta && splitPos < keyCountAfterInsert && accumulatedLeftSpace < totalSpace)
                || !thisPosPossible);
        if (prevPosPossible) {
            splitPos--; // Step back to the pos that most equally divide the available space in two
        }
        return splitPos;
    }

    private int totalActiveSpace(PageCursor cursor, int keyCount) {
        int deadSpace = getDeadSpace(cursor);
        int allocSpace = getAllocSpace(cursor, keyPosOffsetInternal(keyCount));
        return totalSpace - deadSpace - allocSpace;
    }

    @Override
    public int totalSpaceOfKeyChild(KEY key) {
        int keySize = layout.keySize(key);
        boolean canInline = canInline(keySize);
        if (canInline) {
            return DynamicSizeUtil.OFFSET_SIZE + getOverhead(keySize, 0, false) + SIZE_PAGE_REFERENCE + keySize;
        } else {
            return DynamicSizeUtil.OFFSET_SIZE + getOverhead(keySize, 0, true) + SIZE_PAGE_REFERENCE;
        }
    }

    private int totalSpaceOfKeyChild(PageCursor cursor, int pos) {
        placeCursorAtActualKey(cursor, pos);
        long keyValueSize = readKeyValueSize(cursor);
        int keySize = extractKeySize(keyValueSize);
        boolean offload = extractOffload(keyValueSize);
        return DynamicSizeUtil.OFFSET_SIZE + getOverhead(keySize, 0, offload) + SIZE_PAGE_REFERENCE + keySize;
    }

    private void placeCursorAtActualKey(PageCursor cursor, int pos) {
        // Set cursor to correct place in offset array
        int keyPosOffset = keyPosOffsetInternal(pos);
        DynamicSizeUtil.redirectCursor(cursor, keyPosOffset, DynamicSizeUtil.HEADER_LENGTH_DYNAMIC, payloadSize);
    }

    void readUnreliableKeyValueSize(PageCursor cursor, int keySize, int valueSize, long keyValueSize, int pos) {
        cursor.setCursorException(format(
                "Read unreliable key, id=%d, keySize=%d, valueSize=%d, keyValueSizeCap=%d, keyHasTombstone=%b, pos=%d",
                cursor.getCurrentPageId(), keySize, valueSize, keySizeCap, extractTombstone(keyValueSize), pos));
    }

    boolean keyValueSizeTooLarge(int keySize, int valueSize) {
        return keySize + valueSize > keySizeCap;
    }

    private static int keyPosOffsetInternal(int pos) {
        // header + childPointer + pos * (keyPosOffsetSize + childPointer)
        return DynamicSizeUtil.HEADER_LENGTH_DYNAMIC
                + SIZE_PAGE_REFERENCE
                + pos * DynamicSizeUtil.KEY_OFFSET_AND_CHILD_SIZE;
    }

    @Override
    public String toString() {
        return "TreeNodeDynamicSize[pageSize:" + payloadSize + ", keyValueSizeCap:" + keySizeCap + ", inlineKeySizeCap:"
                + inlineKeySizeCap + "]";
    }

    private String asString(
            PageCursor cursor, boolean includeAllocSpace, long stableGeneration, long unstableGeneration) {
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
        String offsetArray = readOffsetArray(cursor, stableGeneration, unstableGeneration);

        // ALLOC SPACE
        String allocSpace = "";
        if (includeAllocSpace) {
            allocSpace = readAllocSpace(cursor, allocOffset);
        }

        // KEYS
        KEY readKey = layout.newKey();
        StringJoiner keys = new StringJoiner(" ");
        cursor.setOffset(allocOffset);
        while (cursor.getOffset() < cursor.getPagedFile().payloadSize()) {
            StringJoiner singleKey = new StringJoiner("|");
            singleKey.add(Integer.toString(cursor.getOffset()));
            long keyValueSize = readKeyValueSize(cursor);
            int keySize = extractKeySize(keyValueSize);
            boolean offload = extractOffload(keyValueSize);
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
                singleKey.add(Integer.toString(keySize));
                singleKey.add(readKey.toString());
            }
            keys.add(singleKey.toString());
        }

        cursor.setOffset(currentOffset);
        return additionalHeader + offsetArray + " " + allocSpace + " " + keys;
    }

    @Override
    public void printNode(
            PageCursor cursor,
            boolean includeAllocSpace,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext) {
        System.out.println(asString(cursor, includeAllocSpace, stableGeneration, unstableGeneration));
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
        int offsetArray = keyPosOffsetInternal(keyCount);
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
            int allocSpace = getAllocSpace(cursor, keyPosOffsetInternal(keyCount));
            if (activeSpace + deadSpace + allocSpace != totalSpace) {
                hasInconsistency = true;
                joiner.add(format(
                        "Space areas did not sum to total space; activeSpace=%d, deadSpace=%d, allocSpace=%d, totalSpace=%d",
                        activeSpace, deadSpace, allocSpace, totalSpace));
            }

            // Verify no overlap between alloc space and active keys
            int lowestActiveKeyOffset = lowestActiveKeyOffset(cursor, keyCount);
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

    private int lowestActiveKeyOffset(PageCursor cursor, int keyCount) {
        int lowestOffsetSoFar = payloadSize;
        for (int pos = 0; pos < keyCount; pos++) {
            // Set cursor to correct place in offset array
            int keyPosOffset = keyPosOffsetInternal(pos);
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
        int offsetArrayEnd = keyPosOffsetInternal(keyCount);
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
        int endOfOffsetArray = keyPosOffsetInternal(keyCount);
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

    private String readOffsetArray(PageCursor cursor, long stableGeneration, long unstableGeneration) {
        int keyCount = TreeNodeUtil.keyCount(cursor);
        StringJoiner offsetArray = new StringJoiner(" ");
        for (int i = 0; i < keyCount; i++) {
            long childPointer =
                    GenerationSafePointerPair.pointer(childAt(cursor, i, stableGeneration, unstableGeneration));
            offsetArray.add("/" + childPointer + "\\");
            cursor.setOffset(keyPosOffsetInternal(i));
            offsetArray.add(Integer.toString(getUnsignedShort(cursor)));
        }
        long childPointer =
                GenerationSafePointerPair.pointer(childAt(cursor, keyCount, stableGeneration, unstableGeneration));
        offsetArray.add("/" + childPointer + "\\");
        return offsetArray.toString();
    }

    @Override
    public long childAt(PageCursor cursor, int pos, long stableGeneration, long unstableGeneration) {
        return childAt(cursor, pos, stableGeneration, unstableGeneration, NO_GENERATION_TARGET);
    }

    @Override
    public long childAt(
            PageCursor cursor,
            int pos,
            long stableGeneration,
            long unstableGeneration,
            GBPTreeGenerationTarget generationTarget) {
        cursor.setOffset(childOffset(pos));
        return read(cursor, stableGeneration, unstableGeneration, generationTarget);
    }

    private boolean canInline(int entrySize) {
        return entrySize <= inlineKeySizeCap;
    }
}
