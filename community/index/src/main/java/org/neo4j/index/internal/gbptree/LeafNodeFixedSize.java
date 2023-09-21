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
import static org.neo4j.index.internal.gbptree.Layout.FIXED_SIZE_KEY;
import static org.neo4j.index.internal.gbptree.Layout.FIXED_SIZE_VALUE;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.BASE_HEADER_LENGTH;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.NO_KEY_VALUE_SIZE_CAP;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.NO_OFFLOAD_ID;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.insertSlotsAt;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.removeSlotAt;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.removeSlotsAt;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.setKeyCount;

import java.io.IOException;
import java.util.Comparator;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;

/**
 * <p>
 * DESIGN
 * <p>
 * Using Separate design the leaf nodes should look like
 *
 * <pre>
 * [                                   HEADER   82B                           ]|[    KEYS  ]|[   VALUES   ]
 * [NODETYPE][TYPE][GENERATION][KEYCOUNT][RIGHTSIBLING][LEFTSIBLING][SUCCESSOR]|[[KEY]...##]|[[VALUE]...##]
 *  0         1     2           6         10            34           58          82
 * </pre>
 *
 * Calc offset for key i (starting from 0)
 * HEADER_LENGTH + i * SIZE_KEY
 * <p>
 * Calc offset for value i
 * HEADER_LENGTH + SIZE_KEY * MAX_KEY_COUNT_LEAF + i * SIZE_VALUE
 *
 * @param <KEY> type of key
 * @param <VALUE> type of value
 */
class LeafNodeFixedSize<KEY, VALUE> implements LeafNodeBehaviour<KEY, VALUE> {
    private final int maxKeyCount;
    private final int keySize;
    private final int valueSize;
    private final int halfSpace;

    protected final Layout<KEY, VALUE> layout;
    private final int payloadSize;

    LeafNodeFixedSize(int pageSize, Layout<KEY, VALUE> layout) {
        this(pageSize, layout, 0);
    }

    /**
     * @param payloadSize - page size
     * @param layout - layout
     * @param valuePadding - extra bytes allocated for each value, can be used by descendants to store additional data
     */
    LeafNodeFixedSize(int payloadSize, Layout<KEY, VALUE> layout, int valuePadding) {
        this.payloadSize = payloadSize;
        this.layout = layout;
        this.keySize = layout.keySize(null);
        this.valueSize = layout.valueSize(null) + valuePadding;
        this.maxKeyCount = Math.floorDiv(payloadSize - BASE_HEADER_LENGTH, keySize + valueSize);
        int halfKeyCount = (maxKeyCount + 1) / 2;
        this.halfSpace = halfKeyCount * (keySize + valueSize);

        if (maxKeyCount < 2) {
            throw new MetadataMismatchException(format(
                    "A page size of %d would only fit %d leaf keys (keySize:%d, valueSize:%d), minimum is 2",
                    payloadSize, maxKeyCount, keySize, valueSize));
        }
    }

    @Override
    public void initialize(PageCursor cursor, byte layerType, long stableGeneration, long unstableGeneration) {
        TreeNodeUtil.writeBaseHeader(cursor, TreeNodeUtil.LEAF_FLAG, layerType, stableGeneration, unstableGeneration);
    }

    @Override
    public long offloadIdAt(PageCursor cursor, int pos) {
        return NO_OFFLOAD_ID;
    }

    @Override
    public KEY keyAt(PageCursor cursor, KEY into, int pos, CursorContext cursorContext) {
        cursor.setOffset(keyOffset(pos));
        layout.readKey(cursor, into, FIXED_SIZE_KEY);
        return into;
    }

    @Override
    public Comparator<KEY> keyComparator() {
        return layout;
    }

    @Override
    public void keyValueAt(
            PageCursor cursor, KEY intoKey, ValueHolder<VALUE> intoValue, int pos, CursorContext cursorContext)
            throws IOException {
        keyAt(cursor, intoKey, pos, cursorContext);
        valueAt(cursor, intoValue, pos, cursorContext);
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
        insertKeyAt(cursor, key, pos, keyCount);
        insertValueAt(cursor, value, pos, keyCount, cursorContext, stableGeneration, unstableGeneration);
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
        removeKeyAt(cursor, pos, keyCount);
        removeValueAt(cursor, pos, keyCount);
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
        removeSlotsAt(cursor, fromPosInclusive, toPosExclusive, keyCount, keyOffset(0), keySize);
        removeSlotsAt(cursor, fromPosInclusive, toPosExclusive, keyCount, valueOffset(0), valueSize);
        return keyCount - toPosExclusive + fromPosInclusive;
    }

    @Override
    public ValueHolder<VALUE> valueAt(PageCursor cursor, ValueHolder<VALUE> value, int pos, CursorContext cursorContext)
            throws IOException {
        cursor.setOffset(valueOffset(pos));
        layout.readValue(cursor, value.value, FIXED_SIZE_VALUE);
        value.defined = true;
        return value;
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
        cursor.setOffset(valueOffset(pos));
        layout.writeValue(cursor, value);
        return true;
    }

    @Override
    public int keyValueSizeCap() {
        return NO_KEY_VALUE_SIZE_CAP;
    }

    @Override
    public int inlineKeyValueSizeCap() {
        return NO_KEY_VALUE_SIZE_CAP;
    }

    @Override
    public void validateKeyValueSize(KEY key, VALUE value) {
        // no-op for fixed size
    }

    private void insertKeyAt(PageCursor cursor, KEY key, int pos, int keyCount) {
        insertKeySlotsAt(cursor, pos, 1, keyCount);
        cursor.setOffset(keyOffset(pos));
        layout.writeKey(cursor, key);
    }

    @Override
    public int maxKeyCount() {
        return maxKeyCount;
    }

    private void removeKeyAt(PageCursor cursor, int pos, int keyCount) {
        removeSlotAt(cursor, pos, keyCount, keyOffset(0), keySize);
    }

    private void insertKeyValueSlots(PageCursor cursor, int numberOfSlots, int keyCount) {
        insertKeySlotsAt(cursor, 0, numberOfSlots, keyCount);
        insertValueSlotsAt(cursor, 0, numberOfSlots, keyCount);
    }

    // Always insert together with key. Use insertKeyValueAt
    protected void insertValueAt(
            PageCursor cursor,
            VALUE value,
            int pos,
            int keyCount,
            CursorContext cursorContext,
            long stableGeneration,
            long unstableGeneration)
            throws IOException {
        insertValueSlotsAt(cursor, pos, 1, keyCount);
        setValueAt(cursor, value, pos, cursorContext, stableGeneration, unstableGeneration);
    }

    // Always insert together with key. Use removeKeyValueAt
    private void removeValueAt(PageCursor cursor, int pos, int keyCount) {
        removeSlotAt(cursor, pos, keyCount, valueOffset(0), valueSize);
    }

    private void insertKeySlotsAt(PageCursor cursor, int pos, int numberOfSlots, int keyCount) {
        insertSlotsAt(cursor, pos, numberOfSlots, keyCount, keyOffset(0), keySize);
    }

    protected void insertValueSlotsAt(PageCursor cursor, int pos, int numberOfSlots, int keyCount) {
        insertSlotsAt(cursor, pos, numberOfSlots, keyCount, valueOffset(0), valueSize);
    }

    private int keyOffset(int pos) {
        return BASE_HEADER_LENGTH + pos * keySize;
    }

    protected int valueOffset(int pos) {
        return BASE_HEADER_LENGTH + maxKeyCount * keySize + pos * valueSize;
    }

    @Override
    public Overflow overflow(PageCursor cursor, int currentKeyCount, KEY newKey, VALUE newValue) {
        return currentKeyCount + 1 > maxKeyCount ? Overflow.YES : Overflow.NO;
    }

    @Override
    public int availableSpace(PageCursor cursor, int currentKeyCount) {
        return (maxKeyCount - currentKeyCount) * (keySize + valueSize);
    }

    @Override
    public int totalSpaceOfKeyValue(KEY key, VALUE value) {
        return keySize + valueSize;
    }

    @Override
    public void printNode(
            PageCursor cursor,
            boolean includeValue,
            boolean includeAllocSpace,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext) {}

    @Override
    public String checkMetaConsistency(PageCursor cursor) {
        return "";
    }

    @Override
    public int underflowThreshold() {
        return halfSpace;
    }

    @Override
    public void defragment(PageCursor cursor) { // no-op
    }

    @Override
    public boolean underflow(PageCursor cursor, int keyCount) {
        return availableSpace(cursor, keyCount) > halfSpace;
    }

    @Override
    public int canRebalance(PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount) {
        if (leftKeyCount + rightKeyCount >= maxKeyCount) {
            int totalKeyCount = rightKeyCount + leftKeyCount;
            int moveFromPosition = totalKeyCount / 2;
            return leftKeyCount - moveFromPosition;
        }
        return -1;
    }

    @Override
    public boolean canMerge(PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount) {
        return leftKeyCount + rightKeyCount <= maxKeyCount;
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
        int keyCountAfterInsert = keyCount + 1;
        int splitPos = TreeNodeUtil.splitPos(keyCountAfterInsert, ratioToKeepInLeftOnSplit);

        if (splitPos == insertPos) {
            layout.copyKey(newKey, newSplitter);
        } else {
            keyAt(cursor, newSplitter, insertPos < splitPos ? splitPos - 1 : splitPos, cursorContext);
        }
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
        int keyCountAfterInsert = leftKeyCount + 1;
        int rightKeyCount = keyCountAfterInsert - splitPos;

        if (insertPos < splitPos) {
            //                v---------v       copy
            // before _,_,_,_,_,_,_,_,_,_
            // insert _,_,_,X,_,_,_,_,_,_,_
            // split            ^
            copyKeysAndValues(leftCursor, splitPos - 1, rightCursor, 0, rightKeyCount);
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
            int countBeforePos = insertPos - splitPos;
            if (countBeforePos > 0) {
                // first copy
                copyKeysAndValues(leftCursor, splitPos, rightCursor, 0, countBeforePos);
            }
            insertKeyValueAt(
                    rightCursor,
                    newKey,
                    newValue,
                    countBeforePos,
                    countBeforePos,
                    stableGeneration,
                    unstableGeneration,
                    cursorContext);
            int countAfterPos = leftKeyCount - insertPos;
            if (countAfterPos > 0) {
                // second copy
                copyKeysAndValues(leftCursor, insertPos, rightCursor, countBeforePos + 1, countAfterPos);
            }
        }
        setKeyCount(leftCursor, splitPos);
        setKeyCount(rightCursor, rightKeyCount);
    }

    @Override
    public void moveKeyValuesFromLeftToRight(
            PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount, int fromPosInLeftNode) {
        int numberOfKeysToMove = leftKeyCount - fromPosInLeftNode;

        // Push keys and values in right sibling to the right
        insertKeyValueSlots(rightCursor, numberOfKeysToMove, rightKeyCount);

        // Move keys and values from left sibling to right sibling
        copyKeysAndValues(leftCursor, fromPosInLeftNode, rightCursor, 0, numberOfKeysToMove);

        setKeyCount(leftCursor, leftKeyCount - numberOfKeysToMove);
        setKeyCount(rightCursor, rightKeyCount + numberOfKeysToMove);
    }

    @Override
    public void copyKeyValuesFromLeftToRight(
            PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount) {
        // Push keys and values in right sibling to the right
        insertKeyValueSlots(rightCursor, leftKeyCount, rightKeyCount);

        // Move keys and values from left sibling to right sibling
        copyKeysAndValues(leftCursor, 0, rightCursor, 0, leftKeyCount);

        // KeyCount
        setKeyCount(rightCursor, rightKeyCount + leftKeyCount);
    }

    private void copyKeysAndValues(PageCursor fromCursor, int fromPos, PageCursor toCursor, int toPos, int count) {
        fromCursor.copyTo(keyOffset(fromPos), toCursor, keyOffset(toPos), count * keySize);
        int valueLength = count * valueSize;
        if (valueLength > 0) {
            fromCursor.copyTo(valueOffset(fromPos), toCursor, valueOffset(toPos), valueLength);
        }
    }

    @Override
    public String toString() {
        return "LeafFixedSize[payloadSize:" + payloadSize + ", maxKeys:" + maxKeyCount + ", " + "keySize:" + keySize
                + ", valueSize:" + valueSize + "]";
    }

    @Override
    public <ROOT_KEY> void deepVisitValue(PageCursor cursor, int pos, GBPTreeVisitor<ROOT_KEY, KEY, VALUE> visitor)
            throws IOException {}

    @Override
    public boolean reasonableKeyCount(int keyCount) {
        return keyCount >= 0 && keyCount <= maxKeyCount;
    }
}
