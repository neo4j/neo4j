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
import static org.neo4j.index.internal.gbptree.GBPTreeGenerationTarget.NO_GENERATION_TARGET;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.read;
import static org.neo4j.index.internal.gbptree.Layout.FIXED_SIZE_KEY;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.BASE_HEADER_LENGTH;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.NO_OFFLOAD_ID;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.SIZE_PAGE_REFERENCE;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.insertSlotsAt;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.removeSlotAt;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.setKeyCount;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.splitPos;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.writeChild;

import java.util.Comparator;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;

/**
 * <p>
 * DESIGN
 * <pre>
 * # = empty space
 *
 * [                                   HEADER   82B                           ]|[   KEYS   ]|[     CHILDREN      ]
 * [NODETYPE][TYPE][GENERATION][KEYCOUNT][RIGHTSIBLING][LEFTSIBLING][SUCCESSOR]|[[KEY]...##]|[[CHILD][CHILD]...##]
 *  0         1     2           6         10            34           58          82
 * </pre>
 * Calc offset for key i (starting from 0)
 * HEADER_LENGTH + i * SIZE_KEY
 * <p>
 * Calc offset for child i
 * HEADER_LENGTH + SIZE_KEY * MAX_KEY_COUNT_INTERNAL + i * SIZE_CHILD
 * @param <KEY> type of key
 */
final class InternalNodeFixedSize<KEY> implements InternalNodeBehaviour<KEY> {
    private final int maxKeyCount;
    private final int keySize;
    private final Layout<KEY, ?> layout;
    private final int payloadSize;

    InternalNodeFixedSize(int payloadSize, Layout<KEY, ?> layout) {
        this.payloadSize = payloadSize;
        this.layout = layout;
        this.keySize = layout.keySize(null);
        this.maxKeyCount =
                Math.floorDiv(payloadSize - (BASE_HEADER_LENGTH + SIZE_PAGE_REFERENCE), keySize + SIZE_PAGE_REFERENCE);

        if (maxKeyCount < 2) {
            throw new MetadataMismatchException(format(
                    "For layout %s a page size of %d would only fit %d internal keys, minimum is 2",
                    layout, payloadSize, maxKeyCount));
        }
    }

    @Override
    public void initialize(PageCursor cursor, byte layerType, long stableGeneration, long unstableGeneration) {
        TreeNodeUtil.writeBaseHeader(
                cursor, TreeNodeUtil.INTERNAL_FLAG, layerType, stableGeneration, unstableGeneration);
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
    public void insertKeyAndRightChildAt(
            PageCursor cursor,
            KEY key,
            long child,
            int pos,
            int keyCount,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext) {
        insertKeyAt(cursor, key, pos, keyCount);
        insertChildAt(cursor, child, pos + 1, keyCount, stableGeneration, unstableGeneration);
    }

    @Override
    public void removeKeyAndLeftChildAt(
            PageCursor cursor,
            int keyPos,
            int keyCount,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext) {
        removeKeyAt(cursor, keyPos, keyCount);
        removeChildAt(cursor, keyPos, keyCount);
    }

    @Override
    public void removeKeyAndRightChildAt(
            PageCursor cursor,
            int keyPos,
            int keyCount,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext) {
        removeKeyAt(cursor, keyPos, keyCount);
        removeChildAt(cursor, keyPos + 1, keyCount);
    }

    @Override
    public boolean setKeyAt(PageCursor cursor, KEY key, int pos) {
        cursor.setOffset(keyOffset(pos));
        layout.writeKey(cursor, key);
        return true;
    }

    @Override
    public void setChildAt(PageCursor cursor, long child, int pos, long stableGeneration, long unstableGeneration) {
        int childOffset = childOffset(pos);
        cursor.setOffset(childOffset);
        writeChild(cursor, child, stableGeneration, unstableGeneration, pos, childOffset);
    }

    @Override
    public int childOffset(int pos) {
        assert pos >= 0 : pos;
        return BASE_HEADER_LENGTH + maxKeyCount * keySize + pos * SIZE_PAGE_REFERENCE;
    }

    @Override
    public int maxKeyCount() {
        return maxKeyCount;
    }

    private void insertKeyAt(PageCursor cursor, KEY key, int pos, int keyCount) {
        insertKeySlotsAt(cursor, pos, 1, keyCount);
        cursor.setOffset(keyOffset(pos));
        layout.writeKey(cursor, key);
    }

    private void removeKeyAt(PageCursor cursor, int pos, int keyCount) {
        removeSlotAt(cursor, pos, keyCount, keyOffset(0), keySize);
    }

    private void insertChildAt(
            PageCursor cursor, long child, int pos, int keyCount, long stableGeneration, long unstableGeneration) {
        insertChildSlot(cursor, pos, keyCount);
        setChildAt(cursor, child, pos, stableGeneration, unstableGeneration);
    }

    private void removeChildAt(PageCursor cursor, int pos, int keyCount) {
        removeSlotAt(cursor, pos, keyCount + 1, childOffset(0), SIZE_PAGE_REFERENCE);
    }

    private void insertKeySlotsAt(PageCursor cursor, int pos, int numberOfSlots, int keyCount) {
        insertSlotsAt(cursor, pos, numberOfSlots, keyCount, keyOffset(0), keySize);
    }

    private void insertChildSlot(PageCursor cursor, int pos, int keyCount) {
        insertSlotsAt(cursor, pos, 1, keyCount + 1, childOffset(0), SIZE_PAGE_REFERENCE);
    }

    private int keyOffset(int pos) {
        return BASE_HEADER_LENGTH + pos * keySize;
    }

    /* SPLIT, MERGE and REBALANCE*/

    @Override
    public Overflow overflow(PageCursor cursor, int currentKeyCount, KEY newKey) {
        return currentKeyCount + 1 > maxKeyCount ? Overflow.YES : Overflow.NO;
    }

    @Override
    public int availableSpace(PageCursor cursor, int currentKeyCount) {
        return maxKeyCount - currentKeyCount * (keySize + SIZE_PAGE_REFERENCE);
    }

    @Override
    public int totalSpaceOfKeyChild(KEY key) {
        return keySize + SIZE_PAGE_REFERENCE;
    }

    @Override
    public void defragment(PageCursor cursor) {
        // no-op
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
            CursorContext cursorContext) {
        int keyCountAfterInsert = leftKeyCount + 1;
        int splitPos = splitPos(keyCountAfterInsert, ratioToKeepInLeftOnSplit);

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

            leftCursor.copyTo(keyOffset(splitPos), rightCursor, keyOffset(0), rightKeyCount * keySize);
            leftCursor.copyTo(
                    childOffset(splitPos), rightCursor, childOffset(0), (rightKeyCount + 1) * SIZE_PAGE_REFERENCE);
            insertKeyAt(leftCursor, newKey, insertPos, splitPos - 1);
            insertChildAt(leftCursor, newRightChild, insertPos + 1, splitPos - 1, stableGeneration, unstableGeneration);
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
            int countBeforePos = insertPos - (splitPos + 1);
            // ... first copy
            if (countBeforePos > 0) {
                leftCursor.copyTo(keyOffset(splitPos + 1), rightCursor, keyOffset(0), countBeforePos * keySize);
            }
            // ... insert
            if (countBeforePos >= 0) {
                insertKeyAt(rightCursor, newKey, countBeforePos, countBeforePos);
            }
            // ... second copy
            int countAfterPos = leftKeyCount - insertPos;
            if (countAfterPos > 0) {
                leftCursor.copyTo(
                        keyOffset(insertPos), rightCursor, keyOffset(countBeforePos + 1), countAfterPos * keySize);
            }

            // Children
            countBeforePos = insertPos - splitPos;
            // ... first copy
            if (countBeforePos > 0) {
                // first copy
                leftCursor.copyTo(
                        childOffset(splitPos + 1), rightCursor, childOffset(0), countBeforePos * SIZE_PAGE_REFERENCE);
            }
            // ... insert
            insertChildAt(
                    rightCursor, newRightChild, countBeforePos, countBeforePos, stableGeneration, unstableGeneration);
            // ... second copy
            if (countAfterPos > 0) {
                leftCursor.copyTo(
                        childOffset(insertPos + 1),
                        rightCursor,
                        childOffset(countBeforePos + 1),
                        countAfterPos * SIZE_PAGE_REFERENCE);
            }
        }
        setKeyCount(leftCursor, splitPos);
        setKeyCount(rightCursor, rightKeyCount);
    }

    @Override
    public void printNode(
            PageCursor cursor,
            boolean includeAllocSpace,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext) {}

    @Override
    public String checkMetaConsistency(PageCursor cursor) {
        return "";
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

    @Override
    public boolean reasonableKeyCount(int keyCount) {
        return keyCount >= 0 && keyCount <= maxKeyCount;
    }

    @Override
    public String toString() {
        return "InternalNodeFixedSize[payloadSize:" + payloadSize + ", maxKeys:" + maxKeyCount + ", " + "keySize:"
                + keySize + "]";
    }
}
