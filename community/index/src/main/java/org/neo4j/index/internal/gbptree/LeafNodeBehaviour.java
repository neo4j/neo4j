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

import java.io.IOException;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;

interface LeafNodeBehaviour<KEY, VALUE> {
    void writeAdditionalHeader(PageCursor cursor);

    long offloadIdAt(PageCursor cursor, int pos);

    KEY keyAt(PageCursor cursor, KEY into, int pos, CursorContext cursorContext);

    void keyValueAt(
            PageCursor cursor, KEY intoKey, TreeNode.ValueHolder<VALUE> intoValue, int pos, CursorContext cursorContext)
            throws IOException;

    void insertKeyValueAt(
            PageCursor cursor,
            KEY key,
            VALUE value,
            int pos,
            int keyCount,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext)
            throws IOException;

    int removeKeyValueAt(
            PageCursor cursor,
            int pos,
            int keyCount,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext)
            throws IOException;

    int removeKeyValues(
            PageCursor cursor,
            int fromPosInclusive,
            int toPosExclusive,
            int keyCount,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext)
            throws IOException;

    TreeNode.ValueHolder<VALUE> valueAt(
            PageCursor cursor, TreeNode.ValueHolder<VALUE> into, int pos, CursorContext cursorContext)
            throws IOException;

    boolean setValueAt(
            PageCursor cursor,
            VALUE value,
            int pos,
            CursorContext cursorContext,
            long stableGeneration,
            long unstableGeneration)
            throws IOException;

    int keyValueSizeCap();

    int inlineKeyValueSizeCap();

    void validateKeyValueSize(KEY key, VALUE value);

    int maxKeyCount();

    TreeNode.Overflow overflow(PageCursor cursor, int currentKeyCount, KEY newKey, VALUE newValue);

    int availableSpace(PageCursor cursor, int currentKeyCount);

    int underflowThreshold();

    void defragment(PageCursor cursor);

    boolean underflow(PageCursor cursor, int keyCount);

    int canRebalance(PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount);

    boolean canMerge(PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount);

    int findSplitter(
            PageCursor cursor,
            int keyCount,
            KEY newKey,
            VALUE newValue,
            int insertPos,
            KEY newSplitter,
            double ratioToKeepInLeftOnSplit,
            CursorContext cursorContext);

    void doSplit(
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
            throws IOException;

    void moveKeyValuesFromLeftToRight(
            PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount, int fromPosInLeftNode);

    void copyKeyValuesFromLeftToRight(
            PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount);

    int totalSpaceOfKeyValue(KEY key, VALUE value);

    void printNode(
            PageCursor cursor,
            boolean includeValue,
            boolean includeAllocSpace,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext);

    String checkMetaConsistency(PageCursor cursor, int keyCount, GBPTreeConsistencyCheckVisitor visitor);

    <ROOT_KEY> void deepVisitValue(PageCursor cursor, int pos, GBPTreeVisitor<ROOT_KEY, KEY, VALUE> visitor)
            throws IOException;
}
