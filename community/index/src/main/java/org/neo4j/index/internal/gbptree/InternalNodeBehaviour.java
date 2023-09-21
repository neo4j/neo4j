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

public sealed interface InternalNodeBehaviour<KEY> extends SharedNodeBehaviour<KEY>
        permits InternalNodeDynamicSize, InternalNodeFixedSize {
    void initialize(PageCursor cursor, byte layerType, long stableGeneration, long unstableGeneration);

    void insertKeyAndRightChildAt(
            PageCursor cursor,
            KEY key,
            long child,
            int pos,
            int keyCount,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext)
            throws IOException;

    void removeKeyAndLeftChildAt(
            PageCursor cursor,
            int keyPos,
            int keyCount,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext)
            throws IOException;

    void removeKeyAndRightChildAt(
            PageCursor cursor,
            int keyPos,
            int keyCount,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext)
            throws IOException;

    boolean setKeyAt(PageCursor cursor, KEY key, int pos);

    void setChildAt(PageCursor cursor, long child, int pos, long stableGeneration, long unstableGeneration);

    boolean reasonableKeyCount(int keyCount);

    int childOffset(int pos);

    int maxKeyCount();

    Overflow overflow(PageCursor cursor, int currentKeyCount, KEY newKey);

    int availableSpace(PageCursor cursor, int currentKeyCount);

    int totalSpaceOfKeyChild(KEY key);

    void defragment(PageCursor cursor);

    void doSplit(
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
            throws IOException;

    void printNode(
            PageCursor cursor,
            boolean includeAllocSpace,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext);

    String checkMetaConsistency(PageCursor cursor);

    long childAt(PageCursor cursor, int pos, long stableGeneration, long unstableGeneration);

    long childAt(
            PageCursor cursor,
            int pos,
            long stableGeneration,
            long unstableGeneration,
            GBPTreeGenerationTarget generationTarget);
}
