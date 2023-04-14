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

import static org.neo4j.io.pagecache.PageCursorUtil.getUnsignedShort;
import static org.neo4j.io.pagecache.PageCursorUtil.putUnsignedShort;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.util.VisibleForTesting;

/**
 * TreeNode with dynamically sized keys and values.
 * See {@link LeafNodeDynamicSize} and {@link InternalNodeDynamicSize} for details
 * See {@link DynamicSizeUtil} for more detailed layout for individual offset array entries and key / key_value entries.
 */
public class TreeNodeDynamicSize<KEY, VALUE> extends TreeNode<KEY, VALUE> {
    static final byte FORMAT_IDENTIFIER = 3;
    static final byte FORMAT_VERSION = 0;

    TreeNodeDynamicSize(int payloadSize, Layout<KEY, VALUE> layout, OffloadStore<KEY, VALUE> offloadStore) {
        super(
                layout,
                new LeafNodeDynamicSize<>(payloadSize, layout, offloadStore),
                new InternalNodeDynamicSize<>(payloadSize, layout, offloadStore));
    }

    @Override
    public String toString() {
        return "TreeNodeDynamicSize[internal:" + internal + ", leaf:" + leaf + "]";
    }

    // some hacks for testing
    @VisibleForTesting
    void setAllocOffset(PageCursor cursor, int allocOffset) {
        putUnsignedShort(cursor, DynamicSizeUtil.BYTE_POS_ALLOC_OFFSET, allocOffset);
    }

    @VisibleForTesting
    int getAllocOffset(PageCursor cursor) {
        return getUnsignedShort(cursor, DynamicSizeUtil.BYTE_POS_ALLOC_OFFSET);
    }

    @VisibleForTesting
    void setDeadSpace(PageCursor cursor, int deadSpace) {
        putUnsignedShort(cursor, DynamicSizeUtil.BYTE_POS_DEAD_SPACE, deadSpace);
    }

    @VisibleForTesting
    int getDeadSpace(PageCursor cursor) {
        return getUnsignedShort(cursor, DynamicSizeUtil.BYTE_POS_DEAD_SPACE);
    }

    @VisibleForTesting
    public int getHeaderLength() {
        return DynamicSizeUtil.HEADER_LENGTH_DYNAMIC;
    }
}
