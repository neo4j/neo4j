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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.DATA_LAYER_FLAG;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.neo4j.io.pagecache.PageCursor;

public class TreeNodeDynamicSizeTest extends TreeNodeTestBase<RawBytes, RawBytes> {
    private static final long STABLE_GENERATION = 3;
    private static final long UNSTABLE_GENERATION = 4;

    private final SimpleByteArrayLayout layout = new SimpleByteArrayLayout();

    @Override
    protected TestLayout<RawBytes, RawBytes> getLayout() {
        return layout;
    }

    @Override
    protected LeafNodeBehaviour<RawBytes, RawBytes> getLeaf(
            int pageSize, Layout<RawBytes, RawBytes> layout, OffloadStore<RawBytes, RawBytes> offloadStore) {
        return new LeafNodeDynamicSize<>(pageSize, layout, offloadStore);
    }

    @Override
    protected InternalNodeBehaviour<RawBytes> getInternal(
            int pageSize, Layout<RawBytes, RawBytes> layout, OffloadStore<RawBytes, RawBytes> offloadStore) {
        return new InternalNodeDynamicSize<>(pageSize, layout, offloadStore);
    }

    @Override
    void assertAdditionalHeader(PageCursor cursor, int pageSize) {
        // When
        int currentAllocSpace = DynamicSizeUtil.getAllocOffset(cursor);

        // Then
        assertEquals(pageSize, currentAllocSpace, "allocSpace point to end of page");
    }

    @Test
    void mustCompactKeyValueSizeHeader() throws IOException {
        int oneByteKeyMax = DynamicSizeUtil.MASK_ONE_BYTE_KEY_SIZE;
        int oneByteValueMax = DynamicSizeUtil.MASK_ONE_BYTE_VALUE_SIZE;

        var node = getLeaf(PAGE_SIZE, layout, createOffloadStore());

        verifyOverhead(node, oneByteKeyMax, 0, 1);
        verifyOverhead(node, oneByteKeyMax, 1, 2);
        verifyOverhead(node, oneByteKeyMax, oneByteValueMax, 2);
        verifyOverhead(node, oneByteKeyMax, oneByteValueMax + 1, 3);
        verifyOverhead(node, oneByteKeyMax + 1, 0, 2);
        verifyOverhead(node, oneByteKeyMax + 1, 1, 3);
        verifyOverhead(node, oneByteKeyMax + 1, oneByteValueMax, 3);
        verifyOverhead(node, oneByteKeyMax + 1, oneByteValueMax + 1, 4);
    }

    private void verifyOverhead(
            LeafNodeBehaviour<RawBytes, RawBytes> leaf, int keySize, int valueSize, int expectedOverhead)
            throws IOException {
        cursor.zapPage();
        leaf.initialize(cursor, DATA_LAYER_FLAG, STABLE_GENERATION, UNSTABLE_GENERATION);

        RawBytes key = layout.newKey();
        RawBytes value = layout.newValue();
        key.bytes = new byte[keySize];
        value.bytes = new byte[valueSize];

        int allocOffsetBefore = DynamicSizeUtil.getAllocOffset(cursor);
        leaf.insertKeyValueAt(cursor, key, value, 0, 0, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);
        int allocOffsetAfter = DynamicSizeUtil.getAllocOffset(cursor);
        assertEquals(allocOffsetBefore - keySize - valueSize - expectedOverhead, allocOffsetAfter);
    }

    @Override
    protected void defragmentLeaf(LeafNodeBehaviour<RawBytes, RawBytes> leaf, PageAwareByteArrayCursor cursor) {
        var allocOffsetBefore = DynamicSizeUtil.getAllocOffset(cursor);
        leaf.defragment(cursor);
        var allocOffsetAfter = DynamicSizeUtil.getAllocOffset(cursor);
        assertThat(allocOffsetAfter).isGreaterThan(allocOffsetBefore);
        var deadSpaceAfter = DynamicSizeUtil.getDeadSpace(cursor);
        assertThat(deadSpaceAfter).isEqualTo(0);
    }
}
