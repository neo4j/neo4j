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
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.pointer;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.DATA_LAYER_FLAG;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.NO_OFFLOAD_ID;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.io.IOException;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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

    @Test
    void shouldSetSmallerValueIfFitsIntoAllocSpace() throws IOException {
        // given a key value pair
        initializeLeaf();
        var key = new RawBytes(new byte[] {3, 2, 1});
        var originalValue = new RawBytes(new byte[10]);
        leaf.insertKeyValueAt(cursor, key, originalValue, 0, 0, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);
        TreeNodeUtil.setKeyCount(cursor, 1);

        // when writing a smaller value
        int keySize = layout.keySize(key);
        var overwriteValue = new RawBytes(new byte[6]);
        int overwriteValueSize = layout.valueSize(overwriteValue);
        int newSize = keySize + overwriteValueSize + DynamicSizeUtil.getOverhead(keySize, overwriteValueSize, false);

        // and plenty of alloc space
        assert leaf.availableSpace(cursor, 1) > newSize;

        // then should write new value
        assertThat(leaf.setValueAt(cursor, overwriteValue, 0, 1, NULL_CONTEXT, STABLE_GENERATION, UNSTABLE_GENERATION))
                .isTrue();
    }

    @Test
    void shouldOffloadNewValueIfCantBeInlined() throws IOException {
        // given a key value pair
        initializeLeaf();
        var key = new RawBytes(new byte[] {3, 2, 1});
        var originalValue = new RawBytes(new byte[10]);

        leaf.insertKeyValueAt(cursor, key, originalValue, 0, 0, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);
        TreeNodeUtil.setKeyCount(cursor, 1);
        assertThat(leaf.offloadIdAt(cursor, 0)).isEqualTo(NO_OFFLOAD_ID);

        // when writing a value that can't inline
        int sizeCannotInline = leaf.inlineKeyValueSizeCap() + 1;
        var overwriteValue = new RawBytes("a".repeat(sizeCannotInline).getBytes());
        assertThat(leaf.setValueAt(cursor, overwriteValue, 0, 1, NULL_CONTEXT, STABLE_GENERATION, UNSTABLE_GENERATION))
                .isTrue();

        // then should be offloaded
        assertThat(leaf.offloadIdAt(cursor, 0)).isNotEqualTo(NO_OFFLOAD_ID);
        var readKey = new RawBytes();
        var valueHolder = new ValueHolder<>(new RawBytes());
        leaf.keyValueAt(cursor, readKey, valueHolder, 0, NULL_CONTEXT);
        assertThat(readKey).isEqualTo(key);
        assertThat(valueHolder.value).isEqualTo(overwriteValue);
    }

    @Test
    void shouldInlinePreviouslyOffloadedValueIfNewValueFits() throws IOException {
        // given an offloaded key value pair
        initializeLeaf();
        int sizeCannotInline = leaf.inlineKeyValueSizeCap() + 1;
        var key = new RawBytes(new byte[] {3, 2, 1});
        var originalValue = new RawBytes("a".repeat(sizeCannotInline).getBytes());
        leaf.insertKeyValueAt(cursor, key, originalValue, 0, 0, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);
        TreeNodeUtil.setKeyCount(cursor, 1);
        assertThat(leaf.offloadIdAt(cursor, 0)).isNotEqualTo(NO_OFFLOAD_ID);

        // when writing a smaller value that can be inlined
        var overwriteValue = new RawBytes("b".repeat(10).getBytes());
        leaf.setValueAt(cursor, overwriteValue, 0, 1, NULL_CONTEXT, STABLE_GENERATION, UNSTABLE_GENERATION);

        // then should inline new value
        assertThat(leaf.offloadIdAt(cursor, 0)).isEqualTo(NO_OFFLOAD_ID);
        var keyHolder = new RawBytes();
        var valueHolder = new ValueHolder<>(new RawBytes());
        leaf.keyValueAt(cursor, keyHolder, valueHolder, 0, NULL_CONTEXT);
        assertThat(valueHolder.value).isEqualTo(overwriteValue);
        assertThat(keyHolder).isEqualTo(key);
    }

    @Test
    void shouldOffloadPreviouslyOffloadedValueIfNewValueIsBig() throws IOException {
        // given an offloaded key value pair
        initializeLeaf();
        int sizeCannotInline = leaf.inlineKeyValueSizeCap() + 1;
        var key = new RawBytes(new byte[] {3, 2, 1});
        var originalValue = new RawBytes("a".repeat(sizeCannotInline).getBytes());
        leaf.insertKeyValueAt(cursor, key, originalValue, 0, 0, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);
        TreeNodeUtil.setKeyCount(cursor, 1);
        assertThat(leaf.offloadIdAt(cursor, 0)).isNotEqualTo(NO_OFFLOAD_ID);

        // when writing a big value
        var overwriteValue = new RawBytes("b".repeat(sizeCannotInline).getBytes());
        assertThat(leaf.setValueAt(cursor, overwriteValue, 0, 1, NULL_CONTEXT, STABLE_GENERATION, UNSTABLE_GENERATION))
                .isTrue();

        // then should offload new value too
        assertThat(leaf.offloadIdAt(cursor, 0)).isNotEqualTo(NO_OFFLOAD_ID);
        var keyHolder = new RawBytes();
        var valueHolder = new ValueHolder<>(new RawBytes());
        leaf.keyValueAt(cursor, keyHolder, valueHolder, 0, NULL_CONTEXT);
        assertThat(valueHolder.value).isEqualTo(overwriteValue);
        assertThat(keyHolder).isEqualTo(key);
    }

    @Test
    void shouldWriteBiggerValueIfItFitsIntoDeadSpacePlusAllocSpace() throws IOException {
        // given a key value pair
        initializeLeaf();
        var key = new RawBytes(new byte[] {3, 2, 1});
        var originalValue = new RawBytes(new byte[10]);
        int keyCount = 0;
        int pos = 0;
        leaf.insertKeyValueAt(
                cursor, key, originalValue, pos, keyCount++, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);

        // and a full-ish leaf
        while (true) {
            pos++;
            var keyToInsert = key(pos);
            var keySize = layout.keySize(keyToInsert);
            var valueToInsert = value(pos);
            var valueSize = layout.valueSize(valueToInsert);
            var totalSize = keySize + valueSize + DynamicSizeUtil.getOverhead(keySize, valueSize, false);

            if (leaf.availableSpace(cursor, keyCount) < totalSize) {
                break;
            }

            leaf.insertKeyValueAt(
                    cursor,
                    keyToInsert,
                    value(pos),
                    pos,
                    keyCount++,
                    STABLE_GENERATION,
                    UNSTABLE_GENERATION,
                    NULL_CONTEXT);
        }
        TreeNodeUtil.setKeyCount(cursor, keyCount);

        // with some dead space
        for (int j = keyCount - 1; j > 0; j--) {
            leaf.removeKeyValueAt(cursor, j, j + 1, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);
        }
        TreeNodeUtil.setKeyCount(cursor, 1);

        // when writing a value that fits in alloc space + dead space
        var newValueSize = leaf.availableSpace(cursor, keyCount);
        var overwriteValue = new RawBytes(new byte[newValueSize]);

        // then should set bigger value
        assertThat(leaf.setValueAt(cursor, overwriteValue, 0, 1, NULL_CONTEXT, STABLE_GENERATION, UNSTABLE_GENERATION))
                .isTrue();
    }

    @Test
    void shouldSetNewValueIfItFitsIntoOldSpace() throws IOException {
        // given a key value pair
        initializeLeaf();
        var key = new RawBytes(new byte[] {3, 2, 1});
        var originalValue = new RawBytes(new byte[11]);
        int keyCount = 0;
        int pos = 0;
        leaf.insertKeyValueAt(
                cursor, key, originalValue, pos, keyCount++, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);

        // and a full-ish leaf
        while (true) {
            pos++;

            var keyToInsert = key(pos);
            var keySize = layout.keySize(keyToInsert);
            var valueToInsert = value(pos);
            var valueSize = layout.valueSize(valueToInsert);
            var totalSize = keySize + valueSize + DynamicSizeUtil.getOverhead(keySize, valueSize, false);

            if (leaf.availableSpace(cursor, keyCount) < totalSize) {
                break;
            }

            leaf.insertKeyValueAt(
                    cursor,
                    key(pos),
                    value(pos),
                    pos,
                    keyCount++,
                    STABLE_GENERATION,
                    UNSTABLE_GENERATION,
                    NULL_CONTEXT);
        }
        TreeNodeUtil.setKeyCount(cursor, keyCount);

        // when writing a value that must use the old space to fit
        var availableSpace = leaf.availableSpace(cursor, keyCount);
        var overwriteValue = new RawBytes(new byte[availableSpace + 11]);

        // then should add new value
        assertThat(leaf.setValueAt(
                        cursor, overwriteValue, 0, keyCount, NULL_CONTEXT, STABLE_GENERATION, UNSTABLE_GENERATION))
                .isTrue();
    }

    @Test
    void totalSpaceOfKeyChildAtShouldMatchForOffloadedKeys() throws IOException {
        initializeInternal();
        long stable = 3;
        long unstable = 4;
        internal.setChildAt(cursor, 10, 0, stable, unstable);
        var inlinedKey = new RawBytes(new byte[] {1, 2, 3});
        internal.insertKeyAndRightChildAt(cursor, inlinedKey, 11, 0, 0, stable, unstable, NULL_CONTEXT);
        TreeNodeUtil.setKeyCount(cursor, 1);
        var offloadedKey = new RawBytes(new byte[DynamicSizeUtil.inlineKeyValueSizeCapInternalNode(PAGE_SIZE) + 1]);
        internal.insertKeyAndRightChildAt(cursor, offloadedKey, 12, 1, 1, stable, unstable, NULL_CONTEXT);
        TreeNodeUtil.setKeyCount(cursor, 2);
        assertThat(internal.offloadIdAt(cursor, 0)).isEqualTo(NO_OFFLOAD_ID);
        assertThat(internal.offloadIdAt(cursor, 1)).isNotEqualTo(NO_OFFLOAD_ID);

        var readKey = layout.newKey();
        for (int pos = 0; pos < 2; pos++) {
            assertEquals(
                    internal.totalSpaceOfKeyChild(internal.keyAt(cursor, readKey, pos, NULL_CONTEXT)),
                    internal.totalSpaceOfKeyChildAt(cursor, pos));
        }
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
    protected void defragmentLeaf(LeafNodeBehaviour<RawBytes, RawBytes> leaf, PageAwareByteArrayCursor cursor)
            throws IOException {
        var allocOffsetBefore = DynamicSizeUtil.getAllocOffset(cursor);
        leaf.defragment(cursor, TreeNodeUtil.keyCount(cursor), STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);
        var allocOffsetAfter = DynamicSizeUtil.getAllocOffset(cursor);
        assertThat(allocOffsetAfter).isGreaterThan(allocOffsetBefore);
        var deadSpaceAfter = DynamicSizeUtil.getDeadSpace(cursor);
        assertThat(deadSpaceAfter).isEqualTo(0);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldStoreTwoKeysOnTheEdgeOfMaxInlineSize(boolean testOldLimit) throws IOException {
        // This test relies on the assumption that a tree node can always store at least 2 keys.

        initializeInternal();
        long stable = 3;
        long unstable = 4;
        int keyCount = 0;
        long childId = 10;

        // At one point both internal and leaf nodes were using the same inline limit even though internal nodes
        // have a bigger overhead. This could cause problems when trying to write the rightmost child pointer as the
        // data growing from the left and the data growing from the right in the internal node would overlap.
        // Depending on the data on the right side this could result in a GSPP error when reading up what was
        // in the child pointer position before doing the actual write.
        int keySize = testOldLimit
                ? DynamicSizeUtil.inlineKeyValueSizeCapLeafNode(PAGE_SIZE, DynamicSizeUtil.HEADER_LENGTH_DYNAMIC)
                : DynamicSizeUtil.inlineKeyValueSizeCapInternalNode(PAGE_SIZE);

        // A key with data so any accidental read among the keys from the offset array will be noticed
        byte[] bytes = new byte[keySize];
        Arrays.fill(bytes, (byte) 0xFF);
        RawBytes key = new RawBytes(bytes);

        internal.setChildAt(cursor, childId, 0, stable, unstable);
        childId++;

        internal.insertKeyAndRightChildAt(cursor, key, childId, keyCount, keyCount, stable, unstable, NULL_CONTEXT);
        keyCount++;
        childId++;

        internal.insertKeyAndRightChildAt(cursor, key, childId, keyCount, keyCount, stable, unstable, NULL_CONTEXT);

        // Assert children
        long firstChild = 10;
        for (int i = 0; i <= keyCount; i++) {
            assertEquals(firstChild + i, pointer(internal.childAt(cursor, i, stable, unstable)));
        }

        // Assert keys (that happen to be the same because I'm lazy)
        RawBytes readKey = layout.newKey();
        for (int i = 0; i < keyCount; i++) {
            assertKeyEquals(key, internal.keyAt(cursor, readKey, i, NULL_CONTEXT));
        }
    }
}
