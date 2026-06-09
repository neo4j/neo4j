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

import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.index.internal.gbptree.Overflow.YES;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class InternalTreeLogicDynamicSizeTest extends InternalTreeLogicTestBase<RawBytes, RawBytes> {
    @Override
    protected ValueMerger<RawBytes, RawBytes> getAdder() {
        return (existingKey, newKey, base, add) -> {
            add(add, base);
            return ValueMerger.MergeResult.MERGED;
        };
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
    protected TestLayout<RawBytes, RawBytes> getLayout() {
        return new SimpleByteArrayLayout();
    }

    private void add(RawBytes add, RawBytes base) {
        long baseSeed = layout.keySeed(base);
        long addSeed = layout.keySeed(add);
        RawBytes merged = layout.value(baseSeed + addSeed);
        base.copyFrom(merged);
    }

    @Test
    void shouldFailToInsertTooLargeKeys() {
        RawBytes key = layout.newKey();
        RawBytes value = layout.newValue();
        key.bytes = new byte[leaf.keyValueSizeCap() + 1];
        value.bytes = EMPTY_BYTE_ARRAY;

        shouldFailToInsertTooLargeKeyAndValue(key, value);
    }

    @Test
    void shouldFailToInsertTooLargeKeyAndValueLargeKey() {
        RawBytes key = layout.newKey();
        RawBytes value = layout.newValue();
        key.bytes = new byte[leaf.keyValueSizeCap()];
        value.bytes = new byte[1];

        shouldFailToInsertTooLargeKeyAndValue(key, value);
    }

    @Test
    void shouldFailToInsertTooLargeKeyAndValueLargeValue() {
        RawBytes key = layout.newKey();
        RawBytes value = layout.newValue();
        key.bytes = new byte[1];
        value.bytes = new byte[leaf.keyValueSizeCap()];

        shouldFailToInsertTooLargeKeyAndValue(key, value);
    }

    private void shouldFailToInsertTooLargeKeyAndValue(RawBytes key, RawBytes value) {
        initialize();
        var e = assertThrows(IllegalArgumentException.class, () -> insert(key, value));
        assertThat(e.getMessage())
                .contains("Index key-value size it too large. Please see index documentation for limitations.");
    }

    @Test
    void storeOnlyMinimalKeyDividerInInternal() throws IOException {
        // given
        initialize();
        long key = 0;
        while (numberOfRootSplits == 0) {
            insert(key(key), value(key));
            key++;
        }

        // when
        RawBytes rawBytes = keyAt(root.id(), 0, true);

        // then
        assertEquals(Long.BYTES, rawBytes.bytes.length, "expected no tail on internal key but was " + rawBytes);
    }

    /* UPDATES */

    @ParameterizedTest
    @MethodSource("generators")
    void shouldSplitWhenUpdatingToLargerValueInFullLeaf(
            String name, GenerationManager generationManager, boolean isCheckpointing) throws Exception {
        // given a full-ish leaf
        initialize();
        int keyCount = 0;
        while (true) {
            var key = key(keyCount);
            var value = value(keyCount);
            if (leaf.overflow(cursor, keyCount, key, value, NULL_CONTEXT) == YES) {
                break;
            }

            insert(key, value);
            keyCount++;
        }

        generationManager.checkpoint();

        // when updating to a slightly too big value
        int posToUpdate = keyCount / 2;
        RawBytes currentValue = value(posToUpdate);
        var availableSpace = leaf.availableSpace(cursor, keyCount);
        int newValueSize = currentValue.bytes.length + availableSpace + 1;
        RawBytes newValue = new RawBytes("a".repeat(newValueSize).getBytes());
        var keyToUpdate = key(posToUpdate);
        insert(keyToUpdate, newValue);

        // then should split
        assertEquals(1, numberOfRootSplits);

        root.goTo(readCursor);
        long child0 = childAt(readCursor, 0, stableGeneration, unstableGeneration);
        int leftKeyCount = keyCount(child0);
        if (posToUpdate < leftKeyCount) {
            // then in left child
            RawBytes key = keyAt(child0, posToUpdate, false);
            RawBytes value = valueAt(child0, posToUpdate);
            assertEqualsKey(key(posToUpdate), key);
            assertEqualsValue(newValue, value);
        } else {
            long child1 = childAt(readCursor, 1, stableGeneration, unstableGeneration);
            RawBytes key = keyAt(child1, posToUpdate - leftKeyCount, false);
            RawBytes value = valueAt(child1, posToUpdate - leftKeyCount);
            assertEqualsKey(key(posToUpdate), key);
            assertEqualsValue(newValue, value);
        }
    }

    @Test
    void shouldSplitCorrectlyWhenUpdatingEntryAtPosZeroFollowedByLargeEntry() throws Exception {
        initialize();
        insert(key(0L), new RawBytes(new byte[1]));
        insert(key(1L), new RawBytes(new byte[70]));
        insert(key(2L), new RawBytes(new byte[46]));
        insert(key(3L), new RawBytes(new byte[0]));
        assertEquals(0, leaf.availableSpace(cursor, 4), "leaf must be fully packed before update");

        var newValA = new RawBytes(new byte[16]);
        insert(key(0L), newValA);

        assertEquals(1, numberOfRootSplits);
        root.goTo(readCursor);
        long leftChild = childAt(readCursor, 0, stableGeneration, unstableGeneration);
        assertEqualsValue(newValA, valueAt(leftChild, 0));
    }

    @ParameterizedTest
    @MethodSource("generators")
    void shouldCreateNewVersionWhenInsertInStableInternal(
            String name, GenerationManager generationManager, boolean isCheckpointing) throws Exception {
        assumeTrue(isCheckpointing, "No checkpointing, no successor");

        // GIVEN
        initialize();
        long someHighMultiplier = 1000;
        for (int i = 0; numberOfRootSplits < 2; i++) {
            long seed = i * someHighMultiplier;
            insert(key(seed), value(seed));
        }
        long rootAfterInitialData = root.id();
        root.goTo(readCursor);
        assertEquals(1, keyCount());
        long leftInternal = childAt(readCursor, 0, stableGeneration, unstableGeneration);
        long rightInternal = childAt(readCursor, 1, stableGeneration, unstableGeneration);
        assertSiblings(leftInternal, rightInternal, TreeNodeUtil.NO_NODE_FLAG);
        goTo(readCursor, leftInternal);
        int leftInternalKeyCount = keyCount();
        assertThat(TreeNodeUtil.isInternal(readCursor)).isTrue();
        long leftLeaf = childAt(readCursor, 0, stableGeneration, unstableGeneration);
        goTo(readCursor, leftLeaf);
        RawBytes firstKeyInLeaf = keyAt(0, false);
        long seedOfFirstKeyInLeaf = getSeed(firstKeyInLeaf);

        // WHEN
        generationManager.checkpoint();
        long targetLastId =
                id.lastId() + 3; /*one for successor in leaf, one for split leaf, one for successor in internal*/
        for (int i = 0; id.lastId() < targetLastId; i++) {
            insert(key(seedOfFirstKeyInLeaf + i), value(seedOfFirstKeyInLeaf + i));
            assertThat(structurePropagation.hasRightKeyInsert).isFalse(); // there should be no root split
        }

        // THEN
        // root hasn't been split further
        assertThat(root.id()).isEqualTo(rootAfterInitialData);

        root.goTo(readCursor);
        long successorLeftInternal = id.lastId();
        assertThat(childAt(readCursor, 0, stableGeneration, unstableGeneration)).isEqualTo(successorLeftInternal);
        goTo(readCursor, successorLeftInternal);
        int successorLeftInternalKeyCount = keyCount();
        // Replace of the value in the leftmost leaf triggered underflow, and we got 1 less key now
        assertEquals(leftInternalKeyCount - 1, successorLeftInternalKeyCount);

        // and left internal points to the successor
        goTo(readCursor, leftInternal);
        assertThat(successor(readCursor, stableGeneration, unstableGeneration)).isEqualTo(successorLeftInternal);
        assertSiblings(successorLeftInternal, rightInternal, TreeNodeUtil.NO_NODE_FLAG);
    }
}
