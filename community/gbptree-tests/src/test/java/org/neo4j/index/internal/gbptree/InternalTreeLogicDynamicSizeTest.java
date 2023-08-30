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

import java.io.IOException;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

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

    @Override
    protected ValueAggregator<RawBytes> getAddingAggregator() {
        return this::add;
    }

    @Override
    protected Function<RawBytes, RawBytes> getIncrementingValueUpdater() {
        return v -> {
            v.copyFrom(layout.value(layout.valueSeed(v) + 1));
            return v;
        };
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
}
