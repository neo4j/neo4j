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
package org.neo4j.values.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.values.storable.Values.FALSE;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.TRUE;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.list;
import static org.neo4j.values.virtual.VirtualValues.map;
import static org.neo4j.values.virtual.VirtualValues.range;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.VirtualValues;

@SuppressWarnings("resource")
class InCacheTest {
    @Test
    void shouldHandleListWithNoNulls() {
        InCache cache = new InCache();
        ListValue list = list(stringValue("a"), stringValue("b"), stringValue("c"));

        Map<Value, Value> expected = Map.of(
                stringValue("a"),
                TRUE,
                stringValue("b"),
                TRUE,
                stringValue("c"),
                TRUE,
                stringValue("d"),
                FALSE,
                NO_VALUE,
                NO_VALUE);

        for (Entry<Value, Value> entry : shuffled(expected)) {
            assertThat(cache.check(entry.getKey(), list, EmptyMemoryTracker.INSTANCE))
                    .isEqualTo(entry.getValue());
        }
    }

    @Test
    void shouldHandleListContainingNulls() {
        InCache cache = new InCache();
        ListValue list = list(stringValue("a"), NO_VALUE, stringValue("c"), NO_VALUE);
        Map<Value, Value> expected = Map.of(
                stringValue("a"),
                TRUE,
                stringValue("b"),
                NO_VALUE,
                stringValue("c"),
                TRUE,
                stringValue("d"),
                NO_VALUE,
                NO_VALUE,
                NO_VALUE);

        for (Entry<Value, Value> entry : shuffled(expected)) {
            assertThat(cache.check(entry.getKey(), list, EmptyMemoryTracker.INSTANCE))
                    .isEqualTo(entry.getValue());
        }
    }

    @Test
    void shouldHandleEmptyListAndNull() {
        // given
        InCache cache = new InCache();

        // when
        Value check = cache.check(NO_VALUE, VirtualValues.EMPTY_LIST, EmptyMemoryTracker.INSTANCE);

        // then
        assertThat(check).isEqualTo(FALSE);
    }

    @Test
    void shouldTrackMemory() {
        // given
        InCache cache = new InCache();
        ListValue list = range(1L, 256L, 1L);

        // when
        MemoryTracker memoryTracker = Mockito.mock(MemoryTracker.class);
        cache.check(longValue(1L), list, memoryTracker);
        cache.check(longValue(2L), list, memoryTracker);

        // then
        ArgumentCaptor<Long> arg = ArgumentCaptor.forClass(Long.class);
        Mockito.verify(memoryTracker).allocateHeap(arg.capture());
        Assertions.assertThat(arg.getValue()).isGreaterThan(0);
    }

    @Test
    void shouldHandleArraysWithNulls() {
        // given
        InCache cache = new InCache();

        // when
        var list = list(list(intValue(1), intValue(2)), list(intValue(3), intValue(4)));

        // then
        assertEquals(BooleanValue.FALSE, cache.check(intValue(0), list, EmptyMemoryTracker.INSTANCE));
        assertEquals(NO_VALUE, cache.check(list(intValue(1), NO_VALUE), list, EmptyMemoryTracker.INSTANCE));
    }

    @Test
    void shouldHandleMapsWithNulls() {
        // given
        InCache cache = new InCache();

        // when
        var list = list(map(new String[] {"a"}, new AnyValue[] {intValue(1)}));

        // then
        assertEquals(BooleanValue.FALSE, cache.check(intValue(0), list, EmptyMemoryTracker.INSTANCE));
        assertEquals(
                NO_VALUE,
                cache.check(map(new String[] {"a"}, new AnyValue[] {NO_VALUE}), list, EmptyMemoryTracker.INSTANCE));
    }

    @Test
    void shouldCloseCacheThatIsNeverUsed() {
        new InCache().close();
    }

    @Test
    void shouldCloseCacheThatIsUsedOnce() {
        InCache cache = new InCache();
        ListValue list = range(1L, 256L, 1L);

        cache.check(intValue(0), list, EmptyMemoryTracker.INSTANCE);
        cache.close();
    }

    private static <K, V> Iterable<Entry<K, V>> shuffled(Map<K, V> map) {
        ArrayList<Entry<K, V>> list = new ArrayList<>(map.entrySet());
        Collections.shuffle(list);
        return list;
    }
}
