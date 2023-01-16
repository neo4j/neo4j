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
package org.neo4j.collection.trackable;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.eclipse.collections.impl.list.Interval;
import org.eclipse.collections.impl.parallel.ParallelIterate;
import org.eclipse.collections.impl.tuple.ImmutableEntry;
import org.junit.jupiter.api.Test;
import org.neo4j.memory.EmptyMemoryTracker;

@SuppressWarnings({"SameParameterValue", "resource"})
public class HeapTrackingConcurrentLongHashMapTest {

    @Test
    public void putIfAbsent() {
        HeapTrackingConcurrentLongHashMap<Integer> map = newMapWithKeysValues(1, 1, 2, 2);
        assertThat(map.putIfAbsent(1, 1)).isEqualTo(1);
        assertThat(map.putIfAbsent(3, 3)).isNull();
    }

    @Test
    public void replace() {
        HeapTrackingConcurrentLongHashMap<Integer> map = newMapWithKeysValues(1, 1, 2, 2);
        assertThat(map.replace(1, 7)).isEqualTo(1);
        assertThat(map.get(1)).isEqualTo(7);
        assertThat(map.replace(3, 3)).isNull();
    }

    @Test
    public void replaceWithOldValue() {
        HeapTrackingConcurrentLongHashMap<Integer> map = newMapWithKeysValues(1, 1, 2, 2);

        assertThat(map.replace(1, 1, 7)).isTrue();
        assertThat(map.get(1)).isEqualTo(7);
        assertThat(map.replace(2, 3, 3)).isFalse();
    }

    @Test
    public void removeWithKeyValue() {
        HeapTrackingConcurrentLongHashMap<Integer> map = newMapWithKeysValues(1, 1, 2, 2);

        assertThat(map.remove(1, 1)).isTrue();
        assertThat(map.remove(2, 3)).isFalse();
    }

    @SuppressWarnings("RedundantCollectionOperation")
    @Test
    public void concurrentPutGetPutRemoveContainsKeyContainsValueGetIfAbsentPutTest() {
        HeapTrackingConcurrentLongHashMap<Integer> map1 =
                HeapTrackingConcurrentLongHashMap.newMap(EmptyMemoryTracker.INSTANCE);
        HeapTrackingConcurrentLongHashMap<Integer> map2 =
                HeapTrackingConcurrentLongHashMap.newMap(EmptyMemoryTracker.INSTANCE);
        ParallelIterate.forEach(
                Interval.oneTo(100),
                each -> {
                    map1.put(each, each);
                    assertThat(each).isEqualTo(map1.get(each));
                    map2.put(each, each);
                    map1.remove(each);
                    map1.put(each, each);
                    assertThat(each).isEqualTo(map2.get(each));
                    map2.remove(each);
                    assertThat(map2.get(each)).isNull();
                    assertThat(map2.containsValue(each)).isFalse();
                    assertThat(map2.containsKey(each)).isFalse();
                    assertThat(map2.computeIfAbsent(each, i -> (int) i)).isEqualTo(each);
                    assertThat(map2.containsValue(each)).isTrue();
                    assertThat(map2.containsKey(each)).isTrue();
                    assertThat(each).isEqualTo(map2.computeIfAbsent(each, i -> (int) i));
                    map2.remove(each);
                    assertThat(map2.putIfAbsent(each, each)).isNull();
                },
                1,
                executor());
        assertThat(map1).isEqualTo(map2);
        assertThat(map1).hasSameHashCodeAs(map2);
    }

    @Test
    public void concurrentClear() {
        HeapTrackingConcurrentLongHashMap<Integer> map =
                HeapTrackingConcurrentLongHashMap.newMap(EmptyMemoryTracker.INSTANCE);
        ParallelIterate.forEach(
                Interval.oneTo(100),
                each -> {
                    for (int i = 0; i < 10; i++) {
                        map.put(each + i * 1000, each);
                    }
                    map.clear();
                },
                1,
                executor());
        assertThat(map.isEmpty()).isTrue();
    }

    @Test
    public void concurrentRemoveAndPutIfAbsent() {
        HeapTrackingConcurrentLongHashMap<Integer> map =
                HeapTrackingConcurrentLongHashMap.newMap(EmptyMemoryTracker.INSTANCE);
        ParallelIterate.forEach(
                Interval.oneTo(100),
                each -> {
                    assertThat(map.put(each, each)).isNull();
                    map.remove(each);
                    assertThat(map.get(each)).isNull();
                    assertThat(map.computeIfAbsent(each, i -> (int) i)).isEqualTo(each);
                    map.remove(each);
                    assertThat(map.get(each)).isNull();
                    assertThat(map.computeIfAbsent(each, i -> (int) i)).isEqualTo(each);
                    map.remove(each);
                    assertThat(map.get(each)).isNull();
                    for (int i = 0; i < 10; i++) {
                        assertThat(map.putIfAbsent(each + i * 1000, each)).isNull();
                    }
                    for (int i = 0; i < 10; i++) {
                        assertThat(map.putIfAbsent(each + i * 1000, each)).isEqualTo(each);
                    }
                    for (int i = 0; i < 10; i++) {
                        assertThat(map.remove(each + i * 1000)).isEqualTo(each);
                    }
                },
                1,
                executor());
    }

    private <K, V> HeapTrackingConcurrentLongHashMap<V> newMapWithKeysValues(long key1, V value1, long key2, V value2) {
        HeapTrackingConcurrentLongHashMap<V> map =
                HeapTrackingConcurrentLongHashMap.newMap(EmptyMemoryTracker.INSTANCE);
        map.put(key1, value1);
        map.put(key2, value2);
        return map;
    }

    private <V> HeapTrackingConcurrentLongHashMap<V> newMapWithKeysValues(
            long key1, V value1, long key2, V value2, long key3, V value3) {
        HeapTrackingConcurrentLongHashMap<V> map =
                HeapTrackingConcurrentLongHashMap.newMap(EmptyMemoryTracker.INSTANCE);
        map.put(key1, value1);
        map.put(key2, value2);
        map.put(key3, value3);
        return map;
    }

    private <K, V> Map.Entry<K, V> entry(K k, V v) {
        return ImmutableEntry.of(k, v);
    }

    private ExecutorService executor() {
        return Executors.newFixedThreadPool(20);
    }
}
