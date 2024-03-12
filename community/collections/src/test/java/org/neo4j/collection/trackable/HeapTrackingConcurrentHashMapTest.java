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
package org.neo4j.collection.trackable;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.eclipse.collections.impl.list.Interval;
import org.eclipse.collections.impl.parallel.ParallelIterate;
import org.eclipse.collections.impl.tuple.ImmutableEntry;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.neo4j.memory.EmptyMemoryTracker;

@SuppressWarnings({"SameParameterValue", "resource"})
public class HeapTrackingConcurrentHashMapTest {
    volatile long volatileLong = 0L;

    @Test
    public void putIfAbsent() {
        HeapTrackingConcurrentHashMap<Integer, Integer> map = newMapWithKeysValues(1, 1, 2, 2);
        assertThat(map.putIfAbsent(1, 1)).isEqualTo(1);
        assertThat(map.putIfAbsent(3, 3)).isNull();
    }

    @Test
    public void replace() {
        HeapTrackingConcurrentHashMap<Integer, Integer> map = newMapWithKeysValues(1, 1, 2, 2);
        assertThat(map.replace(1, 7)).isEqualTo(1);
        assertThat(map.get(1)).isEqualTo(7);
        assertThat(map.replace(3, 3)).isNull();
    }

    @Test
    public void entrySetContains() {
        HeapTrackingConcurrentHashMap<String, Integer> map = newMapWithKeysValues("One", 1, "Two", 2, "Three", 3);
        assertThat(map.entrySet()).doesNotContainNull();
        assertThat(map.entrySet()).doesNotContain(entry("Zero", 0));
        assertThat(map.entrySet()).contains(entry("One", 1));
    }

    @Test
    public void entrySetRemove() {
        HeapTrackingConcurrentHashMap<String, Integer> map = newMapWithKeysValues("One", 1, "Two", 2, "Three", 3);
        assertThat(map.entrySet().remove(null)).isFalse();
        assertThat(map.entrySet().remove(entry("Zero", 0))).isFalse();
        assertThat(map.entrySet().remove(entry("One", 1))).isTrue();
    }

    @Test
    public void replaceWithOldValue() {
        HeapTrackingConcurrentHashMap<Integer, Integer> map = newMapWithKeysValues(1, 1, 2, 2);

        assertThat(map.replace(1, 1, 7)).isTrue();
        assertThat(map.get(1)).isEqualTo(7);
        assertThat(map.replace(2, 3, 3)).isFalse();
    }

    @Test
    public void removeWithKeyValue() {
        HeapTrackingConcurrentHashMap<Integer, Integer> map = newMapWithKeysValues(1, 1, 2, 2);

        assertThat(map.remove(1, 1)).isTrue();
        assertThat(map.remove(2, 3)).isFalse();
    }

    @Test
    public void removeFromEntrySet() {
        HeapTrackingConcurrentHashMap<String, Integer> map = newMapWithKeysValues("One", 1, "Two", 2, "Three", 3);

        assertThat(map.entrySet().remove(entry("Two", 2))).isTrue();
        assertThat(Map.of("One", 1, "Three", 3)).isEqualTo(map);

        assertThat(map.entrySet().remove(entry("Four", 4))).isFalse();
        assertThat(Map.of("One", 1, "Three", 3)).isEqualTo(map);
    }

    @Test
    public void removeAllFromEntrySet() {
        HeapTrackingConcurrentHashMap<String, Integer> map = newMapWithKeysValues("One", 1, "Two", 2, "Three", 3);

        assertThat(map.entrySet().removeAll(List.of(entry("One", 1), entry("Three", 3))))
                .isTrue();
        assertThat(Map.of("Two", 2)).isEqualTo(map);

        assertThat(map.entrySet().removeAll(List.of(entry("Four", 4)))).isFalse();
        assertThat(Map.of("Two", 2)).isEqualTo(map);
    }

    @SuppressWarnings("RedundantCollectionOperation")
    @RepeatedTest(100)
    public void concurrentPutGetPutAllRemoveContainsKeyContainsValueGetIfAbsentPutTest() {
        HeapTrackingConcurrentHashMap<Integer, Integer> map1 =
                HeapTrackingConcurrentHashMap.newMap(EmptyMemoryTracker.INSTANCE);
        HeapTrackingConcurrentHashMap<Integer, Integer> map2 =
                HeapTrackingConcurrentHashMap.newMap(EmptyMemoryTracker.INSTANCE);
        ParallelIterate.forEach(
                Interval.oneTo(100),
                each -> {
                    map1.put(each, each);
                    assertThat(each).isEqualTo(map1.get(each));
                    map2.putAll(Map.of(each, each));
                    map1.remove(each);
                    map1.putAll(Map.of(each, each));
                    assertThat(each).isEqualTo(map2.get(each));
                    map2.remove(each);
                    assertThat(map2.get(each)).isNull();
                    assertThat(map2.containsValue(each)).isFalse();
                    assertThat(map2.containsKey(each)).isFalse();
                    assertThat(map2.putIfAbsent(each, each)).isNull();
                    assertThat(map2.containsValue(each)).isTrue();
                    assertThat(map2.containsKey(each)).isTrue();
                    map2.remove(each);
                    assertThat(map2.containsValue(each)).isFalse();
                    assertThat(map2.containsKey(each)).isFalse();
                    assertThat(map2.computeIfAbsent(each, i -> i)).isEqualTo(each);
                    assertThat(map2.containsValue(each)).isTrue();
                    assertThat(map2.containsKey(each)).isTrue();
                    assertThat(each).isEqualTo(map2.computeIfAbsent(each, i -> i));
                    map2.remove(each);
                    assertThat(map2.putIfAbsent(each, each)).isNull();
                },
                1,
                executor());
        assertThat(map1).isEqualTo(map2);
        assertThat(map1).hasSameHashCodeAs(map2);
    }

    @SuppressWarnings("RedundantCollectionOperation")
    @RepeatedTest(10)
    public void concurrentSlowComputeIfAbsentTest() {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        HeapTrackingConcurrentHashMap<Integer, Integer> map1 =
                HeapTrackingConcurrentHashMap.newMap(EmptyMemoryTracker.INSTANCE);
        HeapTrackingConcurrentHashMap<Integer, Integer> map2 =
                HeapTrackingConcurrentHashMap.newMap(EmptyMemoryTracker.INSTANCE);
        ParallelIterate.forEach(
                Interval.oneTo(100),
                each -> {
                    map1.put(each, each);
                    assertThat(each).isEqualTo(map1.get(each));
                    map2.putAll(Map.of(each, each));
                    map1.remove(each);
                    map1.putAll(Map.of(each, each));
                    assertThat(each).isEqualTo(map2.get(each));
                    map2.remove(each);
                    assertThat(map2.get(each)).isNull();
                    assertThat(map2.containsValue(each)).isFalse();
                    assertThat(map2.containsKey(each)).isFalse();
                    assertThat(map2.putIfAbsent(each, each)).isNull();
                    assertThat(map2.containsValue(each)).isTrue();
                    assertThat(map2.containsKey(each)).isTrue();
                    map2.remove(each);
                    assertThat(map2.containsValue(each)).isFalse();
                    assertThat(map2.containsKey(each)).isFalse();
                    assertThat(map2.computeIfAbsent(each, i -> {
                                long iterations = random.nextLong(500000L, 5000000L);
                                for (long c = 0L; c < iterations; c++) {
                                    volatileLong = c;
                                }
                                return i;
                            }))
                            .isEqualTo(each);
                    assertThat(map2.containsValue(each)).isTrue();
                    assertThat(map2.containsKey(each)).isTrue();
                    assertThat(each).isEqualTo(map2.computeIfAbsent(each, i -> i));
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
        HeapTrackingConcurrentHashMap<Integer, Integer> map =
                HeapTrackingConcurrentHashMap.newMap(EmptyMemoryTracker.INSTANCE);
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
        assertThat(map).isEmpty();
    }

    @Test
    public void concurrentRemoveAndPutIfAbsent() {
        HeapTrackingConcurrentHashMap<Integer, Integer> map =
                HeapTrackingConcurrentHashMap.newMap(EmptyMemoryTracker.INSTANCE);
        ParallelIterate.forEach(
                Interval.oneTo(100),
                each -> {
                    assertThat(map.put(each, each)).isNull();
                    map.remove(each);
                    assertThat(map.get(each)).isNull();
                    assertThat(map.computeIfAbsent(each, i -> i)).isEqualTo(each);
                    map.remove(each);
                    assertThat(map.get(each)).isNull();
                    assertThat(map.computeIfAbsent(each, i -> i)).isEqualTo(each);
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

    @RepeatedTest(100)
    void computeIfAbsentShouldOnlyInvokeFunctionOnceTest() throws Throwable {
        HeapTrackingConcurrentHashMap<Integer, Integer> map =
                HeapTrackingConcurrentHashMap.newMap(EmptyMemoryTracker.INSTANCE);
        ThreadLocalRandom random = ThreadLocalRandom.current();

        int threads = random.nextInt(1, 2 * Runtime.getRuntime().availableProcessors());
        var executor = Executors.newFixedThreadPool(threads);
        int key = 42;
        int value = 1337;

        final AtomicBoolean hasBeenCalledMultipleTimes = new AtomicBoolean(false);
        var callOnce = new Function<Integer, Integer>() {
            private final AtomicBoolean hasBeenCalled = new AtomicBoolean(false);

            @Override
            public Integer apply(Integer anInt) {
                if (hasBeenCalled.compareAndSet(false, true)) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    hasBeenCalledMultipleTimes.set(true);
                }

                return value;
            }
        };
        var getFailed = new AtomicBoolean(false);
        executor.submit(new GetContestant(map, key, value, getFailed));
        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                map.computeIfAbsent(key, callOnce);
            });
        }

        executor.shutdown();
        assertThat(executor.awaitTermination(1, TimeUnit.MINUTES)).isTrue();
        assertThat(map.size()).isEqualTo(1);
        assertThat(hasBeenCalledMultipleTimes.get()).isFalse();
        assertThat(getFailed.get()).isFalse();
    }

    private record GetContestant(
            HeapTrackingConcurrentHashMap<Integer, Integer> map, int key, int expectedValue, AtomicBoolean hasFailed)
            implements Runnable {

        @Override
        public void run() {
            try {
                var getValue = map.get(key);
                if (getValue != null && getValue != expectedValue) {
                    hasFailed.set(true);
                }
            } catch (Exception e) {
                hasFailed.set(true);
            }
        }
    }

    private <K, V> HeapTrackingConcurrentHashMap<K, V> newMapWithKeysValues(K key1, V value1, K key2, V value2) {
        HeapTrackingConcurrentHashMap<K, V> map = HeapTrackingConcurrentHashMap.newMap(EmptyMemoryTracker.INSTANCE);
        map.put(key1, value1);
        map.put(key2, value2);
        return map;
    }

    private <K, V> HeapTrackingConcurrentHashMap<K, V> newMapWithKeysValues(
            K key1, V value1, K key2, V value2, K key3, V value3) {
        HeapTrackingConcurrentHashMap<K, V> map = HeapTrackingConcurrentHashMap.newMap(EmptyMemoryTracker.INSTANCE);
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
