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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongFunction;
import org.eclipse.collections.impl.list.Interval;
import org.eclipse.collections.impl.parallel.ParallelIterate;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.neo4j.memory.EmptyMemoryTracker;

@SuppressWarnings({"SameParameterValue", "resource"})
public class HeapTrackingConcurrentLongObjectHashMapTest {
    public volatile long volatileLong = 0L;

    @Test
    public void putIfAbsent() {
        HeapTrackingConcurrentLongObjectHashMap<Integer> map = newMapWithKeysValues(1, 1, 2, 2);
        assertThat(map.putIfAbsent(1, 1)).isEqualTo(1);
        assertThat(map.putIfAbsent(3, 3)).isNull();
    }

    @Test
    public void replace() {
        HeapTrackingConcurrentLongObjectHashMap<Integer> map = newMapWithKeysValues(1, 1, 2, 2);
        assertThat(map.replace(1, 7)).isEqualTo(1);
        assertThat(map.get(1)).isEqualTo(7);
        assertThat(map.replace(3, 3)).isNull();
    }

    @Test
    public void replaceWithOldValue() {
        HeapTrackingConcurrentLongObjectHashMap<Integer> map = newMapWithKeysValues(1, 1, 2, 2);

        assertThat(map.replace(1, 1, 7)).isTrue();
        assertThat(map.get(1)).isEqualTo(7);
        assertThat(map.replace(2, 3, 3)).isFalse();
    }

    @Test
    public void removeWithKeyValue() {
        HeapTrackingConcurrentLongObjectHashMap<Integer> map = newMapWithKeysValues(1, 1, 2, 2);

        assertThat(map.remove(1, 1)).isTrue();
        assertThat(map.remove(2, 3)).isFalse();
    }

    @SuppressWarnings("RedundantCollectionOperation")
    @RepeatedTest(100)
    public void concurrentPutGetPutRemoveContainsKeyContainsValueGetIfAbsentPutTest() {
        HeapTrackingConcurrentLongObjectHashMap<Integer> map1 =
                HeapTrackingConcurrentLongObjectHashMap.newMap(EmptyMemoryTracker.INSTANCE);
        HeapTrackingConcurrentLongObjectHashMap<Integer> map2 =
                HeapTrackingConcurrentLongObjectHashMap.newMap(EmptyMemoryTracker.INSTANCE);
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
                    assertThat(map2.putIfAbsent(each, each)).isNull();
                    assertThat(map2.containsValue(each)).isTrue();
                    assertThat(map2.containsKey(each)).isTrue();
                    map2.remove(each);
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

    @SuppressWarnings("RedundantCollectionOperation")
    @RepeatedTest(10)
    public void concurrentSlowComputeIfAbsentTest() {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        HeapTrackingConcurrentLongObjectHashMap<Integer> map1 =
                HeapTrackingConcurrentLongObjectHashMap.newMap(EmptyMemoryTracker.INSTANCE);
        HeapTrackingConcurrentLongObjectHashMap<Integer> map2 =
                HeapTrackingConcurrentLongObjectHashMap.newMap(EmptyMemoryTracker.INSTANCE);
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
                                return (int) i;
                            }))
                            .isEqualTo(each);
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
        HeapTrackingConcurrentLongObjectHashMap<Integer> map =
                HeapTrackingConcurrentLongObjectHashMap.newMap(EmptyMemoryTracker.INSTANCE);
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
        HeapTrackingConcurrentLongObjectHashMap<Integer> map =
                HeapTrackingConcurrentLongObjectHashMap.newMap(EmptyMemoryTracker.INSTANCE);
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

    @RepeatedTest(100)
    void concurrentComputeTest() throws Throwable {
        HeapTrackingConcurrentLongObjectHashMap<Integer> map =
                HeapTrackingConcurrentLongObjectHashMap.newMap(EmptyMemoryTracker.INSTANCE);
        int start = 0;
        int end = 100000;
        int offset = 100000;
        ThreadLocalRandom random = ThreadLocalRandom.current();

        int threads = random.nextInt(1, 2 * Runtime.getRuntime().availableProcessors());
        var executor = Executors.newFixedThreadPool(threads);

        var computeFailed = new AtomicBoolean(false);
        var iteratorFailed = new AtomicReference<String>(null);
        var replaceFailed = new AtomicBoolean(false);
        var putFailed = new AtomicBoolean(false);

        int max = end + (threads - 1) * offset;
        for (int i = 0; i < threads; i++) {
            executor.submit(new ComputeContestant(map, start, end, computeFailed));
            executor.submit(new IteratorContestant(map, start, end, max, iteratorFailed));
            executor.submit(new ReplaceContestant(map, start, end, replaceFailed));
            executor.submit(new IteratorContestant(map, start, end, max, iteratorFailed));
            executor.submit(new PutContestant(map, start, end, putFailed));
            executor.submit(new IteratorContestant(map, start, end, max, iteratorFailed));
            start += offset;
            end += offset;
        }
        executor.shutdown();
        assertThat(end).isEqualTo(max + offset);
        assertThat(computeFailed.get()).isFalse();
        assertThat(iteratorFailed.get()).isNull();
        assertThat(replaceFailed.get()).isFalse();
        assertThat(putFailed.get()).isFalse();
        assertThat(executor.awaitTermination(1, TimeUnit.MINUTES)).isTrue();
        assertThat(map.size()).isEqualTo(max);
        for (int i = 0; i < max; i++) {
            Integer actual = map.get(i);
            assertThat(actual).isEqualTo(i);
        }
    }

    @RepeatedTest(100)
    void computeIfAbsentShouldOnlyInvokeFunctionOnceTest() throws Throwable {
        HeapTrackingConcurrentLongObjectHashMap<Integer> map =
                HeapTrackingConcurrentLongObjectHashMap.newMap(EmptyMemoryTracker.INSTANCE);
        ThreadLocalRandom random = ThreadLocalRandom.current();

        int threads = random.nextInt(1, 2 * Runtime.getRuntime().availableProcessors());
        var executor = Executors.newFixedThreadPool(threads);
        int key = 42;
        int value = 1337;

        final AtomicBoolean hasBeenCalledMultipleTimes = new AtomicBoolean(false);
        var callOnce = new LongFunction<Integer>() {
            private final AtomicBoolean hasBeenCalled = new AtomicBoolean(false);

            @Override
            public Integer apply(long aLong) {
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
            HeapTrackingConcurrentLongObjectHashMap<Integer> map, long key, int expectedValue, AtomicBoolean hasFailed)
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

    private record ComputeContestant(
            HeapTrackingConcurrentLongObjectHashMap<Integer> map, int start, int end, AtomicBoolean hasFailed)
            implements Runnable {

        @Override
        public void run() {
            try {
                for (int i = start; i < end; i++) {
                    var v = map.computeIfAbsent(i, integer -> (int) integer);
                    if (v != i) {
                        hasFailed.set(true);
                    }
                }
            } catch (Exception e) {
                hasFailed.set(true);
            }
        }
    }

    private record ReplaceContestant(
            HeapTrackingConcurrentLongObjectHashMap<Integer> map, int start, int end, AtomicBoolean hasFailed)
            implements Runnable {

        @Override
        public void run() {
            try {
                for (int i = start; i < end; i++) {
                    var oldV = map.replace(i, i);
                    if (oldV != null && oldV != i) {
                        hasFailed.set(true);
                        // Do not return, we want to continue to see if we can find more errors
                    }
                }
            } catch (Exception e) {
                hasFailed.set(true);
            }
        }
    }

    private record PutContestant(
            HeapTrackingConcurrentLongObjectHashMap<Integer> map, int start, int end, AtomicBoolean hasFailed)
            implements Runnable {

        @Override
        public void run() {
            try {
                for (int i = start; i < end; i++) {
                    var oldV = map.put(i, i);
                    if (oldV != null && oldV != i) {
                        hasFailed.set(true);
                        // Do not return, we want to continue to see if we can find more errors
                    }
                }
            } catch (Exception e) {
                hasFailed.set(true);
            }
        }
    }

    private record IteratorContestant(
            HeapTrackingConcurrentLongObjectHashMap<Integer> map,
            int start,
            int end,
            int max,
            AtomicReference<String> hasFailed)
            implements Runnable {

        @Override
        public void run() {
            try {
                var valuesIterator = map.values();
                var buffer = new Integer[max];
                int highestObservedInRange = -1;
                while (valuesIterator.hasNext()) {
                    var v = valuesIterator.next();
                    buffer[v] = v;
                    if (v > highestObservedInRange && v >= start && v < end) {
                        highestObservedInRange = v;
                    }
                }
                for (int i = start; i <= highestObservedInRange; i++) {
                    var cachedV = buffer[i];
                    var getV = map.get(i);
                    if ((getV == null || getV != i) // The value must now be observable
                            // And must match the cached value if we have one
                            // Note that the object holding it may have been replaced, so we can't compare references
                            || (cachedV != null && cachedV.intValue() != getV.intValue())) {
                        hasFailed.set(String.format(
                                "Failed at index %d, cached %d, got %d, highest observed %d",
                                i, cachedV, getV, highestObservedInRange));
                        return;
                    }
                }
                if (highestObservedInRange < end) {
                    long highestObservedInRange2 = -1L;
                    var keysIterator = map.keys();
                    while (keysIterator.hasNext()) {
                        var k = keysIterator.next();
                        var v = map.get(k);
                        if (v == null || v != k) {
                            hasFailed.set(String.format("Failed at key %d, value %d", k, v));
                            return;
                        }
                        if (k > highestObservedInRange2 && k >= start && k < end) {
                            highestObservedInRange2 = k;
                        }
                    }
                    if (highestObservedInRange > highestObservedInRange2) {
                        hasFailed.set(String.format(
                                "Failed at highest observed first time %d > highest observed second time %d",
                                highestObservedInRange, highestObservedInRange2));
                    }
                }
            } catch (Exception e) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                PrintStream ps = new PrintStream(baos);
                ps.print("Failed with exception: ");
                e.printStackTrace(ps);
                hasFailed.set(baos.toString());
            }
        }
    }

    private <V> HeapTrackingConcurrentLongObjectHashMap<V> newMapWithKeysValues(
            long key1, V value1, long key2, V value2) {
        HeapTrackingConcurrentLongObjectHashMap<V> map =
                HeapTrackingConcurrentLongObjectHashMap.newMap(EmptyMemoryTracker.INSTANCE);
        map.put(key1, value1);
        map.put(key2, value2);
        return map;
    }

    private ExecutorService executor() {
        return Executors.newFixedThreadPool(20);
    }
}
