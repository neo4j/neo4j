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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.eclipse.collections.impl.list.Interval;
import org.eclipse.collections.impl.parallel.ParallelIterate;
import org.junit.jupiter.api.Test;
import org.neo4j.memory.EmptyMemoryTracker;

@SuppressWarnings({"SameParameterValue", "resource"})
public class HeapTrackingConcurrentHashSetTest {

    @Test
    public void add() {
        HeapTrackingConcurrentHashSet<Integer> set = newSetWith(1, 2);
        assertThat(set.add(1)).isFalse();
        assertThat(set.add(3)).isTrue();
    }

    @Test
    public void remove() {
        HeapTrackingConcurrentHashSet<Integer> set = newSetWith(1, 2);

        assertThat(set.contains(1)).isTrue();
        assertThat(set.remove(1)).isTrue();
        assertThat(set.contains(1)).isFalse();
        assertThat(set.remove(3)).isFalse();
    }

    @Test
    public void concurrentAddAndRemove() {
        HeapTrackingConcurrentHashSet<Integer> set1 = HeapTrackingConcurrentHashSet.newSet(EmptyMemoryTracker.INSTANCE);
        HeapTrackingConcurrentHashSet<Integer> set2 = HeapTrackingConcurrentHashSet.newSet(EmptyMemoryTracker.INSTANCE);
        ParallelIterate.forEach(
                Interval.oneTo(100),
                each -> {
                    assertThat(set1.add(each)).isTrue();
                    assertThat(set1.contains(each)).isTrue();
                    assertThat(set2.addAll(List.of(each, each))).isTrue();
                    assertThat(set1.remove(each)).isTrue();
                    assertThat(set1.addAll(List.of(each, each))).isTrue();
                    assertThat(set2.contains(each)).isTrue();
                    assertThat(set2.remove(each)).isTrue();
                    assertThat(set2.contains(each)).isFalse();
                    assertThat(set2.add(each)).isTrue();
                    assertThat(set2.contains(each)).isTrue();
                    assertThat(set2.add(each)).isFalse();
                    assertThat(set2.remove(each)).isTrue();
                    assertThat(set2.add(each)).isTrue();
                },
                1,
                executor());
        assertThat(set1).isEqualTo(set2);
        assertThat(set1).hasSameHashCodeAs(set2);
    }

    @Test
    public void concurrentClear() {
        HeapTrackingConcurrentHashSet<Integer> set = HeapTrackingConcurrentHashSet.newSet(EmptyMemoryTracker.INSTANCE);
        ParallelIterate.forEach(
                Interval.oneTo(100),
                each -> {
                    for (int i = 0; i < 10; i++) {
                        set.add(each + i * 1000);
                    }
                    set.clear();
                },
                1,
                executor());
        assertThat(set).isEmpty();
    }

    @SafeVarargs
    private <K> HeapTrackingConcurrentHashSet<K> newSetWith(K... ks) {
        HeapTrackingConcurrentHashSet<K> set = HeapTrackingConcurrentHashSet.newSet(EmptyMemoryTracker.INSTANCE);
        set.addAll(Arrays.asList(ks));
        return set;
    }

    private ExecutorService executor() {
        return Executors.newFixedThreadPool(20);
    }
}
