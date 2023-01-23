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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.eclipse.collections.impl.list.Interval;
import org.eclipse.collections.impl.parallel.ParallelIterate;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.memory.EmptyMemoryTracker;

@SuppressWarnings({"SameParameterValue", "resource"})
public class HeapTrackingConcurrentBagTest {

    @Test
    public void add() {
        HeapTrackingConcurrentBag<Integer> bag = newBagWith(1, 2, 1, 1, 2);
        assertSame(bag, 1, 2, 1, 1, 2);
    }

    @Test
    public void concurrentAddAndRead() {
        HeapTrackingConcurrentBag<Integer> bag1 = HeapTrackingConcurrentBag.newBag(EmptyMemoryTracker.INSTANCE);
        HeapTrackingConcurrentBag<Integer> bag2 = HeapTrackingConcurrentBag.newBag(EmptyMemoryTracker.INSTANCE);
        ParallelIterate.forEach(
                Interval.oneTo(100),
                each -> {
                    bag1.add(each);
                    bag2.add(each);
                    bag2.add(each);
                    bag1.add(each);
                    bag1.add(each);
                    bag2.add(each);
                },
                1,
                executor());
        assertThat(asList(bag1)).containsExactlyInAnyOrderElementsOf(asList(bag2));
    }

    @SafeVarargs
    private <K> HeapTrackingConcurrentBag<K> newBagWith(K... ks) {
        HeapTrackingConcurrentBag<K> bag = HeapTrackingConcurrentBag.newBag(EmptyMemoryTracker.INSTANCE);
        for (K k : ks) {
            bag.add(k);
        }
        return bag;
    }

    private <T> void assertSame(HeapTrackingConcurrentBag<T> bag, T... elements) {
        assertThat(asList(bag)).containsExactlyInAnyOrderElementsOf(Arrays.asList(elements));
    }

    private <T> List<T> asList(HeapTrackingConcurrentBag<T> bag) {
        return Iterators.asList(bag.iterator());
    }

    private ExecutorService executor() {
        return Executors.newFixedThreadPool(20);
    }
}
