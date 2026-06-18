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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.memory.EmptyMemoryTracker;

class HeapTrackingOrderedAppendSetTest {

    @Test
    void emptySet() {
        HeapTrackingOrderedAppendSet<Integer> appendSet = create();
        assertThat(appendSet.isEmpty()).isTrue();
        assertThat(appendSet.contains(1337)).isFalse();
        assertThatThrownBy(appendSet::getFirst).isInstanceOf(NoSuchElementException.class);
        assertThatThrownBy(appendSet::getLast).isInstanceOf(NoSuchElementException.class);
        assertThatThrownBy(() -> appendSet.get(0)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThat(appendSet.iterator()).isExhausted();
    }

    @Test
    void singleElementSet() {
        HeapTrackingOrderedAppendSet<Integer> appendSet = create(1337);
        assertThat(appendSet.isEmpty()).isFalse();
        assertThat(appendSet).contains(1337).doesNotContain(1338);
        assertThat(appendSet.getFirst()).isEqualTo(1337);
        assertThat(appendSet.getLast()).isEqualTo(1337);
        assertThat(appendSet.get(0)).isEqualTo(1337);
        assertThatThrownBy(() -> appendSet.get(1)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThat(Iterators.asList(appendSet.iterator())).isEqualTo(List.of(1337));
    }

    @Test
    void addAndRetrieveNoDuplicates() {
        assertSetContains(create(100, 9, 0, 7), 100, 9, 0, 7);
    }

    @Test
    void addAndRetrieveWithDuplicates() {
        assertSetContains(create(100, 100, 9, 0, 9, 7, 0), 100, 9, 0, 7);
    }

    @Test
    void shouldReverseSmallSet() {
        HeapTrackingOrderedAppendSet<Integer> set = create(100, 9, 0, 7);
        assertSetContains(set.reversedOrderedAppendSet(), 7, 0, 9, 100);
        assertSetContains(set.reversedOrderedAppendSet().reversedOrderedAppendSet(), 100, 9, 0, 7);
    }

    @Test
    void shouldReverseBigSet() {
        HeapTrackingOrderedAppendSet<Integer> set = create();
        Random random = new Random();
        int size = random.nextInt(10000, 20000);
        var values = new ArrayList<Integer>(size);
        for (int i = 0; i < size; i++) {
            int value = random.nextInt();
            if (set.add(value)) {
                values.add(value);
            }
        }
        assertSetContains(set, values);
        assertSetContains(set.reversedOrderedAppendSet(), values.reversed());
    }

    @Test
    void randomTest() {
        long seed = System.currentTimeMillis();
        Random random = new Random(seed);
        int size = 10000;
        HeapTrackingOrderedAppendSet<Integer> appendSet = create();
        LinkedHashSet<Integer> linkedSet = new LinkedHashSet<>();
        for (int i = 0; i < size; i++) {
            int next = random.nextInt(1000);
            appendSet.add(next);
            linkedSet.add(next);
        }

        assertThat(Iterators.asList(appendSet.iterator()))
                .as(String.format("LinkedHashSet and HeapTrackingOrderedAppendSet disagreed using seed=%d", seed))
                .isEqualTo(Iterators.asList(linkedSet.iterator()));

        assertThat(Iterators.asList(appendSet.reversed().iterator()))
                .as(String.format(
                        "reversing LinkedHashSet and HeapTrackingOrderedAppendSet disagreed using seed=%d", seed))
                .isEqualTo(Iterators.asList(linkedSet.reversed().iterator()));
    }

    @SafeVarargs
    private <T> void assertSetContains(OrderedAppendSet<T> set, T... objects) {
        assertSetContains(set, Arrays.asList(objects));
    }

    private <T> void assertSetContains(OrderedAppendSet<T> set, List<T> objects) {
        assertThat(set).hasSameSizeAs(objects);
        assertThat(set.isEmpty()).isEqualTo(objects.isEmpty());
        assertThat(set.getFirst()).isEqualTo(objects.getFirst());
        assertThat(set.getLast()).isEqualTo(objects.getLast());
        var iterator = set.iterator();
        for (int i = 0; i < objects.size(); i++) {
            var expected = objects.get(i);
            assertThat(iterator).hasNext();
            assertThat(iterator.next()).isEqualTo(expected);
            assertThat(set.get(i)).isEqualTo(expected);
            assertThat(set).contains(expected);
        }
    }

    @SafeVarargs
    private <T> HeapTrackingOrderedAppendSet<T> create(T... objects) {
        var orderedSet = HeapTrackingOrderedAppendSet.<T>createOrderedSet(EmptyMemoryTracker.INSTANCE);
        for (T anObject : objects) {
            if (orderedSet.contains(anObject)) {
                assertThat(orderedSet.add(anObject)).isFalse();
            } else {
                assertThat(orderedSet.add(anObject)).isTrue();
            }
        }

        return orderedSet;
    }
}
