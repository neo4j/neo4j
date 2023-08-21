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
package org.neo4j.cypher.internal.collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.memory.Measurable;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@SuppressWarnings("ConstantConditions")
@ExtendWith(RandomExtension.class)
class DefaultComparatorTopTableTest {
    @Inject
    private RandomSupport random;

    private static final List<Long> TEST_VALUES = List.of(7L, 4L, 5L, 0L, 3L, 4L, 8L, 6L, 1L, 9L, 2L);

    private static final long[] EXPECTED_VALUES = new long[] {0L, 1L, 2L, 3L, 4L, 4L, 5L, 6L, 7L, 8L, 9L};

    private static final Comparator<MeasurableLong> comparator = Comparator.comparingLong(MeasurableLong::getValue);
    private static final long MEASURABLE_LONG_SHALLOW_SIZE = shallowSizeOfInstance(MeasurableLong.class);

    @Test
    void shouldHandleAddingMoreValuesThanCapacity() {
        DefaultComparatorTopTable<MeasurableLong> table = new DefaultComparatorTopTable<>(comparator, 7);
        TEST_VALUES.forEach(l -> table.add(new MeasurableLong(l)));

        table.sort();

        Iterator<MeasurableLong> iterator = table.iterator();

        for (int i = 0; i < 7; i++) {
            assertTrue(iterator.hasNext());
            long value = iterator.next().getValue();
            assertEquals(EXPECTED_VALUES[i], value);
        }
        assertFalse(iterator.hasNext());
    }

    @Test
    void shouldHandleWhenNotCompletelyFilledToCapacity() {
        DefaultComparatorTopTable<MeasurableLong> table = new DefaultComparatorTopTable<>(comparator, 20);
        TEST_VALUES.forEach(l -> table.add(new MeasurableLong(l)));

        table.sort();

        Iterator<MeasurableLong> iterator = table.iterator();

        for (int i = 0; i < TEST_VALUES.size(); i++) {
            assertTrue(iterator.hasNext());
            long value = iterator.next().getValue();
            assertEquals(EXPECTED_VALUES[i], value);
        }
        assertFalse(iterator.hasNext());
    }

    @Test
    void shouldHandleWhenEmpty() {
        DefaultComparatorTopTable<MeasurableLong> table = new DefaultComparatorTopTable<>(comparator, 10);

        table.sort();

        Iterator<MeasurableLong> iterator = table.iterator();

        assertFalse(iterator.hasNext());
    }

    @Test
    void shouldThrowOnInitializeToZeroCapacity() {
        assertThrows(IllegalArgumentException.class, () -> new DefaultComparatorTopTable<>(comparator, 0));
    }

    @Test
    void shouldThrowOnInitializeToNegativeCapacity() {
        assertThrows(IllegalArgumentException.class, () -> new DefaultComparatorTopTable<>(comparator, -1));
    }

    @Test
    void shouldThrowOnSortNotCalledBeforeIterator() {
        DefaultComparatorTopTable<MeasurableLong> table = new DefaultComparatorTopTable<>(comparator, 5);
        TEST_VALUES.forEach(l -> table.add(new MeasurableLong(l)));

        // We forgot to call sort() here...

        assertThrows(IllegalStateException.class, table::iterator);
    }

    @Test
    void boundCheck() {
        DefaultComparatorTopTable<MeasurableLong> topTable = new DefaultComparatorTopTable<>(comparator, 5);
        topTable.sort();
        assertThrows(NoSuchElementException.class, () -> topTable.iterator().next());
    }

    @Test
    void randomizedTest() {
        final int limit = random.nextInt(1000, 5000);
        DefaultComparatorTopTable<Long> table = new DefaultComparatorTopTable<>(Long::compareTo, limit);
        PriorityQueue<Long> priorityQueue = new PriorityQueue<>(((Comparator<Long>) Long::compareTo).reversed());

        for (int i = 0; i < limit * 4; i++) {
            long l = random.nextInt(limit / 10);

            table.add(l);
            add(priorityQueue, l, limit);
        }

        assertEquals(limit, priorityQueue.size());

        // Sort table
        table.sort();
        Iterator<Long> iterator = table.iterator();

        // Sort priority queue
        long[] longs = new long[limit];
        for (int i = limit - 1; i >= 0; i--) {
            longs[i] = priorityQueue.poll();
        }

        // Compare results
        for (int i = 0; i < limit; i++) {
            assertEquals(longs[i], iterator.next());
        }
    }

    @Test
    void shouldHandleAddingValuesAfterReset() {
        DefaultComparatorTopTable<MeasurableLong> table = new DefaultComparatorTopTable<>(comparator, 20);
        long totalCountAfterReset = 7;

        TEST_VALUES.forEach(l -> table.add(new MeasurableLong(l * 100)));
        table.sort();
        table.reset(totalCountAfterReset);
        assertEquals(table.getSize(), 0);

        TEST_VALUES.forEach(l -> table.add(new MeasurableLong(l)));
        table.sort();

        Iterator<MeasurableLong> iterator = table.iterator();

        for (int i = 0; i < totalCountAfterReset; i++) {
            assertTrue(iterator.hasNext());
            long value = iterator.next().getValue();
            assertEquals(EXPECTED_VALUES[i], value);
        }
        assertFalse(iterator.hasNext());
    }

    private static void add(PriorityQueue<Long> priorityQueue, long e, int limit) {
        if (priorityQueue.size() < limit) {
            priorityQueue.offer(e);
        } else {
            long head = priorityQueue.peek();
            if (head > e) {
                priorityQueue.poll();
                priorityQueue.offer(e);
            }
        }
    }

    private static class MeasurableLong implements Measurable {
        private final long value;

        private MeasurableLong(long value) {
            this.value = value;
        }

        public long getValue() {
            return value;
        }

        @Override
        public long estimatedHeapUsage() {
            return MEASURABLE_LONG_SHALLOW_SIZE;
        }
    }
}
