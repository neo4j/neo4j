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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

import java.util.Comparator;
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
class DefaultComparatorSortTableTest {
    @Inject
    private RandomSupport random;

    private static final List<Long> TEST_VALUES = List.of(7L, 4L, 5L, 0L, 3L, 4L, 8L, 6L, 1L, 9L, 2L);

    private static final long[] EXPECTED_VALUES = new long[] {0L, 1L, 2L, 3L, 4L, 4L, 5L, 6L, 7L, 8L, 9L};

    private static final Comparator<MeasurableLong> comparator = Comparator.comparingLong(MeasurableLong::getValue);
    private static final long MEASURABLE_LONG_SHALLOW_SIZE = shallowSizeOfInstance(MeasurableLong.class);

    @Test
    void shouldHandleAddingMoreValuesThanCapacity() {
        DefaultComparatorSortTable<MeasurableLong> table = new DefaultComparatorSortTable<>(comparator, 7);
        TEST_VALUES.forEach(l -> table.add(new MeasurableLong(l)));

        for (int i = 0; i < EXPECTED_VALUES.length; i++) {
            var next = table.poll();
            assertNotNull(next);
            long value = next.getValue();
            assertEquals(EXPECTED_VALUES[i], value);
        }
        assertEmpty(table);
    }

    @Test
    void shouldHandleWhenNotCompletelyFilledToCapacity() {
        DefaultComparatorSortTable<MeasurableLong> table = new DefaultComparatorSortTable<>(comparator, 20);
        TEST_VALUES.forEach(l -> table.add(new MeasurableLong(l)));

        for (int i = 0; i < TEST_VALUES.size(); i++) {
            var next = table.poll();
            assertNotNull(next);
            long value = next.getValue();
            assertEquals(EXPECTED_VALUES[i], value);
        }
        assertEmpty(table);
    }

    @Test
    void shouldHandleWhenEmpty() {
        DefaultComparatorSortTable<MeasurableLong> table = new DefaultComparatorSortTable<>(comparator, 10);

        assertEmpty(table);
    }

    @Test
    void shouldThrowOnInitializeToZeroCapacity() {
        assertThrows(IllegalArgumentException.class, () -> new DefaultComparatorSortTable<>(comparator, 0));
    }

    @Test
    void shouldThrowOnInitializeToNegativeCapacity() {
        assertThrows(IllegalArgumentException.class, () -> new DefaultComparatorSortTable<>(comparator, -1));
    }

    @Test
    void boundCheck() {
        DefaultComparatorSortTable<MeasurableLong> sortTable = new DefaultComparatorSortTable<>(comparator, 5);
        assertThrows(
                NoSuchElementException.class,
                () -> sortTable.unorderedIterator().next());
    }

    @Test
    void randomizedTest() {
        final int n = random.nextInt(1000, 5000);
        DefaultComparatorSortTable<Long> table = new DefaultComparatorSortTable<>(Long::compareTo, n);
        PriorityQueue<Long> priorityQueue = new PriorityQueue<>(Long::compareTo);

        for (int i = 0; i < n; i++) {
            long l = random.nextInt(n / 10);

            table.add(l);
            add(priorityQueue, l, n);
        }

        assertEquals(n, priorityQueue.size());

        // Sort table
        long[] longsFromTable = new long[n];
        for (int i = 0; i < n; i++) {
            longsFromTable[i] = table.poll();
        }
        assertEmpty(table);

        // Sort priority queue
        long[] longsFromPriorityQueue = new long[n];
        for (int i = 0; i < n; i++) {
            longsFromPriorityQueue[i] = priorityQueue.poll();
        }
        assertTrue(priorityQueue.isEmpty());

        // Compare results
        for (int i = 0; i < n; i++) {
            assertEquals(longsFromPriorityQueue[i], longsFromTable[i]);
        }
    }

    @Test
    void shouldHandleAddingValuesAfterReset() {
        DefaultComparatorSortTable<MeasurableLong> table = new DefaultComparatorSortTable<>(comparator, 20);

        TEST_VALUES.forEach(l -> table.add(new MeasurableLong(l * 100)));
        table.reset();
        assertEquals(0, table.getSize());
        assertTrue(table.isEmpty());
        assertNull(table.peek());
        assertNull(table.poll());

        TEST_VALUES.forEach(l -> table.add(new MeasurableLong(l)));

        for (int i = 0; i < TEST_VALUES.size(); i++) {
            var next = table.poll();
            assertNotNull(next);
            long value = next.getValue();
            assertEquals(EXPECTED_VALUES[i], value);
        }
        assertEmpty(table);
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

    private static void assertEmpty(DefaultComparatorSortTable<?> table) {
        assertTrue(table.isEmpty());
        assertEquals(0, table.getSize());
        assertNull(table.peek());
        assertNull(table.poll());
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
