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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryTracker;

class HeapTrackingArrayDequeTest {
    private final MemoryTracker memoryTracker = new LocalMemoryTracker();
    private HeapTrackingArrayDeque<Object> aDeque;
    private Object[] objArray;

    @BeforeEach
    void setUp() {
        objArray = new Object[100];
        for (int i = 0; i < objArray.length; i++) {
            objArray[i] = i;
        }
        aDeque = HeapTrackingArrayDeque.newArrayDeque(memoryTracker);
        aDeque.addAll(Arrays.asList(objArray));
    }

    @AfterEach
    void tearDown() {
        objArray = null;
        aDeque.close();
        assertEquals(0, memoryTracker.estimatedHeapMemory(), "Leaking memory");
    }

    @Test
    void initialSize() {
        try (HeapTrackingArrayDeque<Object> list = HeapTrackingArrayDeque.newArrayDeque(5, memoryTracker)) {
            assertEquals(0, list.size(), "Should not contain any elements when created");
        }

        try (HeapTrackingArrayDeque<Object> list = HeapTrackingArrayDeque.newArrayDeque(-10, memoryTracker)) {
            assertEquals(0, list.size(), "Should not contain any elements when created");
        }
    }

    @Test
    void add() {
        Object o = new Object();
        aDeque.add(o);
        assertSame(aDeque.peekLast(), o, "Failed to add Object");
        assertEquals(101, aDeque.size());
    }

    @Test
    void offer() {
        Object o = new Object();
        aDeque.offer(o);
        assertSame(aDeque.peekLast(), o, "Failed to add Object");
        assertEquals(101, aDeque.size());
    }

    @Test
    void push() {
        Object o = new Object();
        aDeque.push(o);
        assertSame(aDeque.peekFirst(), o, "Failed to add Object");
        assertEquals(101, aDeque.size());
    }

    @Test
    void poll() {
        assertEquals(0, aDeque.poll());
        assertEquals(99, aDeque.size());
    }

    @Test
    void pollFirst() {
        assertEquals(0, aDeque.pollFirst());
        assertEquals(99, aDeque.size());
    }

    @Test
    void pollLast() {
        assertEquals(99, aDeque.pollLast());
        assertEquals(99, aDeque.size());
    }

    @Test
    void peek() {
        assertEquals(0, aDeque.peek());
        assertEquals(100, aDeque.size());
    }

    @Test
    void peekFirst() {
        assertEquals(0, aDeque.peekFirst());
        assertEquals(100, aDeque.size());
    }

    @Test
    void peekLast() {
        assertEquals(99, aDeque.peekLast());
        assertEquals(100, aDeque.size());
    }

    @Test
    void pop() {
        assertEquals(0, aDeque.pop());
        assertEquals(99, aDeque.size());
    }

    @Test
    void clear() {
        aDeque.clear();
        assertEquals(0, aDeque.size(), "List did not clear");
    }

    @Test
    void contains() {
        assertTrue(aDeque.contains(objArray[99]), "Returned false for valid element");
        assertTrue(aDeque.contains(8), "Returned false for equal element");
        assertFalse(aDeque.contains(new Object()), "Returned true for invalid element");
        assertFalse(aDeque.contains(null), "Returned true for null but should have returned false");
    }

    @Test
    void isEmpty() {
        try (HeapTrackingArrayDeque<Object> list = HeapTrackingArrayDeque.newArrayDeque(memoryTracker)) {
            assertTrue(list.isEmpty(), "isEmpty returned false for new list");
        }
        assertFalse(aDeque.isEmpty(), "Returned true for existing list with elements");
    }

    @Test
    void size() {
        assertEquals(100, aDeque.size(), "Returned incorrect size for existing list");
        try (HeapTrackingArrayDeque<Object> list = HeapTrackingArrayDeque.newArrayDeque(memoryTracker)) {
            assertEquals(0, list.size(), "Returned incorrect size for new list");
        }
    }

    @Test
    void offerPoll() {
        try (HeapTrackingArrayDeque<String> list = HeapTrackingArrayDeque.newArrayDeque(memoryTracker)) {
            assertTrue(list.offer("one"));
            assertTrue(list.offer("two"));
            assertEquals(2, list.size());
            assertEquals("one", list.poll());
            assertEquals(1, list.size());
            assertEquals("two", list.poll());
            assertEquals(0, list.size());
            assertNull(list.poll());
        }
    }

    @Test
    void pushPop() {
        try (HeapTrackingArrayDeque<String> list = HeapTrackingArrayDeque.newArrayDeque(memoryTracker)) {
            list.push("one");
            list.push("two");
            assertEquals(2, list.size());
            assertEquals("two", list.pop());
            assertEquals(1, list.size());
            assertEquals("one", list.pop());
            assertEquals(0, list.size());
            assertThrows(NoSuchElementException.class, list::pop);
        }
    }

    @Test
    void removeElement() {
        try (HeapTrackingArrayDeque<String> list = HeapTrackingArrayDeque.newArrayDeque(memoryTracker)) {
            list.addAll(Arrays.asList("a", "b", "c", "d", "e", "f", "g"));

            assertTrue(list.remove("a"), "Removed wrong element");
            assertTrue(list.remove("f"), "Removed wrong element");
            String[] result = new String[5];
            list.toArray(result);
            assertArrayEquals(result, new String[] {"b", "c", "d", "e", "g"}, "Removed wrong element");
        }

        try (HeapTrackingArrayDeque<String> list = HeapTrackingArrayDeque.newArrayDeque(memoryTracker)) {
            list.addAll(Arrays.asList("a", "b", "c"));

            assertFalse(list.remove("d"), "Removed non-existing element");
            assertTrue(list.remove("b"), "Removed wrong element");
            String[] result = new String[2];
            list.toArray(result);
            assertArrayEquals(result, new String[] {"a", "c"}, "Removed wrong element");
        }
    }

    @Test
    void containsAll() {
        assertTrue(aDeque.containsAll(Arrays.asList(1, 3, 5, 7)));
        assertFalse(aDeque.containsAll(Arrays.asList(1, 3, 5, 7, -1)));
    }

    @Test
    void removeAll() {
        assertTrue(aDeque.removeAll(Arrays.asList(1, 3, 8, 9, 10)));
        assertTrue(aDeque.removeAll(Arrays.asList(1, 3, 8, 9, 10, 11)));
        assertFalse(aDeque.removeAll(Arrays.asList(1, 3, 8, 9, 10, 11)));
    }

    @Test
    void retainAll() {
        assertTrue(aDeque.retainAll(Arrays.asList(1, 3, 5)));
        assertArrayEquals(aDeque.toArray(), new Object[] {1, 3, 5});
    }

    @Test
    void forEach() {
        try (HeapTrackingArrayDeque<Integer> deque = HeapTrackingArrayDeque.newArrayDeque(memoryTracker)) {
            deque.add(0);
            deque.add(1);
            deque.add(2);

            ArrayList<Integer> output = new ArrayList<>();
            deque.forEach(output::add);

            assertEquals(List.of(0, 1, 2), output);
        }
    }

    @Test
    void forEachNPE() {
        try (HeapTrackingArrayDeque<Integer> list = HeapTrackingArrayDeque.newArrayDeque(memoryTracker)) {
            assertThrows(NullPointerException.class, () -> list.forEach(null));
        }
    }
}
