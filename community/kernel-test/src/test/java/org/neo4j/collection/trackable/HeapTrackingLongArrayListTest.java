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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class HeapTrackingLongArrayListTest {
    private final MemoryTracker memoryTracker = new LocalMemoryTracker();
    private HeapTrackingLongArrayList aList;
    private long[] longArray;

    @Inject
    private RandomSupport random;

    @BeforeEach
    void setUp() {
        longArray = new long[100];
        for (int i = 0; i < longArray.length; i++) {
            longArray[i] = i;
        }
        aList = HeapTrackingLongArrayList.newLongArrayList(memoryTracker);
        aList.addAll(longArray);
    }

    @AfterEach
    void tearDown() {
        longArray = null;
        aList.close();
        assertEquals(0, memoryTracker.estimatedHeapMemory(), "Leaking memory");
    }

    @Test
    void initialSize() {
        try (HeapTrackingLongArrayList list = HeapTrackingLongArrayList.newLongArrayList(5, memoryTracker)) {
            assertEquals(0, list.size(), "Should not contain any elements when created");
        }

        assertThrows(
                IllegalArgumentException.class, () -> HeapTrackingLongArrayList.newLongArrayList(-10, memoryTracker));
    }

    @Test
    void addObjectAtIndex() {
        long l;
        aList.add(50, l = random.nextLong());
        assertEquals(aList.get(50), l, "Failed to add Object");
        assertTrue(
                aList.get(51) == longArray[50] && (aList.get(52) == longArray[51]),
                "Failed to fix up list after insert");
        Object oldItem = aList.get(25);
        aList.add(25, -1L);
        assertEquals(aList.get(25), -1L, "Should have returned null");
        assertSame(aList.get(26), oldItem, "Should have returned the old item from slot 25");
        assertThrows(IndexOutOfBoundsException.class, () -> aList.add(-1, -1L));
        assertThrows(IndexOutOfBoundsException.class, () -> aList.add(aList.size() + 1, -1L));
    }

    @Test
    void addObjectLast() {
        long l = random.nextLong();
        aList.add(l);
        assertEquals(aList.get(aList.size() - 1), l, "Failed to add long");
    }

    @Test
    void clear() {
        aList.clear();
        assertEquals(0, aList.size(), "List did not clear");
        aList.add(random.nextLong());
        aList.add(random.nextLong());
        aList.add(random.nextLong());
        aList.add(random.nextLong());
        aList.clear();
        assertEquals(0, aList.size(), "List with nulls did not clear");
    }

    @Test
    void get() {
        assertSame(aList.get(22), longArray[22], "Returned incorrect element");
        assertThrows(IndexOutOfBoundsException.class, () -> aList.get(8765));
    }

    @Test
    void isEmpty() {
        try (HeapTrackingLongArrayList list = HeapTrackingLongArrayList.newLongArrayList(memoryTracker)) {
            assertTrue(list.isEmpty(), "isEmpty returned false for new list");
        }
        assertFalse(aList.isEmpty(), "Returned true for existing list with elements");
    }

    @Test
    void setElement() {
        long l;
        aList.set(65, l = random.nextLong());
        assertEquals(aList.get(65), l, "Failed to set object");
        assertEquals(100, aList.size(), "Setting increased the list's size to: " + aList.size());
        assertThrows(IndexOutOfBoundsException.class, () -> aList.set(-1, random.nextLong()));
        assertThrows(IndexOutOfBoundsException.class, () -> aList.set(aList.size() + 1, random.nextLong()));
    }

    @Test
    void size() {
        assertEquals(100, aList.size(), "Returned incorrect size for exiting list");
        try (HeapTrackingLongArrayList list = HeapTrackingLongArrayList.newLongArrayList(memoryTracker)) {
            assertEquals(0, list.size(), "Returned incorrect size for new list");
        }
    }

    @Test
    void iterator() {
        PrimitiveLongResourceIterator iterator = aList.iterator();
        int i = 0;
        while (iterator.hasNext()) {
            assertTrue(i < longArray.length);
            assertEquals(longArray[i++], iterator.next());
        }
        assertEquals(i, longArray.length);
    }
}
