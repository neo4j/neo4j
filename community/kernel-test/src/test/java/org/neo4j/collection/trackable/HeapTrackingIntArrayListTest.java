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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class HeapTrackingIntArrayListTest {
    private final MemoryTracker memoryTracker = new LocalMemoryTracker();
    private HeapTrackingIntArrayList aList;
    private int[] intArray;

    @Inject
    private RandomSupport random;

    @BeforeEach
    void setUp() {
        intArray = new int[100];
        for (var i = 0; i < intArray.length; i++) {
            intArray[i] = i;
        }
        aList = HeapTrackingIntArrayList.newIntArrayList(memoryTracker);
        aList.addAll(intArray);
    }

    @AfterEach
    void tearDown() {
        intArray = null;
        aList.close();
        assertEquals(0, memoryTracker.estimatedHeapMemory(), "Leaking memory");
    }

    @Test
    void initialSize() {
        try (var list = HeapTrackingIntArrayList.newIntArrayList(5, memoryTracker)) {
            assertEquals(0, list.size(), "Should not contain any elements when created");
        }

        assertThrows(
                IllegalArgumentException.class, () -> HeapTrackingIntArrayList.newIntArrayList(-10, memoryTracker));
    }

    @Test
    void addObjectAtIndex() {
        final var l = random.nextInt();
        aList.add(50, l);
        assertEquals(aList.get(50), l, "Failed to add Object");
        assertTrue(
                aList.get(51) == intArray[50] && (aList.get(52) == intArray[51]), "Failed to fix up list after insert");
        final var oldItem = aList.get(25);
        aList.add(25, -1);
        assertEquals(aList.get(25), -1, "Should have returned null");
        assertSame(aList.get(26), oldItem, "Should have returned the old item from slot 25");
        assertThrows(IndexOutOfBoundsException.class, () -> aList.add(-1, -1));
        assertThrows(IndexOutOfBoundsException.class, () -> aList.add(aList.size() + 1, -1));
    }

    @Test
    void addObjectLast() {
        final var l = random.nextInt();
        aList.add(l);
        assertEquals(aList.get(aList.size() - 1), l, "Failed to add long");
    }

    @Test
    void clear() {
        aList.clear();
        assertEquals(0, aList.size(), "List did not clear");
        aList.add(random.nextInt());
        aList.add(random.nextInt());
        aList.add(random.nextInt());
        aList.add(random.nextInt());
        aList.clear();
        assertEquals(0, aList.size(), "List with nulls did not clear");
    }

    @Test
    void get() {
        assertSame(aList.get(22), intArray[22], "Returned incorrect element");
        assertThrows(IndexOutOfBoundsException.class, () -> aList.get(8765));
    }

    @Test
    void isEmpty() {
        try (var list = HeapTrackingIntArrayList.newIntArrayList(memoryTracker)) {
            assertTrue(list.isEmpty(), "isEmpty returned false for new list");
        }
        assertFalse(aList.isEmpty(), "Returned true for existing list with elements");
    }

    @Test
    void setElement() {
        final var l = random.nextInt();
        aList.set(65, l);
        assertEquals(aList.get(65), l, "Failed to set object");
        assertEquals(100, aList.size(), "Setting increased the list's size to: " + aList.size());
        assertThrows(IndexOutOfBoundsException.class, () -> aList.set(-1, random.nextInt()));
        assertThrows(IndexOutOfBoundsException.class, () -> aList.set(aList.size() + 1, random.nextInt()));
    }

    @Test
    void size() {
        assertEquals(100, aList.size(), "Returned incorrect size for exiting list");
        try (var list = HeapTrackingIntArrayList.newIntArrayList(memoryTracker)) {
            assertEquals(0, list.size(), "Returned incorrect size for new list");
        }
    }
}
