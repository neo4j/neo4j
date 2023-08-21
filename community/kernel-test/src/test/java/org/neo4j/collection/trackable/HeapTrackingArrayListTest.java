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
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryTracker;

class HeapTrackingArrayListTest {
    private final MemoryTracker memoryTracker = new LocalMemoryTracker();
    private HeapTrackingArrayList<Object> aList;
    private Object[] objArray;

    @BeforeEach
    void setUp() {
        objArray = new Object[100];
        for (int i = 0; i < objArray.length; i++) {
            objArray[i] = i;
        }
        aList = HeapTrackingArrayList.newArrayList(memoryTracker);
        aList.addAll(Arrays.asList(objArray));
    }

    @AfterEach
    void tearDown() {
        objArray = null;
        aList.close();
        assertEquals(0, memoryTracker.estimatedHeapMemory(), "Leaking memory");
    }

    @Test
    void initialSize() {
        try (HeapTrackingArrayList<Object> list = HeapTrackingArrayList.newArrayList(5, memoryTracker)) {
            assertEquals(0, list.size(), "Should not contain any elements when created");
        }

        assertThrows(IllegalArgumentException.class, () -> HeapTrackingArrayList.newArrayList(-10, memoryTracker));
    }

    @Test
    void addObjectAtIndex() {
        Object o;
        aList.add(50, o = new Object());
        assertSame(aList.get(50), o, "Failed to add Object");
        assertTrue(
                aList.get(51) == objArray[50] && (aList.get(52) == objArray[51]), "Failed to fix up list after insert");
        Object oldItem = aList.get(25);
        aList.add(25, null);
        assertNull(aList.get(25), "Should have returned null");
        assertSame(aList.get(26), oldItem, "Should have returned the old item from slot 25");
        assertThrows(IndexOutOfBoundsException.class, () -> aList.add(-1, null));
        assertThrows(IndexOutOfBoundsException.class, () -> aList.add(aList.size() + 1, null));
    }

    @Test
    void addObjectLast() {
        Object o = new Object();
        aList.add(o);
        assertSame(aList.get(aList.size() - 1), o, "Failed to add Object");
        aList.add(null);
        assertNull(aList.get(aList.size() - 1), "Failed to add null");
    }

    @Test
    void addAllFromCollectionAtIndex() {
        aList.addAll(50, aList);
        assertEquals(200, aList.size(), "Returned incorrect size after adding to existing list");
        for (int i = 0; i < 50; i++) {
            assertSame(aList.get(i), objArray[i], "Manipulated elements < index");
        }
        for (int i = 50; i < 150; i++) {
            assertSame(aList.get(i), objArray[i - 50], "Failed to add elements properly");
        }
        for (int i = 150; i < 200; i++) {
            assertSame(aList.get(i), objArray[i - 100], "Failed to ad elements properly");
        }

        try (HeapTrackingArrayList<Object> listWithNulls = HeapTrackingArrayList.newArrayList(memoryTracker)) {
            listWithNulls.add(null);
            listWithNulls.add(null);
            listWithNulls.add("yoink");
            listWithNulls.add("kazoo");
            listWithNulls.add(null);
            aList.addAll(100, listWithNulls);
            assertEquals(205, aList.size(), "Incorrect size: " + aList.size());
            assertNull(aList.get(100), "Item at slot 100 should be null");
            assertNull(aList.get(101), "Item at slot 101 should be null");
            assertEquals("yoink", aList.get(102), "Item at slot 102 should be 'yoink'");
            assertEquals("kazoo", aList.get(103), "Item at slot 103 should be 'kazoo'");
            assertNull(aList.get(104), "Item at slot 104 should be null");
            aList.addAll(205, listWithNulls);
            assertEquals(210, aList.size(), "Incorrect size2: " + aList.size());
            assertThrows(IndexOutOfBoundsException.class, () -> aList.addAll(-1, listWithNulls));
            assertThrows(IndexOutOfBoundsException.class, () -> aList.addAll(aList.size() + 1, listWithNulls));
            assertThrows(NullPointerException.class, () -> aList.addAll(0, null));
        }
    }

    @Test
    void addAllFromCollection() {
        try (HeapTrackingArrayList<Object> l = HeapTrackingArrayList.newArrayList(memoryTracker)) {
            l.addAll(aList);
            for (int i = 0; i < aList.size(); i++) {
                assertEquals(l.get(i), aList.get(i), "Failed to add elements properly");
            }

            aList.addAll(aList);
            assertEquals(200, aList.size(), "Returned incorrect size after adding to existing list");
            for (int i = 0; i < 100; i++) {
                assertEquals(aList.get(i), l.get(i), "Added to list in incorrect order");
                assertEquals(aList.get(i + 100), l.get(i), "Failed to add to existing list");
            }
        }
        Set<Object> setWithNulls = new HashSet<>();
        setWithNulls.add(null);
        setWithNulls.add(null);
        setWithNulls.add("yoink");
        setWithNulls.add("kazoo");
        setWithNulls.add(null);
        aList.addAll(100, setWithNulls);
        Iterator<Object> i = setWithNulls.iterator();
        assertSame(aList.get(100), i.next(), "Item at slot 100 is wrong: " + aList.get(100));
        assertSame(aList.get(101), i.next(), "Item at slot 101 is wrong: " + aList.get(101));
        assertSame(aList.get(102), i.next(), "Item at slot 103 is wrong: " + aList.get(102));

        try (HeapTrackingArrayList<Integer> originalList = HeapTrackingArrayList.newArrayList(12, memoryTracker)) {
            for (int j = 0; j < 12; j++) {
                originalList.add(j);
            }
            originalList.remove(0);
            originalList.remove(0);
            try (HeapTrackingArrayList<Integer> additionalList =
                    HeapTrackingArrayList.newArrayList(11, memoryTracker)) {
                for (int j = 0; j < 11; j++) {
                    additionalList.add(j);
                }
                assertTrue(originalList.addAll(additionalList));
            }
            assertEquals(21, originalList.size());
        }
        assertThrows(NullPointerException.class, () -> aList.addAll(null));
    }

    @Test
    void clear() {
        aList.clear();
        assertEquals(0, aList.size(), "List did not clear");
        aList.add(null);
        aList.add(null);
        aList.add(null);
        aList.add("bam");
        aList.clear();
        assertEquals(0, aList.size(), "List with nulls did not clear");
    }

    @Test
    void contains() {
        assertTrue(aList.contains(objArray[99]), "Returned false for valid element");
        assertTrue(aList.contains(8), "Returned false for equal element");
        assertFalse(aList.contains(new Object()), "Returned true for invalid element");
        assertFalse(aList.contains(null), "Returned true for null but should have returned false");
        aList.add(null);
        assertTrue(aList.contains(null), "Returned false for null but should have returned true");
    }

    @Test
    void get() {
        assertSame(aList.get(22), objArray[22], "Returned incorrect element");
        assertThrows(IndexOutOfBoundsException.class, () -> aList.get(8765));
    }

    @Test
    void indexOfElement() {
        assertEquals(87, aList.indexOf(objArray[87]), "Returned incorrect index");
        assertEquals(-1, aList.indexOf(new Object()), "Returned index for invalid Object");
        aList.add(25, null);
        aList.add(50, null);
        assertEquals(25, aList.indexOf(null), "Wrong indexOf for null.  Wanted 25 got: " + aList.indexOf(null));
    }

    @Test
    void isEmpty() {
        try (HeapTrackingArrayList<Object> list = HeapTrackingArrayList.newArrayList(memoryTracker)) {
            assertTrue(list.isEmpty(), "isEmpty returned false for new list");
        }
        assertFalse(aList.isEmpty(), "Returned true for existing list with elements");
    }

    @Test
    void lastIndexOfElement() {
        aList.add(99);
        assertEquals(100, aList.lastIndexOf(objArray[99]), "Returned incorrect index");
        assertEquals(-1, aList.lastIndexOf(new Object()), "Returned index for invalid Object");
        aList.add(25, null);
        aList.add(50, null);
        assertEquals(
                50, aList.lastIndexOf(null), "Wrong lastIndexOf for null.  Wanted 50 got: " + aList.lastIndexOf(null));
    }

    @Test
    void removeIndex() {
        aList.remove(10);
        assertEquals(-1, aList.indexOf(objArray[10]), "Failed to remove element");
        assertThrows(IndexOutOfBoundsException.class, () -> aList.remove(999));

        Object[] objects = aList.toArray();
        aList.add(25, null);
        aList.add(50, null);
        aList.remove(50);
        aList.remove(25);
        assertArrayEquals(objects, aList.toArray(), "Removing nulls did not work");

        try (HeapTrackingArrayList<String> list = HeapTrackingArrayList.newArrayList(memoryTracker)) {
            list.addAll(Arrays.asList("a", "b", "c", "d", "e", "f", "g"));
            assertSame("a", list.remove(0), "Removed wrong element 1");
            assertSame("f", list.remove(4), "Removed wrong element 2");
            String[] result = new String[5];
            list.toArray(result);
            assertArrayEquals(result, new String[] {"b", "c", "d", "e", "g"}, "Removed wrong element 3");
        }
        try (HeapTrackingArrayList<Object> l = HeapTrackingArrayList.newArrayList(memoryTracker)) {
            l.add(new Object());
            l.add(new Object());
            l.remove(0);
            l.remove(0);
            assertThrows(IndexOutOfBoundsException.class, () -> l.remove(-1));
            assertThrows(IndexOutOfBoundsException.class, () -> l.remove(0));
        }
    }

    @Test
    void setElement() {
        Object obj;
        aList.set(65, obj = new Object());
        assertSame(aList.get(65), obj, "Failed to set object");
        aList.set(50, null);
        assertNull(aList.get(50), "Setting to null did not work");
        assertEquals(100, aList.size(), "Setting increased the list's size to: " + aList.size());
        assertThrows(IndexOutOfBoundsException.class, () -> aList.set(-1, null));
        assertThrows(IndexOutOfBoundsException.class, () -> aList.set(aList.size() + 1, null));
    }

    @Test
    void size() {
        assertEquals(100, aList.size(), "Returned incorrect size for existing list");
        try (HeapTrackingArrayList<Object> list = HeapTrackingArrayList.newArrayList(memoryTracker)) {
            assertEquals(0, list.size(), "Returned incorrect size for new list");
        }
    }

    @Test
    void toArray() {
        aList.set(25, null);
        aList.set(75, null);
        Object[] obj = aList.toArray();
        assertEquals(objArray.length, obj.length, "Returned array of incorrect size");
        for (int i = 0; i < obj.length; i++) {
            if ((i == 25) || (i == 75)) {
                assertNull(obj[i], "Should be null at: " + i + " but instead got: " + obj[i]);
            } else {
                assertSame(obj[i], objArray[i], "Returned incorrect array: " + i);
            }
        }
    }

    @Test
    void toArrayWithDestination() {
        aList.set(25, null);
        aList.set(75, null);
        Integer[] argArray = new Integer[100];
        Object[] retArray;
        retArray = aList.toArray(argArray);
        assertSame(retArray, argArray, "Returned different array than passed");
        argArray = new Integer[1000];
        retArray = aList.toArray(argArray);
        assertNull(argArray[aList.size()], "Failed to set first extra element to null");
        for (int i = 0; i < 100; i++) {
            if ((i == 25) || (i == 75)) {
                assertNull(retArray[i], "Should be null: " + i);
            } else {
                assertSame(retArray[i], objArray[i], "Returned incorrect array: " + i);
            }
        }
        String[] strArray = new String[100];
        assertThrows(ArrayStoreException.class, () -> aList.toArray(strArray));
    }

    @Test
    void addAll() {
        try (HeapTrackingArrayList<String> list = HeapTrackingArrayList.newArrayList(memoryTracker);
                HeapTrackingArrayList<String> collection = HeapTrackingArrayList.newArrayList(memoryTracker)) {
            list.add("one");
            list.add("two");
            assertEquals(2, list.size());
            list.remove(0);
            assertEquals(1, list.size());
            collection.add("1");
            collection.add("2");
            collection.add("3");
            assertEquals(3, collection.size());
            list.addAll(0, collection);
            assertEquals(4, list.size());
            list.remove(0);
            list.remove(0);
            assertEquals(2, list.size());
            collection.add("4");
            collection.add("5");
            collection.add("6");
            collection.add("7");
            collection.add("8");
            collection.add("9");
            collection.add("10");
            collection.add("11");
            collection.add("12");
            assertEquals(12, collection.size());
            list.addAll(0, collection);
            assertEquals(14, list.size());
        }
    }

    @Test
    void removeElement() {
        try (HeapTrackingArrayList<String> list = HeapTrackingArrayList.newArrayList(memoryTracker)) {
            list.addAll(Arrays.asList("a", "b", "c", "d", "e", "f", "g"));

            assertTrue(list.remove("a"), "Removed wrong element 1");
            assertTrue(list.remove("f"), "Removed wrong element 2");
            String[] result = new String[5];
            list.toArray(result);
            assertArrayEquals(result, new String[] {"b", "c", "d", "e", "g"}, "Removed wrong element 3");
        }

        try (HeapTrackingArrayList<String> list = HeapTrackingArrayList.newArrayList(memoryTracker)) {
            list.addAll(Arrays.asList("a", null, "c"));

            assertFalse(list.remove("d"), "Removed non-existing element");
            assertTrue(list.remove(null), "Removed wrong element 4");
            String[] result = new String[2];
            list.toArray(result);
            assertArrayEquals(result, new String[] {"a", "c"}, "Removed wrong element 5");
        }
    }

    @Test
    void containsAll() {
        assertTrue(aList.containsAll(Arrays.asList(1, 3, 5, 7)));
        assertFalse(aList.containsAll(Arrays.asList(1, 3, 5, 7, -1)));
    }

    @Test
    void removeAll() {
        assertTrue(aList.removeAll(Arrays.asList(1, 3, 8, 9, 10)));
        assertTrue(aList.removeAll(Arrays.asList(1, 3, 8, 9, 10, 11)));
        assertFalse(aList.removeAll(Arrays.asList(1, 3, 8, 9, 10, 11)));
    }

    @Test
    void retainAll() {
        assertTrue(aList.retainAll(Arrays.asList(1, 3, 5)));
        assertArrayEquals(aList.toArray(), new Object[] {1, 3, 5});
    }

    @Test
    void sort() {
        aList.add(-1);
        assertEquals(0, aList.get(0));
        aList.sort(Comparator.comparingInt(value -> (Integer) value));
        assertEquals(-1, aList.get(0));
    }

    @Test
    void iteratorConcurrentModifications() {
        Iterator<Object> iterator = aList.iterator();
        aList.add(101);
        assertThrows(ConcurrentModificationException.class, iterator::next);

        iterator = aList.iterator();
        aList.addAll(Arrays.asList(102, 103));
        assertThrows(ConcurrentModificationException.class, iterator::next);

        iterator = aList.iterator();
        aList.add(103, 104);
        assertThrows(ConcurrentModificationException.class, iterator::next);

        iterator = aList.iterator();
        aList.addAll(104, Arrays.asList(105, 106));
        assertThrows(ConcurrentModificationException.class, iterator::next);

        iterator = aList.iterator();
        aList.sort(Comparator.comparingInt(value -> (Integer) value));
        assertThrows(ConcurrentModificationException.class, iterator::next);

        iterator = aList.iterator();
        aList.remove(105);
        assertThrows(ConcurrentModificationException.class, iterator::next);

        iterator = aList.iterator();
        aList.remove((Object) 104);
        assertThrows(ConcurrentModificationException.class, iterator::next);

        iterator = aList.iterator();
        aList.removeAll(Arrays.asList(103, 102));
        assertThrows(ConcurrentModificationException.class, iterator::next);

        iterator = aList.iterator();
        aList.removeAll(Arrays.asList(103, 102));
        iterator.next(); // ok

        iterator = aList.iterator();
        aList.retainAll(Arrays.asList(0, 1, 2));
        assertThrows(ConcurrentModificationException.class, iterator::next);

        iterator = aList.iterator();
        aList.retainAll(Arrays.asList(0, 1, 2));
        iterator.next(); // ok

        iterator = aList.iterator();
        aList.clear();
        assertThrows(ConcurrentModificationException.class, iterator::next);
    }

    @Test
    void forEach() {
        try (HeapTrackingArrayList<Integer> list = HeapTrackingArrayList.newArrayList(memoryTracker)) {
            list.add(0);
            list.add(1);
            list.add(2);

            ArrayList<Integer> output = new ArrayList<>();
            list.forEach(output::add);

            assertEquals(list, output);
        }
    }

    @Test
    void forEachNPE() {
        try (HeapTrackingArrayList<Integer> list = HeapTrackingArrayList.newArrayList(memoryTracker)) {
            assertThrows(NullPointerException.class, () -> list.forEach(null));
        }
    }

    @Test
    void forEachCME() {
        try (HeapTrackingArrayList<Integer> list = HeapTrackingArrayList.newArrayList(memoryTracker)) {
            list.add(1);
            list.add(2);
            ArrayList<Integer> processed = new ArrayList<>();

            assertThrows(
                    ConcurrentModificationException.class,
                    () -> list.forEach(t -> {
                        processed.add(t);
                        list.add(t);
                    }));
            assertEquals(1, processed.size());
        }
    }

    @Test
    void forEachCMEonLastElement() {
        try (HeapTrackingArrayList<Integer> list = HeapTrackingArrayList.newArrayList(memoryTracker)) {
            list.add(1);
            list.add(2);
            list.add(3);

            assertThrows(
                    ConcurrentModificationException.class,
                    () -> list.forEach(t -> {
                        if (t == 3) {
                            list.add(t);
                        }
                    }));
        }
    }
}
