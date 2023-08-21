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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryTracker;

class HeapTrackingStackTest {
    private final MemoryTracker memoryTracker = new LocalMemoryTracker();
    private HeapTrackingArrayDeque<Object> aStack;
    private Object[] objArray;

    @BeforeEach
    void setUp() {
        objArray = new Object[100];
        for (int i = 0; i < objArray.length; i++) {
            objArray[i] = i;
        }
        aStack = HeapTrackingCollections.newArrayDeque(memoryTracker);
        for (Object o : objArray) {
            aStack.push(o);
        }
    }

    @AfterEach
    void tearDown() {
        objArray = null;
        aStack.close();
        assertEquals(0, memoryTracker.estimatedHeapMemory(), "Leaking memory");
    }

    @Test
    void pop() {
        assertEquals(100, aStack.size(), "Returned incorrect size for existing list");
        int i = 99;

        while (!aStack.isEmpty()) {
            assertEquals(i--, aStack.pop());
        }
        assertEquals(-1, i);
        assertEquals(0, aStack.size(), "Returned incorrect size for modified list");
    }

    @Test
    void peek() {
        assertEquals(100, aStack.size(), "Returned incorrect size for existing list");
        assertEquals(99, aStack.peek());
        assertEquals(100, aStack.size(), "Returned incorrect size for existing list");
    }

    @Test
    void push() {
        assertEquals(100, aStack.size(), "Returned incorrect size for existing list");
        aStack.push(42);
        assertEquals(42, aStack.peek());
        assertEquals(101, aStack.size(), "Returned incorrect size for modified list");
    }
}
