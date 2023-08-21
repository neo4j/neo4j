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
package org.neo4j.memory;

/**
 * Tracker that is possibly capable of reporting the high water mark of number of allocated bytes on the heap.
 */
public interface HeapHighWaterMarkTracker {
    long ALLOCATIONS_NOT_TRACKED = -1L;

    HeapHighWaterMarkTracker NONE = () -> ALLOCATIONS_NOT_TRACKED;

    HeapHighWaterMarkTracker ZERO = () -> 0L;

    /**
     * Get the total allocated memory, in bytes.
     * The high water mark, i.e. the maximum observed value, of allocated memory in bytes.
     *
     * @return the total number of allocated memory bytes, or {@link #ALLOCATIONS_NOT_TRACKED}, if memory tracking was not enabled.
     */
    long heapHighWaterMark();
}
