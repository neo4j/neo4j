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
 * Memory allocation tracker that tracks bytes allocation and de-allocation on the heap.
 */
public interface HeapMemoryTracker extends HeapHighWaterMarkTracker {
    /**
     * Record an allocation of heap memory.
     *
     * @param bytes the number of bytes about to be allocated.
     * @throws MemoryLimitExceededException if the current quota would be exceeded by allocating the provided number of bytes.
     */
    void allocateHeap(long bytes);

    /**
     * Record the release of heap memory. This should be called when we forget about a reference and that particular object will be garbage collected.
     *
     * @param bytes number of released bytes
     */
    void releaseHeap(long bytes);
}
