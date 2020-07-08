/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.memory;

/**
 * Memory allocation tracker that tracks bytes allocation and de-allocation
 */
public interface MemoryTracker extends AutoCloseable
{
    /**
     * @return number of bytes of native memory that are used
     */
    long usedNativeMemory();

    /**
     * @return estimated number of retained heap in bytes
     */
    long estimatedHeapMemory();

    /**
     * Record allocation of bytes in native memory.
     *
     * @param bytes number of allocated bytes.
     */
    void allocateNative( long bytes );

    /**
     * Record de-allocation of bytes in native memory.
     *
     * @param bytes number of released bytes.
     */
    void releaseNative( long bytes );

    /**
     * Record an allocation of heap memory.
     *
     * @param bytes the number of bytes about to be allocated.
     * @throws MemoryLimitExceededException if the current quota would be exceeded by allocating the provided number of bytes.
     */
    void allocateHeap( long bytes );

    /**
     * Record the release of heap memory. This should be called when we forget about a reference and that particular object will be garbage collected.
     *
     * @param bytes number of released bytes
     */
    void releaseHeap( long bytes );

    /**
     * @return The high water mark, i.e. the maximum observed value, of allocated heap in bytes.
     */
    long heapHighWaterMark();

    void reset();

    @Override
    default void close()
    {
        reset();
    }

    /**
     * Get a memory tracker that can track sub-allocations and be closed in a single call.
     * Can be useful for collections when the items are tracked, to avoid iterating over all
     * of the elements and releasing them individual.
     *
     * @return The scoped memory tracker.
     */
    MemoryTracker getScopedMemoryTracker();
}
