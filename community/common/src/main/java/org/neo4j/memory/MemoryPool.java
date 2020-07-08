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
 * A pool of memory that consumers can reserve memory from.
 * <p>
 * You can also query the usage of it, with the contract that
 * {@code pool.totalUsed() + pool.free() == pool.totalSize()}
 *
 * @implNote There is no obligations for the implementation to verify
 *   that you don't release memory you don't own. It's up to the caller
 *   to do the appropriate checks.
 */
public interface MemoryPool
{
    /**
     * Grab a chunk of memory. This method might throw if there is no available memory.
     *
     * @param bytes number of bytes to reserve
     * @throws MemoryLimitExceededException if the are not enough free memory to fulfill the reservation
     */
    void reserveHeap( long bytes );

    /**
     * Grab a chunk of native memory. This method might throw if there is no available memory.
     *
     * @param bytes number of bytes to reserve
     * @throws MemoryLimitExceededException if the are not enough free memory to fulfill the reservation
     */
    void reserveNative( long bytes );

    /**
     * Give back previously reserved heap memory. This will never throw.
     *
     * @param bytes number of bytes to give back
     */
    void releaseHeap( long bytes );

    /**
     * Give back previously reserved native memory. This will never throw.
     *
     * @param bytes number of bytes to give back
     */
    void releaseNative( long bytes );

    /**
     * Returns the total size of this pool in bytes.
     *
     * @return the size of the pool in bytes, {@link Long#MAX_VALUE} is returned for unbounded pools
     */
    long totalSize();

    /**
     * Returns the number of reserved heap bytes.
     *
     * @return the number or reserved bytes.
     */
    long usedHeap();

    /**
     * Returns the number of reserved native bytes.
     *
     * @return the number or reserved bytes.
     */
    long usedNative();

    /**
     * Returns the number of total heap and native bytes.
     *
     * @return the total number or reserved bytes.
     */
    default long totalUsed()
    {
        return usedHeap() + usedNative();
    }

    /**
     * Returns the number of bytes that can still be reserved from this pool.
     *
     * @return the number of bytes that are not used by anyone.
     */
    long free();

    /**
     * Updates the total size of the pool.
     *
     * @param size the new size of the pool.
     */
    void setSize( long size );
}
