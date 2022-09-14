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
package org.neo4j.memory;

/**
 * An interface that enables explicitly recording allocation and release of heap memory on
 * the outer scope of an {@link RebindableDualScopedMemoryTracker}.
 *
 * This functionality is required for queries that contain CALL IN TRANSACTIONS.
 */
public interface DualScopedHeapMemoryTracker {
    /**
     * Record an allocation of heap memory on the outer transactional scope.
     *
     * @param bytes the number of bytes about to be allocated.
     * @throws MemoryLimitExceededException if the current quota would be exceeded by allocating the provided number of bytes.
     */
    void allocateHeapOuter(long bytes);

    /**
     * Record the release of heap memory on the outer transactional scope. This should be called when we forget about a reference and
     * that particular object will be garbage collected.
     *
     * @param bytes number of released bytes
     */
    void releaseHeapOuter(long bytes);
}
