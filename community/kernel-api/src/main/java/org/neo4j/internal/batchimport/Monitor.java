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
package org.neo4j.internal.batchimport;

public interface Monitor {
    Monitor NO_MONITOR = new Monitor() {
        @Override
        public void mayExceedRelationshipIdCapacity(long capacity, long estimatedCount) { // no-op
        }

        @Override
        public void mayExceedNodeIdCapacity(long capacity, long estimatedCount) { // no-op
        }

        @Override
        public void doubleRelationshipRecordUnitsEnabled() { // no-op
        }

        @Override
        public void insufficientHeapSize(long optimalMinimalHeapSize, long heapSize) { // no-op
        }

        @Override
        public void abundantHeapSize(long optimalMinimalHeapSize, long heapSize) { // no-op
        }

        @Override
        public void insufficientAvailableMemory(
                long estimatedCacheSize, long optimalMinimalHeapSize, long availableMemory) { // no-op
        }
    };

    void doubleRelationshipRecordUnitsEnabled();

    void mayExceedNodeIdCapacity(long capacity, long estimatedCount);

    void mayExceedRelationshipIdCapacity(long capacity, long estimatedCount);

    void insufficientHeapSize(long optimalMinimalHeapSize, long heapSize);

    void abundantHeapSize(long optimalMinimalHeapSize, long heapSize);

    void insufficientAvailableMemory(long estimatedCacheSize, long optimalMinimalHeapSize, long availableMemory);
}
