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
package org.neo4j.batchimport.api;

public interface Monitor {
    Monitor NO_MONITOR = new Monitor() {};

    default void doubleRelationshipRecordUnitsEnabled() {}

    default void mayExceedNodeIdCapacity(long capacity, long estimatedCount) {}

    default void mayExceedRelationshipIdCapacity(long capacity, long estimatedCount) {}

    default void insufficientHeapSize(long optimalMinimalHeapSize, long heapSize) {}

    default void abundantHeapSize(long optimalMinimalHeapSize, long heapSize) {}

    default void insufficientAvailableMemory(
            long estimatedCacheSize, long optimalMinimalHeapSize, long availableMemory) {}

    /**
     * Called right before import is starting.
     */
    default void started() {}

    /**
     * Gets updated with a percentage of the overall import progress.
     * @param percentage overall import progress between 0..100.
     */
    default void percentageCompleted(int percentage) {}

    /**
     * Called after import have been completed.
     */
    default void completed(boolean success) {}

    class Delegate implements Monitor {
        private final Monitor delegate;

        public Delegate(Monitor delegate) {
            this.delegate = delegate;
        }

        @Override
        public void doubleRelationshipRecordUnitsEnabled() {
            delegate.doubleRelationshipRecordUnitsEnabled();
        }

        @Override
        public void mayExceedNodeIdCapacity(long capacity, long estimatedCount) {
            delegate.mayExceedNodeIdCapacity(capacity, estimatedCount);
        }

        @Override
        public void mayExceedRelationshipIdCapacity(long capacity, long estimatedCount) {
            delegate.mayExceedRelationshipIdCapacity(capacity, estimatedCount);
        }

        @Override
        public void insufficientHeapSize(long optimalMinimalHeapSize, long heapSize) {
            delegate.insufficientHeapSize(optimalMinimalHeapSize, heapSize);
        }

        @Override
        public void abundantHeapSize(long optimalMinimalHeapSize, long heapSize) {
            delegate.abundantHeapSize(optimalMinimalHeapSize, heapSize);
        }

        @Override
        public void insufficientAvailableMemory(
                long estimatedCacheSize, long optimalMinimalHeapSize, long availableMemory) {
            delegate.insufficientAvailableMemory(estimatedCacheSize, optimalMinimalHeapSize, availableMemory);
        }

        @Override
        public void started() {
            delegate.started();
        }

        @Override
        public void percentageCompleted(int percentage) {
            delegate.percentageCompleted(percentage);
        }

        @Override
        public void completed(boolean success) {
            delegate.completed(success);
        }
    }
}
