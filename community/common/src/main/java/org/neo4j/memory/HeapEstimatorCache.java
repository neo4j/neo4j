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

import org.github.jamm.Unmetered;

public interface HeapEstimatorCache {
    /**
     * Gives an estimation of the heap usage in bytes for the given value,
     * based on the given estimate.
     *
     * @return an estimation of how many bytes this value consumes.
     */
    long estimatedHeapUsage(Measurable measurable, long estimate);

    /**
     * Resets the cache to a state where it can be used again.
     * Does not clear references to the large objects.
     */
    void fastReset();

    /**
     * Resets the cache to a state where it can be used again.
     * And also clears references to the large objects.
     */
    void fullReset();

    HeapEstimatorCache newWithSameSettings();

    @Unmetered
    final class NoHeapEstimatorCache implements HeapEstimatorCache {
        public static final NoHeapEstimatorCache INSTANCE = new NoHeapEstimatorCache();

        private NoHeapEstimatorCache() {}

        @Override
        public long estimatedHeapUsage(Measurable measurable, long estimate) {
            return estimate;
        }

        @Override
        public void fastReset() {}

        @Override
        public void fullReset() {}

        @Override
        public HeapEstimatorCache newWithSameSettings() {
            return this;
        }
    }
}
