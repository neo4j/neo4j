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

public record HeapEstimatorCacheConfig(int sizeLimit, long largeObjectThreshold) {
    public static final int DEFAULT_SIZE_LIMIT = 16;
    public static final long DEFAULT_LARGE_OBJECT_THRESHOLD = 64L * 1024L; // 64KiB

    public static final HeapEstimatorCacheConfig DISABLED = new HeapEstimatorCacheConfig(0, Long.MAX_VALUE);

    public static final HeapEstimatorCacheConfig SMALL =
            new HeapEstimatorCacheConfig(DEFAULT_SIZE_LIMIT, DEFAULT_LARGE_OBJECT_THRESHOLD);

    public static final HeapEstimatorCacheConfig LARGE = new HeapEstimatorCacheConfig(128, 8L * 1024L);

    public static final HeapEstimatorCacheConfig DEFAULT = HeapEstimatorCacheConfig.SMALL;

    public HeapEstimatorCache newDefaultHeapEstimatorCache() {
        return sizeLimit > 0
                ? new DeduplicateLargeObjectsHeapEstimatorCache(this)
                : HeapEstimatorCache.NoHeapEstimatorCache.INSTANCE;
    }
}
