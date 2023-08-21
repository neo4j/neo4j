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
package org.neo4j.consistency;

class ConsistencyCheckMemoryCalculation {
    static MemoryDistribution calculate(
            long maxOffHeapMemory, long desiredPageCacheMemory, long desiredOffHeapCachingMemory) {
        var pageCacheMemory = desiredPageCacheMemory;
        var offHeapCachingMemory = desiredOffHeapCachingMemory;
        if (desiredPageCacheMemory + desiredOffHeapCachingMemory > maxOffHeapMemory) {
            // There isn't enough memory available to allocate optimal amount of memory,
            // we have to make some sacrifices to one or both of them.
            // The distribution between the two is difficult to empirically reason about since
            // a smaller page cache means more page faults, but a smaller off-heap caching for the checker
            // means more internal rounds of checking or suboptimal data structures.
            pageCacheMemory = Long.max(maxOffHeapMemory - offHeapCachingMemory, (long) (maxOffHeapMemory * 0.75D));
        }
        offHeapCachingMemory = maxOffHeapMemory - pageCacheMemory;
        assert pageCacheMemory + offHeapCachingMemory <= maxOffHeapMemory
                : "Too much memory being used " + "pageCacheMemory:" + pageCacheMemory + " offHeapCachingMemory:"
                        + offHeapCachingMemory;
        return new MemoryDistribution(pageCacheMemory, offHeapCachingMemory);
    }

    record MemoryDistribution(long pageCacheMemory, long offHeapCachingMemory) {}
}
