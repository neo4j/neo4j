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
package org.neo4j.collection.trackable;

import static java.util.Objects.requireNonNull;
import static org.neo4j.memory.HeapEstimator.ARRAY_HEADER_BYTES;
import static org.neo4j.memory.HeapEstimator.OBJECT_REFERENCE_BYTES;
import static org.neo4j.memory.HeapEstimator.alignObjectSize;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.VisibleForTesting;

@SuppressWarnings("ExternalizableWithoutPublicNoArgConstructor")
public class HeapTrackingUnifiedMap<K, V> extends UnifiedMap<K, V> implements AutoCloseable {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(HeapTrackingUnifiedMap.class);
    private final MemoryTracker memoryTracker;
    private long trackedHeap;

    public static <K, V> HeapTrackingUnifiedMap<K, V> createUnifiedMap(MemoryTracker memoryTracker) {
        int initialSizeToAllocate = DEFAULT_INITIAL_CAPACITY << 2;
        long trackedHeap = arrayHeapSize(initialSizeToAllocate);
        memoryTracker.allocateHeap(SHALLOW_SIZE + trackedHeap);
        return new HeapTrackingUnifiedMap<>(memoryTracker, trackedHeap);
    }

    private HeapTrackingUnifiedMap(MemoryTracker memoryTracker, long trackedHeap) {
        this.memoryTracker = requireNonNull(memoryTracker);
        this.trackedHeap = trackedHeap;
    }

    @Override
    protected void allocateTable(int sizeToAllocate) {
        if (memoryTracker != null) {
            long heapToAllocate = arrayHeapSize(sizeToAllocate);
            memoryTracker.allocateHeap(heapToAllocate);
            memoryTracker.releaseHeap(trackedHeap);
            trackedHeap = heapToAllocate;
        }
        super.allocateTable(sizeToAllocate);
    }

    @Override
    protected void rehash(int newCapacity) {
        super.rehash(newCapacity);
        // Add an estimated heap usage of the arrays for the chains of colliding buckets, which grow in multiples of 4
        // (2 key-value pairs).
        // Add a fixed cost for each chain, which amounts to an average chain length of 8 key-value pairs.
        // In actuality chains are usually much shorter, but the number of collisions is also at it lowest just after
        // rehash and can then grow
        // substantially before the next rehash (approximately a factor of 3).
        // So based on experiments this heuristic seems to produce a fair trade-off between underestimation just before
        // rehash and overestimation
        // just after rehash, with a bias toward more overestimation after rehash (e.g. ~3% under --> ~8% over)
        int nChains = getCollidingBuckets();
        long estimatedHeapUsageForChains = nChains * arrayHeapSize(16);
        memoryTracker.allocateHeap(estimatedHeapUsageForChains);
        trackedHeap += estimatedHeapUsageForChains;
    }

    @Override
    public void close() {
        memoryTracker.releaseHeap(SHALLOW_SIZE + trackedHeap);
    }

    @VisibleForTesting
    public static long arrayHeapSize(int arrayLength) {
        return alignObjectSize(ARRAY_HEADER_BYTES + (long) arrayLength * OBJECT_REFERENCE_BYTES);
    }
}
