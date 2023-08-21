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
package org.neo4j.kernel.impl.util.collection;

import static java.util.Collections.emptyIterator;
import static org.neo4j.memory.HeapEstimator.SCOPED_MEMORY_TRACKER_SHALLOW_SIZE;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

import java.util.Iterator;
import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.collection.trackable.HeapTrackingConcurrentLongObjectHashMap;
import org.neo4j.internal.kernel.api.DefaultCloseListenable;
import org.neo4j.memory.Measurable;
import org.neo4j.memory.MemoryTracker;

public class ConcurrentLongProbeTable<V extends Measurable> extends DefaultCloseListenable {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(ConcurrentLongProbeTable.class);

    private final MemoryTracker scopedMemoryTracker;
    private HeapTrackingConcurrentLongObjectHashMap<ConcurrentBag<V>> map;

    public static <V extends Measurable> ConcurrentLongProbeTable<V> createLongProbeTable(MemoryTracker memoryTracker) {
        MemoryTracker scopedMemoryTracker = memoryTracker.getScopedMemoryTracker();
        scopedMemoryTracker.allocateHeap(SHALLOW_SIZE + SCOPED_MEMORY_TRACKER_SHALLOW_SIZE);
        return new ConcurrentLongProbeTable<>(scopedMemoryTracker);
    }

    private ConcurrentLongProbeTable(MemoryTracker scopedMemoryTracker) {
        this.scopedMemoryTracker = scopedMemoryTracker;
        this.map = HeapTrackingConcurrentLongObjectHashMap.newMap(scopedMemoryTracker);
    }

    public void put(long key, V value) {
        MutableLong heapUsage = new MutableLong(value.estimatedHeapUsage() + ConcurrentBag.SIZE_OF_NODE);
        map.computeIfAbsent(key, p -> {
                    heapUsage.add(map.sizeOfWrapperObject() + ConcurrentBag.SIZE_OF_BAG);
                    return new ConcurrentBag<>();
                })
                .add(value);
        scopedMemoryTracker.allocateHeap(heapUsage.longValue());
    }

    public Iterator<V> get(long key) {
        var entry = map.get(key);
        if (entry == null) {
            return emptyIterator();
        }
        return entry.iterator();
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public void closeInternal() {
        if (map != null) {
            map = null;
            scopedMemoryTracker.close();
        }
    }

    @Override
    public boolean isClosed() {
        return map == null;
    }
}
