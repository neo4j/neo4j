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

import static org.neo4j.memory.HeapEstimator.SCOPED_MEMORY_TRACKER_SHALLOW_SIZE;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

import java.util.function.Consumer;
import org.neo4j.collection.trackable.HeapTrackingConcurrentHashSet;
import org.neo4j.internal.kernel.api.DefaultCloseListenable;
import org.neo4j.memory.Measurable;
import org.neo4j.memory.MemoryTracker;

/**
 * A specialized heap tracking set used for distinct query operators.
 * @param <T> element type
 */
public class ConcurrentDistinctSet<T extends Measurable> extends DefaultCloseListenable {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(ConcurrentDistinctSet.class);
    private final MemoryTracker scopedMemoryTracker;
    private final HeapTrackingConcurrentHashSet<T> distinctSet;

    public static <T extends Measurable> ConcurrentDistinctSet<T> createDistinctSet(MemoryTracker memoryTracker) {
        MemoryTracker scopedMemoryTracker = memoryTracker.getScopedMemoryTracker();
        scopedMemoryTracker.allocateHeap(SHALLOW_SIZE + SCOPED_MEMORY_TRACKER_SHALLOW_SIZE);
        return new ConcurrentDistinctSet<>(scopedMemoryTracker);
    }

    private ConcurrentDistinctSet(MemoryTracker scopedMemoryTracker) {
        this.scopedMemoryTracker = scopedMemoryTracker;
        distinctSet = HeapTrackingConcurrentHashSet.newSet(scopedMemoryTracker);
    }

    public boolean add(T element) {
        boolean wasAdded = distinctSet.add(element);
        if (wasAdded) {
            scopedMemoryTracker.allocateHeap(element.estimatedHeapUsage() + distinctSet.sizeOfWrapperObject());
        }
        return wasAdded;
    }

    public void forEach(Consumer<? super T> procedure) {
        distinctSet.forEach(procedure);
    }

    @Override
    public void closeInternal() {
        // No need to close distinctSet individually since it uses scopedMemoryTracker anyway
        scopedMemoryTracker.close();
    }

    @Override
    public boolean isClosed() {
        return false;
    }
}
